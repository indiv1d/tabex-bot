from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from tabex_bot import db
from tabex_bot.config import Settings
from tabex_bot.schedule import build_tabex_schedule


def _commands_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("/today"), KeyboardButton("/taken")],
            [KeyboardButton("/missed"), KeyboardButton("/stats")],
            [KeyboardButton("/plan"), KeyboardButton("/cancel")],
        ],
        resize_keyboard=True,
    )


def _parse_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("Unknown timezone") from exc


def _format_local(utc_iso: str, timezone_name: str) -> str:
    utc_dt = datetime.fromisoformat(utc_iso)
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    local_dt = utc_dt.astimezone(ZoneInfo(timezone_name))
    return local_dt.strftime("%d.%m %H:%M")


def _job_name_for_dose(dose_id: int) -> str:
    return f"dose:{dose_id}"


def _job_name_prefix_for_user(user_id: int) -> str:
    return f"user:{user_id}:"


def _remove_user_jobs(application: Application, user_id: int) -> None:
    prefix = _job_name_prefix_for_user(user_id)
    for job in application.job_queue.jobs():
        if job.name and job.name.startswith(prefix):
            job.schedule_removal()


def _schedule_single_reminder(application: Application, db_path: str, row) -> None:
    scheduled_utc = datetime.fromisoformat(row["scheduled_at_utc"])
    if scheduled_utc.tzinfo is None:
        scheduled_utc = scheduled_utc.replace(tzinfo=timezone.utc)

    if scheduled_utc <= datetime.now(timezone.utc):
        return

    application.job_queue.run_once(
        reminder_job,
        when=scheduled_utc,
        chat_id=row["chat_id"],
        data={"dose_id": row["id"], "user_id": row["user_id"], "db_path": db_path},
        name=f"{_job_name_prefix_for_user(row['user_id'])}{_job_name_for_dose(row['id'])}",
    )


def _reschedule_user(application: Application, db_path: str, user_id: int) -> None:
    _remove_user_jobs(application, user_id)
    rows = db.list_pending_doses(db_path, from_utc=datetime.now(timezone.utc))
    for row in rows:
        if row["user_id"] == user_id:
            _schedule_single_reminder(application, db_path, row)


async def reminder_job(context: CallbackContext) -> None:
    payload = context.job.data
    db_path = payload["db_path"]
    dose_id = payload["dose_id"]

    dose = db.get_dose(db_path, dose_id)
    if not dose or dose["taken_at_utc"] is not None:
        return

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Принял ✅", callback_data=f"take:{dose_id}")]]
    )

    await context.bot.send_message(
        chat_id=context.job.chat_id,
        text="Напоминание: пора принять таблетку Tabex.",
        reply_markup=keyboard,
    )
    db.mark_reminded(db_path, dose_id, datetime.now(timezone.utc))


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user
    chat = update.effective_chat

    if not user or not chat:
        return

    db.upsert_user(settings.db_path, user.id, chat.id)
    await update.message.reply_text(
        "Привет! Я помогу отслеживать приём Tabex.\n\n"
        "Команды:\n"
        "/plan [YYYY-MM-DD HH:MM] - создать 25-дневный график\n"
        "/today - дозы на текущие сутки курса\n"
        "/taken - отметить приём\n"
        "/missed - показать пропущенные\n"
        "/stats - прогресс\n"
        "/timezone Europe/Moscow - часовой пояс\n"
        "/cancel - удалить текущий план",
        reply_markup=_commands_keyboard(),
    )


async def timezone_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    if not context.args:
        row = db.get_user(settings.db_path, user.id)
        zone = row["timezone"] if row else "Europe/Moscow"
        await update.message.reply_text(f"Текущий часовой пояс: {zone}")
        return

    zone_name = context.args[0].strip()
    try:
        _parse_timezone(zone_name)
    except ValueError:
        await update.message.reply_text("Неизвестный часовой пояс. Пример: Europe/Moscow")
        return

    db.set_user_timezone(settings.db_path, user.id, zone_name)
    await update.message.reply_text(f"Часовой пояс обновлён: {zone_name}")


async def plan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user
    chat = update.effective_chat

    if not user or not chat:
        return

    db.upsert_user(settings.db_path, user.id, chat.id)
    row = db.get_user(settings.db_path, user.id)
    timezone_name = row["timezone"] if row else "Europe/Moscow"
    timezone_obj = ZoneInfo(timezone_name)

    if context.args:
        raw = " ".join(context.args).strip()
        parsed_start: datetime | None = None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
            try:
                parsed_start = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        if parsed_start is None:
            await update.message.reply_text("Неверный формат. Используй /plan или /plan YYYY-MM-DD HH:MM")
            return

        if len(raw) == 10:
            parsed_start = parsed_start.replace(hour=8, minute=0)
        start_at_local = parsed_start.replace(tzinfo=timezone_obj)
    else:
        start_at_local = datetime.now(timezone_obj)

    schedule = build_tabex_schedule(start_at_local, timezone_obj)
    count = db.set_plan(settings.db_path, user.id, start_at_local, schedule)
    _reschedule_user(context.application, settings.db_path, user.id)

    await update.message.reply_text(
        "График создан. "
        f"Всего доз: {count}. "
        f"Первая таблетка: {start_at_local.strftime('%Y-%m-%d %H:%M')} ({timezone_name})."
    )


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    row = db.get_user(settings.db_path, user.id)
    if not row:
        await update.message.reply_text("Сначала запусти /start")
        return

    timezone_name = row["timezone"]
    now_local = datetime.now(ZoneInfo(timezone_name))
    day_number, day_start, day_end, rows = db.get_plan_day_doses(
        settings.db_path,
        user.id,
        timezone_name,
        now_local,
    )

    if not rows:
        await update.message.reply_text("Для текущих суток курса доз нет. Используй /plan")
        return

    lines = [
        "Текущие сутки курса "
        f"(день {day_number}): {day_start.strftime('%d.%m %H:%M')} - {day_end.strftime('%d.%m %H:%M')}"
    ]
    for dose in rows:
        mark = "✅" if dose["taken_at_utc"] else "⏳"
        lines.append(f"{mark} {_format_local(dose['scheduled_at_utc'], timezone_name)}")

    await update.message.reply_text("\n".join(lines))


async def taken_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    now_utc = datetime.now(timezone.utc)
    row = db.mark_next_pending_taken(settings.db_path, user.id, now_utc)
    if not row:
        await update.message.reply_text("Нет доз для отметки. Возможно план не создан.")
        return

    user_row = db.get_user(settings.db_path, user.id)
    timezone_name = user_row["timezone"] if user_row else "Europe/Moscow"
    await update.message.reply_text(
        f"Отмечено как принято: {_format_local(row['scheduled_at_utc'], timezone_name)}"
    )


async def missed_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    user_row = db.get_user(settings.db_path, user.id)
    if not user_row:
        await update.message.reply_text("Сначала запусти /start")
        return

    timezone_name = user_row["timezone"]
    rows = db.get_missed_doses(settings.db_path, user.id, datetime.now(timezone.utc))

    if not rows:
        await update.message.reply_text("Пропущенных доз нет.")
        return

    lines = ["Пропущенные дозы:"]
    for row in rows[:20]:
        lines.append(f"- {_format_local(row['scheduled_at_utc'], timezone_name)}")

    await update.message.reply_text("\n".join(lines))


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    taken, total = db.get_stats(settings.db_path, user.id)
    if total == 0:
        await update.message.reply_text("План не создан. Используй /plan")
        return

    progress = (taken / total) * 100
    await update.message.reply_text(f"Принято {taken}/{total} ({progress:.1f}%).")


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    db.clear_plan(settings.db_path, user.id)
    _remove_user_jobs(context.application, user.id)
    await update.message.reply_text("План удалён.")


async def callback_take(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    query = update.callback_query
    if not query or not query.data or not query.from_user:
        return

    await query.answer()
    if not query.data.startswith("take:"):
        return

    dose_id = int(query.data.split(":", 1)[1])
    dose = db.get_dose(settings.db_path, dose_id)
    if not dose:
        await query.edit_message_text("Доза не найдена.")
        return

    if dose["user_id"] != query.from_user.id:
        await query.answer("Эта кнопка не для вас", show_alert=True)
        return

    changed = db.mark_dose_taken(settings.db_path, dose_id, datetime.now(timezone.utc))
    if changed:
        await query.edit_message_text("Отмечено: таблетка принята ✅")
    else:
        await query.edit_message_text("Эта доза уже была отмечена ранее.")


async def post_init(application: Application) -> None:
    settings: Settings = application.bot_data["settings"]
    pending = db.list_pending_doses(settings.db_path, from_utc=datetime.now(timezone.utc))
    for row in pending:
        _schedule_single_reminder(application, settings.db_path, row)


def build_application() -> Application:
    settings = Settings.from_env()
    db.init_db(settings.db_path)

    application = ApplicationBuilder().token(settings.bot_token).post_init(post_init).build()
    application.bot_data["settings"] = settings

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("plan", plan_cmd))
    application.add_handler(CommandHandler("today", today_cmd))
    application.add_handler(CommandHandler("taken", taken_cmd))
    application.add_handler(CommandHandler("missed", missed_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("timezone", timezone_cmd))
    application.add_handler(CommandHandler("cancel", cancel_cmd))
    application.add_handler(CallbackQueryHandler(callback_take, pattern=r"^take:\\d+$"))

    return application


def run() -> None:
    app = build_application()
    app.run_polling()
