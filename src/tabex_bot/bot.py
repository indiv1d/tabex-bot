from __future__ import annotations

from datetime import datetime, timezone
from typing import Awaitable, Callable
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
    MessageHandler,
    filters,
)

from tabex_bot import db
from tabex_bot.config import Settings
from tabex_bot.schedule import build_tabex_schedule


CommandExecutor = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]

BTN_TODAY = "Сегодня"
BTN_TAKEN = "Отметить приём"
BTN_MISSED = "Пропущенные"
BTN_STATS = "Статистика"
BTN_PLAN = "Новый план"
BTN_CANCEL = "Удалить план"


def _commands_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(BTN_TODAY), KeyboardButton(BTN_TAKEN)],
            [KeyboardButton(BTN_MISSED), KeyboardButton(BTN_STATS)],
            [KeyboardButton(BTN_PLAN), KeyboardButton(BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def _command_args(context: ContextTypes.DEFAULT_TYPE) -> list[str]:
    pending_args = context.user_data.get("_confirmed_args")
    if pending_args is not None:
        return list(pending_args)
    return list(context.args)


async def _reply_text(update: Update, text: str, **kwargs) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(text, **kwargs)


async def _request_command_confirmation(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    command_name: str,
) -> None:
    user = update.effective_user
    if user is None:
        return

    context.user_data["pending_command"] = {
        "name": command_name,
        "args": list(context.args),
    }
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Подтвердить", callback_data=f"confirm:ok:{command_name}")],
            [InlineKeyboardButton("Отмена", callback_data=f"confirm:cancel:{command_name}")],
        ]
    )
    await _reply_text(update, f"Подтвердить выполнение /{command_name}?", reply_markup=keyboard)


async def confirm_start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _request_command_confirmation(update, context, "start")


async def confirm_plan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _request_command_confirmation(update, context, "plan")


async def confirm_today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _request_command_confirmation(update, context, "today")


async def confirm_taken_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _request_command_confirmation(update, context, "taken")


async def confirm_missed_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _request_command_confirmation(update, context, "missed")


async def confirm_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _request_command_confirmation(update, context, "stats")


async def confirm_timezone_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _request_command_confirmation(update, context, "timezone")


async def confirm_cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _request_command_confirmation(update, context, "cancel")


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


def _parse_taken_at_local(raw: str, timezone_name: str) -> datetime | None:
    zone = ZoneInfo(timezone_name)
    now_local = datetime.now(zone)
    value = raw.strip()

    # HH:MM means "today at HH:MM" in user's timezone.
    try:
        hhmm = datetime.strptime(value, "%H:%M")
        return now_local.replace(hour=hhmm.hour, minute=hhmm.minute, second=0, microsecond=0)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.replace(tzinfo=zone)
        except ValueError:
            continue

    return None


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
    if not dose or dose["taken_at_utc"] is not None or dose["reminded_at_utc"] is not None:
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


async def due_reminders_check_job(context: CallbackContext) -> None:
    payload = context.job.data
    db_path = payload["db_path"]
    now_utc = datetime.now(timezone.utc)
    due_rows = db.list_due_unreminded_doses(db_path, now_utc)

    for row in due_rows:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Принял ✅", callback_data=f"take:{row['id']}")]]
        )

        await context.bot.send_message(
            chat_id=row["chat_id"],
            text=(
                "Время приёма наступило. "
                f"Запланировано на {_format_local(row['scheduled_at_utc'], row['timezone'])}."
            ),
            reply_markup=keyboard,
        )
        db.mark_reminded(db_path, row["id"], now_utc)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user
    chat = update.effective_chat

    if not user or not chat:
        return

    db.upsert_user(settings.db_path, user.id, chat.id)
    await _reply_text(
        update,
        "Привет! Я помогу отслеживать приём Tabex.\n\n"
        "Команды:\n"
        "/plan [YYYY-MM-DD HH:MM] - создать 25-дневный график\n"
        "/today - дозы на текущие сутки курса\n"
        "/taken [HH:MM] - отметить приём\n"
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

    args = _command_args(context)

    if not args:
        row = db.get_user(settings.db_path, user.id)
        zone = row["timezone"] if row else "Europe/Moscow"
        await _reply_text(update, f"Текущий часовой пояс: {zone}")
        return

    zone_name = args[0].strip()
    try:
        _parse_timezone(zone_name)
    except ValueError:
        await _reply_text(update, "Неизвестный часовой пояс. Пример: Europe/Moscow")
        return

    db.set_user_timezone(settings.db_path, user.id, zone_name)
    await _reply_text(update, f"Часовой пояс обновлён: {zone_name}")


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

    args = _command_args(context)
    if args:
        raw = " ".join(args).strip()
        parsed_start: datetime | None = None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
            try:
                parsed_start = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        if parsed_start is None:
            await _reply_text(update, "Неверный формат. Используй /plan или /plan YYYY-MM-DD HH:MM")
            return

        if len(raw) == 10:
            parsed_start = parsed_start.replace(hour=8, minute=0)
        start_at_local = parsed_start.replace(tzinfo=timezone_obj)
    else:
        start_at_local = datetime.now(timezone_obj)

    schedule = build_tabex_schedule(start_at_local, timezone_obj)
    count = db.set_plan(settings.db_path, user.id, start_at_local, schedule)
    _reschedule_user(context.application, settings.db_path, user.id)

    await _reply_text(
        update,
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
        await _reply_text(update, "Сначала запусти /start")
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
        await _reply_text(update, "Для текущих суток курса доз нет. Используй /plan")
        return

    lines = [
        "Текущие сутки курса "
        f"(день {day_number}): {day_start.strftime('%d.%m %H:%M')} - {day_end.strftime('%d.%m %H:%M')}"
    ]
    for dose in rows:
        mark = "✅" if dose["taken_at_utc"] else "⏳"
        lines.append(f"{mark} {_format_local(dose['scheduled_at_utc'], timezone_name)}")

    await _reply_text(update, "\n".join(lines))


async def taken_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    user_row = db.get_user(settings.db_path, user.id)
    timezone_name = user_row["timezone"] if user_row else "Europe/Moscow"

    args = _command_args(context)
    if args:
        taken_local = _parse_taken_at_local(" ".join(args), timezone_name)
        if taken_local is None:
            await _reply_text(
                update,
                "Неверный формат времени. Используй /taken, /taken HH:MM или /taken YYYY-MM-DD HH:MM"
            )
            return
        now_utc = taken_local.astimezone(timezone.utc)
    else:
        now_utc = datetime.now(timezone.utc)

    row = db.mark_next_pending_taken(settings.db_path, user.id, now_utc)
    if not row:
        await _reply_text(update, "Нет доз для отметки. Возможно план не создан.")
        return

    shifted = db.shift_day_schedule_by_first_taken(
        settings.db_path,
        user.id,
        row["id"],
        timezone_name,
        now_utc,
    )
    if shifted > 0:
        _reschedule_user(context.application, settings.db_path, user.id)

    await _reply_text(
        update,
        f"Отмечено как принято: {_format_local(row['scheduled_at_utc'], timezone_name)}"
    )


async def missed_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    user_row = db.get_user(settings.db_path, user.id)
    if not user_row:
        await _reply_text(update, "Сначала запусти /start")
        return

    timezone_name = user_row["timezone"]
    rows = db.get_missed_doses(settings.db_path, user.id, datetime.now(timezone.utc))

    if not rows:
        await _reply_text(update, "Пропущенных доз нет.")
        return

    lines = ["Пропущенные дозы:"]
    for row in rows[:20]:
        lines.append(f"- {_format_local(row['scheduled_at_utc'], timezone_name)}")

    await _reply_text(update, "\n".join(lines))


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    taken, total = db.get_stats(settings.db_path, user.id)
    if total == 0:
        await _reply_text(update, "План не создан. Используй /plan")
        return

    progress = (taken / total) * 100
    await _reply_text(update, f"Принято {taken}/{total} ({progress:.1f}%).")


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    user = update.effective_user

    if not user:
        return

    db.clear_plan(settings.db_path, user.id)
    _remove_user_jobs(context.application, user.id)
    await _reply_text(update, "План удалён.")


async def callback_confirm_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return

    await query.answer()
    parts = query.data.split(":", 2)
    if len(parts) != 3:
        return
    action, command_name = parts[1], parts[2]

    pending = context.user_data.get("pending_command")
    if not pending or pending.get("name") != command_name:
        await query.edit_message_text("Подтверждение устарело. Повтори команду.")
        return

    if action == "cancel":
        context.user_data.pop("pending_command", None)
        await query.edit_message_text(f"Команда /{command_name} отменена.")
        return

    handlers: dict[str, CommandExecutor] = {
        "start": start_cmd,
        "plan": plan_cmd,
        "today": today_cmd,
        "taken": taken_cmd,
        "missed": missed_cmd,
        "stats": stats_cmd,
        "timezone": timezone_cmd,
        "cancel": cancel_cmd,
    }
    handler = handlers.get(command_name)
    if handler is None:
        context.user_data.pop("pending_command", None)
        await query.edit_message_text("Неизвестная команда.")
        return

    pending_args = pending.get("args", [])
    context.user_data.pop("pending_command", None)
    context.user_data["_confirmed_args"] = list(pending_args)
    try:
        await handler(update, context)
    finally:
        context.user_data.pop("_confirmed_args", None)

    await query.edit_message_text(f"Команда /{command_name} подтверждена.")


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

    now_utc = datetime.now(timezone.utc)
    changed = db.mark_dose_taken(settings.db_path, dose_id, now_utc)
    if changed:
        user_row = db.get_user(settings.db_path, dose["user_id"])
        timezone_name = user_row["timezone"] if user_row else "Europe/Moscow"
        shifted = db.shift_day_schedule_by_first_taken(
            settings.db_path,
            dose["user_id"],
            dose_id,
            timezone_name,
            now_utc,
        )
        if shifted > 0:
            _reschedule_user(context.application, settings.db_path, dose["user_id"])
        await query.edit_message_text("Отмечено: таблетка принята ✅")
    else:
        await query.edit_message_text("Эта доза уже была отмечена ранее.")


async def post_init(application: Application) -> None:
    settings: Settings = application.bot_data["settings"]
    pending = db.list_pending_doses(settings.db_path, from_utc=datetime.now(timezone.utc))
    for row in pending:
        _schedule_single_reminder(application, settings.db_path, row)

    application.job_queue.run_repeating(
        due_reminders_check_job,
        interval=60,
        first=0,
        data={"db_path": settings.db_path},
        name="due-reminders-check",
    )


def build_application() -> Application:
    settings = Settings.from_env()
    db.init_db(settings.db_path)

    application = ApplicationBuilder().token(settings.bot_token).post_init(post_init).build()
    application.bot_data["settings"] = settings

    application.add_handler(CommandHandler("start", confirm_start_cmd))
    application.add_handler(CommandHandler("plan", confirm_plan_cmd))
    application.add_handler(CommandHandler("today", today_cmd))
    application.add_handler(CommandHandler("taken", confirm_taken_cmd))
    application.add_handler(CommandHandler("missed", confirm_missed_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("timezone", confirm_timezone_cmd))
    application.add_handler(CommandHandler("cancel", confirm_cancel_cmd))

    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_TODAY}$"), today_cmd))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_TAKEN}$"), confirm_taken_cmd))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_MISSED}$"), confirm_missed_cmd))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_STATS}$"), stats_cmd))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_PLAN}$"), confirm_plan_cmd))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_CANCEL}$"), confirm_cancel_cmd))

    application.add_handler(CallbackQueryHandler(callback_confirm_command, pattern=r"^confirm:(ok|cancel):"))
    application.add_handler(CallbackQueryHandler(callback_take, pattern=r"^take:\d+$"))

    return application


def run() -> None:
    app = build_application()
    app.run_polling()
