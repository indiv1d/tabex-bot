from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    bot_token: str
    db_path: str

    @classmethod
    def from_env(cls) -> "Settings":
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        if not bot_token:
            raise RuntimeError("BOT_TOKEN is required")

        db_path = os.getenv("BOT_DB_PATH", "tabex.db").strip() or "tabex.db"
        return cls(bot_token=bot_token, db_path=db_path)
