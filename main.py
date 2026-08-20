import os
import re
import json
import random
import asyncio
import logging
from datetime import datetime, timedelta, timezone

import aiohttp
import aiosqlite
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandObject
from aiogram.types import Message


# ============================================================
# НАСТРОЙКИ
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()

GROUP_ID = int(os.getenv("GROUP_ID", "-1004299403919"))

# ID тем
GAME_TOPIC_ID = int(os.getenv("GAME_TOPIC_ID", "5"))
SEMI_RP_TOPIC_ID = int(os.getenv("SEMI_RP_TOPIC_ID", "33"))
INFO_TOPIC_ID = int(os.getenv("INFO_TOPIC_ID", "20"))
ADMIN_TOPIC_ID = int(os.getenv("ADMIN_TOPIC_ID", "38"))
PROFILES_TOPIC_ID = int(os.getenv("PROFILES_TOPIC_ID", "2"))
FLOOD_TOPIC_ID = int(os.getenv("FLOOD_TOPIC_ID", "4"))
STORY_TOPIC_ID = int(os.getenv("STORY_TOPIC_ID", "32"))

RULES_URL = os.getenv("RULES_URL", "").strip()

# ID администраторов Telegram через запятую:
# ADMIN_IDS=123456789,987654321
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

# База
DB_PATH = os.getenv("DB_PATH", "stats.db")

# ИИ
GROQ_MODEL = os.getenv(
    "GROQ_MODEL",
    "llama-3.3-70b-versatile"
)

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# Для статов
MIN_STAT_CHARS = 300

# Для событий новостей.
# Можно оставить 80: короткие сообщения флуд не будут превращаться
# в сотни бессмысленных событий.
MIN_NEWS_CHARS = 80

# Новости каждые 12 часов
NEWS_INTERVAL_HOURS = 12

# Неактивность
INACTIVITY_DAYS = 30

# Статы после возвращения из "Читателя"
INACTIVE_STR = 1
INACTIVE_REP = 1
INACTIVE_CON = 1
INACTIVE_MONEY = 100

# Начальные статы при регистрации
START_STR = 4
START_REP = 4
START_CON = 4
START_MONEY = 500

# Недельные максимумы начисления
WEEKLY_STR_LIMIT = 2
WEEKLY_REP_LIMIT = 7
WEEKLY_CON_LIMIT = 8
WEEKLY_MONEY_LIMIT = 1000


# ============================================================
# ЛОГИ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# TELEGRAM
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN не найден. Добавь BOT_TOKEN в Environment Variables Render."
    )

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ============================================================
# TOPIC SETS
# ============================================================

STAT_TOPIC_IDS = {
    GAME_TOPIC_ID,
    SEMI_RP_TOPIC_ID,
}

NEWS_TOPIC_IDS = {
    GAME_TOPIC_ID,
    SEMI_RP_TOPIC_ID,
    FLOOD_TOPIC_ID,
}

IGNORED_TOPIC_IDS = {
    INFO_TOPIC_ID,
    ADMIN_TOPIC_ID,
    STORY_TOPIC_ID,
}


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_str(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def str_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def normalize_username(username: str | None) -> str:
    if not username:
        return ""
    return username.lstrip("@").strip().lower()


def get_level_status(value: int):
    if value <= 3:
        return 1, "Новичок"
    elif value <= 7:
        return 2, "Заметный"
    elif value <= 11:
        return 3, "Влиятельный"
    elif value <= 15:
        return 4, "Легенда"
    else:
        return 5, "Топ города"


def get_badge(bad_count: int, good_count: int):
    if bad_count > good_count + 5:
        return "Bad boy"

    if good_count > bad_count + 5:
        return "Good boy"

    return ""


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def topic_name(topic_id: int | None) -> str:
    names = {
        GAME_TOPIC_ID: "Игровая",
        SEMI_RP_TOPIC_ID: "Полурол — на обочине",
        INFO_TOPIC_ID: "О чем это вообще",
        ADMIN_TOPIC_ID: "Админка",
        PROFILES_TOPIC_ID: "Анкеты",
        FLOOD_TOPIC_ID: "Флуд",
        STORY_TOPIC_ID: "Повествование",
    }

    return names.get(topic_id, "Неизвестная тема")


def build_message_link(message: Message) -> str | None:
    """
    Формирует ссылку на сообщение группы.

    Для приватной супергруппы Telegram обычно использует:
    https://t.me/c/<internal_id>/<message_id>

    У нас GROUP_ID = -1004299403919,
    поэтому internal_id = 4299403919.
    """

    if not message.message_id:
        return None

    if GROUP_ID < 0:
        internal_id = str(GROUP_ID).replace("-100", "", 1)

        if internal_id.isdigit():
            return (
                f"https://t.me/c/"
                f"{internal_id}/"
                f"{message.message_id}"
            )

    username = getattr(message.chat, "username", None)

    if username:
        return (
            f"https://t.me/{username}/"
            f"{message.message_id}"
        )

    return None


# ============================================================
# DATABASE
# ============================================================

CREATE_PLAYERS_TABLE = """
CREATE TABLE IF NOT EXISTS players (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    str INTEGER DEFAULT 2,
    rep INTEGER DEFAULT 2,
    con INTEGER DEFAULT 2,
    money INTEGER DEFAULT 300,
    last_post TEXT,
    anketa_url TEXT,
    status TEXT DEFAULT 'Игрок',
    bad_boy_count INTEGER DEFAULT 0,
    good_boy_count INTEGER DEFAULT 0,

    str_week_limit INTEGER DEFAULT 0,
    rep_week_limit INTEGER DEFAULT 0,
    con_week_limit INTEGER DEFAULT 0,
    money_week_limit INTEGER DEFAULT 0,

    week_reset TEXT,
    registered_at TEXT,
    first_intro_sent INTEGER DEFAULT 0
);
"""

CREATE_ACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action_type TEXT,
    action_value INTEGER,
    timestamp TEXT
);
"""

CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    username TEXT,
    event_type TEXT,
    summary TEXT,
    source_topic INTEGER,
    source_message_id INTEGER,
    source_url TEXT,
    created_at TEXT,
    published INTEGER DEFAULT 0
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_PLAYERS_TABLE)
        await db.execute(CREATE_ACTIONS_TABLE)
        await db.execute(CREATE_EVENTS_TABLE)

        await db.commit()

    logger.info("База данных готова.")


# ============================================================
# DATABASE HELPERS
# ============================================================

async def get_player(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT * FROM players WHERE user_id = ?",
            (user_id,)
        )

        row = await cursor.fetchone()
        await cursor.close()

        return row


async def get_player_by_username(username: str):
    username = normalize_username(username)

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT * FROM players
            WHERE LOWER(username) = ?
            """,
            (username,)
        )

        row = await cursor.fetchone()
        await cursor.close()

        return row


async def register_player(
    user_id: int,
    username: str
):
    existing = await get_player(user_id)

    if existing:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                UPDATE players
                SET username = ?
                WHERE user_id = ?
                """,
                (normalize_username(username), user_id)
            )

            await db.commit()

        return False

    current = dt_to_str(now_utc())

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO players (
                user_id,
                username,
                str,
                rep,
                con,
                money,
                last_post,
                status,
                week_reset,
                registered_at,
                first_intro_sent
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'Игрок', ?, ?, 1)
            """,
            (
                user_id,
                normalize_username(username),
                START_STR,
                START_REP,
                START_CON,
                START_MONEY,
                current,
                current,
                current
            )
        )

        await db.commit()

    return True


async def update_username(user_id: int, username: str):
    username = normalize_username(username)

    if not username:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE players
            SET username = ?
            WHERE user_id = ?
            """,
            (username, user_id)
        )

        await db.commit()


# ============================================================
# PROFILE
# ============================================================

async def show_profile(message: Message, player):
    user_id = player[0]
    username = player[1] or "без_username"

    str_val = player[2]
    rep_val = player[3]
    con_val = player[4]
    money_val = player[5]

    last_post = player[6]
    anketa_url = player[7]
    status = player[8]

    bad_count = player[9]
    good_count = player[10]

    str_level, str_status = get_level_status(str_val)
    rep_level, rep_status = get_level_status(rep_val)
    con_level, con_status = get_level_status(con_val)

    badge = get_badge(
        bad_count,
        good_count
    )

    if anketa_url:
        anketa_text = f"[Открыть анкету]({anketa_url})"
    else:
        anketa_text = "Не заполнена"

    text = (
        "📋 *ПРОФИЛЬ ИГРОКА*\n\n"

        f"👤 Имя: @{username}\n"
        f"📝 Анкета: {anketa_text}\n\n"

        f"💪 *Сила (STR):* {str_val} "
        f"(ур. {str_level}, {str_status})\n"

        f"🌟 *Репутация (REP):* {rep_val} "
        f"(ур. {rep_level}, {rep_status})\n"

        f"🤝 *Связи (CON):* {con_val} "
        f"(ур. {con_level}, {con_status})\n"

        f"💰 *Кэш:* {money_val} монет\n\n"

        f"🎯 Статус: {status}\n"
        f"🏷 Пометка: {badge or 'Нет'}\n\n"

        f"📅 Последняя активность: "
        f"{last_post or 'Никогда'}"
    )

    await message.answer(
        text,
        parse_mode="Markdown"
    )


# ============================================================
# START
# ============================================================

WELCOME_TEXT = """
🎭 *Добро пожаловать в игру!*

Ты зарегистрирован в игровой системе.

Твои начальные характеристики:

💪 STR — 4
🌟 REP — 4
🤝 CON — 4
💰 MONEY — 500

━━━━━━━━━━━━━━

📋 *Основные команды*

/profile — посмотреть свой профиль

/profile @username — посмотреть профиль другого игрока

/setanket ССЫЛКА — добавить ссылку на свою анкету

/random str @username — случайный исход столкновения по STR

/random rep @username — по REP

/random con @username — по CON

/random money @username — по MONEY

/rules — правила игры

/help — инструкция

━━━━━━━━━━━━━━

🎮 *Как начинают считаться статы*

После регистрации бот следит за игровыми постами в темах:

🎭 «Игровая»
🛣 «Полурол — на обочине»

Для начисления характеристик ИИ анализирует только сообщения от *300 символов*.

Флуд статы не начисляет, но отдельные события из него могут попасть в городские новости.

━━━━━━━━━━━━━━

📰 *Новости*

Бот собирает игровые события, события из полурола и флуд-события.

Периодически ИИ объединяет их в городскую хронику и публикует её в теме «Повествование».

📋 Новые анкеты также могут становиться событиями для новостей.

━━━━━━━━━━━━━━

📜 Правила:

{rules}
"""


@dp.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user

    if not user:
        return

    username = normalize_username(user.username)

    created = await register_player(
        user.id,
        username
    )

    if created:
        rules = RULES_URL or "Ссылка на правила пока не настроена."

        await message.answer(
            WELCOME_TEXT.format(rules=rules),
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "Ты уже зарегистрирован в игре.\n\n"
            "Используй /profile для просмотра профиля."
        )


# ============================================================
# HELP
# ============================================================

@dp.message(Command("help"))
async def cmd_help(message: Message):
    rules = RULES_URL or "Ссылка на правила пока не настроена."

    text = (
        "🎭 *КОМАНДЫ ИГРЫ*\n\n"

        "▶️ /start — регистрация\n"
        "📋 /profile — свой профиль\n"
        "📋 /profile @username — профиль игрока\n"
        "📝 /setanket URL — добавить анкету\n"
        "🎲 /random str @username — рандомайзер\n"
        "🎲 /random rep @username — рандомайзер\n"
        "🎲 /random con @username — рандомайзер\n"
        "🎲 /random money @username — рандомайзер\n"
        "📜 /rules — правила\n\n"

        f"📜 Правила: {rules}\n\n"

        "Для начисления статов учитываются игровые "
        "посты от 300 символов в темах «Игровая» "
        "и «Полурол — на обочине»."
    )

    await message.answer(
        text,
        parse_mode="Markdown"
    )


# ============================================================
# RULES
# ============================================================

@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    if RULES_URL:
        await message.answer(
            f"📜 *Правила игры*\n\n{RULES_URL}",
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "📜 Ссылка на правила пока не настроена администратором."
        )


# ============================================================
# SET ANKET
# ============================================================

@dp.message(Command("setanket"))
async def cmd_setanket(
    message: Message,
    command: CommandObject
):
    user = message.from_user

    if not user:
        return

    player = await get_player(user.id)

    if not player:
        await message.answer(
            "Анкета не найдена.\n\n"
            "Сначала зарегистрируйся командой /start."
        )
        return

    args = (command.args or "").strip()

    url_match = re.search(
        r"https?://\S+",
        args
    )

    if not url_match:
        await message.answer(
            "Анкета не найдена.\n\n"
            "Укажи ссылку на анкету:\n"
            "/setanket https://t.me/..."
        )
        return

    url = url_match.group(0).rstrip(").,!?")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE players
            SET anketa_url = ?
            WHERE user_id = ?
            """,
            (url, user.id)
        )

        await db.commit()

    await message.answer(
        "✅ Анкета добавлена в профиль!"
    )


# ============================================================
# PROFILE
# ============================================================

@dp.message(Command("profile"))
async def cmd_profile(
    message: Message,
    command: CommandObject
):
    user = message.from_user

    if not user:
        return

    args = (command.args or "").strip()

    if args:
        username = args.split()[0].lstrip("@")

        player = await get_player_by_username(
            username
        )

        if not player:
            await message.answer(
                "Игрок не найден."
            )
            return

        await show_profile(
            message,
            player
        )
        return

    player = await get_player(user.id)

    if not player:
        await message.answer(
            "Ты ещё не зарегистрирован.\n\n"
            "Используй /start."
        )
        return

    await show_profile(
        message,
        player
    )


# ============================================================
# RANDOM
# ============================================================

@dp.message(Command("random"))
async def cmd_random(
    message: Message,
    command: CommandObject
):
    user = message.from_user

    if not user:
        return

    args = (command.args or "").split()

    if len(args) < 2:
        await message.answer(
            "Использование:\n"
            "/random str @username\n\n"
            "Доступные статы: str, rep, con, money"
        )
        return

    stat = args[0].lower()
    enemy_username = args[1].lstrip("@")

    stat_map = {
        "str": 2,
        "rep": 3,
        "con": 4,
        "money": 5
    }

    if stat not in stat_map:
        await message.answer(
            "Неверный стат.\n\n"
            "Используй: str, rep, con, money"
        )
        return

    player = await get_player(user.id)

    if not player:
        await message.answer(
            "Ты ещё не зарегистрирован.\n\n"
            "Используй /start."
        )
        return

    enemy = await get_player_by_username(
        enemy_username
    )

    if not enemy:
        await message.answer(
            "Враг не найден."
        )
        return

    player_stat = int(player[stat_map[stat]])
    enemy_stat = int(enemy[stat_map[stat]])

    total = player_stat + enemy_stat

    if total <= 0:
        player_chance = 50
    else:
        player_chance = (
            player_stat / total
        ) * 100

    roll = random.uniform(0, 100)

    if roll <= player_chance:
        winner = f"@{player[1]}"
    else:
        winner = f"@{enemy[1]}"

    text = (
        "🎲 *РЕЗУЛЬТАТ*\n\n"

        f"@{player[1]} "
        f"({stat.upper()} {player_stat})\n"

        "vs\n"

        f"@{enemy[1]} "
        f"({stat.upper()} {enemy_stat})\n\n"

        f"Ваш шанс: {player_chance:.1f}%\n"
        f"Шанс противника: "
        f"{100 - player_chance:.1f}%\n\n"

        f"🏆 Победитель: *{winner}*!"
    )

    await message.answer(
        text,
        parse_mode="Markdown"
    )


# ============================================================
# GROQ
# ============================================================

groq_available = False


async def check_groq_api() -> bool:
    global groq_available

    if not GROQ_API_KEY:
        logger.error(
            "GROQ_API_KEY отсутствует. "
            "ИИ-функции отключены."
        )

        groq_available = False
        return False

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}"
    }

    url = (
        "https://api.groq.com/openai/v1/models/"
        f"{GROQ_MODEL}"
    )

    try:
        timeout = aiohttp.ClientTimeout(
            total=15
        )

        async with aiohttp.ClientSession(
            timeout=timeout
        ) as session:

            async with session.get(
                url,
                headers=headers
            ) as response:

                if response.status == 200:
                    logger.info(
                        "Groq API успешно проверен."
                    )

                    groq_available = True
                    return True

                body = await response.text()

                logger.error(
                    "Groq API не прошёл проверку. "
                    "HTTP %s: %s",
                    response.status,
                    body[:500]
                )

    except Exception:
        logger.exception(
            "Ошибка проверки Groq API."
        )

    groq_available = False
    return False


async def groq_request(
    messages: list,
    response_format: dict | None = None,
    temperature: float = 0.2,
    max_tokens: int = 1000
):
    if not groq_available:
        return None

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    data = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_completion_tokens": max_tokens
    }

    if response_format:
        data["response_format"] = response_format

    timeout = aiohttp.ClientTimeout(
        total=60
    )

    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(
                timeout=timeout
            ) as session:

                async with session.post(
                    GROQ_URL,
                    headers=headers,
                    json=data
                ) as response:

                    body = await response.text()

                    if response.status != 200:
                        logger.error(
                            "Groq HTTP %s: %s",
                            response.status,
                            body[:1000]
                        )

                        if response.status in {
                            429,
                            500,
                            502,
                            503,
                            504
                        }:
                            await asyncio.sleep(
                                2 ** attempt
                            )
                            continue

                        return None

                    result = json.loads(body)

                    content = (
                        result
                        .get("choices", [{}])[0]
                        .get("message", {})
                        .get("content")
                    )

                    if not content:
                        logger.error(
                            "Groq вернул ответ "
                            "без content."
                        )
                        return None

                    return content

        except asyncio.TimeoutError:
            logger.error(
                "Groq timeout, попытка %s/3",
                attempt + 1
            )

        except Exception:
            logger.exception(
                "Ошибка запроса к Groq, "
                "попытка %s/3",
                attempt + 1
            )

        await asyncio.sleep(
            2 ** attempt
        )

    return None


# ============================================================
# AI — STAT ANALYSIS
# ============================================================

async def analyze_stats(text: str):
    schema = {
        "type": "json_schema",
        "json_schema": {
            "name": "game_stats",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "str": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 2
                    },
                    "rep": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 7
                    },
                    "con": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 8
                    },
                    "money": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 1000
                    },
                    "bad_boy": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 1
                    },
                    "good_boy": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 1
                    },
                    "event": {
                        "type": "boolean"
                    },
                    "event_summary": {
                        "type": "string"
                    }
                },
                "required": [
                    "str",
                    "rep",
                    "con",
                    "money",
                    "bad_boy",
                    "good_boy",
                    "event",
                    "event_summary"
                ],
                "additionalProperties": False
            }
        }
    }

    prompt = f"""
Ты аналитик текстовой ролевой игры.

Проанализируй игровой пост.

ВАЖНО:
- Не выдумывай действия, которых нет в тексте.
- Начисляй только небольшие целые значения.
- Если действие явно не относится к характеристике — ставь 0.
- STR: драки, физическая активность, спорт, силовые действия.
- REP: помощь другим, красивые игровые действия, публичные события,
  лидерство, администрирование сюжета, заметные конфликты.
- CON: знакомства, сделки, переговоры, отношения, контакты,
  сюжетные связи.
- MONEY: заработок, сделки, задания, получение денег.
- bad_boy/good_boy — только если текст явно отражает соответствующее
  поведение.

Сообщение:
{text}
"""

    content = await groq_request(
        [
            {
                "role": "system",
                "content": (
                    "Отвечай только в соответствии с "
                    "JSON-схемой."
                )
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        response_format=schema,
        temperature=0.1,
        max_tokens=500
    )

    if not content:
        return None

    try:
        data = json.loads(content)

        return {
            "str": max(0, int(data.get("str", 0))),
            "rep": max(0, int(data.get("rep", 0))),
            "con": max(0, int(data.get("con", 0))),
            "money": max(0, int(data.get("money", 0))),
            "bad_boy": max(
                0,
                min(1, int(data.get("bad_boy", 0)))
            ),
            "good_boy": max(
                0,
                min(1, int(data.get("good_boy", 0)))
            ),
            "event": bool(data.get("event", False)),
            "event_summary": str(
                data.get("event_summary", "")
            ).strip()
        }

    except (json.JSONDecodeError, TypeError, ValueError):
        logger.exception(
            "Некорректный JSON от Groq: %s",
            content[:1000]
        )

        return None


# ============================================================
# AI — NEWS EVENT ANALYSIS
# ============================================================

async def analyze_news_event(
    text: str,
    username: str
):
    schema = {
        "type": "json_schema",
        "json_schema": {
            "name": "news_event",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "is_event": {
                        "type": "boolean"
                    },
                    "event_type": {
                        "type": "string"
                    },
                    "summary": {
                        "type": "string"
                    }
                },
                "required": [
                    "is_event",
                    "event_type",
                    "summary"
                ],
                "additionalProperties": False
            }
        }
    }

    prompt = f"""
Ты редактор криминальной хроники текстовой ролевой игры.

Определи, есть ли в сообщении событие, которое потенциально
может попасть в городскую новость.

Не каждое сообщение является событием.

Интересуют:
- встречи;
- конфликты;
- драки;
- сделки;
- необычные происшествия;
- появление новых людей;
- заметные действия игроков;
- слухи;
- события из флудовой жизни игроков, если они могут быть
  интересно превращены в игровую городскую хронику.

Не выдумывай факты.

Игрок: @{username}

Текст:
{text}
"""

    content = await groq_request(
        [
            {
                "role": "system",
                "content": (
                    "Отвечай только JSON."
                )
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        response_format=schema,
        temperature=0.2,
        max_tokens=300
    )

    if not content:
        return None

    try:
        data = json.loads(content)

        return {
            "is_event": bool(
                data.get("is_event", False)
            ),
            "event_type": str(
                data.get("event_type", "other")
            ),
            "summary": str(
                data.get("summary", "")
            ).strip()
        }

    except (
        json.JSONDecodeError,
        TypeError,
        ValueError
    ):
        logger.exception(
            "Ошибка JSON анализа события."
        )

        return None


# ============================================================
# EVENTS
# ============================================================

async def save_event(
    user_id: int | None,
    username: str,
    event_type: str,
    summary: str,
    source_topic: int,
    source_message_id: int,
    source_url: str | None
):
    if not summary:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO events (
                user_id,
                username,
                event_type,
                summary,
                source_topic,
                source_message_id,
                source_url,
                created_at,
                published
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                user_id,
                username,
                event_type,
                summary,
                source_topic,
                source_message_id,
                source_url,
                dt_to_str(now_utc())
            )
        )

        await db.commit()


async def save_action(
    user_id: int,
    action_type: str,
    value: int
):
    if value == 0:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO actions (
                user_id,
                action_type,
                action_value,
                timestamp
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                user_id,
                action_type,
                value,
                dt_to_str(now_utc())
            )
        )

        await db.commit()


# ============================================================
# INACTIVITY / WEEKLY LIMITS
# ============================================================

async def prepare_player_for_activity(
    player
):
    """
    Проверяет:
    1. нужно ли вернуть игрока из "Читателя";
    2. нужно ли сбросить недельные лимиты.
    """

    user_id = player[0]

    status = player[8]

    last_post = str_to_dt(player[6])
    week_reset = str_to_dt(player[15])

    reactivated = False

    current = now_utc()

    # --------------------------------------------------------
    # Возвращение из "Читателя"
    # --------------------------------------------------------

    if (
        status == "Читатель"
        and last_post
        and current - last_post
        >= timedelta(days=INACTIVITY_DAYS)
    ):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                UPDATE players
                SET
                    status = 'Игрок',
                    str = ?,
                    rep = ?,
                    con = ?,
                    money = ?,
                    last_post = ?,
                    str_week_limit = 0,
                    rep_week_limit = 0,
                    con_week_limit = 0,
                    money_week_limit = 0,
                    week_reset = ?
                WHERE user_id = ?
                """,
                (
                    INACTIVE_STR,
                    INACTIVE_REP,
                    INACTIVE_CON,
                    INACTIVE_MONEY,
                    dt_to_str(current),
                    dt_to_str(current),
                    user_id
                )
            )

            await db.commit()

        reactivated = True

    # --------------------------------------------------------
    # Недельный сброс
    # --------------------------------------------------------

    elif (
        week_reset is None
        or current - week_reset
        >= timedelta(days=7)
    ):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                UPDATE players
                SET
                    str_week_limit = 0,
                    rep_week_limit = 0,
                    con_week_limit = 0,
                    money_week_limit = 0,
                    week_reset = ?
                WHERE user_id = ?
                """,
                (
                    dt_to_str(current),
                    user_id
                )
            )

            await db.commit()

    return reactivated


# ============================================================
# UPDATE STATS
# ============================================================

async def apply_stats(
    user_id: int,
    username: str,
    text: str,
    topic_id: int,
    message: Message
):
    player = await get_player(user_id)

    if not player:
        # На всякий случай автоматически регистрируем.
        await register_player(
            user_id,
            username
        )

        player = await get_player(user_id)

    await update_username(
        user_id,
        username
    )

    reactivated = await prepare_player_for_activity(
        player
    )

    if reactivated:
        try:
            await bot.send_message(
                user_id,
                (
                    "🔄 Ты вернулся в игру!\n\n"
                    "После долгого отсутствия твой статус "
                    "изменён обратно на «Игрок».\n\n"
                    f"Начальные возвращённые статы:\n"
                    f"STR: {INACTIVE_STR}\n"
                    f"REP: {INACTIVE_REP}\n"
                    f"CON: {INACTIVE_CON}\n"
                    f"MONEY: {INACTIVE_MONEY}\n\n"
                    "Продолжай писать в игровых темах."
                )
            )
        except Exception:
            logger.exception(
                "Не удалось отправить сообщение "
                "о возвращении игроку %s",
                user_id
            )

        player = await get_player(user_id)

    # last_post обновляем даже если пост меньше 300 символов:
    # человек считается активным.
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE players
            SET last_post = ?, username = ?
            WHERE user_id = ?
            """,
            (
                dt_to_str(now_utc()),
                normalize_username(username),
                user_id
            )
        )

        await db.commit()

    # Для статистики нужен минимум 300 символов.
    if len(text.strip()) < MIN_STAT_CHARS:
        return

    result = await analyze_stats(text)

    if not result:
        logger.warning(
            "Статы не начислены: ИИ не вернул корректный результат."
        )
        return

    # Снова читаем игрока, потому что недельный сброс мог произойти.
    player = await get_player(user_id)

    current_str_week = player[11]
    current_rep_week = player[12]
    current_con_week = player[13]
    current_money_week = player[14]

    add_str = min(
        result["str"],
        max(
            0,
            WEEKLY_STR_LIMIT - current_str_week
        )
    )

    add_rep = min(
        result["rep"],
        max(
            0,
            WEEKLY_REP_LIMIT - current_rep_week
        )
    )

    add_con = min(
        result["con"],
        max(
            0,
            WEEKLY_CON_LIMIT - current_con_week
        )
    )

    add_money = min(
        result["money"],
        max(
            0,
            WEEKLY_MONEY_LIMIT - current_money_week
        )
    )

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE players
            SET
                str = str + ?,
                rep = rep + ?,
                con = con + ?,
                money = money + ?,

                bad_boy_count =
                    bad_boy_count + ?,

                good_boy_count =
                    good_boy_count + ?,

                str_week_limit =
                    str_week_limit + ?,

                rep_week_limit =
                    rep_week_limit + ?,

                con_week_limit =
                    con_week_limit + ?,

                money_week_limit =
                    money_week_limit + ?
            WHERE user_id = ?
            """,
            (
                add_str,
                add_rep,
                add_con,
                add_money,
                result["bad_boy"],
                result["good_boy"],
                add_str,
                add_rep,
                add_con,
                add_money,
                user_id
            )
        )

        await db.commit()

    await save_action(
        user_id,
        "str",
        add_str
    )

    await save_action(
        user_id,
        "rep",
        add_rep
    )

    await save_action(
        user_id,
        "con",
        add_con
    )

    await save_action(
        user_id,
        "money",
        add_money
    )

    # Сохраняем событие только если ИИ считает его заметным.
    if (
        result["event"]
        and result["event_summary"]
    ):
        await save_event(
            user_id=user_id,
            username=normalize_username(username),
            event_type="game_event",
            summary=result["event_summary"],
            source_topic=topic_id,
            source_message_id=message.message_id,
            source_url=build_message_link(message)
        )


# ============================================================
# MESSAGE HANDLER
# ============================================================

@dp.message()
async def handle_message(message: Message):
    """
    Общий обработчик сообщений.

    ВАЖНО:
    Поскольку новости публикуются в STORY_TOPIC_ID,
    эта тема здесь явно исключается.

    Поэтому бот не будет читать собственные новости.
    """

    if message.chat.id != GROUP_ID:
        return

    topic_id = message.message_thread_id

    if topic_id is None:
        return

    # Повествование, админка и инфо игнорируются
    # как обычные сообщения.
    if topic_id in IGNORED_TOPIC_IDS:
        return

    user = message.from_user

    if not user:
        return

    username = normalize_username(
        user.username
    )

    text = message.text or message.caption or ""

    if not text.strip():
        return

    # --------------------------------------------------------
    # АНКЕТЫ
    # --------------------------------------------------------

    if topic_id == PROFILES_TOPIC_ID:
        # Сохраняем анкету только если игрок уже зарегистрирован.
        player = await get_player(user.id)

        if not player:
            await register_player(
                user.id,
                username
            )

        url = build_message_link(
            message
        )

        if url:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    """
                    UPDATE players
                    SET anketa_url = ?
                    WHERE user_id = ?
                    """,
                    (
                        url,
                        user.id
                    )
                )

                await db.commit()

            await save_event(
                user_id=user.id,
                username=username,
                event_type="new_player",
                summary=(
                    f"В городе появилась новая личность "
                    f"@{username}. "
                    f"Ссылка на досье должна вести на анкету."
                ),
                source_topic=topic_id,
                source_message_id=message.message_id,
                source_url=url
            )

        return

    # --------------------------------------------------------
    # НОВОСТНЫЕ ТЕМЫ
    # --------------------------------------------------------

    if topic_id in NEWS_TOPIC_IDS:

        # Для статистики отдельно.
        if topic_id in STAT_TOPIC_IDS:
            await apply_stats(
                user.id,
                username,
                text,
                topic_id,
                message
            )

        # Новости могут использовать сообщения короче 300.
        if len(text.strip()) >= MIN_NEWS_CHARS:
            event = await analyze_news_event(
                text,
                username
            )

            if (
                event
                and event["is_event"]
                and event["summary"]
            ):
                await save_event(
                    user_id=user.id,
                    username=username,
                    event_type=event["event_type"],
                    summary=event["summary"],
                    source_topic=topic_id,
                    source_message_id=message.message_id,
                    source_url=build_message_link(message)
                )

        return


# ============================================================
# NEWS GENERATOR
# ============================================================

async def get_unpublished_events(limit: int = 30):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id,
                username,
                event_type,
                summary,
                source_url
            FROM events
            WHERE published = 0
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,)
        )

        rows = await cursor.fetchall()
        await cursor.close()

        return rows


async def generate_news_from_events():
    events = await get_unpublished_events()

    if not events:
        logger.info(
            "Новых событий для новости нет."
        )
        return

    event_lines = []

    for row in events:
        event_id = row[0]
        username = row[1]
        event_type = row[2]
        summary = row[3]

        event_lines.append(
            f"- ID события: {event_id}\n"
            f"  Игрок: @{username}\n"
            f"  Тип: {event_type}\n"
            f"  Событие: {summary}"
        )

    events_text = "\n\n".join(
        event_lines
    )

    prompt = f"""
Ты редактор криминальной хроники
для текстовой ролевой игры.

Сеттинг:
современный альтернативный город,
реалистичный криминальный нуар,
без магии и фантастики.

На основе реальных игровых событий ниже
создай одну связанную городскую новость.

Правила:
- 2–5 абзацев.
- Не перечисляй игроков как список.
- Не выдумывай конкретные факты, которых нет в событиях.
- Можно художественно связать подтверждённые события.
- Тон: городская криминальная хроника.
- Новость должна ощущаться как событие внутри города.
- Если среди событий есть новый персонаж,
  можно использовать формулировки вроде:
  "в городе замечена новая личность",
  "новое лицо появилось на улицах",
  "камеры зафиксировали..."
- Если есть ссылка на анкету, её нужно сохранить
  как отдельную строку в конце.
- Не создавай Markdown-ссылки сам.
- Не добавляй URL, если его нет в данных.

События:

{events_text}
"""

    content = await groq_request(
        [
            {
                "role": "system",
                "content": (
                    "Ты пишешь только текст городской новости."
                )
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.8,
        max_tokens=800
    )

    if not content:
        logger.error(
            "Не удалось сгенерировать новость."
        )
        return

    news = content.strip()

    # Добавляем ссылки на анкеты,
    # которые реально есть среди событий.
    profile_links = []

    for row in events:
        event_type = row[2]
        source_url = row[4]

        if (
            event_type == "new_player"
            and source_url
        ):
            profile_links.append(
                source_url
            )

    if profile_links:
        news += (
            "\n\n📋 *Досье новых лиц:*\n"
            + "\n".join(
                f"• {url}"
                for url in profile_links
            )
        )

    try:
        await bot.send_message(
            GROUP_ID,
            "📰 *ГОРОДСКАЯ ХРОНИКА*\n\n" + news,
            message_thread_id=STORY_TOPIC_ID,
            parse_mode="Markdown"
        )

    except Exception:
        logger.exception(
            "Не удалось отправить новость "
            "в тему Повествование."
        )
        return

    event_ids = [
        row[0]
        for row in events
    ]

    placeholders = ",".join(
        "?" for _ in event_ids
    )

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"""
            UPDATE events
            SET published = 1
            WHERE id IN ({placeholders})
            """,
            event_ids
        )

        await db.commit()

    logger.info(
        "Опубликована новость из %s событий.",
        len(events)
    )


async def news_loop():
    while True:
        try:
            await asyncio.sleep(
                NEWS_INTERVAL_HOURS * 60 * 60
            )

            await generate_news_from_events()

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Ошибка в news_loop."
            )

            await asyncio.sleep(60)


# ============================================================
# INACTIVITY LOOP
# ============================================================

async def inactivity_loop():
    while True:
        try:
            await asyncio.sleep(
                24 * 60 * 60
            )

            cutoff = now_utc() - timedelta(
                days=INACTIVITY_DAYS
            )

            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute(
                    """
                    SELECT user_id, username
                    FROM players
                    WHERE last_post IS NOT NULL
                    AND last_post < ?
                    AND status != 'Читатель'
                    """,
                    (dt_to_str(cutoff),)
                )

                inactive = await cursor.fetchall()
                await cursor.close()

                for user_id, username in inactive:
                    await db.execute(
                        """
                        UPDATE players
                        SET
                            status = 'Читатель',
                            str = ?,
                            rep = ?,
                            con = ?,
                            money = ?
                        WHERE user_id = ?
                        """,
                        (
                            INACTIVE_STR,
                            INACTIVE_REP,
                            INACTIVE_CON,
                            INACTIVE_MONEY,
                            user_id
                        )
                    )

                await db.commit()

            for user_id, username in inactive:
                try:
                    await bot.send_message(
                        user_id,
                        (
                            "⚠️ Ты давно не писал в игровых "
                            "темах и переведён в статус "
                            "«Читатель».\n\n"

                            "Твои игровые характеристики "
                            "сброшены до базовых:\n"
                            f"STR: {INACTIVE_STR}\n"
                            f"REP: {INACTIVE_REP}\n"
                            f"CON: {INACTIVE_CON}\n"
                            f"MONEY: {INACTIVE_MONEY}\n\n"

                            "Чтобы вернуться в игру, "
                            "просто начни снова писать."
                        )
                    )

                except Exception:
                    logger.exception(
                        "Не удалось уведомить "
                        "неактивного игрока %s",
                        user_id
                    )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Ошибка inactivity_loop."
            )

            await asyncio.sleep(60)


# ============================================================
# WEEKLY TOP
# ============================================================

async def publish_top():
    while True:
        try:
            await asyncio.sleep(
                7 * 24 * 60 * 60
            )

            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute(
                    """
                    SELECT
                        username,
                        str,
                        rep,
                        con,
                        money,
                        status,
                        bad_boy_count,
                        good_boy_count
                    FROM players
                    WHERE status = 'Игрок'
                    ORDER BY rep DESC
                    LIMIT 10
                    """
                )

                top = await cursor.fetchall()
                await cursor.close()

            if not top:
                continue

            text = "🏆 *ТОП ИГРОКОВ*\n\n"

            for i, player in enumerate(top, 1):
                username = player[0] or "unknown"

                badge = get_badge(
                    player[6],
                    player[7]
                )

                text += (
                    f"{i}. @{username} — "
                    f"REP: {player[2]}, "
                    f"STR: {player[1]}, "
                    f"CON: {player[3]}, "
                    f"MONEY: {player[4]}"
                )

                if badge:
                    text += f" — {badge}"

                text += "\n"

            await bot.send_message(
                GROUP_ID,
                text,
                message_thread_id=STORY_TOPIC_ID,
                parse_mode="Markdown"
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Ошибка publish_top."
            )

            await asyncio.sleep(60)


# ============================================================
# ADMIN COMMANDS
# ============================================================

@dp.message(Command("admin_stats"))
async def admin_stats(
    message: Message,
    command: CommandObject
):
    if not message.from_user or not is_admin(
        message.from_user.id
    ):
        return

    args = (command.args or "").split()

    if not args:
        await message.answer(
            "Использование:\n"
            "/admin_stats @username"
        )
        return

    player = await get_player_by_username(
        args[0].lstrip("@")
    )

    if not player:
        await message.answer(
            "Игрок не найден."
        )
        return

    await show_profile(
        message,
        player
    )


@dp.message(Command("admin_addstat"))
async def admin_addstat(
    message: Message,
    command: CommandObject
):
    if not message.from_user or not is_admin(
        message.from_user.id
    ):
        return

    args = (command.args or "").split()

    if len(args) != 3:
        await message.answer(
            "Использование:\n"
            "/admin_addstat @username rep 2\n\n"
            "Статы: str, rep, con, money"
        )
        return

    username = args[0].lstrip("@").lower()
    stat = args[1].lower()

    try:
        value = int(args[2])
    except ValueError:
        await message.answer(
            "Количество должно быть числом."
        )
        return

    if stat not in {
        "str",
        "rep",
        "con",
        "money"
    }:
        await message.answer(
            "Стат должен быть: "
            "str, rep, con или money."
        )
        return

    player = await get_player_by_username(
        username
    )

    if not player:
        await message.answer(
            "Игрок не найден."
        )
        return

    column = stat

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"""
            UPDATE players
            SET {column} = {column} + ?
            WHERE user_id = ?
            """,
            (
                value,
                player[0]
            )
        )

        await db.commit()

    await message.answer(
        f"✅ @{username}: "
        f"{stat.upper()} изменён на {value}."
    )


@dp.message(Command("admin_reset"))
async def admin_reset(
    message: Message,
    command: CommandObject
):
    if not message.from_user or not is_admin(
        message.from_user.id
    ):
        return

    args = (command.args or "").split()

    if not args:
        await message.answer(
            "Использование:\n"
            "/admin_reset @username"
        )
        return

    username = args[0].lstrip("@")

    player = await get_player_by_username(
        username
    )

    if not player:
        await message.answer(
            "Игрок не найден."
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE players
            SET
                str = 1,
                rep = 1,
                con = 1,
                money = 100,
                status = 'Игрок',
                str_week_limit = 0,
                rep_week_limit = 0,
                con_week_limit = 0,
                money_week_limit = 0,
                week_reset = ?
            WHERE user_id = ?
            """,
            (
                dt_to_str(now_utc()),
                player[0]
            )
        )

        await db.commit()

    await message.answer(
        f"✅ @{username} сброшен до:\n"
        "STR 1\n"
        "REP 1\n"
        "CON 1\n"
        "MONEY 100\n"
        "Статус: Игрок"
    )


@dp.message(Command("admin_news"))
async def admin_news(message: Message):
    if not message.from_user or not is_admin(
        message.from_user.id
    ):
        return

    await message.answer(
        "📰 Запускаю генерацию новости..."
    )

    await generate_news_from_events()

    await message.answer(
        "Готово."
    )


# ============================================================
# STARTUP
# ============================================================

async def startup_checks():
    logger.info("Запуск проверки конфигурации...")

    logger.info(
        "GROUP_ID: %s",
        GROUP_ID
    )

    logger.info(
        "Темы: игра=%s, полурол=%s, анкеты=%s, "
        "флуд=%s, повествование=%s",
        GAME_TOPIC_ID,
        SEMI_RP_TOPIC_ID,
        PROFILES_TOPIC_ID,
        FLOOD_TOPIC_ID,
        STORY_TOPIC_ID
    )

    try:
        me = await bot.get_me()

        logger.info(
            "Telegram Bot OK: @%s (%s)",
            me.username,
            me.id
        )

    except Exception:
        logger.exception(
            "BOT_TOKEN не прошёл проверку."
        )
        raise

    await check_groq_api()


# ============================================================
# MAIN
# ============================================================

async def main():
    await init_db()

    await startup_checks()

    if not groq_available:
        logger.warning(
            "⚠️ Groq недоступен. "
            "Бот продолжит работать, но "
            "ИИ-анализ и новости будут временно отключены."
        )

    asyncio.create_task(
        news_loop()
    )

    asyncio.create_task(
        inactivity_loop()
    )

    asyncio.create_task(
        publish_top()
    )

    logger.info(
        "Бот запущен."
    )

    await dp.start_polling(
        bot
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info(
            "Бот остановлен."
        )
