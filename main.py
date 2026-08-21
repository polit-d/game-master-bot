import os
import asyncio
import json
import logging
import re
from html import escape
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message
from dotenv import load_dotenv


# ============================================================
# НАСТРОЙКА
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = os.getenv("GROUP_ID")

GAME_TOPIC_ID = os.getenv("GAME_TOPIC_ID")
SEMI_RP_TOPIC_ID = os.getenv("SEMI_RP_TOPIC_ID")
INFO_TOPIC_ID = os.getenv("INFO_TOPIC_ID")
ADMIN_TOPIC_ID = os.getenv("ADMIN_TOPIC_ID")
PROFILES_TOPIC_ID = os.getenv("PROFILES_TOPIC_ID")
FLOOD_TOPIC_ID = os.getenv("FLOOD_TOPIC_ID")
STORY_TOPIC_ID = os.getenv("STORY_TOPIC_ID")

RULES_URL = os.getenv("RULES_URL")

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = {
    int(x.strip())
    for x in ADMIN_IDS_RAW.split(",")
    if x.strip().isdigit()
}

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_STRUCTURED_MODEL = os.getenv(
    "GROQ_STRUCTURED_MODEL",
    "openai/gpt-oss-20b"
)

NEWS_STYLE = os.getenv(
    "NEWS_STYLE",
    (
        "Пиши на русском языке в литературном стиле "
        "мрачной криминальной хроники альтернативного "
        "современного города. Используй атмосферные детали, "
        "запахи, звуки, погоду, свет и контраст роскоши "
        "Не выдумывай факты и действия персонажей."
    )
)

DATABASE_URL = os.getenv("DATABASE_URL")


# ============================================================
# ЛОГИ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("rpg_bot")


# ============================================================
# ПРОВЕРКА НАСТРОЕК
# ============================================================

REQUIRED_ENV = {
    "BOT_TOKEN": BOT_TOKEN,
    "GROUP_ID": GROUP_ID,
    "GAME_TOPIC_ID": GAME_TOPIC_ID,
    "SEMI_RP_TOPIC_ID": SEMI_RP_TOPIC_ID,
    "INFO_TOPIC_ID": INFO_TOPIC_ID,
    "ADMIN_TOPIC_ID": ADMIN_TOPIC_ID,
    "PROFILES_TOPIC_ID": PROFILES_TOPIC_ID,
    "FLOOD_TOPIC_ID": FLOOD_TOPIC_ID,
    "STORY_TOPIC_ID": STORY_TOPIC_ID,
    "RULES_URL": RULES_URL,
    "GROQ_API_KEY": GROQ_API_KEY,
    "DATABASE_URL": DATABASE_URL,
}

missing_env = [
    name for name, value in REQUIRED_ENV.items()
    if not value
]

if missing_env:
    raise RuntimeError(
        "Не хватает переменных окружения: "
        + ", ".join(missing_env)
    )


try:
    GROUP_ID_INT = int(GROUP_ID)

    GAME_TOPIC_ID_INT = int(GAME_TOPIC_ID)
    SEMI_RP_TOPIC_ID_INT = int(SEMI_RP_TOPIC_ID)
    INFO_TOPIC_ID_INT = int(INFO_TOPIC_ID)
    ADMIN_TOPIC_ID_INT = int(ADMIN_TOPIC_ID)
    PROFILES_TOPIC_ID_INT = int(PROFILES_TOPIC_ID)
    FLOOD_TOPIC_ID_INT = int(FLOOD_TOPIC_ID)
    STORY_TOPIC_ID_INT = int(STORY_TOPIC_ID)

except ValueError as exc:
    raise RuntimeError(
        "GROUP_ID и ID тем должны быть числами."
    ) from exc


# ============================================================
# БОТ
# ============================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


class RegistrationState(StatesGroup):
    waiting_for_character_name = State()


# ============================================================
# ГЛОБАЛЬНЫЕ ОБЪЕКТЫ
# ============================================================

db_pool = None

# Последние сообщения для будущей генерации новостей.
# Храним только ограниченное количество в памяти.
event_buffer = []

event_buffer_lock = asyncio.Lock()

MAX_EVENT_BUFFER = 100

RANDOM_CITY_EVENTS = [
    "в городе произошёл пожар",
    "на одной из улиц случилась авария",
    "празднично украсили главную улицу",
    "в городе появились важные политические новости",
    "полиция провела задержание",
    "произошло ограбление",
    "началась предвыборная кампания нового мэра",
    "в городе возникли серьёзные проблемы с транспортом",
    "радиоведущий подавился во время прямого эфира",
    "в нескольких районах отключилось электричество",
    "началась забастовка",
    "открылось новое заведение",
]

MIN_TEXT_FOR_STATS = 300

NEWS_INTERVAL_HOURS = 6

INACTIVITY_DAYS = 30

WEEK_DAYS = 7


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def normalize_datetime(value):
    """
    PostgreSQL может вернуть datetime с timezone.
    Приводим всё к timezone-aware UTC.
    """
    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def get_level_status(value):
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


def get_badge(bad_count, good_count):
    if bad_count > good_count + 5:
        return "Bad boy"

    if good_count > bad_count + 5:
        return "Good boy"

    return ""


def get_topic_name(topic_id):
    if topic_id == GAME_TOPIC_ID_INT:
        return "Игровая"

    if topic_id == SEMI_RP_TOPIC_ID_INT:
        return "Полурол - на обочине"

    if topic_id == INFO_TOPIC_ID_INT:
        return "О чем это вообще"

    if topic_id == ADMIN_TOPIC_ID_INT:
        return "Админка"

    if topic_id == PROFILES_TOPIC_ID_INT:
        return "Анкеты"

    if topic_id == FLOOD_TOPIC_ID_INT:
        return "Флуд"

    if topic_id == STORY_TOPIC_ID_INT:
        return "Повествование"

    return "Неизвестная тема"


def is_admin(user_id):
    return user_id in ADMIN_IDS


def get_topic_id(message: Message):
    return message.message_thread_id


def is_stats_topic(message: Message):
    return get_topic_id(message) in {
        GAME_TOPIC_ID_INT,
        SEMI_RP_TOPIC_ID_INT,
    }


def is_news_topic_source(message: Message):
    return get_topic_id(message) in {
        GAME_TOPIC_ID_INT,
        SEMI_RP_TOPIC_ID_INT,
        FLOOD_TOPIC_ID_INT,
        PROFILES_TOPIC_ID_INT,
    }


def is_ignored_topic(message: Message):
    return get_topic_id(message) in {
        INFO_TOPIC_ID_INT,
        ADMIN_TOPIC_ID_INT,
        STORY_TOPIC_ID_INT,
    }


def clean_username(username):
    if not username:
        return None

    return username.lstrip("@").strip()


def make_profile_link(username):
    username = clean_username(username)

    if not username:
        return None

    return f"https://t.me/{username}"


# ============================================================
# DATABASE
# ============================================================

async def create_database_pool():
    global db_pool

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )

    logger.info("Подключение к PostgreSQL установлено.")


async def close_database_pool():
    global db_pool

    if db_pool:
        await db_pool.close()
        logger.info("Соединение с PostgreSQL закрыто.")


async def init_database():
    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            character_name TEXT,
            str INTEGER NOT NULL DEFAULT 1,
            rep INTEGER NOT NULL DEFAULT 1,
            con INTEGER NOT NULL DEFAULT 1,
            money INTEGER NOT NULL DEFAULT 100,

            last_post TIMESTAMPTZ,

            anketa_url TEXT,

            status TEXT NOT NULL DEFAULT 'Игрок',

            bad_boy_count INTEGER NOT NULL DEFAULT 0,
            good_boy_count INTEGER NOT NULL DEFAULT 0,

            str_week_limit INTEGER NOT NULL DEFAULT 0,
            rep_week_limit INTEGER NOT NULL DEFAULT 0,
            con_week_limit INTEGER NOT NULL DEFAULT 0,
            money_week_limit INTEGER NOT NULL DEFAULT 0,

            week_reset TIMESTAMPTZ,

            first_start_seen BOOLEAN NOT NULL DEFAULT FALSE
        )
        """
    )

    await db_pool.execute(
        """
        ALTER TABLE players
        ADD COLUMN IF NOT EXISTS character_name TEXT
        """
    )

    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS actions (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT,
            action_type TEXT,
            action_value INTEGER,
            timestamp TIMESTAMPTZ,
            telegram_message_id BIGINT UNIQUE,
            topic_id INTEGER
        )
        """
    )

    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_messages (
            message_id BIGINT PRIMARY KEY,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS news_events (
            id BIGSERIAL PRIMARY KEY,

            source_chat_id BIGINT NOT NULL,
            telegram_message_id BIGINT NOT NULL,

            user_id BIGINT,
            character_name TEXT,
            username TEXT,

            event_type TEXT NOT NULL,
            text TEXT NOT NULL,

            topic_id INTEGER,
            topic_name TEXT,
            anketa_url TEXT,

            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            used_in_news_at TIMESTAMPTZ
        )
        """
    )

    await db_pool.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
        news_events_source_message_idx
        ON news_events (
            source_chat_id,
            telegram_message_id
        )
        """
    )

    await db_pool.execute(
        """
        CREATE INDEX IF NOT EXISTS
        news_events_created_at_idx
        ON news_events (created_at)
        """
    )

    logger.info(
        "Таблицы PostgreSQL проверены/созданы."
    )


async def get_player(user_id):
    return await db_pool.fetchrow(
        """
        SELECT *
        FROM players
        WHERE user_id = $1
        """,
        user_id
    )


async def get_player_by_username(username):
    username = clean_username(username)

    if not username:
        return None

    return await db_pool.fetchrow(
        """
        SELECT *
        FROM players
        WHERE LOWER(username) = LOWER($1)
        """,
        username
    )


async def register_player(
    user_id,
    username,
    character_name,
    initial_stats=True
):
    existing = await get_player(user_id)

    if existing:
        return existing, False

    if initial_stats:
        initial_str = 4
        initial_rep = 4
        initial_con = 4
        initial_money = 500
    else:
        initial_str = 1
        initial_rep = 1
        initial_con = 1
        initial_money = 100

    player = await db_pool.fetchrow(
        """
        INSERT INTO players (
            user_id,
            username,
            character_name,
            str,
            rep,
            con,
            money,
            last_post,
            status,
            week_reset,
            first_start_seen
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            'Игрок',
            $8,
            TRUE
        )
        RETURNING *
        """,
        user_id,
        clean_username(username),
        character_name,
        initial_str,
        initial_rep,
        initial_con,
        initial_money,
        now_utc()
    )

    return player, True


async def ensure_registered(user_id, username):
    """
    Совместимый вспомогательный метод: теперь бот НЕ регистрирует
    новых игроков автоматически. Регистрация выполняется только
    через /start и имя персонажа.
    """
    player = await get_player(user_id)

    if not player:
        return None, False

    current_username = clean_username(username)

    if current_username and current_username != player["username"]:
        player = await db_pool.fetchrow(
            """
            UPDATE players
            SET username = $1
            WHERE user_id = $2
            RETURNING *
            """,
            current_username,
            user_id
        )

    return player, False


# ============================================================
# ОБРАБОТКА НЕДЕЛЬНЫХ ЛИМИТОВ
# ============================================================

async def reset_week_if_needed(player):
    week_reset = normalize_datetime(player["week_reset"])

    if not week_reset:
        await db_pool.execute(
            """
            UPDATE players
            SET week_reset = $1
            WHERE user_id = $2
            """,
            now_utc(),
            player["user_id"]
        )
        return await get_player(player["user_id"])

    if now_utc() - week_reset >= timedelta(days=WEEK_DAYS):
        player = await db_pool.fetchrow(
            """
            UPDATE players
            SET
                str_week_limit = 0,
                rep_week_limit = 0,
                con_week_limit = 0,
                money_week_limit = 0,
                week_reset = $1
            WHERE user_id = $2
            RETURNING *
            """,
            now_utc(),
            player["user_id"]
        )

    return player


# ============================================================
# GROQ
# ============================================================

async def groq_request(
    messages,
    model=None,
    temperature=0.4
):
    model = model or GROQ_MODEL

    url = "https://api.groq.com/openai/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    data = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }

    timeout = aiohttp.ClientTimeout(total=60)

    try:
        async with aiohttp.ClientSession(
            timeout=timeout
        ) as session:

            async with session.post(
                url,
                headers=headers,
                json=data
            ) as response:

                response_text = await response.text()

                if response.status != 200:
                    logger.error(
                        "Groq API error %s: %s",
                        response.status,
                        response_text[:1000]
                    )
                    return None

                try:
                    return json.loads(response_text)
                except json.JSONDecodeError:
                    logger.error(
                        "Groq вернул некорректный JSON ответа API: %s",
                        response_text[:1000]
                    )
                    return None

    except asyncio.TimeoutError:
        logger.error("Groq API timeout.")
        return None

    except aiohttp.ClientError as exc:
        logger.error("Ошибка соединения с Groq: %s", exc)
        return None

    except Exception:
        logger.exception("Неожиданная ошибка Groq.")
        return None


def extract_json_object(text):
    """
    Пытаемся достать JSON даже если модель обернула его
    в ```json ... ```.
    """

    if not text:
        return None

    text = text.strip()

    # Убираем markdown code fence.
    text = re.sub(
        r"^```(?:json)?\s*",
        "",
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        r"\s*```$",
        "",
        text
    )

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Ищем первый объект JSON внутри текста.
    match = re.search(
        r"\{.*\}",
        text,
        flags=re.DOTALL
    )

    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None

    return None


def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


async def analyze_text(text):
    prompt = f"""
Ты аналитик текстовой ролевой игры.

Проанализируй сообщение игрока.

ВАЖНО:
- Не придумывай действия, которых в тексте нет.
- Если категория не подходит, ставь 0.
- Значения должны быть целыми числами.
- Не начисляй огромные значения за одно сообщение.
- Максимально разумные значения за один текст:
  STR: 0-2
  REP: 0-2
  CON: 0-2
  MONEY: 0-10

Категории:

STR:
- драки;
- победы в драках;
- спорт;
- физические действия;
- силовые действия.

REP:
- помощь другим;
- заметные события;
- красивые и значимые посты;
- участие в мероприятиях;
- администрирование;
- конфликты, влияющие на репутацию.

CON:
- знакомства;
- общение;
- сделки;
- договорённости;
- сюжетные связи.

MONEY:
- сделки;
- заработок;
- задания;
- получение денег.

bad_boy:
Поставь 1, если сообщение явно показывает криминальное,
жестокое, агрессивное или крайне плохое поведение персонажа.

good_boy:
Поставь 1, если сообщение явно показывает помощь,
доброе или социально полезное поведение персонажа.

Верни ТОЛЬКО JSON:

{{
  "str": 0,
  "rep": 0,
  "con": 0,
  "money": 0,
  "bad_boy": 0,
  "good_boy": 0
}}

Сообщение:

{text}
"""

    messages = [
        {
            "role": "system",
            "content": (
                "Ты аналитик текстовой ролевой игры. "
                "Отвечай только JSON."
            )
        },
        {
            "role": "user",
            "content": prompt
        }
    ]

    result = await groq_request(
        messages,
        model=GROQ_STRUCTURED_MODEL,
        temperature=0.1
    )

    if not result:
        return {
            "str": 0,
            "rep": 0,
            "con": 0,
            "money": 0,
            "bad_boy": 0,
            "good_boy": 0,
        }

    try:
        content = result["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        logger.error("Не удалось получить content из ответа Groq.")
        return {
            "str": 0,
            "rep": 0,
            "con": 0,
            "money": 0,
            "bad_boy": 0,
            "good_boy": 0,
        }

    parsed = extract_json_object(content)

    if not isinstance(parsed, dict):
        logger.error(
            "Groq вернул JSON, который не является объектом: %s",
            content[:1000]
        )

        return {
            "str": 0,
            "rep": 0,
            "con": 0,
            "money": 0,
            "bad_boy": 0,
            "good_boy": 0,
        }

    return {
        "str": max(0, min(2, safe_int(parsed.get("str")))),
        "rep": max(0, min(2, safe_int(parsed.get("rep")))),
        "con": max(0, min(2, safe_int(parsed.get("con")))),
        "money": max(0, min(10, safe_int(parsed.get("money")))),
        "bad_boy": max(
            0,
            min(1, safe_int(parsed.get("bad_boy")))
        ),
        "good_boy": max(
            0,
            min(1, safe_int(parsed.get("good_boy")))
        ),
    }


# ============================================================
# НОВОСТИ
# ============================================================

async def add_event_to_buffer(
    message,
    event_type,
    text,
    username=None,
    anketa_url=None
):
    if not text:
        return

    user_id = None
    character_name = None

    if message.from_user:
        user_id = message.from_user.id

    current_username = clean_username(username)

    # Получаем имя персонажа из профиля.
    # Если игрок ещё не зарегистрирован,
    # имя останется пустым.
    if user_id and db_pool:
        player = await get_player(user_id)

        if player:
            character_name = player["character_name"]

            # Берём актуальный username из профиля,
            # если он есть.
            if not current_username:
                current_username = clean_username(
                    player["username"]
                )

    topic_id = get_topic_id(message)
    topic_name = get_topic_name(topic_id)

    event = {
        "type": event_type,
        "username": current_username,
        "character_name": character_name,
        "text": text[:4000],
        "anketa_url": anketa_url,
        "topic": topic_name,
        "timestamp": now_utc().isoformat(),
    }

    # Пока сохраняем событие и в старый временный буфер.
    # Это позволит текущей системе новостей продолжить работать.
    async with event_buffer_lock:
        event_buffer.append(event)

        if len(event_buffer) > MAX_EVENT_BUFFER:
            del event_buffer[
                :len(event_buffer) - MAX_EVENT_BUFFER
            ]

    # Одновременно сохраняем событие в PostgreSQL.
    # Повторное получение одного и того же сообщения
    # не создаст дубликат.
    await db_pool.execute(
        """
        INSERT INTO news_events (
            source_chat_id,
            telegram_message_id,
            user_id,
            character_name,
            username,
            event_type,
            text,
            topic_id,
            topic_name,
            anketa_url
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10
        )
        ON CONFLICT (
            source_chat_id,
            telegram_message_id
        )
        DO NOTHING
        """,
        message.chat.id,
        message.message_id,
        user_id,
        character_name,
        current_username,
        event_type,
        text[:4000],
        topic_id,
        topic_name,
        anketa_url
    )

    logger.info(
        "Событие сохранено для новостей: "
        "персонаж=%s, username=@%s, тема=%s",
        character_name or "не указан",
        current_username or "нет",
        topic_name
    )


def format_news_character(
    character_name,
    username
):
    character_name = (
        str(character_name or "").strip()
    )

    username = clean_username(username)

    if character_name and username:
        return f"{character_name} (@{username})"

    if character_name:
        return character_name

    if username:
        return f"персонаж без указанного имени (@{username})"

    return "персонаж без указанного имени"


async def get_news_events(
    hours=6
):
    cutoff = now_utc() - timedelta(
        hours=hours
    )

    return await db_pool.fetch(
        """
        SELECT
            id,
            user_id,
            character_name,
            username,
            event_type,
            text,
            topic_name,
            anketa_url,
            created_at
        FROM news_events
        WHERE
            created_at >= $1
            AND used_in_news_at IS NULL
        ORDER BY created_at ASC
        LIMIT 300
        """,
        cutoff
    )


async def mark_news_events_used(
    event_ids
):
    if not event_ids:
        return

    await db_pool.execute(
        """
        UPDATE news_events
        SET used_in_news_at = NOW()
        WHERE id = ANY($1::BIGINT[])
        """,
        event_ids
    )


async def generate_news_from_events(
    hours=6
):
    events = await get_news_events(
        hours=hours
    )

    event_ids = []

    period_phrase = (
        "за последний час"
        if hours == 1
        else f"за последние {hours} часов"
    )

    if events:
        formatted_events = []

        for event in events:
            event_ids.append(event["id"])

            author = format_news_character(
                event["character_name"],
                event["username"]
            )

            topic_name = (
                event["topic_name"]
                or "неизвестная тема"
            )

            formatted_events.append(
                f"""
Имя персонажа: {author}
Тема: {topic_name}
Тип события: {event["event_type"]}
Текст поста:
{event["text"]}
"""
            )

        source_text = "\n---\n".join(
            formatted_events
        )

        prompt = f"""
Ты пишешь городскую новость для текстовой
ролевой игры.

Мир — альтернативный современный город
без магии и сверхъестественного.

Используй только события из переданных
сообщений игроков.

Стиль проекта:
{NEWS_STYLE}

Правила:

- Пиши литературно, атмосферно и живо.
- Используй детали города, свет, погоду,
  звуки, запахи и контраст роскоши
  с разрухой, если это подходит.
- Объём — 2–4 коротких абзаца.
- Не превращай текст в сухой список.
- Не придумывай действия, которых нет
  в исходных сообщениях.
- Не управляй персонажами и не решай
  за них, что они сделали после поста.
- Не добавляй магию и сверхъестественное.
- Не добавляй новых персонажей.
- Не используй хэштеги.
- Не используй Telegram ID.
- Сохраняй имя персонажа и username
  в формате: Имя персонажа (@username).
- Если username отсутствует, используй
  только имя персонажа.
- Упомяни в тексте тех персонажей,
  чьи действия важны для новости.
- Не добавляй заголовок — его добавит бот.

События игроков:

{source_text}
"""
    else:
        import random

        random_event = random.choice(
            RANDOM_CITY_EVENTS
        )

        source_text = random_event

        prompt = f"""
Ты пишешь короткую городскую новость
для текстовой ролевой игры.

Мир — альтернативный современный город
без магии и сверхъестественного.

За период {period_phrase} игроки ничего
не написали, поэтому создай фоновое событие
для города.

Случайная основа события:
{random_event}

Стиль проекта:
{NEWS_STYLE}

Правила:

- Напиши 2–3 литературных предложения.
- Сделай событие атмосферным и правдоподобным.
- Можно добавить место, время суток,
  погоду, реакцию жителей или городские слухи.
- Не связывай событие с игровыми персонажами.
- Не добавляй магию и сверхъестественное.
- Не используй хэштеги.
- Не используй Telegram ID.
- Не добавляй заголовок — его добавит бот.
"""
    
    result = await groq_request(
        [
            {
                "role": "system",
                "content": (
                    "Ты литературный автор "
                    "городской криминальной хроники. "
                    "Пиши только текст новости."
                )
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        model=GROQ_MODEL,
        temperature=0.8
    )

    if not result:
        return None, []

    try:
        news_text = (
            result["choices"][0]["message"]
            ["content"]
            .strip()
        )
    except (
        KeyError,
        IndexError,
        TypeError
    ):
        logger.error(
            "Не удалось получить текст новости."
        )
        return None, []

    return news_text, event_ids


async def generate_events_loop():
    while True:
        try:
            await asyncio.sleep(
                NEWS_INTERVAL_HOURS * 60 * 60
            )

            news_text, event_ids = (
                await generate_news_from_events(
                    hours=6
                )
            )

            if not news_text:
                logger.info(
                    "Новость не создана: "
                    "ИИ не вернул текст."
                )
                continue

            final_text = (
                "📰 НОВОСТИ ГОРОДА\n\n"
                + news_text
            )

            await bot.send_message(
                GROUP_ID_INT,
                final_text,
                message_thread_id=STORY_TOPIC_ID_INT
            )

            await mark_news_events_used(
                event_ids
            )

            logger.info(
                "Обычная новость опубликована. "
                "Использовано событий: %s",
                len(event_ids)
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Ошибка фоновой генерации новостей."
            )

# ============================================================
# ОБРАБОТКА СТАТИСТИКИ
# ============================================================

async def update_stats(
    user_id,
    username,
    text,
    message_id,
    topic_id
):
    if len(text) < MIN_TEXT_FOR_STATS:
        logger.info(
            "Сообщение %s слишком короткое для статов: %s символов.",
            message_id,
            len(text)
        )
        return

    # Статы начисляются только уже зарегистрированным игрокам.
    player = await get_player(user_id)

    if not player:
        logger.info(
            "Игрок %s написал игровой пост, но ещё не зарегистрирован через /start.",
            user_id
        )
        return

    # Проверяем, не обрабатывали ли это сообщение раньше.
    inserted = await db_pool.fetchval(
        """
        INSERT INTO processed_messages (
            message_id
        )
        VALUES ($1)
        ON CONFLICT (message_id)
        DO NOTHING
        RETURNING message_id
        """,
        message_id
    )

    if inserted is None:
        logger.info(
            "Сообщение %s уже обрабатывалось.",
            message_id
        )
        return

    # Обновляем username, если игрок его поменял.
    current_username = clean_username(username)
    if current_username and current_username != player["username"]:
        player = await db_pool.fetchrow(
            """
            UPDATE players
            SET username = $1
            WHERE user_id = $2
            RETURNING *
            """,
            current_username,
            user_id
        )

    player = await reset_week_if_needed(player)

    result = await analyze_text(text)

    str_remaining = max(
        0,
        2 - player["str_week_limit"]
    )

    rep_remaining = max(
        0,
        7 - player["rep_week_limit"]
    )

    con_remaining = max(
        0,
        8 - player["con_week_limit"]
    )

    money_remaining = max(
        0,
        1000 - player["money_week_limit"]
    )

    str_add = min(result["str"], str_remaining)
    rep_add = min(result["rep"], rep_remaining)
    con_add = min(result["con"], con_remaining)
    money_add = min(result["money"], money_remaining)

    await db_pool.execute(
        """
        UPDATE players
        SET
            str = str + $1,
            rep = rep + $2,
            con = con + $3,
            money = money + $4,

            last_post = $5,

            bad_boy_count = bad_boy_count + $6,
            good_boy_count = good_boy_count + $7,

            str_week_limit = str_week_limit + $1,
            rep_week_limit = rep_week_limit + $2,
            con_week_limit = con_week_limit + $3,
            money_week_limit = money_week_limit + $4,

            status = CASE
                WHEN status = 'Читатель'
                THEN 'Игрок'
                ELSE status
            END
        WHERE user_id = $8
        """,
        str_add,
        rep_add,
        con_add,
        money_add,
        now_utc(),
        result["bad_boy"],
        result["good_boy"],
        user_id
    )

    await db_pool.execute(
        """
        INSERT INTO actions (
            user_id,
            action_type,
            action_value,
            timestamp,
            telegram_message_id,
            topic_id
        )
        VALUES
            ($1, 'str', $2, $3, $4, $5),
            ($1, 'rep', $6, $3, $4, $5),
            ($1, 'con', $7, $3, $4, $5),
            ($1, 'money', $8, $3, $4, $5)
        ON CONFLICT (telegram_message_id)
        DO NOTHING
        """,
        user_id,
        str_add,
        now_utc(),
        message_id,
        topic_id,
        rep_add,
        con_add,
        money_add
    )

    logger.info(
        "Статы обновлены для @%s: STR +%s, REP +%s, CON +%s, MONEY +%s",
        username,
        str_add,
        rep_add,
        con_add,
        money_add
    )


# ============================================================
# ПРОФИЛЬ
# ============================================================

async def build_profile_text(player):
    str_level, str_status = get_level_status(
        player["str"]
    )

    rep_level, rep_status = get_level_status(
        player["rep"]
    )

    con_level, con_status = get_level_status(
        player["con"]
    )

    badge = get_badge(
        player["bad_boy_count"],
        player["good_boy_count"]
    )

    # Ссылка на анкету
    if player["anketa_url"]:
        safe_anketa_url = escape(
            str(player["anketa_url"]),
            quote=True
        )

        anketa_text = (
            f'<a href="{safe_anketa_url}">Открыть анкету</a>'
        )
    else:
        anketa_text = "Не заполнена"

    # Дата последнего поста
    last_post = player["last_post"]

    if last_post:
        last_post_text = normalize_datetime(
            last_post
        ).strftime("%d.%m.%Y %H:%M")
    else:
        last_post_text = "Никогда"

    # Имя персонажа и Telegram username
    character_name = player["character_name"] or "Имя не указано"
    username = player["username"] or "без_username"

    # Экранируем данные пользователя,
    # чтобы специальные символы не ломали HTML Telegram
    safe_username = escape(str(username))
    safe_character_name = escape(str(character_name))
    safe_status = escape(
        str(player["status"] or "Игрок")
    )

    safe_str_status = escape(str(str_status))
    safe_rep_status = escape(str(rep_status))
    safe_con_status = escape(str(con_status))

    safe_badge = escape(
        badge if badge else "Нет"
    )

    safe_last_post_text = escape(
        str(last_post_text)
    )

    text = (
        "📋 <b>ПРОФИЛЬ ИГРОКА</b>\n\n"

        f"🎭 <b>Персонаж:</b> {safe_character_name}\n"
        f"👤 <b>Telegram:</b> @{safe_username}\n"
        f"📝 <b>Анкета:</b> {anketa_text}\n\n"

        f"💪 <b>Сила (STR):</b> {player['str']} "
        f"(ур. {str_level}, {safe_str_status})\n"

        f"🌟 <b>Репутация (REP):</b> {player['rep']} "
        f"(ур. {rep_level}, {safe_rep_status})\n"

        f"🤝 <b>Связи (CON):</b> {player['con']} "
        f"(ур. {con_level}, {safe_con_status})\n"

        f"💰 <b>Кэш (MONEY):</b> {player['money']} монет\n\n"

        f"🎯 <b>Статус:</b> {safe_status}\n"
        f"🏷 <b>Пометка:</b> {safe_badge}\n\n"

        f"📅 <b>Последний пост:</b> "
        f"{safe_last_post_text}"
    )

    return text


async def show_profile(message, player):
    text = await build_profile_text(player)

    await message.answer(
        text,
        parse_mode="HTML"
    )


# ============================================================
# /START
# ============================================================

@dp.message(Command("start"))
async def cmd_start(
    message: Message,
    state: FSMContext
):
    user_id = message.from_user.id
    username = message.from_user.username

    player = await get_player(user_id)

    # Новый игрок — сначала спрашиваем имя персонажа.
    if not player:
        await state.set_state(
            RegistrationState.waiting_for_character_name
        )

        await message.answer(
            "👋 Добро пожаловать в город!\n\n"
            "🎭 Для начала регистрации напиши имя своего персонажа.\n\n"
            "Например:\n"
            "Виктор Кравцов\n\n"
            "Это имя будет отображаться в твоём профиле."
        )
        return

    # Если игрок уже был зарегистрирован старой версией бота,
    # но имени персонажа ещё нет — тоже просим его указать.
    if not player["character_name"]:
        await state.set_state(
            RegistrationState.waiting_for_character_name
        )

        await message.answer(
            "🎭 У тебя уже есть игровой профиль, но имя персонажа ещё не указано.\n\n"
            "Напиши имя персонажа, которое будет отображаться в профиле."
        )
        return

    await db_pool.execute(
        """
        UPDATE players
        SET
            username = $1,
            first_start_seen = TRUE
        WHERE user_id = $2
        """,
        clean_username(username),
        user_id
    )

    player = await get_player(user_id)
    await state.clear()

    await send_welcome_message(
        message,
        player,
        newly_registered=False
    )


async def send_welcome_message(
    message,
    player,
    newly_registered=False
):
    safe_rules_url = escape(
        str(RULES_URL),
        quote=True
    )

    if newly_registered:
        registration_text = (
            "🎉 <b>Ты зарегистрирован в игре!</b>\n\n"
            "Твои стартовые характеристики:\n\n"
            "💪 STR: 4\n"
            "🌟 REP: 4\n"
            "🤝 CON: 4\n"
            "💰 MONEY: 500\n"
        )
    else:
        registration_text = (
            "Ты уже зарегистрирован в игре.\n"
        )

    safe_character_name = escape(
        str(player["character_name"] or "Имя не указано")
    )

    await message.answer(
        f"""
👋 <b>Добро пожаловать в город!</b>

Я бот статистики текстовой ролевой игры.

{registration_text}

🎭 <b>Имя персонажа:</b> {safe_character_name}

Теперь твои игровые сообщения могут влиять на статистику.

📋 <b>Основные команды:</b>

/start — регистрация и инструкция
/profile — посмотреть свой профиль
/profile @username — посмотреть профиль игрока
/setanket ССЫЛКА — добавить анкету
/random str @username — случайное противостояние по STR
/random rep @username — по REP
/random con @username — по CON
/random money @username — по MONEY

📝 Для начисления статов бот анализирует сообщения
длиной от <b>300 символов</b> в игровых темах.

📚 <b>Правила игры:</b>
<a href="{safe_rules_url}">Открыть правила</a>

💡 После регистрации можешь сразу открыть:
<code>/profile</code>

Чтобы добавить анкету:
<code>/setanket https://t.me/...</code>

Приятной игры!
""",
        parse_mode="HTML"
    )


@dp.message(
    StateFilter(
        RegistrationState.waiting_for_character_name
    )
)
async def process_character_name(
    message: Message,
    state: FSMContext
):
    character_name = (message.text or "").strip()

    if not character_name:
        await message.answer(
            "❌ Я не вижу имени.\n\n"
            "Напиши имя персонажа обычным текстом."
        )
        return

    if len(character_name) < 2:
        await message.answer(
            "❌ Имя слишком короткое.\n\n"
            "Минимальная длина — 2 символа."
        )
        return

    if len(character_name) > 50:
        await message.answer(
            "❌ Имя слишком длинное.\n\n"
            "Максимальная длина — 50 символов."
        )
        return

    user_id = message.from_user.id
    username = message.from_user.username
    player = await get_player(user_id)

    if player:
        # Старый игрок без имени персонажа.
        await db_pool.execute(
            """
            UPDATE players
            SET
                character_name = $1,
                username = $2,
                first_start_seen = TRUE
            WHERE user_id = $3
            """,
            character_name,
            clean_username(username),
            user_id
        )

        player = await get_player(user_id)
        created = False
    else:
        player, created = await register_player(
            user_id=user_id,
            username=username,
            character_name=character_name,
            initial_stats=True
        )

    await state.clear()

    await send_welcome_message(
        message,
        player,
        newly_registered=created
    )


# ============================================================
# /PROFILE
# ============================================================

@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    args = message.text.split()

    # /profile @username
    if len(args) >= 2:
        username = clean_username(args[1])

        player = await get_player_by_username(
            username
        )

        if not player:
            await message.answer(
                "Игрок не найден."
            )
            return

        await show_profile(message, player)
        return

    user_id = message.from_user.id

    player = await get_player(user_id)

    if not player:
        await message.answer(
            "Вы ещё не зарегистрированы в игре.\n\n"
            "Напишите /start для регистрации."
        )
        return

    await show_profile(message, player)


# ============================================================
# /SETANKET
# ============================================================

@dp.message(Command("setanket"))
async def cmd_setanket(message: Message):
    user_id = message.from_user.id

    player = await get_player(user_id)

    if not player:
        await message.answer(
            "Анкета не найдена.\n\n"
            "Сначала зарегистрируйтесь в игре через /start."
        )
        return

    text = message.text or ""

    url_match = re.search(
        r"https?://\S+",
        text
    )

    if not url_match:
        await message.answer(
            "Анкета не найдена.\n\n"
            "Укажите ссылку на пост анкеты.\n"
            "Пример:\n"
            "/setanket https://t.me/..."
        )
        return

    url = url_match.group(0).rstrip(
        ".,!?)"
    )

    await db_pool.execute(
        """
        UPDATE players
        SET anketa_url = $1
        WHERE user_id = $2
        """,
        url,
        user_id
    )

    await message.answer(
        "✅ Анкета сохранена!"
    )

    # Добавляем событие о новой/обновлённой анкете.
    await add_event_to_buffer(
        message,
        event_type="Новая анкета",
        text=(
           f"Игрок @{clean_username(message.from_user.username) or 'без username'} добавил анкету."
        ),
        username=message.from_user.username,
        anketa_url=url
    )


# ============================================================
# /RANDOM
# ============================================================

@dp.message(Command("random"))
async def cmd_random(message: Message):
    args = message.text.split()

    if len(args) < 3:
        await message.answer(
            "Использование:\n"
            "/random str @username\n\n"
            "Доступные характеристики:\n"
            "str, rep, con, money"
        )
        return

    stat = args[1].lower()
    enemy_username = clean_username(args[2])

    stat_map = {
        "str": "str",
        "rep": "rep",
        "con": "con",
        "money": "money",
    }

    if stat not in stat_map:
        await message.answer(
            "Неверный стат.\n\n"
            "Используйте:\n"
            "str, rep, con, money"
        )
        return

    player = await get_player(
        message.from_user.id
    )

    if not player:
        await message.answer(
            "Вы ещё не зарегистрированы.\n"
            "Напишите /start."
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

    player_stat = player[stat_map[stat]]
    enemy_stat = enemy[stat_map[stat]]

    total = player_stat + enemy_stat

    if total <= 0:
        player_chance = 50
        enemy_chance = 50
    else:
        player_chance = (
            player_stat / total
        ) * 100

        enemy_chance = (
            enemy_stat / total
        ) * 100

    import random

    roll = random.uniform(0, 100)

    if roll <= player_chance:
        winner = f"Вы (@{player['username']})"
    else:
        winner = f"@{enemy['username']}"

    await message.answer(
        f"""
🎲 *РЕЗУЛЬТАТ*

Вы ({stat.upper()} {player_stat})
vs
@{enemy['username']} ({stat.upper()} {enemy_stat})

Ваш шанс: {player_chance:.1f}%
Шанс врага: {enemy_chance:.1f}%

🎯 Бросок: {roll:.1f}

🏆 Победитель: {winner}!
""",
        parse_mode="Markdown"
    )

# ============================================================
# /NEWS — СРОЧНАЯ НОВОСТЬ
# ============================================================

@dp.message(Command("news"))
async def cmd_news(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer(
            "Недостаточно прав для публикации новостей."
        )
        return

    await message.answer(
        "⏳ Собираю события за последний час "
        "и готовлю срочную новость..."
    )

    news_text, event_ids = (
        await generate_news_from_events(
            hours=1
        )
    )

    if not news_text:
        await message.answer(
            "Не удалось создать новость. "
            "Проверь логи Render."
        )
        return

    final_text = (
        "📰 СРОЧНЫЕ НОВОСТИ\n\n"
        + news_text
    )

    await bot.send_message(
        GROUP_ID_INT,
        final_text,
        message_thread_id=STORY_TOPIC_ID_INT
    )

    await mark_news_events_used(
        event_ids
    )

    await message.answer(
        "✅ Срочная новость опубликована "
        "в теме «Повествование»."
    )

    logger.info(
        "Срочная новость опубликована. "
        "Использовано событий: %s",
        len(event_ids)
    )

# ============================================================
# АДМИНСКИЕ КОМАНДЫ
# ============================================================

@dp.message(Command("setname"))
async def cmd_setname(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Недостаточно прав.")
        return

    args = (message.text or "").split(maxsplit=2)

    if len(args) < 3:
        await message.answer(
            "Использование:\n"
            "/setname @username Новое имя"
        )
        return

    username = clean_username(args[1])
    character_name = args[2].strip()

    if not username:
        await message.answer("❌ Не указан username игрока.")
        return

    if len(character_name) < 2:
        await message.answer("❌ Имя слишком короткое.")
        return

    if len(character_name) > 50:
        await message.answer(
            "❌ Имя слишком длинное. Максимум 50 символов."
        )
        return

    player = await get_player_by_username(username)

    if not player:
        await message.answer("❌ Игрок не найден.")
        return

    await db_pool.execute(
        """
        UPDATE players
        SET character_name = $1
        WHERE user_id = $2
        """,
        character_name,
        player["user_id"]
    )

    await message.answer(
        f"✅ Имя персонажа игрока @{escape(username)} изменено на:\n\n"
        f"🎭 {escape(character_name)}",
        parse_mode="HTML"
    )

    try:
        await bot.send_message(
            player["user_id"],
            f"🎭 Администратор изменил имя твоего персонажа на:\n\n"
            f"{escape(character_name)}",
            parse_mode="HTML"
        )
    except Exception:
        logger.exception(
            "Не удалось уведомить игрока %s о смене имени.",
            player["user_id"]
        )


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer(
            "Недостаточно прав."
        )
        return

    player_count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM players"
    )

    action_count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM actions"
    )

    await message.answer(
        f"""
📊 *СТАТИСТИКА БОТА*

Игроков: {player_count}
Действий: {action_count}
Новостей в буфере: {len(event_buffer)}
База: PostgreSQL
""",
        parse_mode="Markdown"
    )


@dp.message(Command("player"))
async def cmd_player(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer(
            "Недостаточно прав."
        )
        return

    args = message.text.split()

    if len(args) < 2:
        await message.answer(
            "Использование:\n"
            "/player @username"
        )
        return

    player = await get_player_by_username(
        args[1]
    )

    if not player:
        await message.answer(
            "Игрок не найден."
        )
        return

    await show_profile(message, player)


# ============================================================
# ОБРАБОТКА СООБЩЕНИЙ
# ============================================================

@dp.message()
async def handle_message(message: Message):
    try:
        # Нас интересует только наша группа.
        if message.chat.id != GROUP_ID_INT:
            return

        # В канале/группе без автора не работаем.
        if not message.from_user:
            return

        text = message.text or message.caption or ""

        if not text:
            return

        topic_id = get_topic_id(message)

        # ----------------------------------------------------
        # АДМИНКА
        # ----------------------------------------------------

        if topic_id == ADMIN_TOPIC_ID_INT:
            return

        # ----------------------------------------------------
        # ИНФОРМАЦИОННАЯ ТЕМА
        # ----------------------------------------------------

        if topic_id == INFO_TOPIC_ID_INT:
            return

        # ----------------------------------------------------
        # ПОВЕСТВОВАНИЕ
        # ----------------------------------------------------

        if topic_id == STORY_TOPIC_ID_INT:
            return

        # ----------------------------------------------------
        # ИГРОВЫЕ ТЕМЫ
        # ----------------------------------------------------

        if topic_id in {
            GAME_TOPIC_ID_INT,
            SEMI_RP_TOPIC_ID_INT
        }:

            # Статы считаются только из сообщений
            # длиной от 300 символов.
            if len(text) >= MIN_TEXT_FOR_STATS:
                await update_stats(
                    user_id=message.from_user.id,
                    username=message.from_user.username,
                    text=text,
                    message_id=message.message_id,
                    topic_id=topic_id
                )

            # А новости могут использовать и короткие сообщения.
            await add_event_to_buffer(
                message,
                event_type="Игровое событие",
                text=text,
                username=message.from_user.username
            )

            return

        # ----------------------------------------------------
        # ФЛУД
        # ----------------------------------------------------

        if topic_id == FLOOD_TOPIC_ID_INT:

            # Во флуде статы НЕ начисляются.

            # Но сообщения могут использоваться
            # как материал для городской хроники.
            await add_event_to_buffer(
                message,
                event_type="Флуд",
                text=text,
                username=message.from_user.username
            )

            return

        # ----------------------------------------------------
        # АНКЕТЫ
        # ----------------------------------------------------

        if topic_id == PROFILES_TOPIC_ID_INT:

            await handle_profile_post(
                message,
                text
            )

            return

    except Exception:
        logger.exception(
            "Ошибка обработки сообщения %s",
            message.message_id
        )


# ============================================================
# ОБРАБОТКА ПОСТА АНКЕТЫ
# ============================================================

async def handle_profile_post(
    message,
    text
):
    user_id = message.from_user.id
    username = message.from_user.username

    player = await get_player(user_id)

    # Анкета не регистрирует игрока автоматически.
    # Сначала игрок должен пройти /start и указать имя персонажа.
    if not player:
        logger.info(
            "Анкета пользователя %s получена, но игрок ещё не зарегистрирован.",
            user_id
        )
        return

    # Ссылка на конкретный Telegram-пост.
    if message.chat.type in {
        "group",
        "supergroup"
    }:
        # Для private supergroup корректная ссылка
        # формируется через /c/ + internal chat id.
        internal_chat_id = str(
            abs(message.chat.id)
        )

        if internal_chat_id.startswith("100"):
            internal_chat_id = internal_chat_id[3:]

        anketa_url = (
            f"https://t.me/c/"
            f"{internal_chat_id}/"
            f"{message.message_id}"
        )
    else:
        anketa_url = None

    await db_pool.execute(
        """
        UPDATE players
        SET anketa_url = $1
        WHERE user_id = $2
        """,
        anketa_url,
        user_id
    )

    await add_event_to_buffer(
        message,
        event_type="Новая анкета",
        text=(
            "В теме анкет появился новый персонаж. "
            f"Текст анкеты: {text[:1800]}"
        ),
        username=username,
        anketa_url=anketa_url
    )

    logger.info(
        "Обработана анкета пользователя %s",
        user_id
    )


# ============================================================
# НЕАКТИВНОСТЬ
# ============================================================

async def check_inactivity():
    while True:
        try:
            await asyncio.sleep(
                24 * 60 * 60
            )

            cutoff = now_utc() - timedelta(
                days=INACTIVITY_DAYS
            )

            inactive_players = await db_pool.fetch(
                """
                SELECT *
                FROM players
                WHERE
                    last_post IS NOT NULL
                    AND last_post < $1
                    AND status != 'Читатель'
                """,
                cutoff
            )

            for player in inactive_players:

                await db_pool.execute(
                    """
                    UPDATE players
                    SET
                        status = 'Читатель',
                        str = 1,
                        rep = 1,
                        con = 1,
                        money = 100
                    WHERE user_id = $1
                    """,
                    player["user_id"]
                )

                try:
                    await bot.send_message(
                        player["user_id"],
                        """
⚠️ Ты долго не писал в игровых темах.

Твой статус изменён на «Читатель».

Статы возвращены к базовым:

💪 STR: 1
🌟 REP: 1
🤝 CON: 1
💰 MONEY: 100

Чтобы вернуться в игру, просто начни снова писать
в игровых темах.
После нового игрового сообщения статус автоматически
изменится обратно на «Игрок».
"""
                    )

                except Exception:
                    logger.exception(
                        "Не удалось отправить уведомление "
                        "неактивному игроку %s",
                        player["user_id"]
                    )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Ошибка проверки неактивности."
            )


# ============================================================
# ТОП ИГРОКОВ
# ============================================================

async def publish_top():
    while True:
        try:
            await asyncio.sleep(
                7 * 24 * 60 * 60
            )

            top = await db_pool.fetch(
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
                ORDER BY rep DESC, str DESC
                LIMIT 10
                """
            )

            if not top:
                continue

            text = "🏆 *ТОП ИГРОКОВ*\n\n"

            for i, player in enumerate(top, 1):
                badge = get_badge(
                    player["bad_boy_count"],
                    player["good_boy_count"]
                )

                text += (
                    f"{i}. @{player['username'] or 'unknown'} "
                    f"— REP: {player['rep']}, "
                    f"STR: {player['str']}, "
                    f"CON: {player['con']}, "
                    f"MONEY: {player['money']}"
                )

                if badge:
                    text += f" — {badge}"

                text += "\n"

            await bot.send_message(
                GROUP_ID_INT,
                text,
                message_thread_id=STORY_TOPIC_ID_INT,
                parse_mode="Markdown"
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Ошибка публикации топа."
            )


# ============================================================
# ЗАПУСК
# ============================================================
async def health_handler(request):
    return web.Response(text="Bot is alive")


async def start_web_server():
    app = web.Application()

    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        port
    )

    await site.start()

    logger.info(
        "HTTP-сервер для Render запущен на порту %s",
        port
    )
    
async def main():
    logger.info("Запуск бота...")

    await create_database_pool()

    try:
        await init_database()

        await start_web_server()
        
        logger.info(
            "Бот подключён к группе %s.",
            GROUP_ID_INT
        )

        tasks = [
            asyncio.create_task(
                generate_events_loop()
            ),
            asyncio.create_task(
                check_inactivity()
            ),
            asyncio.create_task(
                publish_top()
            ),
        ]

        try:
            await dp.start_polling(bot)

        finally:
            for task in tasks:
                task.cancel()

            await asyncio.gather(
                *tasks,
                return_exceptions=True
            )

    finally:
        await close_database_pool()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
