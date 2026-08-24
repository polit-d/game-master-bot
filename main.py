import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("galo_bot")

MSK = ZoneInfo("Europe/Moscow")
UTC = timezone.utc


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name) or default


def env_int(name: str, default: int = 0) -> int:
    value = env(name)
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


BOT_TOKEN = env("BOTTOKEN", env("BOT_TOKEN"))
DATABASE_URL = env("DATABASEURL", env("DATABASE_URL"))
GROQ_API_KEY = env("GROQAPIKEY", env("GROQ_API_KEY"))
GROQ_MODEL = env("GROQMODEL", "llama-3.3-70b-versatile")
GROQ_STRUCTURED_MODEL = env("GROQSTRUCTUREDMODEL", "openai/gpt-oss-20b")

GROUP_ID = env_int("GROUPID")
GAME_TOPIC_ID = env_int("GAMETOPICID")
SEMI_TOPIC_ID = env_int("SEMIRPTOPICID")
INFO_TOPIC_ID = env_int("INFOTOPICID")
ADMIN_TOPIC_ID = env_int("ADMINTOPICID")
PROFILES_TOPIC_ID = env_int("PROFILESTOPICID")
FLOOD_TOPIC_ID = env_int("FLOODTOPICID")
STORY_TOPIC_ID = env_int("STORYTOPICID")
RULES_URL = env("RULESURL", "")
PORT = env_int("PORT", 10000)

ADMIN_IDS = {
    int(item.strip())
    for item in (env("ADMINIDS", "") or "").split(",")
    if item.strip().lstrip("-").isdigit()
}

NEWS_INTERVAL_HOURS = 6
INACTIVITY_DAYS = 30
MIN_TEXT_FOR_STATS = 300
MAX_EVENT_TEXT = 4000
WEEK_STR_CAP = 3
WEEK_REP_CAP = 7
WEEK_CON_CAP = 8
WEEK_CASH_CAP = 1000

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOTTOKEN или BOT_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("Не задан DATABASEURL или DATABASE_URL")
if not GROQ_API_KEY:
    raise RuntimeError("Не задан GROQAPIKEY или GROQ_API_KEY")
if not GROUP_ID:
    raise RuntimeError("Не задан GROUPID")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
db_pool: asyncpg.Pool | None = None


class RegistrationState(StatesGroup):
    waiting_for_character_name = State()


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_msk() -> datetime:
    return datetime.now(MSK)


def clean_username(username: str | None) -> str | None:
    if not username:
        return None
    value = username.strip().lstrip("@").strip()
    return value or None


def display_name(player: Any) -> str:
    character = str(player["charactername"] or "").strip()
    username = clean_username(player["username"])
    if character and username:
        return f"{character} (@{username})"
    if character:
        return character
    if username:
        return f"@{username}"
    return str(player["userid"])


def profile_link(username: str | None) -> str | None:
    username = clean_username(username)
    return f"https://t.me/{username}" if username else None


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def topic_id(message: Message) -> int:
    return safe_int(message.message_thread_id, 0)


def topic_name(value: int) -> str:
    names = {
        GAME_TOPIC_ID: "игровой",
        SEMI_TOPIC_ID: "полуигровой",
        INFO_TOPIC_ID: "информационный",
        ADMIN_TOPIC_ID: "админский",
        PROFILES_TOPIC_ID: "профили",
        FLOOD_TOPIC_ID: "флуд",
        STORY_TOPIC_ID: "сюжет",
    }
    return names.get(value, f"тема {value}")


def is_admin(user_id: int | None) -> bool:
    return bool(user_id in ADMIN_IDS)


def is_event_topic(value: int) -> bool:
    return value in {GAME_TOPIC_ID, SEMI_TOPIC_ID, PROFILES_TOPIC_ID}


def is_ignored_topic(value: int) -> bool:
    return value in {INFO_TOPIC_ID, ADMIN_TOPIC_ID, FLOOD_TOPIC_ID, STORY_TOPIC_ID}


STORY_TAG_RE = re.compile(r"(?<!\w)#([A-Za-zА-Яа-яЁё0-9_-]+)")


def extract_story_tag(text: str | None) -> str | None:
    if not text:
        return None
    match = STORY_TAG_RE.search(text)
    return match.group(1).lower() if match else None


def rget(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


async def create_database_pool() -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=60,
    )
    logger.info("PostgreSQL pool created")


async def close_database_pool() -> None:
    global db_pool
    if db_pool:
        await db_pool.close()
        db_pool = None
        logger.info("PostgreSQL pool closed")


async def init_database() -> None:
    assert db_pool is not None
    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            userid BIGINT PRIMARY KEY,
            username TEXT,
            charactername TEXT,
            str INTEGER NOT NULL DEFAULT 1,
            rep INTEGER NOT NULL DEFAULT 1,
            con INTEGER NOT NULL DEFAULT 1,
            money INTEGER NOT NULL DEFAULT 100,
            lastpost TIMESTAMPTZ,
            anketaurl TEXT,
            status TEXT NOT NULL DEFAULT '',
            badboycount INTEGER NOT NULL DEFAULT 0,
            goodboycount INTEGER NOT NULL DEFAULT 0,
            strweeklimit INTEGER NOT NULL DEFAULT 0,
            repweeklimit INTEGER NOT NULL DEFAULT 0,
            conweeklimit INTEGER NOT NULL DEFAULT 0,
            moneyweeklimit INTEGER NOT NULL DEFAULT 0,
            weekreset TIMESTAMPTZ,
            firststartseen BOOLEAN NOT NULL DEFAULT FALSE,
            cash INTEGER,
            activitystatus TEXT NOT NULL DEFAULT 'reader',
            cashstatus TEXT NOT NULL DEFAULT 'normal',
            repgame INTEGER NOT NULL DEFAULT 0,
            repcommunity INTEGER NOT NULL DEFAULT 0,
            admintitle TEXT,
            businessname TEXT,
            businesssalary INTEGER NOT NULL DEFAULT 0,
            physicalpotential INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    migrations = [
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS charactername TEXT",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS cash INTEGER",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS activitystatus TEXT NOT NULL DEFAULT 'reader'",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS cashstatus TEXT NOT NULL DEFAULT 'normal'",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS repgame INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS repcommunity INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS admintitle TEXT",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS businessname TEXT",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS businesssalary INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS physicalpotential INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE players ALTER COLUMN cash SET DEFAULT 100",
        "UPDATE players SET cash = COALESCE(cash, money, 100) WHERE cash IS NULL",
    ]
    for statement in migrations:
        await db_pool.execute(statement)

    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS actions (
            id BIGSERIAL PRIMARY KEY,
            userid BIGINT,
            actiontype TEXT NOT NULL,
            actionvalue INTEGER NOT NULL DEFAULT 0,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            telegrammessageid BIGINT UNIQUE,
            topicid INTEGER
        )
        """
    )
    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS processedmessages (
            messageid BIGINT PRIMARY KEY,
            processedat TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS newsevents (
            id BIGSERIAL PRIMARY KEY,
            sourcechatid BIGINT NOT NULL,
            telegrammessageid BIGINT NOT NULL,
            userid BIGINT,
            charactername TEXT,
            username TEXT,
            eventtype TEXT NOT NULL DEFAULT '',
            text TEXT NOT NULL,
            topicid INTEGER,
            topicname TEXT,
            anketaurl TEXT,
            storytag TEXT,
            createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            usedinnewsat TIMESTAMPTZ,
            statsprocessedat TIMESTAMPTZ
        )
        """
    )
    await db_pool.execute("ALTER TABLE newsevents ADD COLUMN IF NOT EXISTS storytag TEXT")
    await db_pool.execute("ALTER TABLE newsevents ADD COLUMN IF NOT EXISTS statsprocessedat TIMESTAMPTZ")
    await db_pool.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS newsevents_source_message_idx
        ON newsevents(sourcechatid, telegrammessageid)
        """
    )
    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_progress (
            id BIGSERIAL PRIMARY KEY,
            userid BIGINT NOT NULL REFERENCES players(userid) ON DELETE CASCADE,
            progressdate DATE NOT NULL,
            posts_count INTEGER NOT NULL DEFAULT 0,
            text_chars INTEGER NOT NULL DEFAULT 0,
            str_delta INTEGER NOT NULL DEFAULT 0,
            rep_delta INTEGER NOT NULL DEFAULT 0,
            con_delta INTEGER NOT NULL DEFAULT 0,
            cash_delta INTEGER NOT NULL DEFAULT 0,
            goodboy_delta INTEGER NOT NULL DEFAULT 0,
            badboy_delta INTEGER NOT NULL DEFAULT 0,
            material TEXT,
            processedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(userid, progressdate)
        )
        """
    )
    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS economy_ledger (
            id BIGSERIAL PRIMARY KEY,
            userid BIGINT NOT NULL REFERENCES players(userid) ON DELETE CASCADE,
            amount INTEGER NOT NULL,
            reason TEXT NOT NULL,
            progressdate DATE,
            createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS player_achievements (
            userid BIGINT NOT NULL REFERENCES players(userid) ON DELETE CASCADE,
            achievement TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 0,
            updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(userid, achievement)
        )
        """
    )
    await db_pool.execute(
        """
        CREATE TABLE IF NOT EXISTS story_threads (
            tag TEXT PRIMARY KEY,
            first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            event_count INTEGER NOT NULL DEFAULT 0,
            archived BOOLEAN NOT NULL DEFAULT FALSE
        )
        """
    )
    logger.info("Database schema is ready")


async def get_player(user_id: int):
    assert db_pool is not None
    return await db_pool.fetchrow("SELECT * FROM players WHERE userid = $1", user_id)


async def get_player_by_username(username: str | None):
    assert db_pool is not None
    username = clean_username(username)
    if not username:
        return None
    return await db_pool.fetchrow(
        "SELECT * FROM players WHERE LOWER(username) = LOWER($1) LIMIT 1",
        username,
    )


async def register_player(
    user_id: int,
    username: str | None,
    character_name: str,
    initial_stats: bool = True,
):
    assert db_pool is not None
    existing = await get_player(user_id)
    if existing:
        return existing, False

    initial = (4, 4, 4, 500) if initial_stats else (1, 1, 1, 100)
    return await db_pool.fetchrow(
        """
        INSERT INTO players(
            userid, username, charactername, str, rep, con, money, cash,
            lastpost, status, weekreset, firststartseen, activitystatus
        )
        VALUES($1, $2, $3, $4, $5, $6, $7, $7, $8, '', $8, TRUE, 'active')
        RETURNING *
        """,
        user_id,
        clean_username(username),
        character_name,
        initial[0],
        initial[1],
        initial[2],
        initial[3],
        now_utc(),
    ), True


async def ensure_player(user_id: int, username: str | None):
    player = await get_player(user_id)
    if not player:
        return None, False
    current = clean_username(username)
    if current and current != clean_username(rget(player, "username")):
        assert db_pool is not None
        player = await db_pool.fetchrow(
            "UPDATE players SET username = $1 WHERE userid = $2 RETURNING *",
            current,
            user_id,
        )
    return player, False


async def reset_week_if_needed(player):
    assert db_pool is not None
    reset = rget(player, "weekreset")
    if reset is None or now_utc() - reset >= timedelta(days=7):
        return await db_pool.fetchrow(
            """
            UPDATE players
            SET strweeklimit = 0, repweeklimit = 0, conweeklimit = 0,
                moneyweeklimit = 0, weekreset = $1
            WHERE userid = $2
            RETURNING *
            """,
            now_utc(),
            player["userid"],
        )
    return player


async def groq_request(messages: list[dict[str, str]], model: str, temperature: float):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"model": model, "messages": messages, "temperature": temperature}
    timeout = aiohttp.ClientTimeout(total=90)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as response:
                raw = await response.text()
                if response.status != 200:
                    logger.error("Groq error %s: %s", response.status, raw[:1000])
                    return None
                return json.loads(raw)
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
        logger.error("Groq request failed: %s", exc)
        return None
    except Exception:
        logger.exception("Unexpected Groq error")
        return None


def extract_json(text: str | None) -> dict | None:
    if not text:
        return None
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.I)
    try:
        value = json.loads(cleaned)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, flags=re.S)
        if not match:
            return None
        try:
            value = json.loads(match.group(0))
            return value if isinstance(value, dict) else None
        except json.JSONDecodeError:
            return None


async def analyze_daily_material(material: str) -> dict[str, int]:
    system = (
        "Ты анализируешь дневной игровой материал на русском языке. "
        "Верни только JSON без markdown: "
        '{"str":0,"rep":0,"con":0,"cash":0,"goodboy":0,"badboy":0}. '
        "STR, REP и CON могут быть только от 0 до 2. CASH — от 0 до 10. "
        "goodboy и badboy — только 0 или 1. Не начисляй статы за пустой флуд."
    )
    prompt = "Дневной материал игроков:\n\n" + material[:20000]
    result = await groq_request(
        [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        GROQ_STRUCTURED_MODEL,
        0.1,
    )
    if not result:
        return {"str": 0, "rep": 0, "con": 0, "cash": 0, "goodboy": 0, "badboy": 0}
    try:
        content = result["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return {"str": 0, "rep": 0, "con": 0, "cash": 0, "goodboy": 0, "badboy": 0}
    parsed = extract_json(content) or {}
    return {
        "str": max(0, min(2, safe_int(parsed.get("str")))),
        "rep": max(0, min(2, safe_int(parsed.get("rep")))),
        "con": max(0, min(2, safe_int(parsed.get("con")))),
        "cash": max(0, min(10, safe_int(parsed.get("cash")))),
        "goodboy": max(0, min(1, safe_int(parsed.get("goodboy")))),
        "badboy": max(0, min(1, safe_int(parsed.get("badboy")))),
    }


async def add_event(message: Message, event_type: str, text: str, anketa_url: str | None = None):
    assert db_pool is not None
    text = (text or "").strip()
    if not text:
        return
    user_id = message.from_user.id if message.from_user else None
    player = await get_player(user_id) if user_id else None
    username = clean_username(message.from_user.username if message.from_user else None)
    character = rget(player, "charactername") if player else None
    if not username and player:
        username = clean_username(rget(player, "username"))
    tag = extract_story_tag(text)
    topic = topic_id(message)
    await db_pool.execute(
        """
        INSERT INTO newsevents(
            sourcechatid, telegrammessageid, userid, charactername, username,
            eventtype, text, topicid, topicname, anketaurl, storytag
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT(sourcechatid, telegrammessageid) DO NOTHING
        """,
        message.chat.id,
        message.message_id,
        user_id,
        character,
        username,
        event_type,
        text[:MAX_EVENT_TEXT],
        topic,
        topic_name(topic),
        anketa_url,
        tag,
    )
    if tag:
        await db_pool.execute(
            """
            INSERT INTO story_threads(tag, last_seen, event_count)
            VALUES($1, NOW(), 1)
            ON CONFLICT(tag) DO UPDATE SET
                last_seen = NOW(), event_count = story_threads.event_count + 1,
                archived = FALSE
            """,
            tag,
        )


async def mark_player_active(user_id: int):
    assert db_pool is not None
    await db_pool.execute(
        """
        UPDATE players
        SET lastpost = $1, activitystatus = 'active', status =
            CASE WHEN status = 'Нищий' THEN status ELSE '' END
        WHERE userid = $2
        """,
        now_utc(),
        user_id,
    )


async def update_stats_immediate(message: Message, text: str):
    if len(text) < MIN_TEXT_FOR_STATS:
        return
    assert db_pool is not None
    user_id = message.from_user.id
    player = await get_player(user_id)
    if not player:
        return
    inserted = await db_pool.fetchval(
        """
        INSERT INTO processedmessages(messageid)
        VALUES($1) ON CONFLICT(messageid) DO NOTHING
        RETURNING messageid
        """,
        message.message_id,
    )
    if inserted is None:
        return

    result = await analyze_daily_material(text)
    player = await reset_week_if_needed(player)
    str_add = min(result["str"], max(0, WEEK_STR_CAP - safe_int(player["strweeklimit"])))
    rep_add = min(result["rep"], max(0, WEEK_REP_CAP - safe_int(player["repweeklimit"])))
    con_add = min(result["con"], max(0, WEEK_CON_CAP - safe_int(player["conweeklimit"])))
    cash_add = min(result["cash"], max(0, WEEK_CASH_CAP - safe_int(player["moneyweeklimit"])))
    await db_pool.execute(
        """
        UPDATE players SET
            str = str + $1, rep = rep + $2, con = con + $3,
            money = money + $4, cash = COALESCE(cash, money) + $4,
            strweeklimit = strweeklimit + $1,
            repweeklimit = repweeklimit + $2,
            conweeklimit = conweeklimit + $3,
            moneyweeklimit = moneyweeklimit + $4,
            badboycount = badboycount + $5,
            goodboycount = goodboycount + $6,
            lastpost = $7, activitystatus = 'active'
        WHERE userid = $8
        """,
        str_add,
        rep_add,
        con_add,
        cash_add,
        result["badboy"],
        result["goodboy"],
        now_utc(),
        user_id,
    )
    for action, value in (("str", str_add), ("rep", rep_add), ("con", con_add), ("cash", cash_add)):
        if value:
            await db_pool.execute(
                """
                INSERT INTO actions(userid, actiontype, actionvalue, telegrammessageid, topicid)
                VALUES($1,$2,$3,$4,$5)
                ON CONFLICT(telegrammessageid) DO NOTHING
                """,
                user_id,
                action,
                value,
                message.message_id,
                topic_id(message),
            )


async def process_daily_day(progress_date: date):
    assert db_pool is not None
    start = datetime.combine(progress_date, datetime.min.time(), tzinfo=MSK).astimezone(UTC)
    end = start + timedelta(days=1)
    rows = await db_pool.fetch(
        """
        SELECT * FROM newsevents
        WHERE createdat >= $1 AND createdat < $2
          AND statsprocessedat IS NULL
          AND userid IS NOT NULL
          AND (topicid IS NULL OR topicid <> $3)
        ORDER BY userid, createdat
        """,
        start,
        end,
        FLOOD_TOPIC_ID,
    )
    grouped: dict[int, list[Any]] = {}
    for row in rows:
        grouped.setdefault(safe_int(row["userid"]), []).append(row)

    for user_id, events in grouped.items():
        material_parts = []
        total_chars = 0
        for event in events:
            tag = f"#{event['storytag']}" if event["storytag"] else "без тега"
            material_parts.append(
                f"[{tag}] {event['topicname'] or ''}: "
                f"{event['text'][:3000]}"
            )
            total_chars += len(event["text"] or "")
        material = "\n\n".join(material_parts)
        result = await analyze_daily_material(material) if total_chars >= MIN_TEXT_FOR_STATS else {
            "str": 0, "rep": 0, "con": 0, "cash": 0, "goodboy": 0, "badboy": 0
        }
        player = await get_player(user_id)
        if not player:
            continue
        player = await reset_week_if_needed(player)
        str_add = min(result["str"], max(0, WEEK_STR_CAP - safe_int(player["strweeklimit"])))
        rep_add = min(result["rep"], max(0, WEEK_REP_CAP - safe_int(player["repweeklimit"])))
        con_add = min(result["con"], max(0, WEEK_CON_CAP - safe_int(player["conweeklimit"])))
        ai_cash = min(result["cash"], max(0, WEEK_CASH_CAP - safe_int(player["moneyweeklimit"])))
        daily_expense = -5
        active_reward = 10
        cash_delta = daily_expense + active_reward + ai_cash
        await db_pool.execute(
            """
            INSERT INTO daily_progress(
                userid, progressdate, posts_count, text_chars, str_delta,
                rep_delta, con_delta, cash_delta, goodboy_delta, badboy_delta, material
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT(userid, progressdate) DO NOTHING
            """,
            user_id,
            progress_date,
            len(events),
            total_chars,
            str_add,
            rep_add,
            con_add,
            cash_delta,
            result["goodboy"],
            result["badboy"],
            material[:20000],
        )
        await db_pool.execute(
            """
            UPDATE players SET
                str = str + $1, rep = rep + $2, con = con + $3,
                money = money + $4, cash = COALESCE(cash, money) + $4,
                strweeklimit = strweeklimit + $1,
                repweeklimit = repweeklimit + $2,
                conweeklimit = conweeklimit + $3,
                moneyweeklimit = moneyweeklimit + $4,
                goodboycount = goodboycount + $5,
                badboycount = badboycount + $6,
                activitystatus = 'active', lastpost = $7,
                cashstatus = CASE WHEN COALESCE(cash, money) + $4 <= 0 THEN 'Нищий' ELSE 'normal' END,
                status = CASE WHEN COALESCE(cash, money) + $4 <= 0 THEN 'Нищий' ELSE status END
            WHERE userid = $8
            """,
            str_add,
            rep_add,
            con_add,
            cash_delta,
            result["goodboy"],
            result["badboy"],
            now_utc(),
            user_id,
        )
        await db_pool.execute(
            "INSERT INTO economy_ledger(userid, amount, reason, progressdate) VALUES($1,$2,$3,$4)",
            user_id, daily_expense, "ежедневные расходы активного игрока", progress_date,
        )
        await db_pool.execute(
            "INSERT INTO economy_ledger(userid, amount, reason, progressdate) VALUES($1,$2,$3,$4)",
            user_id, active_reward + ai_cash, "активный день и результат анализа", progress_date,
        )
        await db_pool.execute(
            """
            INSERT INTO player_achievements(userid, achievement, count)
            VALUES($1,'GoodBoy',$2)
            ON CONFLICT(userid, achievement) DO UPDATE SET
                count = player_achievements.count + EXCLUDED.count, updatedat = NOW()
            """,
            user_id, result["goodboy"],
        )
        await db_pool.execute(
            """
            INSERT INTO player_achievements(userid, achievement, count)
            VALUES($1,'BadBoy',$2)
            ON CONFLICT(userid, achievement) DO UPDATE SET
                count = player_achievements.count + EXCLUDED.count, updatedat = NOW()
            """,
            user_id, result["badboy"],
        )
        ids = [event["id"] for event in events]
        await db_pool.execute(
            "UPDATE newsevents SET statsprocessedat = NOW() WHERE id = ANY($1::bigint[])",
            ids,
        )

    readers = await db_pool.fetch(
        """
        SELECT p.userid FROM players p
        WHERE NOT EXISTS(
            SELECT 1 FROM daily_progress d
            WHERE d.userid = p.userid AND d.progressdate = $1
        )
        """,
        progress_date,
    )
    for reader in readers:
        user_id = reader["userid"]
        await db_pool.execute(
            """
            INSERT INTO daily_progress(userid, progressdate, cash_delta)
            VALUES($1,$2,-2)
            ON CONFLICT(userid, progressdate) DO NOTHING
            """,
            user_id, progress_date,
        )
        await db_pool.execute(
            """
            UPDATE players SET
                cash = COALESCE(cash, money) - 2,
                money = money - 2,
                activitystatus = 'reader',
                cashstatus = CASE WHEN COALESCE(cash, money) - 2 <= 0 THEN 'Нищий' ELSE cashstatus END,
                status = CASE WHEN COALESCE(cash, money) - 2 <= 0 THEN 'Нищий' ELSE status END
            WHERE userid = $1
            """,
            user_id,
        )
        await db_pool.execute(
            "INSERT INTO economy_ledger(userid, amount, reason, progressdate) VALUES($1,-2,'расходы читателя',$2)",
            user_id, progress_date,
        )
    logger.info("Daily progress processed for %s", progress_date)


async def daily_loop():
    while True:
        try:
            current = now_msk()
            next_run = current.replace(hour=5, minute=0, second=0, microsecond=0)
            if current >= next_run:
                next_run += timedelta(days=1)
            await asyncio.sleep(max(1, (next_run - current).total_seconds()))
            await process_daily_day((next_run - timedelta(days=1)).date())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Daily loop failed")
            await asyncio.sleep(60)


async def generate_news(catchup: bool = False):
    assert db_pool is not None
    if catchup:
        events = await db_pool.fetch(
            """
            SELECT * FROM newsevents
            WHERE usedinnewsat IS NULL
              AND (topicid IS NULL OR topicid <> $1)
            ORDER BY createdat ASC LIMIT 300
            """,
            FLOOD_TOPIC_ID,
        )
    else:
        events = await db_pool.fetch(
            """
            SELECT * FROM newsevents
            WHERE usedinnewsat IS NULL
              AND createdat >= $1
              AND (topicid IS NULL OR topicid <> $2)
            ORDER BY createdat ASC LIMIT 300
            """,
            now_utc() - timedelta(hours=NEWS_INTERVAL_HOURS),
            FLOOD_TOPIC_ID,
        )
    if not events:
        return None
    blocks: dict[str, list[str]] = {}
    for event in events:
        tag = event["storytag"] or "без тега"
        author = display_name({"charactername": event["charactername"], "username": event["username"], "userid": event["userid"]})
        blocks.setdefault(tag, []).append(
            f"{author} | {event['topicname'] or ''} | {event['text'][:2500]}"
        )
    source = "\n\n".join(
        f"#{tag}\n" + "\n".join(items)
        for tag, items in blocks.items()
    )
    prompt = (
        "Составь короткую живую новость для Telegram по игровым событиям. "
        "Не выдумывай факты, сохрани имена и теги, не используй markdown-заголовки. "
        "Пиши на русском, 3–8 абзацев.\n\n" + source[:30000]
    )
    result = await groq_request(
        [
            {"role": "system", "content": "Ты редактор новостей ролевого города."},
            {"role": "user", "content": prompt},
        ],
        GROQ_MODEL,
        0.7,
    )
    if not result:
        return None
    try:
        text = result["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError):
        return None
    return text, [event["id"] for event in events]


async def news_loop():
    await asyncio.sleep(30)
    while True:
        try:
            result = await generate_news(catchup=True)
            if result:
                text, ids = result
                await bot.send_message(GROUP_ID, text, message_thread_id=STORY_TOPIC_ID or None)
                await db_pool.execute("UPDATE newsevents SET usedinnewsat = NOW() WHERE id = ANY($1::bigint[])", ids)
            await asyncio.sleep(NEWS_INTERVAL_HOURS * 3600)
            result = await generate_news()
            if result:
                text, ids = result
                await bot.send_message(GROUP_ID, text, message_thread_id=STORY_TOPIC_ID or None)
                await db_pool.execute("UPDATE newsevents SET usedinnewsat = NOW() WHERE id = ANY($1::bigint[])", ids)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("News loop failed")
            await asyncio.sleep(60)


async def inactivity_loop():
    while True:
        try:
            await asyncio.sleep(24 * 3600)
            cutoff = now_utc() - timedelta(days=INACTIVITY_DAYS)
            rows = await db_pool.fetch(
                "SELECT * FROM players WHERE lastpost IS NOT NULL AND lastpost < $1 AND activitystatus <> 'inactive'",
                cutoff,
            )
            for player in rows:
                await db_pool.execute(
                    """
                    UPDATE players SET activitystatus='inactive', status='Неактивен',
                        str=1, rep=1, con=1, money=100, cash=100
                    WHERE userid=$1
                    """,
                    player["userid"],
                )
                try:
                    await bot.send_message(
                        player["userid"],
                        "Ты не появлялся в игре 30 дней. Статы сброшены до базовых значений.",
                    )
                except Exception:
                    logger.info("Could not notify inactive player %s", player["userid"])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Inactivity loop failed")
            await asyncio.sleep(60)


async def top_loop():
    while True:
        try:
            await asyncio.sleep(7 * 24 * 3600)
            rows = await db_pool.fetch(
                """
                SELECT * FROM players
                ORDER BY rep DESC, str DESC, con DESC, COALESCE(cash, money) DESC
                LIMIT 10
                """
            )
            if not rows:
                continue
            lines = ["🏆 <b>Топ игроков недели</b>", ""]
            for index, player in enumerate(rows, 1):
                lines.append(
                    f"{index}. {escape(display_name(player))} — "
                    f"REP {player['rep']} · STR {player['str']} · "
                    f"CON {player['con']} · CASH {rget(player, 'cash', player['money'])}"
                )
            await bot.send_message(GROUP_ID, "\n".join(lines), parse_mode="HTML", message_thread_id=STORY_TOPIC_ID or None)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Top loop failed")
            await asyncio.sleep(60)


def level_status(value: int) -> tuple[int, str]:
    if value >= 15:
        return 5, "легендарный"
    if value >= 11:
        return 4, "высокий"
    if value >= 7:
        return 3, "уверенный"
    if value >= 3:
        return 2, "развивающийся"
    return 1, "начальный"


def badge(player: Any) -> str:
    bad = safe_int(rget(player, "badboycount"))
    good = safe_int(rget(player, "goodboycount"))
    if bad >= good + 5:
        return "😈 BadBoy"
    if good >= bad + 5:
        return "😇 GoodBoy"
    return ""


async def build_profile_text(player: Any) -> str:
    str_level, str_status = level_status(safe_int(player["str"]))
    rep_level, rep_status = level_status(safe_int(player["rep"]))
    con_level, con_status = level_status(safe_int(player["con"]))
    username = clean_username(rget(player, "username"))
    character = str(rget(player, "charactername") or "Без имени")
    cash = safe_int(rget(player, "cash", rget(player, "money", 0)))
    anketa = rget(player, "anketaurl")
    name_line = escape(character)
    if username:
        name_line += f" (@{escape(username)})"
    lines = [
        f"👤 <b>{name_line}</b>",
        "",
        f"💪 STR: {player['str']} — {str_status} ({str_level})",
        f"🤝 REP: {player['rep']} — {rep_status} ({rep_level})",
        f"🫀 CON: {player['con']} — {con_status} ({con_level})",
        f"💰 CASH: {cash}",
        f"📊 Статус активности: {escape(str(rget(player, 'activitystatus', 'reader')))}",
        f"💳 Денежный статус: {escape(str(rget(player, 'cashstatus', 'normal')))}",
        f"🎭 Игровая репутация: {rget(player, 'repgame', 0)}",
        f"🏙 Репутация в сообществе: {rget(player, 'repcommunity', 0)}",
    ]
    if rget(player, "admintitle"):
        lines.append(f"🛡 Админский титул: {escape(str(player['admintitle']))}")
    if rget(player, "businessname"):
        lines.append(f"🏢 Бизнес: {escape(str(player['businessname']))}")
    if badge(player):
        lines.append(f"{badge(player)}")
    if anketa:
        lines.append(f'📄 <a href="{escape(str(anketa), quote=True)}">Анкета</a>')
    last = rget(player, "lastpost")
    if last:
        lines.append(f"🕒 Последний пост: {last.astimezone(MSK).strftime('%d.%m.%Y %H:%M')}")
    return "\n".join(lines)


async def show_profile(message: Message, player: Any):
    await message.answer(await build_profile_text(player), parse_mode="HTML")


async def send_welcome(message: Message, player: Any, new: bool):
    rules = f'\n<a href="{escape(RULES_URL, quote=True)}">Правила</a>' if RULES_URL else ""
    if new:
        text = (
            f"🎉 Добро пожаловать, <b>{escape(str(player['charactername']))}</b>!\n\n"
            "Твои стартовые значения: STR 4 · REP 4 · CON 4 · CASH 500."
        )
    else:
        text = f"С возвращением, <b>{escape(str(player['charactername'] or 'игрок'))}</b>!"
    await message.answer(text + rules, parse_mode="HTML")


@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    player = await get_player(user_id)
    if not player:
        await state.set_state(RegistrationState.waiting_for_character_name)
        await message.answer("Напиши имя персонажа одним сообщением. От 2 до 50 символов.")
        return
    if not rget(player, "charactername"):
        await state.set_state(RegistrationState.waiting_for_character_name)
        await message.answer("Напиши имя персонажа одним сообщением. От 2 до 50 символов.")
        return
    await send_welcome(message, player, False)


@dp.message(StateFilter(RegistrationState.waiting_for_character_name))
async def process_character_name(message: Message, state: FSMContext):
    character_name = (message.text or "").strip()
    if not 2 <= len(character_name) <= 50:
        await message.answer("Имя должно содержать от 2 до 50 символов.")
        return
    user_id = message.from_user.id
    player = await get_player(user_id)
    if player:
        await db_pool.execute(
            "UPDATE players SET charactername=$1, username=$2, firststartseen=TRUE WHERE userid=$3",
            character_name, clean_username(message.from_user.username), user_id,
        )
        player = await get_player(user_id)
        created = False
    else:
        player, created = await register_player(
            user_id, message.from_user.username, character_name, True
        )
    await state.clear()
    await send_welcome(message, player, created)


@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    args = (message.text or "").split()
    player = await get_player_by_username(args[1]) if len(args) > 1 else await get_player(message.from_user.id)
    if not player:
        await message.answer("Игрок не найден. Используй /start для регистрации.")
        return
    await show_profile(message, player)


@dp.message(Command("setanket"))
async def cmd_setanket(message: Message):
    player = await get_player(message.from_user.id)
    if not player:
        await message.answer("Сначала используй /start.")
        return
    match = re.search(r"https?://\S+", message.text or "")
    if not match:
        await message.answer("Формат: /setanket https://t.me/...")
        return
    url = match.group(0).rstrip(".,!?)]}")
    await db_pool.execute("UPDATE players SET anketaurl=$1 WHERE userid=$2", url, message.from_user.id)
    await message.answer("Анкета сохранена.")


@dp.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "<b>Команды</b>\n\n"
        "/start — регистрация\n"
        "/profile — свой профиль\n"
        "/profile @username — профиль игрока\n"
        "/setanket URL — добавить анкету\n"
        "/random str|rep|con|cash @username — случайное сравнение\n"
        "/economy — последние операции с CASH"
    )
    if is_admin(message.from_user.id):
        text += "\n\n<b>Админские</b>\n/stats\n/player @username\n/setname @username Имя\n/awardcash @username сумма причина\n/business @username Название зарплата"
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    players = await db_pool.fetchval("SELECT COUNT(*) FROM players")
    events = await db_pool.fetchval("SELECT COUNT(*) FROM newsevents")
    actions = await db_pool.fetchval("SELECT COUNT(*) FROM actions")
    await message.answer(f"Игроков: {players}\nСобытий: {events}\nДействий: {actions}")


@dp.message(Command("player"))
async def cmd_player(message: Message):
    if not is_admin(message.from_user.id):
        return
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer("Формат: /player @username")
        return
    player = await get_player_by_username(args[1])
    if not player:
        await message.answer("Игрок не найден.")
        return
    await show_profile(message, player)


@dp.message(Command("setname"))
async def cmd_setname(message: Message):
    if not is_admin(message.from_user.id):
        return
    args = (message.text or "").split(maxsplit=2)
    if len(args) < 3:
        await message.answer("Формат: /setname @username Имя")
        return
    player = await get_player_by_username(args[1])
    if not player:
        await message.answer("Игрок не найден.")
        return
    await db_pool.execute("UPDATE players SET charactername=$1 WHERE userid=$2", args[2].strip(), player["userid"])
    await message.answer("Имя изменено.")


@dp.message(Command("awardcash"))
async def cmd_award_cash(message: Message):
    if not is_admin(message.from_user.id):
        return
    args = (message.text or "").split(maxsplit=3)
    if len(args) < 3:
        await message.answer("Формат: /awardcash @username сумма причина")
        return
    player = await get_player_by_username(args[1])
    if not player:
        await message.answer("Игрок не найден.")
        return
    amount = safe_int(args[2])
    reason = args[3] if len(args) > 3 else "админская операция"
    await db_pool.execute(
        "UPDATE players SET cash=COALESCE(cash,money)+$1, money=money+$1 WHERE userid=$2",
        amount, player["userid"],
    )
    await db_pool.execute(
        "INSERT INTO economy_ledger(userid, amount, reason) VALUES($1,$2,$3)",
        player["userid"], amount, reason,
    )
    await message.answer(f"Операция выполнена: {amount:+d} CASH.")


@dp.message(Command("business"))
async def cmd_business(message: Message):
    if not is_admin(message.from_user.id):
        return
    args = (message.text or "").split(maxsplit=3)
    if len(args) < 4:
        await message.answer("Формат: /business @username Название зарплата")
        return
    player = await get_player_by_username(args[1])
    if not player:
        await message.answer("Игрок не найден.")
        return
    parts = args[3].rsplit(" ", 1)
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await message.answer("Последним параметром укажи зарплату числом.")
        return
    name = f"{args[2]} {parts[0]}".strip()
    salary = int(parts[1])
    await db_pool.execute("UPDATE players SET businessname=$1, businesssalary=$2 WHERE userid=$3", name, salary, player["userid"])
    await message.answer("Бизнес сохранён.")


@dp.message(Command("economy"))
async def cmd_economy(message: Message):
    player = await get_player(message.from_user.id)
    if not player:
        await message.answer("Сначала используй /start.")
        return
    rows = await db_pool.fetch(
        "SELECT amount, reason, createdat FROM economy_ledger WHERE userid=$1 ORDER BY createdat DESC LIMIT 10",
        message.from_user.id,
    )
    cash = safe_int(rget(player, "cash", rget(player, "money", 0)))
    lines = [f"💰 CASH: {cash}", "", "Последние операции:"]
    lines.extend(f"{row['amount']:+d} — {escape(row['reason'])}" for row in rows)
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("random"))
async def cmd_random(message: Message):
    import random
    args = (message.text or "").split()
    if len(args) < 3:
        await message.answer("Формат: /random str|rep|con|cash @username")
        return
    stat = args[1].lower()
    if stat not in {"str", "rep", "con", "cash"}:
        await message.answer("Стат: str, rep, con или cash.")
        return
    enemy = await get_player_by_username(args[2])
    player = await get_player(message.from_user.id)
    if not player or not enemy:
        await message.answer("Один из игроков не найден.")
        return
    key = "cash" if stat == "cash" else stat
    own = safe_int(rget(player, key, rget(player, "money", 0)))
    theirs = safe_int(rget(enemy, key, rget(enemy, "money", 0)))
    total = own + theirs
    chance = 50 if total == 0 else own / total * 100
    winner = display_name(player) if random.uniform(0, 100) < chance else display_name(enemy)
    await message.answer(
        f"{stat.upper()}: {display_name(player)} {own} vs {display_name(enemy)} {theirs}\n"
        f"Шанс первого: {chance:.1f}%\nПобедитель: {winner}"
    )


@dp.message()
async def handle_message(message: Message, state: FSMContext):
    if not message.text or not message.from_user:
        return
    if message.chat.id != GROUP_ID:
        return
    current_topic = topic_id(message)
    if is_ignored_topic(current_topic) or not is_event_topic(current_topic):
        return
    player = await get_player(message.from_user.id)
    if not player:
        return
    await mark_player_active(message.from_user.id)
    await add_event(message, "пост", message.text)
    if len(message.text) >= MIN_TEXT_FOR_STATS:
        await update_stats_immediate(message, message.text)


async def health_handler(request: web.Request):
    return web.Response(text="Bot is alive")


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health server started on port %s", PORT)
    return runner


async def main():
    logger.info("Starting bot")
    await create_database_pool()
    try:
        await init_database()
        web_runner = await start_web_server()
        tasks = [
            asyncio.create_task(news_loop()),
            asyncio.create_task(daily_loop()),
            asyncio.create_task(inactivity_loop()),
            asyncio.create_task(top_loop()),
        ]
        try:
            await dp.start_polling(bot)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await web_runner.cleanup()
    finally:
        await close_database_pool()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
