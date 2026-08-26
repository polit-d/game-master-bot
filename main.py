import asyncio
import json
import logging
import os
import random
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
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("galo_bot")
MSK = ZoneInfo("Europe/Moscow")
UTC = timezone.utc


def env(name: str, old: str | None = None, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None and old:
        value = os.getenv(old)
    return value if value is not None else default


def env_int(name: str, old: str | None = None, default: int = 0) -> int:
    value = env(name, old)
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


BOT_TOKEN = env("BOT_TOKEN", "BOTTOKEN")
DATABASE_URL = env("DATABASE_URL", "DATABASEURL")
GROQ_API_KEY = env("GROQ_API_KEY", "GROQAPIKEY")
GROQ_MODEL = env("GROQ_MODEL", "GROQMODEL", "llama-3.3-70b-versatile")
GROQ_STRUCTURED_MODEL = env("GROQ_STRUCTURED_MODEL", "GROQSTRUCTUREDMODEL", "openai/gpt-oss-20b")
GROUP_ID = env_int("GROUP_ID", "GROUPID")
GAME_TOPIC_ID = env_int("GAME_TOPIC_ID", "GAMETOPICID")
SEMI_TOPIC_ID = env_int("SEMI_RP_TOPIC_ID", "SEMIRPTOPICID")
INFO_TOPIC_ID = env_int("INFO_TOPIC_ID", "INFOTOPICID")
ADMIN_TOPIC_ID = env_int("ADMIN_TOPIC_ID", "ADMINTOPICID")
PROFILES_TOPIC_ID = env_int("PROFILES_TOPIC_ID", "PROFILESTOPICID")
FLOOD_TOPIC_ID = env_int("FLOOD_TOPIC_ID", "FLOODTOPICID")
STORY_TOPIC_ID = env_int("STORY_TOPIC_ID", "STORYTOPICID")
RULES_URL = env("RULES_URL", "RULESURL", "")
NEWS_STYLE = env("NEWS_STYLE", "NEWSSTYLE", "") or ""
PORT = env_int("PORT", default=10000)
ADMIN_IDS = {
    int(x.strip())
    for x in (env("ADMIN_IDS", "ADMINIDS", "") or "").split(",")
    if x.strip().lstrip("-").isdigit()
}
RANDOMCITYEVENTS = [
    "На одной из улиц произошла авария, движение затруднено.",
    "На площади началась уличная вечеринка с музыкой и танцами.",
    "Прохожий нашёл на тротуаре потерянный кошелёк с крупной суммой.",
    "У старого дома замечена странная встреча двух незнакомцев.",
    "Центральную улицу перекрыли из-за поломки транспорта.",
    "В жилом квартале вспыхнул пожар, началась эвакуация.",
    "По улице пробежала убежавшая собака без хозяина.",
    "Полиция проводит проверку документов у группы прохожих.",
    "На дороге идут ремонтные работы, работает строительная техника.",
    "У входа в торговый центр собралась большая толпа людей.",
    "По проспекту на большой скорости промчался велосипедист.",
    "Над городом завис дрон, который снимает улицы сверху.",
    "Внезапно начался сильный ливень, улицы быстро опустели.",
    "В город приехал важный гость, у здания собрались журналисты.",
    "На набережной идёт съёмка фильма, работают блогеры.",
    "В центре города открылось новое крупное заведение, много людей.",
    "На улице совершенно обычная сцена — люди спешат по своим делам.",
]

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("Не задан DATABASE_URL")
if not GROQ_API_KEY:
    raise RuntimeError("Не задан GROQ_API_KEY")
if not GROUP_ID:
    raise RuntimeError("Не задан GROUP_ID")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
db_pool: asyncpg.Pool | None = None

class RegistrationState(StatesGroup):
    waiting_for_character_name = State()


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_msk() -> datetime:
    return datetime.now(MSK)


def clean_username(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip().lstrip("@").strip()
    return value or None


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def rget(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


def topic_id(message: Message) -> int:
    return safe_int(message.message_thread_id)


def topic_name(value: int) -> str:
    return {
        GAME_TOPIC_ID: "игровой",
        SEMI_TOPIC_ID: "полуигровой",
        INFO_TOPIC_ID: "информационный",
        ADMIN_TOPIC_ID: "админский",
        PROFILES_TOPIC_ID: "профили",
        FLOOD_TOPIC_ID: "флуд",
        STORY_TOPIC_ID: "сюжет",
    }.get(value, f"тема {value}")


def is_admin(user_id: int | None) -> bool:
    return user_id in ADMIN_IDS


def display_name(row: Any) -> str:
    character = str(rget(row, "charactername", "") or "").strip()
    username = clean_username(rget(row, "username"))
    if character and username:
        return f"{character} (@{username})"
    return character or (f"@{username}" if username else str(rget(row, "userid", "игрок")))


TAG_RE = re.compile(r"(?<!\w)#([A-Za-zА-Яа-яЁё0-9_-]+)")


def story_tag(text: str | None) -> str | None:
    match = TAG_RE.search(text or "")
    return match.group(1).lower() if match else None


async def create_pool() -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, command_timeout=90)
    logger.info("PostgreSQL pool created")


async def close_pool() -> None:
    global db_pool
    if db_pool:
        await db_pool.close()
        db_pool = None


async def normalize_players_schema() -> None:
    assert db_pool is not None
    columns = {
        row["column_name"]
        for row in await db_pool.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'players'
            """
        )
    }
    if "userid" not in columns:
        if "user_id" in columns:
            await db_pool.execute("ALTER TABLE players RENAME COLUMN user_id TO userid")
        elif "telegram_id" in columns:
            await db_pool.execute("ALTER TABLE players RENAME COLUMN telegram_id TO userid")
        elif "id" in columns:
            await db_pool.execute("ALTER TABLE players ADD COLUMN userid BIGINT")
            await db_pool.execute("UPDATE players SET userid = id WHERE userid IS NULL")
        else:
            raise RuntimeError(f"В таблице players нет идентификатора игрока. Колонки: {sorted(columns)}")
    aliases = {
        "character_name": "charactername",
        "last_post": "lastpost",
        "anketa_url": "anketaurl",
        "bad_boy_count": "badboycount",
        "good_boy_count": "goodboycount",
    }
    for old, new in aliases.items():
        columns = {
            row["column_name"]
            for row in await db_pool.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='players'"
            )
        }
        if new not in columns:
            await db_pool.execute(f"ALTER TABLE players ADD COLUMN {new} TEXT")
            if old in columns:
                await db_pool.execute(f"UPDATE players SET {new} = {old} WHERE {new} IS NULL")


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
            firststartseen BOOLEAN NOT NULL DEFAULT FALSE
        )
        """
    )

    await normalize_players_schema()

    await db_pool.execute(
        """
        DO $$
        DECLARE column_type TEXT;
        BEGIN
            SELECT data_type
            INTO column_type
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'players'
              AND column_name = 'lastpost';

            IF column_type IN ('text', 'character varying') THEN
                ALTER TABLE players
                ALTER COLUMN lastpost TYPE TIMESTAMPTZ
                USING CASE
                    WHEN lastpost IS NULL OR btrim(lastpost) = '' THEN NULL
                    WHEN lastpost ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                        THEN lastpost::TIMESTAMPTZ
                    ELSE NULL
                END;
            END IF;
        END
        $$;
        """
    )

    for statement in (
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS username TEXT",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS charactername TEXT",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS str INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS rep INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS con INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS money INTEGER NOT NULL DEFAULT 100",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS lastpost TIMESTAMPTZ",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS anketaurl TEXT",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS badboycount INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS goodboycount INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS strweeklimit INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS repweeklimit INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS conweeklimit INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS moneyweeklimit INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS weekreset TIMESTAMPTZ",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS firststartseen BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS cash INTEGER",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS activitystatus TEXT NOT NULL DEFAULT 'reader'",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS cashstatus TEXT NOT NULL DEFAULT 'normal'",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS repgame INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS repcommunity INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS admintitle TEXT",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS businessname TEXT",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS businesssalary INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE players ADD COLUMN IF NOT EXISTS physicalpotential INTEGER NOT NULL DEFAULT 1",
        "UPDATE players SET cash = COALESCE(cash, money, 100) WHERE cash IS NULL",
    ):
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

    await db_pool.execute(
        "ALTER TABLE newsevents ADD COLUMN IF NOT EXISTS storytag TEXT"
    )

    await db_pool.execute(
        "ALTER TABLE newsevents ADD COLUMN IF NOT EXISTS statsprocessedat TIMESTAMPTZ"
    )

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
            userid BIGINT NOT NULL,
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
            userid BIGINT NOT NULL,
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
            userid BIGINT NOT NULL,
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
    return await db_pool.fetchrow("SELECT * FROM players WHERE userid=$1", user_id)


async def get_player_by_username(username: str | None):
    username = clean_username(username)
    if not username:
        return None
    return await db_pool.fetchrow("SELECT * FROM players WHERE LOWER(username)=LOWER($1) LIMIT 1", username)


async def register_player(user_id: int, username: str | None, character: str):
    existing = await get_player(user_id)
    if existing:
        return existing, False
    return await db_pool.fetchrow(
        """
        INSERT INTO players(userid, username, charactername, str, rep, con, money, cash, lastpost, weekreset, firststartseen, activitystatus)
        VALUES($1,$2,$3,4,4,4,500,500,$4,$4,TRUE,'active') RETURNING *
        """,
        user_id, clean_username(username), character, now_utc(),
    ), True


async def update_username(player: Any, username: str | None):
    username = clean_username(username)
    if username and username != clean_username(rget(player, "username")):
        return await db_pool.fetchrow("UPDATE players SET username=$1 WHERE userid=$2 RETURNING *", username, player["userid"])
    return player


async def reset_week(player: Any):
    stamp = rget(player, "weekreset")
    if stamp is None or now_utc() - stamp >= timedelta(days=7):
        return await db_pool.fetchrow(
            """
            UPDATE players SET strweeklimit=0, repweeklimit=0, conweeklimit=0, moneyweeklimit=0, weekreset=$1
            WHERE userid=$2 RETURNING *
            """,
            now_utc(), player["userid"],
        )
    return player


async def groq(
    messages: list[dict[str, str]],
    model: str,
    temperature: float,
    purpose: str = "unknown",
    max_completion_tokens: int = 500,
):
    input_chars = sum(
        len(str(message.get("content", "")))
        for message in messages
    )

    logger.info(
        "GROQ START purpose=%s model=%s input_chars=%s max_completion_tokens=%s",
        purpose,
        model,
        input_chars,
        max_completion_tokens,
    )

    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_completion_tokens": max_completion_tokens,
    }

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=90)
        ) as session:
            async with session.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
            ) as response:
                raw = await response.text()

                if response.status == 413:
                    logger.error(
                        "GROQ TOO LARGE purpose=%s model=%s input_chars=%s response=%s",
                        purpose,
                        model,
                        input_chars,
                        raw[:1000],
                    )
                    return None

                if response.status != 200:
                    logger.error(
                        "GROQ ERROR purpose=%s status=%s input_chars=%s response=%s",
                        purpose,
                        response.status,
                        input_chars,
                        raw[:1000],
                    )
                    return None

                data = json.loads(raw)

                logger.info(
                    "GROQ SUCCESS purpose=%s model=%s input_chars=%s",
                    purpose,
                    model,
                    input_chars,
                )

                return data

    except asyncio.TimeoutError:
        logger.error(
            "GROQ TIMEOUT purpose=%s input_chars=%s",
            purpose,
            input_chars,
        )
        return None

    except json.JSONDecodeError:
        logger.exception(
            "GROQ INVALID JSON purpose=%s",
            purpose,
        )
        return None

    except aiohttp.ClientError:
        logger.exception(
            "GROQ HTTP ERROR purpose=%s",
            purpose,
        )
        return None

    except Exception:
        logger.exception(
            "GROQ UNEXPECTED ERROR purpose=%s",
            purpose,
        )
        return None

def json_object(text: str | None) -> dict:
    if not text:
        return {}

    cleaned = text.strip()

    cleaned = re.sub(
        r"^```(?:json)?\s*|\s*```$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )

    try:
        value = json.loads(cleaned)

        if isinstance(value, dict):
            return value

    except json.JSONDecodeError:
        pass

    match = re.search(
        r"\{.*\}",
        cleaned,
        flags=re.DOTALL,
    )

    if not match:
        return {}

    try:
        value = json.loads(match.group(0))

        if isinstance(value, dict):
            return value

    except json.JSONDecodeError:
        pass

    return {}

async def analyze(material: str) -> dict[str, int]:
    result = await groq(
        [
            {"role": "system", "content": "Верни только JSON: {\"str\":0,\"rep\":0,\"con\":0,\"cash\":0,\"goodboy\":0,\"badboy\":0}. STR/REP/CON от 0 до 2, CASH от 0 до 10, GoodBoy/BadBoy 0 или 1. Не выдумывай факты."},
            {"role": "user", "content": material[:24000]},
        ],
        GROQ_STRUCTURED_MODEL,
        0.1,
    )
    try:
        parsed = json_object(result["choices"][0]["message"]["content"])
    except (KeyError, IndexError, TypeError):
        parsed = {}
    return {
        "str": max(0, min(2, safe_int(parsed.get("str")))),
        "rep": max(0, min(2, safe_int(parsed.get("rep")))),
        "con": max(0, min(2, safe_int(parsed.get("con")))),
        "cash": max(0, min(10, safe_int(parsed.get("cash")))),
        "goodboy": max(0, min(1, safe_int(parsed.get("goodboy")))),
        "badboy": max(0, min(1, safe_int(parsed.get("badboy")))),
    }


async def add_event(message: Message, text: str, event_type: str = "пост"):
    text = (text or "").strip()
    if not text:
        return
    player = await get_player(message.from_user.id) if message.from_user else None
    username = clean_username(message.from_user.username if message.from_user else None)
    if not username and player:
        username = clean_username(rget(player, "username"))
    tag = story_tag(text)
    topic = topic_id(message)
    await db_pool.execute(
        """
        INSERT INTO newsevents(sourcechatid, telegrammessageid, userid, charactername, username, eventtype, text, topicid, topicname, storytag)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(sourcechatid, telegrammessageid) DO NOTHING
        """,
        message.chat.id, message.message_id, message.from_user.id if message.from_user else None,
        rget(player, "charactername") if player else None, username, event_type, text[:4000], topic, topic_name(topic), tag,
    )
    if tag:
        await db_pool.execute(
            """
            INSERT INTO story_threads(tag,last_seen,event_count) VALUES($1,NOW(),1)
            ON CONFLICT(tag) DO UPDATE SET last_seen=NOW(), event_count=story_threads.event_count+1, archived=FALSE
            """,
            tag,
        )


async def mark_active(user_id: int):
    await db_pool.execute("UPDATE players SET lastpost=$1, activitystatus='active' WHERE userid=$2", now_utc(), user_id)


async def process_daily(progress_date: date):
    start = datetime.combine(progress_date, datetime.min.time(), tzinfo=MSK).astimezone(UTC)
    end = start + timedelta(days=1)
    events = await db_pool.fetch(
        """
        SELECT e.*
        FROM newsevents e
        INNER JOIN players p ON p.userid = e.userid
        WHERE e.createdat >= $1 AND e.createdat < $2
          AND e.statsprocessedat IS NULL
          AND e.topicid = ANY($3::int[])
        ORDER BY e.userid, e.createdat
        """,
        start, end, [GAME_TOPIC_ID, SEMI_TOPIC_ID, PROFILES_TOPIC_ID],
    )
    grouped: dict[int, list[Any]] = {}
    for event in events:
        grouped.setdefault(safe_int(event["userid"]), []).append(event)
    for user_id, player_events in grouped.items():
        if await db_pool.fetchval("SELECT 1 FROM daily_progress WHERE userid=$1 AND progressdate=$2", user_id, progress_date):
            continue
        material = "\n\n".join(
            f"#{event['storytag'] if event['storytag'] else 'без-тега'} | {event['topicname'] or ''}\n{event['text']}"
            for event in player_events
        )
        chars = sum(len(event["text"] or "") for event in player_events)
        result = await analyze(material) if chars >= 300 else {"str": 0, "rep": 0, "con": 0, "cash": 0, "goodboy": 0, "badboy": 0}
        player = await reset_week(await get_player(user_id))
        if not player:
            continue
        str_add = min(result["str"], max(0, 3 - safe_int(player["strweeklimit"])))
        rep_add = min(result["rep"], max(0, 7 - safe_int(player["repweeklimit"])))
        con_add = min(result["con"], max(0, 8 - safe_int(player["conweeklimit"])))
        ai_cash = min(result["cash"], max(0, 1000 - safe_int(player["moneyweeklimit"])))
        cash_delta = -5 + 10 + ai_cash
        await db_pool.execute(
            """
            INSERT INTO daily_progress(userid,progressdate,posts_count,text_chars,str_delta,rep_delta,con_delta,cash_delta,goodboy_delta,badboy_delta,material)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            """,
            user_id, progress_date, len(player_events), chars, str_add, rep_add, con_add, cash_delta, result["goodboy"], result["badboy"], material[:24000],
        )
        await db_pool.execute(
            """
            UPDATE players SET str=str+$1, rep=rep+$2, con=con+$3,
            money=money+$4, cash=COALESCE(cash,money)+$4,
            strweeklimit=strweeklimit+$1, repweeklimit=repweeklimit+$2, conweeklimit=conweeklimit+$3,
            moneyweeklimit=moneyweeklimit+$5, goodboycount=goodboycount+$6, badboycount=badboycount+$7,
            activitystatus='active', lastpost=$8,
            cashstatus=CASE WHEN COALESCE(cash,money)+$4 <= 0 THEN 'Нищий' ELSE 'normal' END
            WHERE userid=$9
            """,
            str_add, rep_add, con_add, cash_delta, ai_cash, result["goodboy"], result["badboy"], now_utc(), user_id,
        )
        await db_pool.execute("INSERT INTO economy_ledger(userid,amount,reason,progressdate) VALUES($1,-5,'расходы активного дня',$2)", user_id, progress_date)
        await db_pool.execute("INSERT INTO economy_ledger(userid,amount,reason,progressdate) VALUES($1,$2,'награда активного дня и анализ',$3)", user_id, 10 + ai_cash, progress_date)
        for achievement, value in (("GoodBoy", result["goodboy"]), ("BadBoy", result["badboy"])):
            await db_pool.execute(
                """
                INSERT INTO player_achievements(userid,achievement,count) VALUES($1,$2,$3)
                ON CONFLICT(userid,achievement) DO UPDATE SET count=player_achievements.count+EXCLUDED.count, updatedat=NOW()
                """,
                user_id, achievement, value,
            )
        await db_pool.execute("UPDATE newsevents SET statsprocessedat=NOW() WHERE id=ANY($1::bigint[])", [event["id"] for event in player_events])
    readers = await db_pool.fetch(
        """
        SELECT p.userid FROM players p WHERE NOT EXISTS(
            SELECT 1 FROM daily_progress d WHERE d.userid=p.userid AND d.progressdate=$1
        )
        """,
        progress_date,
    )
    for row in readers:
        user_id = row["userid"]
        await db_pool.execute("INSERT INTO daily_progress(userid,progressdate,cash_delta) VALUES($1,$2,-2) ON CONFLICT DO NOTHING", user_id, progress_date)
        await db_pool.execute(
            """
            UPDATE players SET money=money-2, cash=COALESCE(cash,money)-2, activitystatus='reader',
            cashstatus=CASE WHEN COALESCE(cash,money)-2 <= 0 THEN 'Нищий' ELSE cashstatus END
            WHERE userid=$1
            """,
            user_id,
        )
        await db_pool.execute("INSERT INTO economy_ledger(userid,amount,reason,progressdate) VALUES($1,-2,'расходы читателя',$2)", user_id, progress_date)
    logger.info("Daily progress processed: %s", progress_date)

# Сброс неактивных (30 дней без постов)
    thirty_days_ago = now_utc() - timedelta(days=30)
    inactive = await db_pool.fetch(
        """
        SELECT userid FROM players
        WHERE lastpost IS NOT NULL
          AND lastpost < $1
          AND activitystatus != 'inactive'
        """,
        thirty_days_ago,
    )
    for row in inactive:
        user_id = row["userid"]
        await db_pool.execute(
            """
            UPDATE players
            SET str=4, rep=4, con=4, money=500,
                cash=500,
                activitystatus='inactive',
                strweeklimit=0, repweeklimit=0, conweeklimit=0, moneyweeklimit=0
            WHERE userid=$1
            """,
            user_id,
        )
        logger.info("Player %s archived (inactive 30+ days)", user_id)
        try:
            await bot.send_message(
                user_id,
                "📦 Вы переведены в статус «Архив» из-за неактивности (30+ дней).\n\n"
                "Ваши статы сброшены: STR 4, REP 4, CON 4, MONEY 500.\n"
                "Начните писать снова, чтобы вернуться в игру."
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление игроку %s", user_id)

async def process_daily(progress_date: date):
    start = datetime.combine(progress_date, datetime.min.time(), tzinfo=MSK).astimezone(UTC)
    end = start + timedelta(days=1)
    events = await db_pool.fetch(
        """
        SELECT e.*
        FROM newsevents e
        INNER JOIN players p ON p.userid = e.userid
        WHERE e.createdat >= $1 AND e.createdat < $2
          AND e.statsprocessedat IS NULL
          AND e.topicid = ANY($3::int[])
        ORDER BY e.userid, e.createdat
        """,
        start, end, [GAME_TOPIC_ID, SEMI_TOPIC_ID, PROFILES_TOPIC_ID],
    )
    grouped: dict[int, list[Any]] = {}
    for event in events:
        grouped.setdefault(safe_int(event["userid"]), []).append(event)
    for user_id, player_events in grouped.items():
        if await db_pool.fetchval("SELECT 1 FROM daily_progress WHERE userid=$1 AND progressdate=$2", user_id, progress_date):
            continue
        material = "\n\n".join(
            f"#{event['storytag'] if event['storytag'] else 'Р±РµР·-С‚РµРіР°'} | {event['topicname'] or ''}\n{event['text']}"
            for event in player_events
        )
        chars = sum(len(event["text"] or "") for event in player_events)
        result = await analyze(material) if chars >= 300 else {"str": 0, "rep": 0, "con": 0, "cash": 0, "goodboy": 0, "badboy": 0}
        player = await reset_week(await get_player(user_id))
        if not player:
            continue
        str_add = min(result["str"], max(0, 3 - safe_int(player["strweeklimit"])))
        rep_add = min(result["rep"], max(0, 7 - safe_int(player["repweeklimit"])))
        con_add = min(result["con"], max(0, 8 - safe_int(player["conweeklimit"])))
        ai_cash = min(result["cash"], max(0, 1000 - safe_int(player["moneyweeklimit"])))
        cash_delta = -5 + 10 + ai_cash
        await db_pool.execute(
            """
            INSERT INTO daily_progress(userid,progressdate,posts_count,text_chars,str_delta,rep_delta,con_delta,cash_delta,goodboy_delta,badboy_delta,material)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            """,
            user_id, progress_date, len(player_events), chars, str_add, rep_add, con_add, cash_delta, result["goodboy"], result["badboy"], material[:24000],
        )
        await db_pool.execute(
            """
            UPDATE players SET str=str+$1, rep=rep+$2, con=con+$3,
            money=money+$4, cash=COALESCE(cash,money)+$4,
            strweeklimit=strweeklimit+$1, repweeklimit=repweeklimit+$2, conweeklimit=conweeklimit+$3,
            moneyweeklimit=moneyweeklimit+$5, goodboycount=goodboycount+$6, badboycount=badboycount+$7,
            activitystatus='active', lastpost=$8,
            cashstatus=CASE WHEN COALESCE(cash,money)+$4 <= 0 THEN 'РќРёС‰РёР№' ELSE 'normal' END
            WHERE userid=$9
            """,
            str_add, rep_add, con_add, cash_delta, ai_cash, result["goodboy"], result["badboy"], now_utc(), user_id,
        )
        await db_pool.execute("INSERT INTO economy_ledger(userid,amount,reason,progressdate) VALUES($1,-5,'СЂР°СЃС…РѕРґС‹ Р°РєС‚РёРІРЅРѕРіРѕ РґРЅСЏ',$2)", user_id, progress_date)
        await db_pool.execute("INSERT INTO economy_ledger(userid,amount,reason,progressdate) VALUES($1,$2,'РЅР°РіСЂР°РґР° Р°РєС‚РёРІРЅРѕРіРѕ РґРЅСЏ Рё Р°РЅР°Р»РёР·',$3)", user_id, 10 + ai_cash, progress_date)
        for achievement, value in (("GoodBoy", result["goodboy"]), ("BadBoy", result["badboy"])):
            await db_pool.execute(
                """
                INSERT INTO player_achievements(userid,achievement,count) VALUES($1,$2,$3)
                ON CONFLICT(userid,achievement) DO UPDATE SET count=player_achievements.count+EXCLUDED.count, updatedat=NOW()
                """,
                user_id, achievement, value,
            )
        await db_pool.execute("UPDATE newsevents SET statsprocessedat=NOW() WHERE id=ANY($1::bigint[])", [event["id"] for event in player_events])
    readers = await db_pool.fetch(
        """
        SELECT p.userid FROM players p WHERE NOT EXISTS(
            SELECT 1 FROM daily_progress d WHERE d.userid=p.userid AND d.progressdate=$1
        )
        """,
        progress_date,
    )
    for row in readers:
        user_id = row["userid"]
        await db_pool.execute("INSERT INTO daily_progress(userid,progressdate,cash_delta) VALUES($1,$2,-2) ON CONFLICT DO NOTHING", user_id, progress_date)
        await db_pool.execute(
            """
            UPDATE players SET money=money-2, cash=COALESCE(cash,money)-2, activitystatus='reader',
            cashstatus=CASE WHEN COALESCE(cash,money)-2 <= 0 THEN 'РќРёС‰РёР№' ELSE cashstatus END
            WHERE userid=$1
            """,
            user_id,
        )
        await db_pool.execute("INSERT INTO economy_ledger(userid,amount,reason,progressdate) VALUES($1,-2,'СЂР°СЃС…РѕРґС‹ С‡РёС‚Р°С‚РµР»СЏ',$2)", user_id, progress_date)
    logger.info("Daily progress processed: %s", progress_date)

async def daily_loop():
    while True:
        try:
            current = now_msk()
            run_at = current.replace(hour=5, minute=0, second=0, microsecond=0)
            if current >= run_at:
                run_at += timedelta(days=1)
            await asyncio.sleep(max(1, (run_at - current).total_seconds()))
            await process_daily((run_at - timedelta(days=1)).date())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Daily loop failed")
            await asyncio.sleep(60)


async def get_news_events(*, since: datetime | None = None, limit: int = 300) -> list[Any]:
    """Return unconsumed events from registered players in the three RP topics."""
    allowed_topics = [GAME_TOPIC_ID, SEMI_TOPIC_ID, PROFILES_TOPIC_ID]
    query = """
        SELECT e.*
        FROM newsevents e
        INNER JOIN players p ON p.userid = e.userid
        WHERE e.usedinnewsat IS NULL
          AND e.topicid = ANY($1::int[])
    """
    params: list[Any] = [allowed_topics]
    if since is not None:
        query += " AND e.createdat >= $2"
        params.append(since)
    query += f" ORDER BY e.createdat ASC LIMIT ${len(params) + 1}"
    params.append(limit)
    return await db_pool.fetch(query, *params)

async def build_story_groups(
    events: list[Any],
) -> list[dict[str, Any]]:
    groups: dict[str, list[Any]] = {}

    for event in events:
        tag = (event["storytag"] or "").strip().lower()

        if not tag:
            tag = "без-тега"

        groups.setdefault(tag, []).append(event)

    result: list[dict[str, Any]] = []

    for tag, tag_events in groups.items():
        result.append(
            {
                "title": tag,
                "tags": [tag],
                "event_ids": [
                    event["id"]
                    for event in tag_events
                ],
            }
        )

    logger.info(
        "STORY GROUPS STRICT tags=%s groups=%s details=%s",
        len(groups),
        len(result),
        [
            {
                "title": group["title"],
                "events": len(group["event_ids"]),
            }
            for group in result
        ],
    )

    return result

async def build_news_from_events(
    events: list[Any],
    urgent: bool = False,
) -> tuple[str, list[int]] | None:
    if not events:
        return None

    story_groups = await build_story_groups(events)

    if not story_groups:
        logger.warning("NEWS NO STORY GROUPS")
        return None

    news_parts: list[str] = []
    used_event_ids: list[int] = []

    for group_index, group in enumerate(story_groups, start=1):
        group_tags = set(group["tags"])

        group_events = [
            event
            for event in events
            if (
                (event["storytag"] or "").strip().lower()
                or "без-тега"
            ) in group_tags
        ]

        if not group_events:
            continue

        source_parts = []

        for event in group_events:
            author = display_name(
                {
                    "userid": event["userid"],
                    "charactername": event["charactername"],
                    "username": event["username"],
                }
            )

            text = (event["text"] or "").strip()

            source_parts.append(
                f"{author}: {text[:1200]}"
            )

        source = "\n\n".join(source_parts)

        # Безопасный лимит для одного сюжетного запроса.
        source = source[:9000]

        mode = (
            "СРОЧНАЯ НОВОСТЬ"
            if urgent
            else "ОБЫЧНАЯ НОВОСТЬ"
        )

        prompt = (
            f"{mode}.\n"
            f"Название сюжетной линии: {group['title']}\n"
            f"Связанные теги: "
            f"{', '.join('#' + tag for tag in group['tags'])}\n\n"
            "Напиши одну цельную новость по этой сюжетной линии. "
            "Свяжи события в последовательное развитие истории. "
            "Не превращай каждый пост в отдельный заголовок. "
            "Не выдумывай факты. Сохрани имена персонажей, "
            "важные детали и причинно-следственные связи. "
            "Пиши на русском языке. Каждый сюжетный тег — не больше одного "
            "короткого абзаца, только суть, как сводка новостей.\n\n"
            f"{source}"
        )

        result = await groq(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты язвительного и интеллектуальный редактор новостей ролевого города. "
                        "Пиши кратко, как репортажная выжимка. "
                        "На каждый сюжетный тег выделяй не больше одного абзаца. "
                        "Описывай только важное, без лишних деталей и повторов. "
                        "Не выдумывай факты."
                    ),
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            model=GROQ_MODEL,
            temperature=0.7,
            purpose=f"news_story_group_{group_index}",
            max_completion_tokens=350,
        )

        try:
            text = result["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError, TypeError, AttributeError):
            logger.exception(
                "NEWS GROUP INVALID RESPONSE group=%s title=%s",
                group_index,
                group["title"],
            )
            continue

        if not text:
            logger.warning(
                "NEWS GROUP EMPTY group=%s title=%s",
                group_index,
                group["title"],
            )
            continue

        news_parts.append(text)
        used_event_ids.extend(
            event["id"]
            for event in group_events
        )

        logger.info(
            "NEWS STORY GENERATED group=%s title=%s tags=%s events=%s source_chars=%s",
            group_index,
            group["title"],
            group["tags"],
            len(group_events),
            len(source),
        )

    if not news_parts or not used_event_ids:
        logger.warning("NEWS NOTHING GENERATED")
        return None

    final_text = "\n\n━━━━━━━━━━━━━━\n\n".join(news_parts)

    logger.info(
        "NEWS READY stories=%s events=%s chars=%s",
        len(news_parts),
        len(set(used_event_ids)),
        len(final_text),
    )

    return final_text, list(dict.fromkeys(used_event_ids))

async def build_random_news(
    urgent: bool = False,
) -> tuple[str, list[int]] | None:
    if not RANDOMCITYEVENTS:
        return None

    event = random.choice(RANDOMCITYEVENTS)

    mode = "СРОЧНАЯ НОВОСТЬ" if urgent else "ОБЫЧНАЯ НОВОСТЬ"

    prompt = (
        f"{mode}.\n"
        "Это событие из жизни города, о котором сообщили очевидцы.\n\n"
        "Напиши короткую новость, как репортажную выжимку: "
        "не больше одного абзаца, только важное, без выдуманных "
        "имен и деталей.\n\n"
        f"Событие: {event}"
    )

    result = await groq(
        messages=[
            {
                "role": "system",
                "content": (
                    "Ты редактор новостей ролевого города. "
                    "Пиши кратко и живо, без выдуманных фактов."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        model=GROQ_MODEL,
        temperature=0.8,
        purpose="random_city_news",
        max_completion_tokens=250,
    )

    try:
        text = result["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError, AttributeError):
        logger.exception("RANDOM NEWS INVALID RESPONSE")
        return None

    if not text:
        logger.warning("RANDOM NEWS EMPTY")
        return None

    logger.info("RANDOM NEWS GENERATED chars=%s", len(text))

    return text, []

async def generate_news() -> tuple[str, list[int]] | None:
    events = await get_news_events(limit=300)

    if events:
        result = await build_news_from_events(events)

        # Редко (примерно раз в 12 циклов) добавляем рандомную
        # новость к основным событиям.
        if result and random.random() < 0.08:
            random_result = await build_random_news()

            if random_result:
                random_text, _ = random_result
                main_text, ids = result
                combined = (
                    f"{main_text}\n\n"
                    f"━━━━━━━━━━━━━━\n\n"
                    f"{random_text}"
                )
                logger.info(
                    "RANDOM NEWS ADDED TO MAIN events=%s",
                    len(ids),
                )
                return combined, ids

        return result

    # Если событий нет — публикуем рандомную новость.
    logger.info("NO EVENTS — using random city news")
    return await build_random_news()


async def news_loop():
    await asyncio.sleep(30)
    while True:
        try:
            result = await generate_news()
            if result:
                news_text, ids = result
                await bot.send_message(
                    GROUP_ID,
                    news_text,
                    message_thread_id=STORY_TOPIC_ID or None,
                )
                await db_pool.execute(
                    "UPDATE newsevents SET usedinnewsat=NOW() WHERE id=ANY($1::bigint[])",
                    ids,
                )
                logger.info("NEWS SENT | events=%s | ids=%s", len(ids), ids)
            else:
                logger.info("NEWS LOOP | no eligible registered-player events")
            await asyncio.sleep(6 * 3600)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("News loop failed")
            await asyncio.sleep(60)


def level(value: int) -> tuple[int, str]:
    if value >= 15: return 5, "легендарный"
    if value >= 11: return 4, "высокий"
    if value >= 7: return 3, "уверенный"
    if value >= 3: return 2, "развивающийся"
    return 1, "начальный"


def badge(player: Any) -> str:
    good = safe_int(rget(player, "goodboycount"))
    bad = safe_int(rget(player, "badboycount"))
    if good >= bad + 5: return "😇 GoodBoy"
    if bad >= good + 5: return "😈 BadBoy"
    return ""


async def profile_text(player: Any) -> str:
    sl, ss = level(safe_int(player["str"]))
    rl, rs = level(safe_int(player["rep"]))
    cl, cs = level(safe_int(player["con"]))
    cash = safe_int(rget(player, "cash", rget(player, "money", 0)))

    character = escape(str(rget(player, "charactername") or "Без имени"))

    lines = [f"👤 <b>{character}</b>"]

    if rget(player, "anketaurl"):
        lines.append(
            f'<a href="{escape(str(player["anketaurl"]), quote=True)}">📄 Анкета</a>'
        )

    lines.append("")
    lines.append(f"💪 STR: {player['str']} — {ss} ({sl})")
    lines.append(f"🤝 REP: {player['rep']} — {rs} ({rl})")
    lines.append(f"🫀 CON: {player['con']} — {cs} ({cl})")
    lines.append(f"💰 CASH: {cash}")

    lines.append("")
    status = str(rget(player, "activitystatus", "reader"))
    status_label = {
        "active": "Игрок",
        "reader": "Читатель",
        "inactive": "Архив",
    }.get(status, status)
    lines.append(f"📌 Статус: {status_label}")

    if badge(player):
        lines.append("")
        lines.append("🏅 <b>Ачивки</b>")
        lines.append(badge(player))

    if rget(player, "businessname"):
        lines.append(
            f"🏢 Бизнес: {escape(str(player['businessname']))} · "
            f"ЗП {safe_int(rget(player, 'businesssalary', 0))}"
        )

    last = rget(player, "lastpost")
    if last:
        lines.append(
            f"🕒 Последний пост: {last.astimezone(MSK).strftime('%d.%m.%Y %H:%M')}"
        )

    return "\n".join(lines)



@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    player = await get_player(message.from_user.id)
    if not player or not rget(player, "charactername"):
        await state.set_state(RegistrationState.waiting_for_character_name)
        await message.answer("Напиши имя персонажа — от 2 до 50 символов.")
        return
    player = await update_username(player, message.from_user.username)
    await message.answer(f"С возвращением, <b>{escape(str(player['charactername']))}</b>!", parse_mode="HTML")




def is_plain_text_message(message: Message) -> bool:
    return bool(
        message.text
        and not message.text.lstrip().startswith("/")
    )


@dp.message(is_plain_text_message)
async def handle_group_message(message: Message, state: FSMContext):
    """
    First-line handler for RP text posts.

    Only the three configured RP topics are stored. Registration is NOT
    required to store the event; registration is checked later for news/stats.
    """
    # Telegram commands are handled by their dedicated handlers.
    # Never treat /news (or any other command) as an RP post.
    if not message.text or message.text.lstrip().startswith('/'):
        return

    current_topic = topic_id(message)

    logger.info(
        "INCOMING RP TEXT | chat=%s | user=%s | message=%s | "
        "topic=%s | thread=%s | is_topic=%s",
        message.chat.id,
        message.from_user.id if message.from_user else None,
        message.message_id,
        current_topic,
        message.message_thread_id,
        message.is_topic_message,
    )

    allowed_topics = {GAME_TOPIC_ID, SEMI_TOPIC_ID, PROFILES_TOPIC_ID}

    if current_topic not in allowed_topics:
        logger.info(
            "POST IGNORED | reason=topic | topic=%s | allowed=%s | message=%s",
            current_topic,
            sorted(allowed_topics),
            message.message_id,
        )
        return

    player = await get_player(message.from_user.id) if message.from_user else None

    if player:
        await mark_active(message.from_user.id)

    try:
        await add_event(message, message.text)
        logger.info(
            "POST STORED | registered=%s | topic=%s | message=%s",
            bool(player),
            current_topic,
            message.message_id,
        )
    except Exception:
        logger.exception(
            "POST STORE FAILED | topic=%s | message=%s | user=%s",
            current_topic,
            message.message_id,
            message.from_user.id if message.from_user else None,
        )

@dp.message(StateFilter(RegistrationState.waiting_for_character_name))
async def process_name(message: Message, state: FSMContext):
    character = (message.text or "").strip()
    if not 2 <= len(character) <= 50:
        await message.answer("Имя должно содержать от 2 до 50 символов.")
        return
    player = await get_player(message.from_user.id)
    if player:
        await db_pool.execute("UPDATE players SET charactername=$1, username=$2, firststartseen=TRUE WHERE userid=$3", character, clean_username(message.from_user.username), message.from_user.id)
        player = await get_player(message.from_user.id)
    else:
        player, _ = await register_player(message.from_user.id, message.from_user.username, character)
    await state.clear()
    await message.answer(f"🎉 Добро пожаловать, <b>{escape(character)}</b>!\nSTR 4 · REP 4 · CON 4 · CASH 500", parse_mode="HTML")


@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    args = (message.text or "").split()
    player = await get_player_by_username(args[1]) if len(args) > 1 else await get_player(message.from_user.id)
    if not player:
        await message.answer("Игрок не найден. Используй /start.")
        return
    await message.answer(await profile_text(player), parse_mode="HTML")


@dp.message(Command("setanket"))
async def cmd_anket(message: Message):
    player = await get_player(message.from_user.id)
    if not player:
        await message.answer("Сначала используй /start.")
        return
    match = re.search(r"https?://\S+", message.text or "")
    if not match:
        await message.answer("Формат: /setanket https://t.me/...")
        return
    await db_pool.execute("UPDATE players SET anketaurl=$1 WHERE userid=$2", match.group(0).rstrip(".,!?)]}"), message.from_user.id)
    await message.answer("Анкета сохранена.")


@dp.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "<b>Команды игрока</b>\n\n"
        "/start — регистрация и имя персонажа\n"
        "/profile — свой профиль и характеристики\n"
        "/profile @username — профиль другого игрока\n"
        "/setanket URL — прикрепить ссылку на анкету\n"
        "/random str|rep|con|cash @username — случайное сравнение статов\n"
        "/economy — история операций с CASH\n"

    )

    if is_admin(message.from_user.id):
        text += (
            "\n\n<b>Админские команды</b>\n"
            "/stats — количество игроков, событий и действий\n"
            "/player @username — профиль игрока\n"
            "/setname @username Имя — сменить имя персонажа\n"
            "/awardcash @username сумма причина — начислить или списать CASH\n"
            "/business @username Название зарплата — назначить бизнес\n"
            "/news — срочная новость по последним событиям"
        )

    await message.answer(text, parse_mode="HTML")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id): return
    await message.answer(
        f"Игроков: {await db_pool.fetchval('SELECT COUNT(*) FROM players')}\n"
        f"Событий: {await db_pool.fetchval('SELECT COUNT(*) FROM newsevents')}\n"
        f"Действий: {await db_pool.fetchval('SELECT COUNT(*) FROM actions')}"
    )


@dp.message(Command("player"))
async def cmd_player(message: Message):
    if not is_admin(message.from_user.id): return
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer("Формат: /player @username")
        return
    player = await get_player_by_username(args[1])
    if not player:
        await message.answer("Игрок не найден.")
        return
    await message.answer(await profile_text(player), parse_mode="HTML")


@dp.message(Command("setname"))
async def cmd_setname(message: Message):
    if not is_admin(message.from_user.id): return
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
async def cmd_awardcash(message: Message):
    if not is_admin(message.from_user.id): return
    args = (message.text or "").split(maxsplit=3)
    if len(args) < 3:
        await message.answer("Формат: /awardcash @username сумма причина")
        return
    player = await get_player_by_username(args[1])
    amount = safe_int(args[2])
    if not player:
        await message.answer("Игрок не найден.")
        return
    reason = args[3] if len(args) > 3 else "админская операция"
    await db_pool.execute("UPDATE players SET money=money+$1, cash=COALESCE(cash,money)+$1 WHERE userid=$2", amount, player["userid"])
    await db_pool.execute("INSERT INTO economy_ledger(userid,amount,reason) VALUES($1,$2,$3)", player["userid"], amount, reason)
    await message.answer(f"Готово: {amount:+d} CASH")


@dp.message(Command("business"))
async def cmd_business(message: Message):
    if not is_admin(message.from_user.id): return
    args = (message.text or "").split(maxsplit=2)
    if len(args) < 3:
        await message.answer("Формат: /business @username Название зарплата")
        return
    player = await get_player_by_username(args[1])
    parts = args[2].rsplit(" ", 1)
    if not player or len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await message.answer("Игрок не найден или зарплата указана неверно.")
        return
    await db_pool.execute("UPDATE players SET businessname=$1,businesssalary=$2 WHERE userid=$3", parts[0], int(parts[1]), player["userid"])
    await message.answer("Бизнес сохранён.")


@dp.message(Command("economy"))
async def cmd_economy(message: Message):
    player = await get_player(message.from_user.id)
    if not player:
        await message.answer("Сначала используй /start.")
        return
    rows = await db_pool.fetch("SELECT amount,reason FROM economy_ledger WHERE userid=$1 ORDER BY createdat DESC LIMIT 10", message.from_user.id)
    cash = safe_int(rget(player, "cash", rget(player, "money", 0)))
    lines = [f"💰 CASH: {cash}", "", "Последние операции:"]
    lines.extend(f"{row['amount']:+d} — {escape(row['reason'])}" for row in rows)
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("random"))
async def cmd_random(message: Message):
    args = (message.text or "").split()
    if len(args) < 3 or args[1].lower() not in {"str", "rep", "con", "cash"}:
        await message.answer("Формат: /random str|rep|con|cash @username")
        return
    player = await get_player(message.from_user.id)
    enemy = await get_player_by_username(args[2])
    if not player or not enemy:
        await message.answer("Игрок не найден.")
        return
    key = args[1].lower()
    own = safe_int(rget(player, key, rget(player, "money", 0)))
    other = safe_int(rget(enemy, key, rget(enemy, "money", 0)))
    chance = 50 if own + other == 0 else own / (own + other) * 100
    winner = display_name(player) if random.uniform(0, 100) < chance else display_name(enemy)
    await message.answer(f"{key.upper()}: {own} vs {other}\nШанс первого: {chance:.1f}%\nПобедитель: {winner}")


@dp.message(Command("news"))
async def cmd_news(message: Message):
    """Admin-only urgent news from the last two hours."""
    if not message.from_user:
        return

    admin = is_admin(message.from_user.id)
    logger.info(
        "URGENT NEWS REQUEST | user=%s | chat=%s | admin=%s",
        message.from_user.id,
        message.chat.id,
        admin,
    )

    if not admin:
        await message.answer("⛔ Команда /news доступна только администратору.")
        return

    since = now_utc() - timedelta(hours=2)
    events = await get_news_events(since=since, limit=300)
    if not events:
        await message.answer(
            "За последние 2 часа нет новых постов зарегистрированных игроков."
        )
        return

    await message.answer("📰 Собираю срочную новость за последние 2 часа…")

    try:
        result = await build_news_from_events(events, urgent=True)
        if not result:
            await message.answer("Не удалось сформировать срочную новость.")
            return

        news_text, ids = result
        await bot.send_message(
            GROUP_ID,
            news_text,
            message_thread_id=STORY_TOPIC_ID or None,
        )
        await db_pool.execute(
            "UPDATE newsevents SET usedinnewsat=NOW() WHERE id=ANY($1::bigint[])",
            ids,
        )
        logger.info("URGENT NEWS SENT | events=%s | ids=%s", len(ids), ids)
    except Exception:
        logger.exception("Urgent news generation failed")
        await message.answer(
            "Ошибка при создании срочной новости. Подробности есть в Render Logs."
        )




async def health(request: web.Request):
    return web.Response(text="Bot is alive")


async def start_web() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info("Health server started on port %s", PORT)
    return runner


async def main():
    await create_pool()
    try:
        await init_database()
        runner = await start_web()
        tasks = [asyncio.create_task(news_loop()), asyncio.create_task(daily_loop())]
        try:
            await dp.start_polling(bot)
        finally:
            for task in tasks: task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await runner.cleanup()
    finally:
        await close_pool()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
