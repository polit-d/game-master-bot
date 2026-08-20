import os
import asyncio
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv
import sqlite3
import aiohttp
import json
import re

# Загружаем переменные из .env
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# База данных
conn = sqlite3.connect('stats.db')
cursor = conn.cursor()

# Создаём таблицы
cursor.execute('''
CREATE TABLE IF NOT EXISTS players (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    str INTEGER DEFAULT 2,
    rep INTEGER DEFAULT 2,
    con INTEGER DEFAULT 2,
    money INTEGER DEFAULT 300,
    last_post DATETIME,
    anketa_url TEXT,
    status TEXT DEFAULT 'Игрок',
    bad_boy_count INTEGER DEFAULT 0,
    good_boy_count INTEGER DEFAULT 0,
    str_week_limit INTEGER DEFAULT 0,
    rep_week_limit INTEGER DEFAULT 0,
    con_week_limit INTEGER DEFAULT 0,
    money_week_limit INTEGER DEFAULT 0,
    week_reset DATETIME
)
''')
conn.commit()

cursor.execute('''
CREATE TABLE IF NOT EXISTS actions (
    user_id INTEGER,
    action_type TEXT,
    action_value INTEGER,
    timestamp DATETIME
)
''')
conn.commit()

# Функция получения уровня и статуса
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

# Функция получения пометки
def get_badge(bad_count, good_count):
    if bad_count > good_count + 5:
        return "Bad boy"
    elif good_count > bad_count + 5:
        return "Good boy"
    return ""

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Привет! Я бот для учёта статистики в ролевой игре. Используй /profile для просмотра своего профиля.")

# Команда /profile
@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    # Если есть упоминание - показываем профиль другого игрока
    if message.entities:
        for entity in message.entities:
            if entity.type == "mention":
                username = message.text[entity.offset:entity.offset+entity.length]
                # Ищем игрока по username
                cursor.execute("SELECT * FROM players WHERE username = ?", (username,))
                player = cursor.fetchone()
                if player:
                    await show_profile(message, player)
                else:
                    await message.answer("Игрок не найден.")
                return
    
    # Показываем свой профиль
    user_id = message.from_user.id
    cursor.execute("SELECT * FROM players WHERE user_id = ?", (user_id,))
    player = cursor.fetchone()
    
    if player:
        await show_profile(message, player)
    else:
        await message.answer("Вы ещё не зарегистрированы в системе. Начните играть в канале!")

async def show_profile(message: Message, player):
    user_id = player[0]
    username = player[1]
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
    
    badge = get_badge(bad_count, good_count)
    
    anketa_text = f"[ссылка на анкету]({anketa_url})" if anketa_url else "Не заполнена"
    
    text = f"""
📋 ПРОФАЙЛ ИГРОКА

👤 Имя: @{username}
📝 Анкета: {anketa_text}

💪 Сила (STR): {str_val} (ур. {str_level}, {str_status})
🌟 Репутация (REP): {rep_val} (ур. {rep_level}, {rep_status})
🤝 Связи (CON): {con_val} (ур. {con_level}, {con_status})
💰 Кэш (MONEY): {money_val} монет

🎯 Статус: {status}
🏷 Пометка: {badge if badge else "Нет"}

📅 Последний пост: {last_post or 'Никогда'}
"""
    
    await message.answer(text, parse_mode="Markdown")

# Команда /profile setanket
@dp.message(Command("setanket"))
async def cmd_setanket(message: Message):
    user_id = message.from_user.id
    text = message.text
    
    # Извлекаем URL
    url_match = re.search(r'https?://\S+', text)
    if url_match:
        url = url_match.group()
        cursor.execute("UPDATE players SET anketa_url = ? WHERE user_id = ?", (url, user_id))
        conn.commit()
        await message.answer("Анкета обновлена!")
    else:
        await message.answer("Пожалуйста, укажите ссылку на анкету. Пример: /setanket https://t.me/.../123")

# Команда /random
@dp.message(Command("random"))
async def cmd_random(message: Message):
    args = message.text.split()
    
    if len(args) < 3:
        await message.answer("Использование: /random <stat> @username\nПример: /random str @enemy")
        return
    
    stat = args[1].lower()
    enemy_username = args[2]
    
    # Получаем статы текущего игрока
    user_id = message.from_user.id
    cursor.execute("SELECT * FROM players WHERE user_id = ?", (user_id,))
    player = cursor.fetchone()
    
    if not player:
        await message.answer("Вы ещё не зарегистрированы в системе.")
        return
    
    # Получаем статы врага
    cursor.execute("SELECT * FROM players WHERE username = ?", (enemy_username.replace('@', ''),))
    enemy = cursor.fetchone()
    
    if not enemy:
        await message.answer("Враг не найден.")
        return
    
    # Определяем статы
    stat_map = {
        'str': 2,
        'rep': 3,
        'con': 4,
        'money': 5
    }
    
    if stat not in stat_map:
        await message.answer("Неверный стат. Используйте: str, rep, con, money")
        return
    
    player_stat = player[stat_map[stat]]
    enemy_stat = enemy[stat_map[stat]]
    
    # Рассчитываем шанс
    total = player_stat + enemy_stat
    if total == 0:
        player_chance = 50
        enemy_chance = 50
    else:
        player_chance = (player_stat / total) * 100
        enemy_chance = (enemy_stat / total) * 100
    
    # Бросаем кубик
    import random
    roll = random.randint(1, 100)
    
    if roll <= player_chance:
        winner = f"Вы (@{player[1]})"
        winner_stat = player_stat
        loser = f"@{enemy[1]}"
        loser_stat = enemy_stat
    else:
        winner = f"@{enemy[1]}"
        winner_stat = enemy_stat
        loser = f"Вы (@{player[1]})"
        loser_stat = player_stat
    
    text = f"""
🎲 РЕЗУЛЬТАТ

Вы ({stat.upper()} {player_stat}) vs {enemy_username} ({stat.upper()} {enemy_stat})
Ваш шанс: {player_chance:.1f}%
Шанс врага: {enemy_chance:.1f}%

🏆 Победитель: {winner}!
"""
    
    await message.answer(text)

# Функция анализа текста через Groq API
async def analyze_text(text: str):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    prompt = f"""
Ты аналитик текстовой ролевой игры.
Твоя задача: проанализировать сообщения игрока и определить, какие действия он совершил.

Категории:
- Драка (STR) - выигрыш в драке, занятия спортом, активные действия
- Репутация (REP) - помощь новичкам, красивые посты, ивенты, администрирование, конфликты
- Связи (CON) - сделки, общение, сюжеты
- Кэш (MONEY) - сделки, задания

Верни JSON:
{{
  "str": 0,
  "rep": 0,
  "con": 0,
  "money": 0,
  "bad_boy": 0,
  "good_boy": 0
}}

Текст: {text}
"""
    
    data = {
        "model": "llama-3.1-70b-versatile",
        "messages": [
            {"role": "system", "content": "Ты аналитик текстовой ролевой игры. Верни только JSON."},
            {"role": "user", "content": prompt}
        ]
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                result = await response.json()
                content = result['choices'][0]['message']['content']
                return json.loads(content)
    except:
        return {"str": 0, "rep": 0, "con": 0, "money": 0, "bad_boy": 0, "good_boy": 0}

# Функция обновления статистики
async def update_stats(user_id: int, username: str, text: str):
    # Анализируем текст
    result = await analyze_text(text)
    
    # Проверяем лимиты
    cursor.execute("SELECT * FROM players WHERE user_id = ?", (user_id,))
    player = cursor.fetchone()
    
    if not player:
        # Первый вход - повышенные статы
        cursor.execute('''
        INSERT INTO players (user_id, username, str, rep, con, money, last_post)
        VALUES (?, ?, 4, 4, 4, 500, ?)
        ''', (user_id, username, datetime.now()))
        conn.commit()
        return
    
    # Проверяем сброс лимитов (раз в неделю)
    week_reset = player[16]
    if week_reset and (datetime.now() - week_reset).days >= 7:
        cursor.execute('''
        UPDATE players 
        SET str_week_limit = 0, rep_week_limit = 0, con_week_limit = 0, money_week_limit = 0, week_reset = ?
        WHERE user_id = ?
        ''', (datetime.now(), user_id))
        conn.commit()
    
    # Проверяем лимиты
    str_limit = player[12] + result["str"]
    rep_limit = player[13] + result["rep"]
    con_limit = player[14] + result["con"]
    money_limit = player[15] + result["money"]
    
    # Ограничиваем
    if str_limit > 2:
        result["str"] = 2 - player[12]
    if rep_limit > 7:
        result["rep"] = 7 - player[13]
    if con_limit > 8:
        result["con"] = 8 - player[14]
    if money_limit > 1000:
        result["money"] = 1000 - player[15]
    
    # Обновляем статы
    cursor.execute('''
    UPDATE players 
    SET str = str + ?, rep = rep + ?, con = con + ?, money = money + ?, 
        last_post = ?, bad_boy_count = bad_boy_count + ?, good_boy_count = good_boy_count + ?,
        str_week_limit = str_week_limit + ?, rep_week_limit = rep_week_limit + ?, 
        con_week_limit = con_week_limit + ?, money_week_limit = money_week_limit + ?
    WHERE user_id = ?
    ''', (result["str"], result["rep"], result["con"], result["money"], 
          datetime.now(), result["bad_boy"], result["good_boy"],
          result["str"], result["rep"], result["con"], result["money"], user_id))
    conn.commit()

# Обработчик сообщений из канала
@dp.message()
async def handle_message(message: Message):
    if message.chat.id == int(CHANNEL_ID):
        user_id = message.from_user.id
        username = message.from_user.username
        text = message.text
        
        if text:
            # Проверяем, что сообщение не старше 7 дней
            if message.date and (datetime.now() - message.date).days <= 7:
                await update_stats(user_id, username, text)

# Функция генерации событий (раз в 12 часов)
async def generate_events():
    while True:
        await asyncio.sleep(12 * 60 * 60)  # 12 часов
        
        # Генерируем новость через ИИ
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        
        prompt = """
Сгенерируй новость для текстовой ролевой игры в стиле криминальной хроники.
Город, альтернативная современность, без магии.
Коротко, 2-3 предложения.
"""
        
        data = {
            "model": "llama-3.1-70b-versatile",
            "messages": [
                {"role": "system", "content": "Ты нарративный ИИ для ролевой игры."},
                {"role": "user", "content": prompt}
            ]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=data) as response:
                    result = await response.json()
                    news = result['choices'][0]['message']['content']
                    
                    await bot.send_message(
                        int(CHANNEL_ID),
                        f"📰 НОВОСТИ ГОРОДА\n\n{news}"
                    )
        except:
            pass

# Функция проверки на неактивность (раз в день)
async def check_inactivity():
    while True:
        await asyncio.sleep(24 * 60 * 60)  # 24 часа
        
        # Проверяем игроков
        cursor.execute("SELECT * FROM players WHERE last_post < ?", (datetime.now() - timedelta(days=30),))
        inactive = cursor.fetchall()
        
        for player in inactive:
            user_id = player[0]
            username = player[1]
            
            # Переводим в статус "Читатель"
            cursor.execute('''
            UPDATE players 
            SET status = 'Читатель', str = 2, rep = 2, con = 2, money = 300
            WHERE user_id = ?
            ''', (user_id,))
            conn.commit()
            
            # Отправляем сообщение
            try:
                await bot.send_message(
                    user_id,
                    f"⚠️ Ты долго не писал в канале и переведён в статус «Читатель».\n\nТвои статы обнулены до базовых.\n\nНачни писать снова, чтобы вернуться в игру!"
                )
            except:
                pass

# Функция публикации таблицы игроков (раз в неделю)
async def publish_top():
    while True:
        await asyncio.sleep(7 * 24 * 60 * 60)  # 7 дней
        
        cursor.execute("SELECT username, str, rep, con, money, status, bad_boy_count, good_boy_count FROM players ORDER BY rep DESC LIMIT 10")
        top = cursor.fetchall()
        
        text = "🏆 ТОП ИГРОКОВ\n\n"
        for i, player in enumerate(top, 1):
            badge = ""
            if player[6] > player[7] + 5:
                badge = "🔴 Bad boy"
            elif player[7] > player[6] + 5:
                badge = "🟢 Good boy"
            
            text += f"{i}. @{player[0]} — REP: {player[2]}, STR: {player[1]}, CON: {player[3]}, MONEY: {player[4]} {badge}\n"
        
        try:
            await bot.send_message(
                int(CHANNEL_ID),
                text
            )
        except:
            pass

# Запуск бота
async def main():
    # Запускаем фоновые задачи
    asyncio.create_task(generate_events())
    asyncio.create_task(check_inactivity())
    asyncio.create_task(publish_top())
    
    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
