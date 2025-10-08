import os
import asyncio
import sqlite3
import random
from datetime import datetime, timedelta
import requests
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ========== Конфигурация ==========
TOKEN = "8250715277:AAHnxkwtZSjelvwHTJy_MBte1fuy_dLqk4o"  # ⚠️ замени на свой
DB = "stats.db"
VIP_PHOTO_URL = "https://avatars.mds.yandex.net/i?id=0a32206d7db896dc1412d53ff74ef5b0_l-5386437-images-thumbs&n=13"
PROMO_CODE = "PENISS"
OWNER_ID = 1909652995  # <-- твой Telegram ID (замени на свой)
REL_LEVELS = {
    1: {"name": "Симпатия 😊", "need": 0},
    2: {"name": "Влюблённость 💘", "need": 100},
    3: {"name": "Пара 💑", "need": 300},
    4: {"name": "Души 💞", "need": 600},
    5: {"name": "Семья 💍", "need": 1000},
}
IMPROVE_COST_DIAMONDS = 100
IMPROVE_AFFECTION = 100


# ========== БД и инициализация ==========
def safe_add_column(cursor, table, column, coltype):
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
    except sqlite3.OperationalError:
        pass

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER,
            group_id INTEGER,
            username TEXT,
            total INTEGER DEFAULT 0,
            daily INTEGER DEFAULT 0,
            weekly INTEGER DEFAULT 0,
            monthly INTEGER DEFAULT 0,
            last_msg_date TEXT,
            vip INTEGER DEFAULT 0,
            diamonds INTEGER DEFAULT 0,
            tickets INTEGER DEFAULT 0,
            last_case_time TEXT,
            vip_until TEXT,
            rep INTEGER DEFAULT 0,
            boost_until TEXT,
            PRIMARY KEY(user_id, group_id)
        )
    ''')

    # на всякий случай: добавляем колонки, если таблица была создана старой схемой
    safe_add_column(c, "users", "diamonds", "INTEGER DEFAULT 0")
    safe_add_column(c, "users", "tickets", "INTEGER DEFAULT 0")
    safe_add_column(c, "users", "last_case_time", "TEXT")
    safe_add_column(c, "users", "vip_until", "TEXT")
    safe_add_column(c, "users", "rep", "INTEGER DEFAULT 0")
    safe_add_column(c, "users", "boost_until", "TEXT")

    c.execute('''
        CREATE TABLE IF NOT EXISTS marriages (
            group_id INTEGER,
            user1 INTEGER,
            user2 INTEGER,
            status TEXT,
            timestamp TEXT,
            PRIMARY KEY (group_id, user1, user2)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS relationships (
            group_id INTEGER,
            user1 INTEGER,
            user2 INTEGER,
            level INTEGER DEFAULT 1,
            affection INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            since TEXT,
            PRIMARY KEY (group_id, user1, user2)
        )
    ''')

    conn.commit()
    conn.close()


# ========== Утилиты ==========
def get_username(user: types.User):
    return user.username or (user.full_name if hasattr(user, "full_name") else f"id{user.id}")

def get_username_by_id(user_id: int, group_id: int = None):
    if group_id is None:
        return f"id{user_id}"
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('SELECT username FROM users WHERE user_id=? AND group_id=?', (user_id, group_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else f"id{user_id}"

# ========== Статистика, VIP, алмазы ==========
def update_stats(user: types.User, group_id: int):
    user_id = user.id
    username = get_username(user)
    now_date = datetime.utcnow().date()

    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('SELECT total, daily, weekly, monthly, last_msg_date FROM users WHERE user_id=? AND group_id=?',
              (user_id, group_id))
    row = c.fetchone()

    if not row:
        c.execute(
            "INSERT OR REPLACE INTO users (user_id, group_id, username, total, daily, weekly, monthly, last_msg_date, vip, diamonds, tickets, last_case_time, vip_until) "
            "VALUES (?, ?, ?, 1, 1, 1, 1, ?, 0, 0, 0, NULL, NULL)",
            (user_id, group_id, username, now_date.isoformat())
        )
    else:
        total, daily, weekly, monthly, last_msg = row
        last_msg_date = None
        if last_msg:
            try:
                last_msg_date = datetime.strptime(last_msg, "%Y-%m-%d").date()
            except Exception:
                last_msg_date = None

        new_daily = daily + 1 if last_msg_date == now_date else 1
        last_week = last_msg_date.isocalendar()[1] if last_msg_date else -1
        new_week = weekly + 1 if (last_msg_date and last_week == now_date.isocalendar()[1]
                                  and last_msg_date.year == now_date.year) else 1
        new_month = monthly + 1 if (last_msg_date and (last_msg_date.year, last_msg_date.month) == (now_date.year, now_date.month)) else 1

        c.execute('''
            UPDATE users
            SET username=?, total=total+1, daily=?, weekly=?, monthly=?, last_msg_date=?
            WHERE user_id=? AND group_id=?
        ''', (username, new_daily, new_week, new_month, now_date.isoformat(), user_id, group_id))

    conn.commit()
    conn.close()

def add_diamonds(user_id: int, group_id: int, amount: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('UPDATE users SET diamonds=COALESCE(diamonds,0)+? WHERE user_id=? AND group_id=?',
              (amount, user_id, group_id))
    conn.commit()
    conn.close()

def get_user_diamonds(user_id: int, group_id: int) -> int:
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT COALESCE(diamonds,0) FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def spend_diamonds(user_id: int, group_id: int, amount: int) -> bool:
    have = get_user_diamonds(user_id, group_id)
    if have < amount:
        return False
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("UPDATE users SET diamonds=diamonds-? WHERE user_id=? AND group_id=?", (amount, user_id, group_id))
    conn.commit()
    conn.close()
    return True

def can_open_case(user_id: int, group_id: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('SELECT last_case_time FROM users WHERE user_id=? AND group_id=?', (user_id, group_id))
    row = c.fetchone()
    conn.close()
    if not row or not row[0]:
        return True, None
    try:
        last_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return True, None
    now = datetime.utcnow()
    if now - last_time >= timedelta(hours=3):
        return True, None
    else:
        left = timedelta(hours=3) - (now - last_time)
        return False, left

def set_case_time(user_id: int, group_id: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('UPDATE users SET last_case_time=? WHERE user_id=? AND group_id=?',
              (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), user_id, group_id))
    conn.commit()
    conn.close()

def convert_diamonds_to_ticket(user_id: int, group_id: int) -> bool:
    if get_user_diamonds(user_id, group_id) < 300:
        return False
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("UPDATE users SET diamonds=diamonds-300, tickets=COALESCE(tickets,0)+1 WHERE user_id=? AND group_id=?",
              (user_id, group_id))
    conn.commit()
    conn.close()
    return True

def activate_diamond_boost(user_id: int, group_id: int, hours: int = 3):
    until = (datetime.utcnow() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    # гарантируем колонку
    safe_add_column(c, "users", "boost_until", "TEXT")
    # если пользователя нет — создаём минимальную запись
    c.execute("SELECT 1 FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    if not c.fetchone():
        c.execute("INSERT OR IGNORE INTO users (user_id, group_id, username) VALUES (?, ?, ?)", (user_id, group_id, f"id{user_id}"))
    c.execute("UPDATE users SET boost_until=? WHERE user_id=? AND group_id=?", (until, user_id, group_id))
    conn.commit()
    conn.close()

def is_diamond_boost_active(user_id: int, group_id: int) -> bool:
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    try:
        c.execute("SELECT boost_until FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    except sqlite3.OperationalError:
        conn.close()
        return False
    row = c.fetchone()
    conn.close()
    if not row or not row[0]:
        return False
    try:
        until = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
        return datetime.utcnow() < until
    except Exception:
        return False

def boost_time_left(user_id: int, group_id: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    try:
        c.execute("SELECT boost_until FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    except sqlite3.OperationalError:
        conn.close()
        return None
    row = c.fetchone()
    conn.close()
    if not row or not row[0]:
        return None
    try:
        until = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
        left = until - datetime.utcnow()
        return left if left.total_seconds() > 0 else None
    except Exception:
        return None

def give_diamonds(admin_id: int, target_id: int, group_id: int, amount: int):
    if admin_id != OWNER_ID:
        return False, "Ты не владелец бота."
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    safe_add_column(c, "users", "diamonds", "INTEGER DEFAULT 0")
    # гарантируем запись пользователя
    c.execute("INSERT OR IGNORE INTO users (user_id, group_id, username) VALUES (?, ?, ?)",
              (target_id, group_id, f"id{target_id}"))
    c.execute("UPDATE users SET diamonds = COALESCE(diamonds,0) + ? WHERE user_id=? AND group_id=?",
              (amount, target_id, group_id))
    conn.commit()
    conn.close()
    return True, f"💎 Выдал {amount} алмаз(ов)."

def give_tickets(admin_id: int, target_id: int, group_id: int, amount: int):
    if admin_id != OWNER_ID:
        return False, "Ты не владелец бота."
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    safe_add_column(c, "users", "tickets", "INTEGER DEFAULT 0")
    c.execute("INSERT OR IGNORE INTO users (user_id, group_id, username) VALUES (?, ?, ?)",
              (target_id, group_id, f"id{target_id}"))
    c.execute("UPDATE users SET tickets = COALESCE(tickets,0) + ? WHERE user_id=? AND group_id=?",
              (amount, target_id, group_id))
    conn.commit()
    conn.close()
    return True, f"🎫 Выдал {amount} билет(ов)."

def give_vip(admin_id: int, target_id: int, group_id: int, days: int):
    if admin_id != OWNER_ID:
        return False, "Ты не владелец бота."
    until = (datetime.utcnow() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    safe_add_column(c, "users", "vip_until", "TEXT")
    c.execute("INSERT OR IGNORE INTO users (user_id, group_id, username) VALUES (?, ?, ?)",
              (target_id, group_id, f"id{target_id}"))
    c.execute("UPDATE users SET vip = 1, vip_until = ? WHERE user_id = ? AND group_id = ?",
              (until, target_id, group_id))
    conn.commit()
    conn.close()
    return True, f"🌟 VIP выдан на {days} дней."

def gift_diamonds(from_id: int, to_id: int, group_id: int, amount: int):
    if amount <= 0:
        return False, "Сумма должна быть положительной."
    have = get_user_diamonds(from_id, group_id)
    if have < amount:
        return False, "Недостаточно алмазов."
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    # убедимся, что запись для to_id есть
    safe_add_column(c, "users", "diamonds", "INTEGER DEFAULT 0")
    c.execute("INSERT OR IGNORE INTO users (user_id, group_id, username) VALUES (?, ?, ?)",
              (to_id, group_id, f"id{to_id}"))
    # списываем
    c.execute("UPDATE users SET diamonds = diamonds - ? WHERE user_id=? AND group_id=?",
              (amount, from_id, group_id))
    # даём
    c.execute("UPDATE users SET diamonds = COALESCE(diamonds,0) + ? WHERE user_id=? AND group_id=?",
              (amount, to_id, group_id))
    conn.commit()
    conn.close()
    return True, f"🎁 Вы подарили {amount} алмазов."

def get_full_stats(group_id: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    # сколько пользователей
    c.execute("SELECT COUNT(*) FROM users WHERE group_id=?", (group_id,))
    total_users = c.fetchone()[0]
    # топ по сообщениям (total)
    c.execute("SELECT username, total FROM users WHERE group_id=? ORDER BY total DESC LIMIT 5", (group_id,))
    top_msgs = c.fetchall()
    # топ по алмазам
    c.execute("SELECT username, diamonds FROM users WHERE group_id=? ORDER BY diamonds DESC LIMIT 5", (group_id,))
    top_diamonds = c.fetchall()
    # топ по репутации
    c.execute("SELECT username, rep FROM users WHERE group_id=? ORDER BY rep DESC LIMIT 5", (group_id,))
    top_rep = c.fetchall()
    # общая сумма алмазов, среднее
    c.execute("SELECT SUM(diamonds), AVG(diamonds) FROM users WHERE group_id=?", (group_id,))
    sum_avg = c.fetchone()
    conn.close()
    return {
        "total_users": total_users,
        "top_msgs": top_msgs,
        "top_diamonds": top_diamonds,
        "top_rep": top_rep,
        "sum_diamonds": sum_avg[0] or 0,
        "avg_diamonds": sum_avg[1] or 0.0,
    }






# === Репутация ===
def safe_add_rep_column():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    safe_add_column(c, "users", "rep", "INTEGER DEFAULT 0")
    
    conn.commit()
    conn.close()

def change_rep(group_id: int, from_id: int, to_id: int, delta: int):
    if from_id == to_id:
        return "❌ Нельзя изменить репутацию самому себе."
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    # создаём минимальную запись, если отсутствует
    c.execute("INSERT OR IGNORE INTO users (user_id, group_id, username) VALUES (?, ?, ?)",
              (to_id, group_id, f"id{to_id}"))
    c.execute("UPDATE users SET rep=COALESCE(rep,0)+? WHERE user_id=? AND group_id=?", (delta, to_id, group_id))
    conn.commit()
    c.execute("SELECT rep FROM users WHERE user_id=? AND group_id=?", (to_id, group_id))
    new_rep = c.fetchone()[0]
    conn.close()
    if delta > 0:
        return f"👍 Репутация повышена! Сейчас: {new_rep}."
    else:
        return f"👎 Репутация снижена. Сейчас: {new_rep}."



def set_vip_for_3_days(user_id: int, group_id: int):
    until = (datetime.utcnow() + timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("UPDATE users SET vip=1, vip_until=? WHERE user_id=? AND group_id=?", (until, user_id, group_id))
    conn.commit()
    conn.close()

def has_activated_promo(user_id: int, group_id: int) -> bool:
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT vip_until FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    row = c.fetchone()
    conn.close()
    if row and row[0]:
        try:
            until = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            return datetime.utcnow() < until
        except Exception:
            return False
    return False

def is_vip_active(user_id: int, group_id: int) -> bool:
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT vip, vip_until FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    row = c.fetchone()
    conn.close()
    if not row or row[0] != 1:
        return False
    if row[1]:
        try:
            until = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
            if datetime.utcnow() > until:
                conn2 = sqlite3.connect(DB)
                c2 = conn2.cursor()
                c2.execute("UPDATE users SET vip=0 WHERE user_id=? AND group_id=?", (user_id, group_id))
                conn2.commit()
                conn2.close()
                return False
            return True
        except Exception:
            return False
    return True



def get_top(group_id: int, period: str = "total", limit: int = 10):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    if period not in ["daily", "weekly", "monthly", "total"]:
        period = "total"
    c.execute(f"SELECT username, {period} FROM users WHERE group_id=? ORDER BY {period} DESC LIMIT ?", (group_id, limit))
    rows = c.fetchall()
    conn.close()
    return rows

def format_top(rows):
    text = ""
    for i, (username, count) in enumerate(rows, 1):
        text += f"{i}. {username} — {count}\n"
    return text



# ========== Отношения ==========
def propose_relationship(group_id: int, proposer_id: int, responder_id: int):
    if proposer_id == responder_id:
        return False, "Нельзя предложить отношения самому себе."
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT * FROM relationships WHERE group_id=? AND status='dating' AND (user1=? OR user2=?)",
              (group_id, proposer_id, proposer_id))
    if c.fetchone():
        conn.close()
        return False, "У тебя уже есть отношения."
    c.execute("SELECT * FROM relationships WHERE group_id=? AND status='dating' AND (user1=? OR user2=?)",
              (group_id, responder_id, responder_id))
    if c.fetchone():
        conn.close()
        return False, "У пользователя уже есть отношения."
    c.execute("SELECT * FROM relationships WHERE group_id=? AND user1=? AND user2=?", (group_id, proposer_id, responder_id))
    if c.fetchone():
        conn.close()
        return False, "Ты уже сделал предложение этому человеку."
    c.execute("INSERT OR REPLACE INTO relationships (group_id, user1, user2, level, affection, status, since) VALUES (?, ?, ?, 1, 0, 'pending', ?)",
              (group_id, proposer_id, responder_id, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return True, "OK"

def respond_relationship(group_id: int, proposer_id: int, responder_id: int, accept: bool):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT status FROM relationships WHERE group_id=? AND user1=? AND user2=?", (group_id, proposer_id, responder_id))
    row = c.fetchone()
    if not row:
        conn.close()
        return False, "Нет такого предложения."
    status = row[0]
    if status != "pending":
        conn.close()
        return False, "Предложение уже обработано."
    if accept:
        c.execute("UPDATE relationships SET status='dating', since=? WHERE group_id=? AND user1=? AND user2=?",
                  (datetime.utcnow().isoformat(), group_id, proposer_id, responder_id))
        msg = "💞 Вы теперь встречаетесь!"
    else:
        c.execute("DELETE FROM relationships WHERE group_id=? AND user1=? AND user2=?", (group_id, proposer_id, responder_id))
        msg = "❌ Предложение отклонено."
    conn.commit()
    conn.close()
    return accept, msg

def get_relationship_info(group_id: int, a_id: int, b_id: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""
        SELECT user1, user2, level, affection, status, since
        FROM relationships
        WHERE group_id=? AND ((user1=? AND user2=?) OR (user1=? AND user2=?))
    """, (group_id, a_id, b_id, b_id, a_id))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    u1, u2, lvl, aff, st, sinc = row
    return {"user1": u1, "user2": u2, "level": lvl, "affection": aff, "status": st, "since": sinc}

def improve_relationship(group_id: int, actor_id: int, other_id: int):
    info = get_relationship_info(group_id, actor_id, other_id)
    if not info or info["status"] != "dating":
        return False, "У вас нет отношений для улучшения."
    if not spend_diamonds(actor_id, group_id, IMPROVE_COST_DIAMONDS):
        return False, f"Недостаточно алмазов. Нужно {IMPROVE_COST_DIAMONDS}."
    new_aff = info["affection"] + IMPROVE_AFFECTION
    new_lvl = info["level"]
    while (new_lvl + 1) in REL_LEVELS and new_aff >= REL_LEVELS[new_lvl + 1]["need"]:
        new_lvl += 1
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""
        UPDATE relationships
        SET affection=?, level=?
        WHERE group_id=? AND ((user1=? AND user2=?) OR (user1=? AND user2=?))
    """, (new_aff, new_lvl, group_id, actor_id, other_id, other_id, actor_id))
    conn.commit()
    conn.close()
    return True, f"Отношения улучшены: ❤={new_aff}, уровень={new_lvl} ({REL_LEVELS.get(new_lvl, {}).get('name')})"

def break_relationship(group_id: int, user_id: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("DELETE FROM relationships WHERE group_id=? AND (user1=? OR user2=?)", (group_id, user_id, user_id))
    cnt = c.rowcount
    conn.commit()
    conn.close()
    if cnt:
        return True, "Отношения завершены."
    else:
        return False, "У тебя нет отношений."

# ========== Брак ==========
def propose_marriage(group_id: int, proposer_id: int, responder_id: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT * FROM marriages WHERE group_id=? AND status='accepted' AND (user1=? OR user2=?)",
              (group_id, proposer_id, proposer_id))
    if c.fetchone():
        conn.close()
        return False, "У тебя уже есть брак!"
    c.execute("SELECT * FROM marriages WHERE group_id=? AND status='accepted' AND (user1=? OR user2=?)",
              (group_id, responder_id, responder_id))
    if c.fetchone():
        conn.close()
        return False, "У пользователя уже есть брак!"
    c.execute("SELECT * FROM marriages WHERE group_id=? AND status='pending' AND user1=? AND user2=?",
              (group_id, proposer_id, responder_id))
    if c.fetchone():
        conn.close()
        return False, "Ты уже сделал предложение этому человеку."
    c.execute("INSERT INTO marriages (group_id, user1, user2, status, timestamp) VALUES (?, ?, ?, 'pending', ?)",
              (group_id, proposer_id, responder_id, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return True, "OK"

def respond_marriage(group_id: int, proposer_id: int, responder_id: int, accept: bool):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT * FROM marriages WHERE group_id=? AND user1=? AND user2=? AND status='pending'",
              (group_id, proposer_id, responder_id))
    if not c.fetchone():
        conn.close()
        return "Нет такого предложения."
    if accept:
        c.execute("UPDATE marriages SET status='accepted' WHERE group_id=? AND user1=? AND user2=?",
                  (group_id, proposer_id, responder_id))
        msg = "💍 Брак заключён!"
    else:
        c.execute("DELETE FROM marriages WHERE group_id=? AND user1=? AND user2=?",
                  (group_id, proposer_id, responder_id))
        msg = "❌ Предложение отклонено."
    conn.commit()
    conn.close()
    return msg

def get_my_marriage(group_id: int, user_id: int) -> str:
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT user1, user2 FROM marriages WHERE group_id=? AND status='accepted' AND (user1=? OR user2=?)",
              (group_id, user_id, user_id))
    row = c.fetchone()
    conn.close()
    if row:
        other = row[1] if row[0] == user_id else row[0]
        return f"💍 Ты в браке с <a href='tg://user?id={other}'>пользователем</a>"
    conn2 = sqlite3.connect(DB)
    c2 = conn2.cursor()
    c2.execute("SELECT user1 FROM marriages WHERE group_id=? AND user2=? AND status='pending'", (group_id, user_id))
    row2 = c2.fetchone()
    conn2.close()
    if row2:
        return "💌 У тебя есть предложение в ожидании."
    return "У тебя нет брака."

def divorce(group_id: int, user_id: int) -> str:
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("DELETE FROM marriages WHERE group_id=? AND status='accepted' AND (user1=? OR user2=?)",
              (group_id, user_id, user_id))
    conn.commit()
    conn.close()
    return "❌ Брак расторгнут."

# ========== Бот и обработчики ==========
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

@dp.message()
async def handle_all(message: Message):
    if not message.chat or not message.chat.type or not message.chat.type.endswith("group"):
        return
    if not message.text:
        return

    text = message.text.strip()
    tl = text.lower()
    group_id = message.chat.id
    user = message.from_user

    try:
        update_stats(user, group_id)
    except Exception as e:
        print("update_stats error:", e)

    # — кейсы —
    if tl.startswith("кейс"):
        can, left = can_open_case(user.id, group_id)
        if not can and left:
            sec = int(left.total_seconds())
            h, rem = divmod(sec, 3600)
            m, s = divmod(rem, 60)
            await message.reply(f"⏳ Уже открывал! Ждать: <b>{h}ч {m}м {s}с</b>")
        else:
            amt = random.randint(5, 35)
            if is_diamond_boost_active(user.id, group_id):
                amt *= 2
            add_diamonds(user.id, group_id, amt)
            set_case_time(user.id, group_id)
            await message.reply(f"🎁 Ты открыл кейс и получил <b>{amt} алмазов</b>!")
        return

    # — обмен —
    if tl.startswith("билет") or tl.startswith("обмен"):
        if convert_diamonds_to_ticket(user.id, group_id):
            await message.reply("🎫 Ты обменял 300 алмазов на 1 билет.")
        else:
            await message.reply("❌ Недостаточно алмазов (нужно 300).")
        return

    # — промокод —
    if tl.startswith("промик"):
        parts = message.text.split()
        if len(parts) == 2 and parts[1].upper() == PROMO_CODE:
            if has_activated_promo(user.id, group_id):
                await message.reply("❗️Ты уже активировал промокод или VIP уже действует.")
            else:
                set_vip_for_3_days(user.id, group_id)
                await message.reply("🎉 Промокод активирован! VIP на 3 дня.")
        else:
            await message.reply("❌ Неверный промокод.")
        return

        # — помощь —
    if tl.startswith("помощ") or tl == "help":
        help_text = (
            "💡 <b>Команды бота:</b>\n\n"
            "📈 <b>Статистика</b>\n"
            "• стата [день/неделя/месяц/все] — показать активных пользователей\n"
            "• топ [день/неделя/месяц/все] — топ по сообщениям\n\n"
            "💎 <b>Экономика</b>\n"
            "• кейс — открыть кейс (раз в 3 часа)\n"
            "• билет / обмен — обменять 300 алмазов на билет\n"
            "❤️ <b>Отношения</b>\n"
            "• предложить [@ник или ответом] — предложить отношения\n"
            "• отношения [@ник] — посмотреть отношения\n"
            "• улучшить (ответом) — повысить уровень за 100 алмазов\n"
            "• расстаться — закончить отношения\n\n"
            "💍 <b>Брак</b>\n"
            "• брак [@ник или ответом] — предложение руки и сердца\n"
            "• принять / отклонить (ответом) — ответ на предложение\n"
            "• развод — расторгнуть брак\n"
            "• брак — посмотреть текущий брак\n\n"
            "⭐️ <b>Профиль</b>\n"
            "• профиль — показать свою статистику и данные\n"
            "• реп [@ник или ответом] + / - — изменить репутацию пользователя\n\n"
            "📚 <b>Прочее</b>\n"
            "• помощь — показать это сообщение"
        )
        await message.reply(help_text)
        return



    # — алмазный буст —
    if tl.startswith("буст"):
        if is_diamond_boost_active(user.id, group_id):
            left = boost_time_left(user.id, group_id)
            if left:
                h, m = divmod(int(left.total_seconds()) // 60, 60)
                await message.reply(f"⚡ Алмазный буст уже активен! Осталось {h}ч {m}м.")
            return
        if not spend_diamonds(user.id, group_id, 500):
            await message.reply("❌ Нужно 500 алмазов для активации буста.")
            return
        activate_diamond_boost(user.id, group_id)
        await message.reply("💎 Алмазный буст активирован на 3 часа! Все награды из кейсов x2.")
        return
    
    # — профиль —
    if tl.startswith("профиль"):
        txt, vip = get_user_profile(user.id, group_id)
        if vip:
            await message.answer_photo(VIP_PHOTO_URL, caption=txt)
        else:
            await message.reply(txt)
        return

    # — отношения: предложить —
    if tl.startswith("предложить"):
        partner_id = None
        if message.reply_to_message:
            partner_id = message.reply_to_message.from_user.id
        else:
            parts = text.split()
            if len(parts) >= 2:
                nick = parts[1].lstrip("@")
                partner_id = find_user_id_by_nick(group_id, nick)
        if not partner_id:
            await message.reply("Не удалось определить пользователя. Ответь или укажи ник: `предложить @ник`.")
            return
        if partner_id == user.id:
            await message.reply("Нельзя предложить отношения самому себе.")
            return
        ok, msg = propose_relationship(group_id, user.id, partner_id)
        if not ok:
            await message.reply(msg)
        else:
            proposer = get_username(user)
            partner_name = get_username_by_id(partner_id, group_id)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="💞 Принять",
                        callback_data=f"rel_accept:{user.id}:{partner_id}:{group_id}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Отклонить",
                        callback_data=f"rel_decline:{user.id}:{partner_id}:{group_id}"
                    )
                ]
            ])
            await message.answer(f"💌 <b>{proposer}</b> предлагает отношения <b>{partner_name}</b>!", reply_markup=kb)
        return



    # — показать отношения —
    if tl.startswith("отношения"):
        target = None
        if message.reply_to_message:
            target = message.reply_to_message.from_user.id
        else:
            parts = text.split()
            if len(parts) >= 2:
                nick = parts[1]
                conn = sqlite3.connect(DB)
                c = conn.cursor()
                c.execute('SELECT user_id FROM users WHERE group_id=? AND username LIKE ?', (group_id, f"%{nick}%"))
                row = c.fetchone()
                conn.close()
                if row:
                    target = row[0]
        if not target:
            await message.reply("Ответь на сообщение пользователя.")
            return
        info = get_relationship_info(group_id, user.id, target)
        if not info:
            await message.reply("У вас нет отношений.")
        else:
            lvl = info["level"]
            aff = info["affection"]
            st = info["status"]
            sinc = info["since"] or "неизвестно"
            pid = info["user2"] if info["user1"] == user.id else info["user1"]
            pname = get_username_by_id(pid, group_id)
            lvl_name = REL_LEVELS.get(lvl, {}).get("name", f"Уровень {lvl}")
            await message.reply(
                f"💞 Отношения с {pname}\n"
                f"Статус: <b>{st}</b>\n"
                f"Уровень: <b>{lvl_name}</b> ({lvl})\n"
                f"Очки симпатии: <b>{aff}</b>\n"
                f"С: <b>{sinc}</b>"
            )
        return

    # — улучшить отношения —
    if tl.startswith("улучшить"):
        if not message.reply_to_message:
            await message.reply("Ответь на сообщение партнёра командой `улучшить`.")
            return
        partner_id = message.reply_to_message.from_user.id
        ok, msg = improve_relationship(group_id, user.id, partner_id)
        await message.reply(msg)
        return
    
    # === Команда владельца "выдать ..." ===
    OWNER_ID = 1909652995  # 👈 замени на свой ID, если нужно

    if user.id == OWNER_ID and tl.startswith("выдать"):
        parts = text.split()
        if len(parts) < 3:
            await message.reply("⚙️ Формат: выдать [алмазы/вип/билеты] [число] [@ник или ответом]")
            return

        what = parts[1].lower()
        try:
            amount = int(parts[2])
        except ValueError:
            await message.reply("❗ Кол-во должно быть числом.")
            return

        # определяем кому выдавать
        target_id = None
        if message.reply_to_message:
            target_id = message.reply_to_message.from_user.id
            target_name = get_username(message.reply_to_message.from_user)
        elif len(parts) >= 4 and parts[3].startswith("@"):
            nick = parts[3].lstrip("@")
            target_id = find_user_id_by_nick(group_id, nick)
            target_name = nick
        else:
            await message.reply("⚙️ Укажи пользователя через @ или ответом.")
            return

        if not target_id:
    # если пользователь не найден — создаём запись вручную
            target_name = nick
            conn = sqlite3.connect(DB)
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO users (user_id, group_id, username) VALUES (?, ?, ?)",
                      (random.randint(100000000, 999999999), group_id, target_name))
            conn.commit()
            conn.close()
            await message.reply(f"⚠️ Пользователь {target_name} не найден в БД, добавлен вручную.")
            return

        conn = sqlite3.connect(DB)
        c = conn.cursor()

        # выдача по типу
        if what in ["алмазы", "алмаз", "diamond", "diamonds"]:
            c.execute("UPDATE users SET diamonds = COALESCE(diamonds,0) + ? WHERE user_id=? AND group_id=?", (amount, target_id, group_id))
            res_icon = "💎"
            msg = f"✅ Выдано {amount} {res_icon} пользователю <b>{target_name}</b>."
        elif what in ["билеты", "билет", "ticket", "tickets"]:
            c.execute("UPDATE users SET tickets = COALESCE(tickets,0) + ? WHERE user_id=? AND group_id=?", (amount, target_id, group_id))
            res_icon = "🎫"
            msg = f"✅ Выдано {amount} {res_icon} пользователю <b>{target_name}</b>."
        elif what in ["вип", "vip"]:
            # VIP выдаётся в днях
            until = (datetime.utcnow() + timedelta(days=amount)).strftime("%Y-%m-%d %H:%M:%S")
            c.execute("UPDATE users SET vip=1, vip_until=? WHERE user_id=? AND group_id=?", (until, target_id, group_id))
            res_icon = "🌟"
            msg = f"✅ Выдан VIP {res_icon} на {amount} дн. пользователю <b>{target_name}</b>."
        else:
            conn.close()
            await message.reply("❌ Неизвестный тип. Используй: алмазы, билеты, вип.")
            return

        conn.commit()
        conn.close()

        await message.reply(msg)
        return



    # — топ пользователей —
    if tl.startswith("топ"):
        parts = tl.split()
        period = parts[1] if len(parts) > 1 else "все"
        period_map = {"день": "daily", "неделя": "weekly", "месяц": "monthly", "все": "total"}
        top_period = period_map.get(period, "total")
        top_users = get_top(group_id, top_period)
        if not top_users:
            await message.reply("Пока нет данных для топа.")
        else:
            txt = f"🏆 Топ {period}:\n" + format_top(top_users)
            await message.reply(txt)
        return

    # — подарить — команда "подарить @ник 100" или "подарить 100" ответом
    if tl.startswith("подарить"):
        parts = text.split()
        target_id = None
        amount = None
        if message.reply_to_message:
            target_id = message.reply_to_message.from_user.id
            if len(parts) >= 2:
                try:
                    amount = int(parts[1])
                except:
                    pass
        else:
            if len(parts) >= 3:
                # parts[1] — ник, parts[2] — сумма
                nick = parts[1].lstrip("@")
                try:
                    amount = int(parts[2])
                except:
                    amount = None
                target_id = find_user_id_by_nick(group_id, nick)
        if not target_id or amount is None:
            await message.reply("Используй: `подарить @ник <число>` или ответив на сообщение: `подарить <число>`")
            return
        ok, msg = gift_diamonds(user.id, target_id, group_id, amount)
        await message.reply(msg)
        return

    if tl.startswith("всястат") or tl.startswith("/всястат"):
        st = get_full_stats(group_id)
        txt = (
            f"📊 Всестатистика группы\n\n"
            f"👥 Пользователей: {st['total_users']}\n"
            f"💎 Всего алмазов у участников: {st['sum_diamonds']}\n"
            f"💠 Среднее алмазов: {st['avg_diamonds']:.2f}\n\n"
            "🏆 Топ по сообщениям:\n"
        )
        for i, (name, cnt) in enumerate(st["top_msgs"], 1):
            txt += f"{i}. {name} — {cnt}\n"
        txt += "\n💎 Топ по алмазам:\n"
        for i, (name, cnt) in enumerate(st["top_diamonds"], 1):
            txt += f"{i}. {name} — {cnt}\n"
        txt += "\n📝 Топ по репутации:\n"
        for i, (name, cnt) in enumerate(st["top_rep"], 1):
            txt += f"{i}. {name} — {cnt}\n"
        await message.reply(txt)
        return


    # — статистика пользователей —
    if tl.startswith("стата"):
        parts = tl.split()
        period = parts[1] if len(parts) > 1 else "день"
        period_map = {"день": "daily", "неделя": "weekly", "месяц": "monthly", "все": "total"}
        stat_period = period_map.get(period, "daily")
        conn = sqlite3.connect(DB)
        c = conn.cursor()
        c.execute(f"SELECT username, {stat_period} FROM users WHERE group_id=? ORDER BY {stat_period} DESC LIMIT 10", (group_id,))
        rows = c.fetchall()
        conn.close()
        if not rows:
            await message.reply("Нет данных для статистики.")
        else:
            txt = f"📊 Статистика за {period}:\n" + format_top(rows)
            await message.reply(txt)
        return

    # — расстаться —
    if tl.startswith("расстаться"):
        ok, msg = break_relationship(group_id, user.id)
        await message.reply(msg)
        return

    # — брак —
    if tl.startswith("брак"):
        parts = text.split()
        partner_id = None
        if len(parts) == 1 and not message.reply_to_message:
            await message.reply(get_my_marriage(group_id, user.id))
            return
        if message.reply_to_message:
            partner_id = message.reply_to_message.from_user.id
        elif len(parts) >= 2:
            partner_id = find_user_id_by_nick(group_id, parts[1])
        if not partner_id:
            await message.reply("Пользователь не найден.")
            return
        if partner_id == user.id:
            await message.reply("Нельзя жениться на себе :)")
            return
        ok, msg = propose_marriage(group_id, user.id, partner_id)
        if not ok:
            await message.reply(msg)
        else:
            proposer = get_username(user)
            pname = get_username_by_id(partner_id, group_id)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="💍 Принять",
                        callback_data=f"marriage_accept:{user.id}:{partner_id}:{group_id}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Отклонить",
                        callback_data=f"marriage_decline:{user.id}:{partner_id}:{group_id}"
                    )
                ]
            ])
            await message.answer(f"💌 <b>{proposer}</b> предлагает брак <b>{pname}</b>!", reply_markup=kb)
        return
    


    # --- ПРОШЕПТАТЬ ---
    if tl.startswith("прошептать"):
        import uuid, time, asyncio
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

        # глобальное временное хранилище секретов
        global whispers, whispers_lock
        if "whispers" not in globals():
            whispers = {}
            whispers_lock = asyncio.Lock()
        WHISPER_TTL = 60 * 60 * 24  # сутки

        parts = text.split(maxsplit=2)
        recipient_id = None
        whisper_text = None

        # Вариант 1: ответом на сообщение
        if message.reply_to_message:
            recipient_id = message.reply_to_message.from_user.id
            whisper_text = text[len("прошептать"):].strip()
            if not whisper_text:
                await message.reply("Напиши текст прошептанного сообщения.")
                return

        # Вариант 2: ник и текст в одной строке
        elif len(parts) >= 3:
            nick = parts[1].lstrip("@")
            recipient_id = find_user_id_by_nick(group_id, nick)
            whisper_text = parts[2].strip()

        else:
            await message.reply("Используй: `прошептать @ник текст` или ответом на сообщение: `прошептать текст`")
            return

        if not recipient_id:
            await message.reply("❌ Не удалось определить пользователя.")
            return
        if recipient_id == user.id:
            await message.reply("Нельзя прошептать самому себе.")
            return

        # сохраняем сообщение
        token = uuid.uuid4().hex
        async with whispers_lock:
            whispers[token] = {
                "text": whisper_text,
                "from": user.id,
                "to": recipient_id,
                "created": time.time()
            }

        sender_name = get_username(user)
        rec_name = get_username_by_id(recipient_id, group_id)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="🔒 Открыть прошептанное",
                callback_data=f"whisper_open:{token}:{recipient_id}:{user.id}"
            )
        ]])
        await message.reply(
            f"💬 <b>{sender_name}</b> прошептал(а) <b>{rec_name}</b>.",
            reply_markup=kb
        )
        return



    # — репутация —
    if tl.startswith("+реп"):
        target_id = None
        if message.reply_to_message:
            target_id = message.reply_to_message.from_user.id
        else:
            parts = text.split()
            if len(parts) >= 2:
                target_id = find_user_id_by_nick(group_id, parts[1].lstrip("@"))
        if not target_id:
            await message.reply("Укажи пользователя — ответом или ником: +реп @ник")
            return
        msg = change_rep(group_id, user.id, target_id, +1)
        await message.reply(msg)
        return

    if tl.startswith("-реп"):
        target_id = None
        if message.reply_to_message:
            target_id = message.reply_to_message.from_user.id
        else:
            parts = text.split()
            if len(parts) >= 2:
                target_id = find_user_id_by_nick(group_id, parts[1].lstrip("@"))
        if not target_id:
            await message.reply("Укажи пользователя — ответом или ником: -реп @ник")
            return
        msg = change_rep(group_id, user.id, target_id, -1)
        await message.reply(msg)
        return


    if tl.startswith("принять") and message.reply_to_message:
        pid = message.reply_to_message.from_user.id
        rid = user.id
        await message.reply(respond_marriage(group_id, pid, rid, accept=True))
        return
    if tl.startswith("отклонить") and message.reply_to_message:
        pid = message.reply_to_message.from_user.id
        rid = user.id
        await message.reply(respond_marriage(group_id, pid, rid, accept=False))
        return
    if tl.startswith("развод"):
        await message.reply(divorce(group_id, user.id))
        return

@dp.callback_query(F.data.startswith("rel_accept"))
async def cb_rel_accept(callback: CallbackQuery):
    _, pid, rid, gid = callback.data.split(":")
    pid, rid, gid = int(pid), int(rid), int(gid)
    if callback.from_user.id != rid:
        await callback.answer("Это не для тебя!", show_alert=True)
        return
    ok, msg = respond_relationship(gid, pid, rid, accept=True)
    if ok:
        pname = get_username_by_id(pid, gid)
        rname = get_username(callback.from_user)
        await callback.message.edit_text(f"🎉 <b>{rname}</b> и <b>{pname}</b> теперь встречаются! ❤️")
    else:
        await callback.message.edit_text(msg)
    await callback.answer(msg)
@dp.callback_query(F.data.startswith("whisper_open"))
async def cb_whisper_open(callback: CallbackQuery):
    import time
    try:
        _, token, rid, sid = callback.data.split(":")
        rid, sid = int(rid), int(sid)
    except:
        await callback.answer("Ошибка формата.", show_alert=True)
        return

    if callback.from_user.id != rid:
        await callback.answer("Это не для тебя!", show_alert=True)
        return

    async with whispers_lock:
        data = whispers.get(token)
        if not data:
            await callback.answer("Прошептанное сообщение не найдено или уже открыто.", show_alert=True)
            return
        if data["to"] != rid:
            await callback.answer("Это не для тебя!", show_alert=True)
            return
        text = data["text"]
        del whispers[token]

    await callback.answer(text, show_alert=True)
    try:
        await callback.message.edit_text(callback.message.text + "\n\n🔓 Прошептанное открыто.")
    except:
        pass

@dp.callback_query(F.data.startswith("rel_decline"))
async def cb_rel_decline(callback: CallbackQuery):
    _, pid, rid, gid = callback.data.split(":")
    pid, rid, gid = int(pid), int(rid), int(gid)
    if callback.from_user.id != rid:
        await callback.answer("Это не для тебя!", show_alert=True)
        return
    ok, msg = respond_relationship(gid, pid, rid, accept=False)
    await callback.message.edit_text("❌ <b>Предложение отклонено.</b>")
    await callback.answer(msg)

@dp.callback_query(F.data.startswith("whisper_open"))
async def cb_whisper_open(callback: CallbackQuery):
    import time
    try:
        _, token, rid, sid = callback.data.split(":")
        rid, sid = int(rid), int(sid)
    except:
        await callback.answer("Ошибка формата.", show_alert=True)
        return

    if callback.from_user.id != rid:
        await callback.answer("Это не для тебя!", show_alert=True)
        return

    async with whispers_lock:
        data = whispers.get(token)
        if not data:
            await callback.answer("Прошептанное сообщение не найдено или уже открыто.", show_alert=True)
            return
        if data["to"] != rid:
            await callback.answer("Это не для тебя!", show_alert=True)
            return
        text = data["text"]
        del whispers[token]

    await callback.answer(text, show_alert=True)
    try:
        await callback.message.edit_text(callback.message.text + "\n\n🔓 Прошептанное открыто.")
    except:
        pass

@dp.callback_query(F.data.startswith("marriage_accept"))
async def cb_mar_accept(callback: CallbackQuery):
    _, pid, rid, gid = callback.data.split(":")
    pid, rid, gid = int(pid), int(rid), int(gid)
    if callback.from_user.id != rid:
        await callback.answer("Это не для тебя!", show_alert=True)
        return
    msg = respond_marriage(gid, pid, rid, accept=True)
    pname = get_username_by_id(pid, gid)
    rname = get_username(callback.from_user)
    await callback.message.edit_text(f"🎉 <b>{rname}</b> и <b>{pname}</b> теперь в браке!")
    await callback.answer(msg)

@dp.callback_query(F.data.startswith("marriage_decline"))
async def cb_mar_decline(callback: CallbackQuery):
    _, pid, rid, gid = callback.data.split(":")
    pid, rid, gid = int(pid), int(rid), int(gid)
    if callback.from_user.id != rid:
        await callback.answer("Это не для тебя!", show_alert=True)
        return
    msg = respond_marriage(gid, pid, rid, accept=False)
    await callback.message.edit_text("❌ <b>Предложение отклонено.</b>")
    await callback.answer(msg)

def get_user_profile(user_id: int, group_id: int):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''
        SELECT username, total, daily, weekly, monthly, vip, diamonds, tickets, vip_until
        FROM users
        WHERE user_id=? AND group_id=?
    ''', (user_id, group_id))
    row = c.fetchone()
    conn.close()

    if not row:
        return "Нет данных о тебе.", False

    username, total, daily, weekly, monthly, vip, diamonds, tickets, vip_until = row

    # 🧩 добавляем запрос репутации
    conn_r = sqlite3.connect(DB)
    cr = conn_r.cursor()
    cr.execute("SELECT COALESCE(rep, 0) FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    result = cr.fetchone()
    rep_val = result[0] if result else 0
    conn_r.close()

    # 🏆 место в рейтинге
    conn2 = sqlite3.connect(DB)
    c2 = conn2.cursor()
    c2.execute('SELECT COUNT(*)+1 FROM users WHERE group_id=? AND total > ?', (group_id, total))
    place = c2.fetchone()[0]
    conn2.close()

    rank = get_rank(total)
    vip_act = is_vip_active(user_id, group_id)
    vip_str = "✨ Да" if vip_act else "Нет"
    vip_time = ""
    if vip_act and vip_until:
        try:
            until = datetime.strptime(vip_until, "%Y-%m-%d %H:%M:%S")
            vip_time = f"\n🕒 VIP до: <b>{until.strftime('%d.%m.%Y %H:%M:%S')}</b>"
        except Exception:
            vip_time = ""

    # 💬 итоговый текст
    text = (
        f"👤 <b>Профиль {username}</b>\n"
        f"🏆 Всего сообщений: <b>{total}</b>\n"
        f"📅 За день: <b>{daily}</b>\n"
        f"🗓 За неделю: <b>{weekly}</b>\n"
        f"🗓 За месяц: <b>{monthly}</b>\n"
        f"🎖 Ранг: <b>{rank}</b>\n"
        f"🥇 Место: <b>{place}</b>\n"
        f"💎 Алмазы: <b>{diamonds}</b>\n"
        f"🎫 Билеты: <b>{tickets}</b>\n"
        f"📝 Репутация: <b>{rep_val}</b>\n"
        f"🌟 VIP: <b>{vip_str}</b>{vip_time}"
    )

    return text, vip_act

def get_rank(messages: int) -> str:
    if messages >= 350_000:
        return "🟪 Обсидиан"
    elif messages >= 200_000:
        return "⬜️ Платина"
    elif messages >= 100_000:
        return "💚 Изумруд"
    elif messages >= 50_000:
        return "🔷 Алмаз"
    elif messages >= 25_000:
        return "🟡 Золото"
    elif messages >= 10_000:
        return "⚪️ Серебро"
    return "Нет ранга"

def find_user_id_by_nick(group_id: int, nick: str):
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    # Убираем @, пробелы, приводим к нижнему регистру
    nick = nick.strip().lstrip("@").lower()

    # Пытаемся найти по username (частичное совпадение)
    c.execute('SELECT user_id FROM users WHERE group_id=? AND LOWER(username) LIKE ?', (group_id, f"%{nick}%"))
    row = c.fetchone()

    # Если не нашли — пробуем по имени (на случай, если username пуст)
    if not row:
        c.execute('SELECT user_id FROM users WHERE group_id=? AND LOWER(username) LIKE ?', (group_id, f"%{nick}%"))
        row = c.fetchone()

    conn.close()
    return row[0] if row else None


# Запуск
async def main():
    init_db()
    print("Bot started")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
