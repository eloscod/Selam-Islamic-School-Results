from collections import defaultdict
import os
import logging
import secrets
from datetime import datetime, timedelta
from threading import Timer
from functools import lru_cache, wraps
from enum import Enum

import psycopg2
from dotenv import load_dotenv
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException
import telebot
from telebot import types

# === Load Environment ===
load_dotenv()

class Config:
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    ADMIN_ID = os.getenv('ADMIN_ID')
    DATABASE_URL = os.getenv('DATABASE_URL')
    DATA_DIR = 'data/'
    LOG_FILE = 'bot.log'
    REGISTRATION_TIMEOUT = 300  # 5 minutes
    RATE_LIMIT = {'max_attempts': 5, 'period': 60}  # 5 attempts per minute
    CACHE_SIZE = 8

class SchoolLevel(Enum):
    ELEMENTARY = 'elementary'
    MIDDLE = 'middle'

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.LOG_FILE),
        logging.StreamHandler()
    ]
)

# === Database Setup (PostgreSQL on Railway) ===
def get_db_connection():
    return psycopg2.connect(Config.DATABASE_URL)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        grade_section TEXT,
        student_no TEXT,
        pin TEXT,
        language TEXT DEFAULT 'en',
        school_level TEXT,
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS student_identifiers (
        pin TEXT PRIMARY KEY,
        grade_section TEXT,
        student_no TEXT,
        telegram_id TEXT,
        school_level TEXT
    )
    ''')
    
    conn.commit()
    cur.close()
    conn.close()

init_db()

# === Bot Initialization ===
bot = telebot.TeleBot(Config.BOT_TOKEN)

# === Data Structures ===
class ColumnMapping:
    ELEMENTARY = {
        'S1': {'student_no': 1, 'name': 3, 'sex': 4, 'age': 5,
               'subjects': [6, 7, 8, 9, 10, 11, 12, 13],
               'conduct': 14, 'sum': 15, 'average': 16, 'rank': 17},
        'S2': {'student_no': 1, 'name': 3, 'sex': 4, 'age': 5,
               'subjects': [6, 7, 8, 9, 10, 11, 12, 13],
               'conduct': 14, 'sum': 15, 'average': 16, 'rank': 17},
        'Ave': {'student_no': 1, 'name': 2, 'sex': 3, 'age': 4,
                'subjects': [5, 6, 7, 8, 9, 10, 11, 12],
                'sum': 13, 'average': 14, 'rank': 15, 'remark': 16}
    }
    
    MIDDLE = {
        'S1': {'student_no': 1, 'name': 3, 'sex': 4, 'age': 5,
               'subjects': [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
               'conduct': 17, 'sum': 18, 'average': 19, 'rank': 20},
        'S2': {'student_no': 1, 'name': 3, 'sex': 4, 'age': 5,
               'subjects': [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
               'conduct': 17, 'sum': 18, 'average': 19, 'rank': 20},
        'Ave': {'student_no': 1, 'name': 2, 'sex': 3, 'age': 4,
                'subjects': [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                'sum': 16, 'average': 17, 'rank': 18, 'remark': 19}
    }

class Subjects:
    ELEMENTARY = {
        'en': ["Amharic", "English", "Arabic", "Maths", "E.S", "Moral Edu", "Art", "HPE"],
        'am': ["አማርኛ", "እንግሊዝኛ", "አረብኛ", "ሒሳብ", "ኢ.ኤስ", "ሥነ ምግባር ትምህርት", "ሥነ ጥበብ", "ኤች.ፒ.ኢ"]
    }
    
    MIDDLE = {
        'en': ["Amharic", "English", "Arabic", "Maths", "General Science", "Geography", 
               "Citizenship", "CTE", "ICT", "Art", "HPE"],
        'am': ["አማርኛ", "እንግሊዝኛ", "አረብኛ", "ሒሳብ", "አጠቃላይ ሳይንስ", "ጂኦግራፊ", 
               "ዜግነት", "ሲ.ቲ.ኢ", "አይ.ሲ.ቲ", "ሥነ ጥበብ", "ኤች.ፒ.ኢ"]
    }

# === Utility Functions ===
def db_execute(query, params=(), fetch=False):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    if fetch:
        result = cur.fetchall()
    else:
        conn.commit()
        result = None
    cur.close()
    conn.close()
    return result

def is_number(val):
    try:
        return float(val) if val else False
    except (ValueError, TypeError):
        return False

def get_value(cell):
    return cell.value if cell and hasattr(cell, 'value') and cell.value is not None else 'N/A'

@lru_cache(maxsize=Config.CACHE_SIZE)
def get_workbook(grade_section, school_level):
    file_path = f"{Config.DATA_DIR}{school_level.value}/{grade_section}.xlsx"
    return load_workbook(file_path, data_only=True)

def generate_unique_pin():
    max_attempts = 100
    for _ in range(max_attempts):
        pin = f"{secrets.randbelow(1000000):06d}"
        if not db_execute("SELECT pin FROM student_identifiers WHERE pin=%s", (pin,), True):
            return pin
    raise ValueError("Could not generate unique PIN")

def get_user_language(user_id):
    result = db_execute(
        "SELECT language FROM users WHERE user_id=%s", 
        (str(user_id),), 
        True
    )
    return result[0][0] if result else 'en'

def get_user_school_level(user_id):
    result = db_execute(
        "SELECT school_level FROM users WHERE user_id=%s", 
        (str(user_id),), 
        True
    )
    return SchoolLevel(result[0][0]) if result else None

# === Rate Limiting ===
user_attempts = defaultdict(list)

def is_rate_limited(user_id):
    now = datetime.now()
    attempts = [t for t in user_attempts[user_id] if now - t < timedelta(seconds=Config.RATE_LIMIT['period'])]
    user_attempts[user_id] = attempts
    if len(attempts) >= Config.RATE_LIMIT['max_attempts']:
        return True
    user_attempts[user_id].append(now)
    return False

def rate_limited(func):
    @wraps(func)
    def wrapped(message, *args, **kwargs):
        if is_rate_limited(str(message.from_user.id)):
            lang = get_user_language(message.from_user.id)
            bot.reply_to(message, "⏳ Too many requests. Please wait a minute.", parse_mode="Markdown")
            return
        return func(message, *args, **kwargs)
    return wrapped

# === Localization ===
MESSAGES = {
    'en': {
        'welcome': "🎓 *Welcome to Selam Islamic School Results Bot* 🎓\n\nPlease select your school level:",
        'select_school_level': "🏫 *Select Your School Level*",
        'elementary': "🧒 Elementary School (Grades 1-6)",
        'middle': "🧑 Middle School (Grades 7-8)",
        'register_usage': "Usage: /register <grade_section> <student_no> (e.g., /register 7A 10)",
        'login_usage': "Usage: /login <PIN> (e.g., /login 123456)",
        'help': "📚 *Help*\n\n- /start: Begin\n- /register: Get a PIN\n- /login: Access results\n- /help: Show this message",
    },
    'am': {
        'welcome': "🎓 *እንኳን ወደ ሰላም እስላማዊ ትምህርት ቤት ውጤት ቦት ተግባቢ እንኳን ደህና መጡ* 🎓\n\nእባክዎ የትምህርት ደረጃዎን ይምረጡ:",
        'select_school_level': "🏫 *የትምህርት ደረጃ ይምረጡ*",
        'elementary': "🧒 አንደኛ ደረጃ (ክፍል 1-6)",
        'middle': "🧑 መካከለኛ ደረጃ (ክፍል 7-8)",
        'register_usage': "አጠቃቀም: /register <grade_section> <student_no> (ለምሳሌ፣ /register 7A 10)",
        'login_usage': "አጠቃቀም: /login <PIN> (ለምሳሌ፣ /login 123456)",
        'help': "📚 *እገዛ*\n\n- /start: ይጀምሩ\n- /register: ፒን ያግኙ\n- /login: ውጤቶችን ይመልከቱ\n- /help: ይህን መልዕክት ያሳያል",
    }
}

# === Bot Commands ===
@bot.message_handler(commands=['start'])
def send_welcome(message):
    user_id = str(message.from_user.id)
    lang = get_user_language(user_id)
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            MESSAGES[lang]['elementary'], 
            callback_data='school_level_elementary'
        ),
        types.InlineKeyboardButton(
            MESSAGES[lang]['middle'], 
            callback_data='school_level_middle'
        )
    )
    
    bot.reply_to(message, 
        MESSAGES[lang]['welcome'],
        reply_markup=markup,
        parse_mode="Markdown"
    )

@bot.message_handler(commands=['help'])
def send_help(message):
    lang = get_user_language(message.from_user.id)
    bot.reply_to(message, MESSAGES[lang]['help'], parse_mode="Markdown")

@bot.message_handler(commands=['register'])
@rate_limited
def register_user(message):
    user_id = str(message.from_user.id)
    lang = get_user_language(user_id)
    school_level = get_user_school_level(user_id)
    
    if not school_level:
        bot.reply_to(message, "Please select your school level first with /start")
        return
    
    args = message.text.split()
    if len(args) != 3:
        bot.reply_to(message, MESSAGES[lang]['register_usage'], parse_mode="Markdown")
        return

    grade_section, student_no = args[1], args[2]
    
    # Validate input based on school level
    if school_level == SchoolLevel.ELEMENTARY:
        valid_sections = ['1A', '1B', '1C', '2A', '2B', '2C', '3A', '3B', '4A', '4B', '5A', '5B', '6A', '6B']
    else:
        valid_sections = ['7A', '7B', '8A', '8B']
    
    if grade_section not in valid_sections:
        bot.reply_to(message, "❌ Invalid grade/section.", parse_mode="Markdown")
        return

    if not student_no.isdigit() or not (1 <= int(student_no) <= 60):
        bot.reply_to(message, "❌ Invalid student number (1-60).", parse_mode="Markdown")
        return

    # Check if user is already registered
    existing_user = db_execute(
        "SELECT grade_section, student_no FROM users WHERE user_id=%s", 
        (user_id,), 
        True
    )
    
    if existing_user:
        bot.reply_to(message, "❌ You are already registered.", parse_mode="Markdown")
        return

    # Generate PIN and store registration
    try:
        pin = generate_unique_pin()
        
        db_execute(
            "INSERT INTO student_identifiers (pin, grade_section, student_no, telegram_id, school_level) VALUES (%s, %s, %s, %s, %s)",
            (pin, grade_section, student_no, user_id, school_level.value)
        )
        
        db_execute(
            "UPDATE users SET grade_section=%s, student_no=%s, pin=%s WHERE user_id=%s",
            (grade_section, student_no, pin, user_id)
        )
        
        bot.reply_to(message, f"✅ Registration successful! Your PIN: `{pin}`", parse_mode="Markdown")
        
    except Exception as e:
        logging.error(f"Registration error: {str(e)}")
        bot.reply_to(message, "❌ Registration failed. Please try again.", parse_mode="Markdown")

# === Run Bot ===
if __name__ == '__main__':
    logging.info("📡 Bot is running...")
    bot.polling()