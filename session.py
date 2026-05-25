from pyrogram import Client

api_id = 26867853
api_hash = "b0c1361eb5eaa5cc619644fa4a17e226"

with Client("new_session", api_id, api_hash) as app:
    print(app.export_session_string())