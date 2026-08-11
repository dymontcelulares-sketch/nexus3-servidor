import asyncio
import json
import os
import hashlib
import websockets
from motor.motor_asyncio import AsyncIOMotorClient

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 9000))
MONGO_URI = os.environ.get("MONGO_URI")

# Conexão com MongoDB
client = AsyncIOMotorClient(MONGO_URI)
db = client["nexus2_db"]
users_col = db["users"]
messages_col = db["messages"]

clients = {}
users = {}
messages = []

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

async def load_data():
    global users, messages
    # Carrega usuários do MongoDB para a memória
    async for user in users_col.find({}):
        users[user["id"]] = user
    
    # Carrega mensagens recentes
    async for msg in messages_col.find({}).sort("timestamp", -1).limit(500):
        messages.insert(0, msg)

async def save_user_to_db(uid, data):
    await users_col.replace_one({"id": uid}, data, upsert=True)

async def save_message_to_db(msg):
    await messages_col.insert_one(msg)

def new_id():
    n = 1
    while True:
        uid = f"000.{n:04d}"
        if uid not in users:
            return uid
        n += 1

def public_users():
    return [{"id": u["id"], "nome": u["nome"], "sobrenome": u["sobrenome"]} for u in users.values()]

async def send(ws, data):
    try: await ws.send(json.dumps(data, ensure_ascii=False))
    except: pass

async def send_user(uid, data):
    ws = clients.get(uid)
    if ws: await send(ws, data)

async def handle(ws):
    uid = None
    try:
        async for raw in ws:
            data = json.loads(raw)
            tipo = data.get("tipo")

            if tipo == "registro":
                nome, sobrenome, senha = data.get("nome", "").strip(), data.get("sobrenome", "").strip(), data.get("senha", "")
                uid = new_id()
                user_data = {"id": uid, "nome": nome, "sobrenome": sobrenome, "senha": hash_password(senha), "senha_original": senha}
                users[uid] = user_data
                await save_user_to_db(uid, user_data)
                await send(ws, {"tipo": "registro_ok", "id": uid, "nome": nome, "sobrenome": sobrenome})

            elif tipo == "login":
                login_id = data.get("id", "").strip()
                user = users.get(login_id)
                if user and user["senha"] == hash_password(data.get("senha", "")):
                    uid = login_id
                    clients[uid] = ws
                    await send(ws, {"tipo": "login_ok", "id": uid, "nome": user["nome"], "sobrenome": user["sobrenome"]})
                    await send(ws, {"tipo": "usuarios", "usuarios": public_users()})
                    pendentes = [m for m in messages if m["para"] == uid]
                    await send(ws, {"tipo": "historico", "mensagens": pendentes})
            
            elif tipo == "mensagem":
                if not uid: continue
                para, texto = data.get("para", "").strip(), data.get("texto", "").strip()
                msg = {"de": uid, "para": para, "texto": texto, "hora": data.get("hora", ""), "timestamp": data.get("timestamp", 0)}
                messages.append(msg)
                await save_message_to_db(msg)
                pacote = {"tipo": "mensagem", "mensagem": msg}
                await send_user(para, pacote)
                if uid != para: await send_user(uid, pacote)

    finally:
        if uid: del clients[uid]

async def main():
    await load_data()
    print(f"Servidor online no MongoDB. {len(users)} usuários carregados.")
    async with websockets.serve(handle, HOST, PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
