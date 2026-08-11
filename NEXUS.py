import asyncio
import json
import os
import hashlib
import websockets
from motor.motor_asyncio import AsyncIOMotorClient

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 9000))
MONGO_URI = os.environ.get("MONGO_URI")

if not MONGO_URI:
    raise ValueError("A variável de ambiente MONGO_URI não está definida!")

# Conexão otimizada com o MongoDB Atlas
client = AsyncIOMotorClient(MONGO_URI)
db = client["nexus2_db"]
users_col = db["users"]
messages_col = db["messages"]

clients = {}
users = {}
messages = []

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

async def setup_database():
    # Criação de índices para buscas instantâneas
    await users_col.create_index("id", unique=True)
    await messages_col.create_index([("timestamp", -1)])
    await messages_col.create_index([("para", 1)])

async def load_data():
    global users, messages
    await setup_database()
    
    # Carrega usuários rapidamente para a memória RAM
    async for user in users_col.find({}, {"_id": 0}):
        users[user["id"]] = user
    
    # Carrega as últimas mensagens para histórico rápido
    async for msg in messages_col.find({}, {"_id": 0}).sort("timestamp", -1).limit(300):
        messages.insert(0, msg)

async def save_user_to_db(uid, data):
    await users_col.update_one({"id": uid}, {"$set": data}, upsert=True)

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
    try:
        await ws.send(json.dumps(data, ensure_ascii=False))
    except Exception:
        pass

async def send_user(uid, data):
    ws = clients.get(uid)
    if ws:
        await send(ws, data)

async def handle(ws):
    uid = None
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            tipo = data.get("tipo")

            if tipo == "registro":
                nome = data.get("nome", "").strip()
                sobrenome = data.get("sobrenome", "").strip()
                senha = data.get("senha", "")
                
                if not nome or not senha:
                    await send(ws, {"tipo": "erro", "mensagem": "Preencha os campos obrigatórios."})
                    continue

                uid = new_id()
                user_data = {
                    "id": uid,
                    "nome": nome,
                    "sobrenome": sobrenome,
                    "senha": hash_password(senha)
                }
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
                    
                    pendentes = [m for m in messages if m.get("para") == uid or m.get("de") == uid]
                    await send(ws, {"tipo": "historico", "mensagens": pendentes[-100:]})
                else:
                    await send(ws, {"tipo": "erro", "mensagem": "ID ou senha incorretos."})
            
            elif tipo == "mensagem":
                if not uid:
                    continue
                
                para = data.get("para", "").strip()
                texto = data.get("texto", "")
                msg_id = data.get("id", "")
                msg_tipo = data.get("tipo_mensagem") or data.get("tipo") or "texto"
                if msg_tipo == "mensagem":
                    msg_tipo = data.get("tipo_mensagem", "texto")

                hora = data.get("hora", "")
                timestamp = data.get("timestamp", 0)
                seq = data.get("seq", timestamp)

                msg = {
                    "id": msg_id,
                    "de": uid,
                    "para": para,
                    "texto": texto,
                    "tipo": msg_tipo,
                    "hora": hora,
                    "timestamp": timestamp,
                    "seq": seq
                }

                messages.append(msg)
                await save_message_to_db(msg)

                pacote = {"tipo": "mensagem", "mensagem": msg}
                await send_user(para, pacote)
                if uid != para:
                    await send_user(uid, pacote)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if uid and clients.get(uid) == ws:
            del clients[uid]

async def main():
    await load_data()
    print(f"[NEXUS2] Servidor ultra-rápido online no MongoDB. {len(users)} usuários carregados.")
    async with websockets.serve(handle, HOST, PORT, ping_interval=20, ping_timeout=10):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
    
