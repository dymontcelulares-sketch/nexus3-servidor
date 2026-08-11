import asyncio
import json
import os
import websockets

USERS_FILE = "users.json"
MESSAGES_FILE = "messages.json"

def carregar_dados(arquivo, padrao):
    if os.path.exists(arquivo):
        try:
            with open(arquivo, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return padrao

def salvar_dados(arquivo, dados):
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)

usuarios = carregar_dados(USERS_FILE, {})
mensagens_db = carregar_dados(MESSAGES_FILE, {})
clientes_ativos = {}

def gerar_id():
    import random
    return f"{random.randint(100, 999)}.{random.randint(1000, 9999)}"

def chave_conversa(a, b):
    ids = sorted([str(a), str(b)])
    return f"{ids[0]}_{ids[1]}"

async def handler(websocket):
    usuario_atual_id = None
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            tipo = data.get("tipo")

            if tipo == "registro":
                nome = data.get("nome", "").strip()
                sobrenome = data.get("sobrenome", "").strip()
                senha = data.get("senha", "")
                
                if not nome or not sobrenome or not senha:
                    await websocket.send(json.dumps({"tipo": "erro", "mensagem": "Preencha todos os campos."}))
                    continue

                novo_id = gerar_id()
                while novo_id in usuarios:
                    novo_id = gerar_id()

                usuarios[novo_id] = {
                    "id": novo_id,
                    "nome": nome,
                    "sobrenome": sobrenome,
                    "senha": senha
                }
                salvar_dados(USERS_FILE, usuarios)

                usuario_atual_id = novo_id
                clientes_ativos[novo_id] = websocket

                await websocket.send(json.dumps({
                    "tipo": "registro_ok",
                    "id": novo_id,
                    "nome": nome,
                    "sobrenome": sobrenome
                }))
                await broadcast_usuarios()

            elif tipo == "login":
                uid = data.get("id")
                senha = data.get("senha")

                if uid not in usuarios or usuarios[uid]["senha"] != senha:
                    await websocket.send(json.dumps({"tipo": "erro", "mensagem": "Credenciais inválidas."}))
                    continue

                usuario_atual_id = uid
                clientes_ativos[uid] = websocket

                user = usuarios[uid]
                await websocket.send(json.dumps({
                    "tipo": "login_ok",
                    "id": user["id"],
                    "nome": user["nome"],
                    "sobrenome": user["sobrenome"]
                }))
                await broadcast_usuarios()

            elif tipo == "usuarios":
                lista_publica = [
                    {"id": u["id"], "nome": u["nome"], "sobrenome": u["sobrenome"]}
                    for u in usuarios.values()
                ]
                await websocket.send(json.dumps({"tipo": "usuarios", "usuarios": lista_publica}))

            elif tipo == "historico":
                if not usuario_atual_id:
                    continue
                para = data.get("para")
                key = chave_conversa(usuario_atual_id, para)
                msgs = mensagens_db.get(key, [])
                await websocket.send(json.dumps({
                    "tipo": "historico_conversa",
                    "mensagens": msgs
                }))

            elif tipo == "mensagem":
                if not usuario_atual_id:
                    continue
                para = data.get("para")
                texto = data.get("texto")
                tipo_msg = data.get("tipo_mensagem", "texto")
                hora = data.get("hora")
                timestamp = data.get("timestamp")
                seq = data.get("seq")
                msg_id = data.get("id")

                msg_obj = {
                    "id": msg_id,
                    "de": usuario_atual_id,
                    "para": para,
                    "texto": texto,
                    "tipo": tipo_msg,
                    "hora": hora,
                    "timestamp": timestamp,
                    "seq": seq
                }

                key = chave_conversa(usuario_atual_id, para)
                if key not in mensagens_db:
                    mensagens_db[key] = []
                
                if not any(m.get("id") == msg_id for m in mensagens_db[key]):
                    mensagens_db[key].append(msg_obj)
                    salvar_dados(MESSAGES_FILE, mensagens_db)

                if para in clientes_ativos:
                    try:
                        await clientes_ativos[para].send(json.dumps({
                            "tipo": "mensagem",
                            "mensagem": msg_obj
                        }))
                    except:
                        pass

            elif tipo == "apagar_conta":
                if usuario_atual_id and usuario_atual_id in usuarios:
                    del usuarios[usuario_atual_id]
                    salvar_dados(USERS_FILE, usuarios)
                    await websocket.send(json.dumps({"tipo": "conta_apagada"}))
                    break

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if usuario_atual_id and usuario_atual_id in clientes_ativos:
            del clientes_ativos[usuario_atual_id]
        await broadcast_usuarios()

async def broadcast_usuarios():
    if not clientes_ativos:
        return
    lista_publica = [
        {"id": u["id"], "nome": u["nome"], "sobrenome": u["sobrenome"]}
        for u in usuarios.values()
    ]
    payload = json.dumps({"tipo": "usuarios", "usuarios": lista_publica})
    for ws in list(clientes_ativos.values()):
        try:
            await ws.send(payload)
        except:
            pass

async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Servidor NEXUS rodando na porta {port}...")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
                
