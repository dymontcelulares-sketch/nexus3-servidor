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
        except Exception:
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
    while True:
        novo = f"{random.randint(0,999):03d}.{random.randint(0,9999):04d}"
        if novo not in usuarios:
            return novo

def chave_conversa(a, b):
    return "_".join(sorted([str(a), str(b)]))

async def enviar(ws, dados):
    try:
        await ws.send(json.dumps(dados, ensure_ascii=False))
        return True
    except Exception:
        return False

async def handler(websocket):
    usuario_atual_id = None

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except Exception:
                continue

            tipo = data.get("tipo")

            # =========================
            # REGISTRO
            # =========================
            if tipo == "registro":
                nome = str(data.get("nome", "")).strip()
                sobrenome = str(data.get("sobrenome", "")).strip()
                senha = str(data.get("senha", ""))

                if not nome or not sobrenome or not senha:
                    await enviar(websocket, {
                        "tipo": "erro",
                        "mensagem": "Preencha todos os campos."
                    })
                    continue

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

                await enviar(websocket, {
                    "tipo": "registro_ok",
                    "id": novo_id,
                    "nome": nome,
                    "sobrenome": sobrenome
                })

                await broadcast_usuarios()

            # =========================
            # LOGIN
            # =========================
            elif tipo == "login":
                uid = str(data.get("id", "")).strip()
                senha = str(data.get("senha", ""))

                if uid not in usuarios or usuarios[uid]["senha"] != senha:
                    await enviar(websocket, {
                        "tipo": "erro",
                        "mensagem": "Credenciais inválidas."
                    })
                    continue

                # Remove conexão anterior do mesmo usuário
                antiga = clientes_ativos.get(uid)

                if antiga and antiga != websocket:
                    try:
                        await antiga.close()
                    except Exception:
                        pass

                usuario_atual_id = uid
                clientes_ativos[uid] = websocket

                user = usuarios[uid]

                await enviar(websocket, {
                    "tipo": "login_ok",
                    "id": user["id"],
                    "nome": user["nome"],
                    "sobrenome": user["sobrenome"]
                })

                await broadcast_usuarios()

            # =========================
            # LISTA DE USUÁRIOS
            # =========================
            elif tipo == "usuarios":
                lista = [
                    {
                        "id": u["id"],
                        "nome": u["nome"],
                        "sobrenome": u["sobrenome"]
                    }
                    for u in usuarios.values()
                ]

                await enviar(websocket, {
                    "tipo": "usuarios",
                    "usuarios": lista
                })

            # =========================
            # HISTÓRICO
            # =========================
            elif tipo == "historico":
                if not usuario_atual_id:
                    continue

                para = data.get("para")

                if not para:
                    continue

                key = chave_conversa(usuario_atual_id, para)
                msgs = mensagens_db.get(key, [])

                await enviar(websocket, {
                    "tipo": "historico_conversa",
                    "mensagens": msgs
                })

            # =========================
            # MENSAGEM
            # =========================
            elif tipo == "mensagem":
                if not usuario_atual_id:
                    continue

                para = str(data.get("para", "")).strip()
                texto = data.get("texto", "")
                tipo_msg = data.get("tipo_mensagem", "texto")
                hora = data.get("hora", "")
                timestamp = data.get("timestamp", 0)
                seq = data.get("seq", timestamp)
                msg_id = data.get("id")

                if not para or not msg_id:
                    continue

                # Mantém texto, GIF e imagem
                msg_obj = {
                    "id": msg_id,
                    "de": usuario_atual_id,
                    "para": para,
                    "texto": texto,
                    "tipo": tipo_msg,
                    "tipo_mensagem": tipo_msg,
                    "hora": hora,
                    "timestamp": timestamp,
                    "seq": seq
                }

                key = chave_conversa(usuario_atual_id, para)

                if key not in mensagens_db:
                    mensagens_db[key] = []

                # Evita duplicação
                existe = any(
                    str(m.get("id")) == str(msg_id)
                    for m in mensagens_db[key]
                )

                if not existe:
                    mensagens_db[key].append(msg_obj)
                    salvar_dados(MESSAGES_FILE, mensagens_db)

                # =========================
                # ENTREGA
                # =========================
                destino = clientes_ativos.get(para)

                if destino:
                    await enviar(destino, {
                        "tipo": "mensagem",
                        "mensagem": msg_obj
                    })

            # =========================
            # APAGAR CONTA
            # =========================
            elif tipo == "apagar_conta":
                if usuario_atual_id and usuario_atual_id in usuarios:

                    del usuarios[usuario_atual_id]
                    salvar_dados(USERS_FILE, usuarios)

                    await enviar(websocket, {
                        "tipo": "conta_apagada"
                    })

                    break

    except websockets.exceptions.ConnectionClosed:
        pass

    except Exception as e:
        print("[ERRO]", e)

    finally:
        if (
            usuario_atual_id
            and clientes_ativos.get(usuario_atual_id) == websocket
        ):
            del clientes_ativos[usuario_atual_id]

        await broadcast_usuarios()


async def broadcast_usuarios():
    if not clientes_ativos:
        return

    lista = [
        {
            "id": u["id"],
            "nome": u["nome"],
            "sobrenome": u["sobrenome"]
        }
        for u in usuarios.values()
    ]

    payload = json.dumps({
        "tipo": "usuarios",
        "usuarios": lista
    }, ensure_ascii=False)

    for ws in list(clientes_ativos.values()):
        try:
            await ws.send(payload)
        except Exception:
            pass


async def main():
    port = int(os.environ.get("PORT", 8765))

    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=16 * 1024 * 1024,
        ping_interval=20,
        ping_timeout=60
    ):
        print("=" * 40)
        print("          NEXUS SERVER")
        print("=" * 40)
        print(f"Servidor online na porta {port}")
        print(f"Usuários: {len(usuarios)}")
        print("=" * 40)

        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
                
