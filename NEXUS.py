import asyncio
import json
import websockets
import os

# Estruturas de memória (RAM)
conexoes = {}  # {user_id: websocket}
usuarios = {}  # {user_id: {"id":..., "nome":..., "senha":...}}
mensagens = {} # {conversa_key: [lista_de_mensagens]}

def get_conversa_key(id1, id2):
    ids = sorted([str(id1), str(id2)])
    return f"{ids[0]}_{ids[1]}"

async def handler(websocket):
    try:
        async for message in websocket:
            data = json.loads(message)
            tipo = data.get("tipo")

            # REGISTRO
            if tipo == "registro":
                id_usuario = data.get("nome", "user").lower() + "_" + str(len(usuarios) + 1)
                usuarios[id_usuario] = {
                    "id": id_usuario,
                    "nome": data.get("nome"),
                    "sobrenome": data.get("sobrenome"),
                    "senha": data.get("senha")
                }
                conexoes[id_usuario] = websocket
                await websocket.send(json.dumps({
                    "tipo": "registro_ok", 
                    "id": id_usuario, 
                    "nome": data.get("nome"), 
                    "sobrenome": data.get("sobrenome")
                }))

            # LOGIN
            elif tipo == "login":
                user_id = data.get("id")
                senha = data.get("senha")
                user = usuarios.get(user_id)
                if user and user["senha"] == senha:
                    conexoes[user_id] = websocket
                    await websocket.send(json.dumps({
                        "tipo": "login_ok", 
                        "id": user["id"], 
                        "nome": user["nome"], 
                        "sobrenome": user["sobrenome"]
                    }))
                else:
                    await websocket.send(json.dumps({"tipo": "erro", "mensagem": "Credenciais inválidas"}))

            # LISTA USUÁRIOS
            elif tipo == "usuarios":
                lista = [{"id": u["id"], "nome": u["nome"], "sobrenome": u["sobrenome"]} for u in usuarios.values()]
                await websocket.send(json.dumps({"tipo": "usuarios", "usuarios": lista}))

            # MENSAGEM
            elif tipo == "mensagem":
                de = next((id for id, ws in conexoes.items() if ws == websocket), None)
                para = data.get("para")
                msg_obj = {
                    "id": data.get("id"),
                    "de": de,
                    "para": para,
                    "texto": data.get("texto"),
                    "tipo_mensagem": data.get("tipo_mensagem", "texto"),
                    "hora": data.get("hora"),
                    "timestamp": data.get("timestamp")
                }
                
                key = get_conversa_key(de, para)
                if key not in mensagens: mensagens[key] = []
                mensagens[key].append(msg_obj)

                await websocket.send(json.dumps({"tipo": "mensagem_enviada", "mensagem": msg_obj}))
                if para in conexoes:
                    await conexoes[para].send(json.dumps({"tipo": "mensagem", "mensagem": msg_obj}))

            # HISTÓRICO
            elif tipo == "historico":
                de = next((id for id, ws in conexoes.items() if ws == websocket), None)
                para = data.get("para")
                key = get_conversa_key(de, para)
                await websocket.send(json.dumps({"tipo": "historico", "mensagens": mensagens.get(key, [])}))

    except Exception as e:
        print(f"Erro na conexão: {e}")
    finally:
        for uid, ws in list(conexoes.items()):
            if ws == websocket:
                del conexoes[uid]

async def main():
    port = int(os.environ.get("PORT", 8080))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Servidor NEXUS iniciado com sucesso na porta {port}")
        await asyncio.Future()  # Mantém o processo ativo no Render

if __name__ == "__main__":
    asyncio.run(main())
                                                     
