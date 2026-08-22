import asyncio
import json
import websockets

# Dicionário global para guardar quem está conectado: { "id_do_usuario": websocket }
conexoes_ativas = {}

async def gerenciar_conexao(websocket):
    usuario_id = None
    try:
        async for mensagem_texto in websocket:
            dados = json.loads(mensagem_texto)
            tipo = dados.get("tipo")

            # 1. Quando o usuário faz login
            if tipo == "login":
                usuario_id = dados.get("id")
                conexoes_ativas[usuario_id] = websocket
                print(f"Usuário conectado: {usuario_id}")
                
                # Responde que o login deu certo
                await websocket.send(json.dumps({
                    "tipo": "login_ok",
                    "id": usuario_id
                }))
                continue

            # 2. Quando o usuário envia uma mensagem para outro
            elif tipo == "mensagem":
                destinatario_id = dados.get("para")
                
                # Preenche o pacote da mensagem que será entregue
                payload_resposta = {
                    "tipo": "mensagem",
                    "mensagem": {
                        "id": dados.get("id"),
                        "de": usuario_id,
                        "para": destinatario_id,
                        "texto": dados.get("texto"),
                        "tipo_mensagem": dados.get("tipo_mensagem", "texto"),
                        "hora": dados.get("hora"),
                        "timestamp": dados.get("timestamp"),
                        "seq": dados.get("seq")
                    }
                }

                # Envia para o destinatário se ele estiver online no momento
                if destinatario_id in conexoes_ativas:
                    dest_socket = conexoes_ativas[destinatario_id]
                    try:
                        await dest_socket.send(json.dumps(payload_resposta))
                    except websockets.exceptions.ConnectionClosed:
                        # Se a conexão caiu, remove da lista
                        del conexoes_ativas[destinatario_id]

                # (Opcional) Você também pode salvar a mensagem num banco de dados aqui 
                # se quiser que o histórico fique salvo no servidor.

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # Remove da lista de conexões ativas ao desconectar
        if usuario_id and usuario_id in conexoes_ativas:
            del conexoes_ativas[usuario_id]
            print(f"Usuário desconectado: {usuario_id}")

# Inicialização do Servidor Websocket (geralmente porta 10000 ou a que o Render usa)
async def main():
    async with websockets.serve(gerenciar_conexao, "0.0.0.0", 10000):
        await asyncio.Future()  # Mantém o servidor rodando

if __name__ == "__main__":
    asyncio.run(main())
    
