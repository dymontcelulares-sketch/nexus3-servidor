import asyncio,json,os,time,secrets,hashlib
import websockets
from motor.motor_asyncio import AsyncIOMotorClient

HOST="0.0.0.0"
PORT=int(os.getenv("PORT","10000"))

MONGO_URI="mongodb+srv://dymontcelulares_db_user:r3uYIQqHdvL5lnNs@cluster0.owpwsaz.mongodb.net/?appName=Cluster0"
DB_NAME="nexus"

ADMIN_PASSWORD=os.getenv("ADMIN_PASSWORD","41914598")
MAX_FOTO=3*1024*1024
MAX_MSG=10*1024*1024

mongo=AsyncIOMotorClient(MONGO_URI)
db=mongo[DB_NAME]

usuarios=db.usuarios
mensagens=db.mensagens
config=db.config

lock=asyncio.Lock()
online={}


def senha_hash(s):
    return hashlib.sha256(s.encode()).hexdigest()


def chave(a,b):
    return "_".join(sorted([str(a),str(b)]))


async def gerar_id():
    r=await config.find_one_and_update(
        {"_id":"contador"},
        {"$inc":{"ultimo_id":1}},
        upsert=True,
        return_document=True
    )
    n=r["ultimo_id"]
    return f"{n//10000:03d}.{n%10000:04d}"


async def perfil(uid):
    u=await usuarios.find_one({"_id":str(uid)})
    if not u:return None

    return {
        "id":u["_id"],
        "nome":u.get("nome",""),
        "sobrenome":u.get("sobrenome",""),
        "foto":u.get("foto",""),
        "online":str(uid) in online,
        "verificado":bool(u.get("verificado",False)),
        "admin":bool(u.get("admin",False)),
        "banido":bool(u.get("banido",False))
    }


async def send(ws,data):
    try:
        await ws.send(json.dumps(data,ensure_ascii=False,separators=(",",":")))
        return True
    except:
        return False


async def sendto(uid,data):
    ws=online.get(str(uid))
    if not ws:return False

    ok=await send(ws,data)

    if not ok and online.get(str(uid))==ws:
        online.pop(str(uid),None)

    return ok


async def error(ws,msg,codigo=None):
    d={"tipo":"erro","mensagem":msg}
    if codigo:d["codigo"]=codigo
    await send(ws,d)


async def transmitir_perfil(uid):
    p=await perfil(uid)
    if not p:return

    pacote={"tipo":"perfil_update","perfil":p}

    for x in list(online):
        await sendto(x,pacote)


async def registro(ws,d):
    nome=str(d.get("nome","")).strip()
    sobrenome=str(d.get("sobrenome","")).strip()
    senha=str(d.get("senha",""))

    if not nome:return await error(ws,"Digite seu nome.")
    if not sobrenome:return await error(ws,"Digite seu sobrenome.")
    if not senha:return await error(ws,"Digite uma senha.")

    async with lock:
        uid=await gerar_id()
        admin=senha==ADMIN_PASSWORD

        u={
            "_id":uid,
            "nome":nome,
            "sobrenome":sobrenome,
            "senha":senha_hash(senha),
            "foto":"",
            "verificado":admin,
            "admin":admin,
            "banido":False,
            "criado_em":int(time.time())
        }

        try:
            await usuarios.insert_one(u)
        except Exception as e:
            print("[MONGO]",e)
            return await error(ws,"Não foi possível criar sua conta.")

    online[uid]=ws
    p=await perfil(uid)

    await send(ws,{
        "tipo":"registro_ok",
        "id":uid,
        "nome":nome,
        "sobrenome":sobrenome,
        "foto":"",
        "verificado":admin,
        "admin":admin,
        "perfil":p
    })

    print(f"[REGISTRO] {uid} | {nome} {sobrenome} | ADMIN={admin}")
    return uid


async def login(ws,d):
    uid=str(d.get("id","")).strip()
    senha=str(d.get("senha",""))

    u=await usuarios.find_one({"_id":uid})

    if not uid or not u or senha_hash(senha)!=u.get("senha",""):
        return await error(ws,"Nexus ID ou senha incorretos.")

    if u.get("banido",False):
        return await error(ws,"Esta conta foi banida.","BANIDO")

    old=online.get(uid)

    if old and old!=ws:
        try:await old.close()
        except:pass

    online[uid]=ws
    p=await perfil(uid)

    await send(ws,{
        "tipo":"login_ok",
        "id":uid,
        "nome":u.get("nome",""),
        "sobrenome":u.get("sobrenome",""),
        "foto":u.get("foto",""),
        "verificado":bool(u.get("verificado",False)),
        "admin":bool(u.get("admin",False)),
        "perfil":p
    })

    print(f"[LOGIN] {uid} | ADMIN={u.get('admin',False)} | VERIFICADO={u.get('verificado',False)}")
    return uid


async def usuarios_lista(ws):
    lista=[]

    async for u in usuarios.find({}):
        lista.append({
            "id":u["_id"],
            "nome":u.get("nome",""),
            "sobrenome":u.get("sobrenome",""),
            "foto":u.get("foto",""),
            "online":u["_id"] in online,
            "verificado":bool(u.get("verificado",False)),
            "admin":bool(u.get("admin",False)),
            "banido":bool(u.get("banido",False))
        })

    await send(ws,{"tipo":"usuarios","usuarios":lista})


async def foto(uid,d,ws):
    foto=d.get("foto","")

    if not isinstance(foto,str):
        return await error(ws,"Foto inválida.")

    if len(foto)>MAX_FOTO:
        return await error(ws,"A foto é muito grande.","FOTO_GRANDE")

    if foto and not foto.startswith("data:image/"):
        return await error(ws,"Formato de foto inválido.")

    r=await usuarios.update_one(
        {"_id":uid},
        {"$set":{"foto":foto}}
    )

    if not r.matched_count:
        return await error(ws,"Usuário não encontrado.")

    p=await perfil(uid)

    await send(ws,{
        "tipo":"perfil_atualizado",
        "perfil":p
    })

    await transmitir_perfil(uid)


async def perfil_req(ws,d):
    uid=str(d.get("id","")).strip()
    p=await perfil(uid)

    if not p:
        return await error(ws,"Usuário não encontrado.")

    await send(ws,{
        "tipo":"perfil",
        "perfil":p
    })


async def historico(ws,uid,d):
    outro=str(d.get("para","")).strip()

    if not await usuarios.find_one({"_id":outro}):
        return await send(ws,{
            "tipo":"historico",
            "para":outro,
            "mensagens":[]
        })

    k=chave(uid,outro)

    doc=await mensagens.find_one(
        {"_id":k},
        {"mensagens":{"$slice":-1000}}
    )

    lista=doc.get("mensagens",[]) if doc else []

    await send(ws,{
        "tipo":"historico",
        "para":outro,
        "mensagens":lista
    })


async def mensagem(uid,d):
    u=await usuarios.find_one({"_id":uid})

    if not u:return

    if u.get("banido",False):
        ws=online.get(uid)
        if ws:
            await error(ws,"Esta conta foi banida.","BANIDO")
        return

    para=str(d.get("para","")).strip()

    destino=await usuarios.find_one({"_id":para})

    if not destino:
        return await sendto(uid,{
            "tipo":"erro",
            "mensagem":"Usuário não encontrado."
        })

    if destino.get("banido",False):
        return await sendto(uid,{
            "tipo":"erro",
            "mensagem":"Este usuário está banido."
        })

    texto=d.get("texto","")

    if not isinstance(texto,str):
        texto=str(texto)

    if len(texto)>MAX_MSG:
        return await sendto(uid,{
            "tipo":"erro",
            "mensagem":"Mensagem muito grande."
        })

    tipo=d.get("tipo_mensagem","texto")

    if tipo not in ("texto","imagem","gif","audio"):
        tipo="texto"

    mid=d.get("id") or f"{uid}_{int(time.time()*1000)}_{secrets.token_hex(4)}"

    try:
        timestamp=int(d.get("timestamp",0))
    except:
        timestamp=int(time.time()*1000)

    try:
        seq=int(d.get("seq",timestamp))
    except:
        seq=timestamp

    msg={
        "id":mid,
        "de":uid,
        "para":para,
        "texto":texto,
        "tipo":tipo,
        "tipo_mensagem":tipo,
        "hora":d.get("hora",""),
        "timestamp":timestamp,
        "seq":seq,
        "perfil_remetente":await perfil(uid),
        "nome_remetente":u.get("nome",""),
        "sobrenome_remetente":u.get("sobrenome",""),
        "foto_perfil_remetente":u.get("foto",""),
        "verificado_remetente":bool(u.get("verificado",False)),
        "admin_remetente":bool(u.get("admin",False))
    }

    k=chave(uid,para)

    async with lock:
        doc=await mensagens.find_one({"_id":k})

        if doc and any(x.get("id")==mid for x in doc.get("mensagens",[])):
            return

        if doc:
            arr=doc.get("mensagens",[])
            arr.append(msg)
            arr=arr[-2000:]

            await mensagens.update_one(
                {"_id":k},
                {"$set":{"mensagens":arr}}
            )
        else:
            await mensagens.insert_one({
                "_id":k,
                "usuarios":[uid,para],
                "mensagens":[msg]
            })

    entregue=await sendto(para,{
        "tipo":"mensagem",
        "mensagem":msg
    })

    await sendto(uid,{
        "tipo":"mensagem_enviada",
        "mensagem":msg,
        "entregue":entregue
    })


def eh_admin(uid,u=None):
    return bool(
        u and
        u.get("admin",False) and
        not u.get("banido",False)
    )


async def exigir_admin(ws,uid):
    u=await usuarios.find_one({"_id":uid})

    if not eh_admin(uid,u):
        await error(ws,"Acesso negado.","SEM_PERMISSAO")
        return False

    return True


async def admin_acao(uid,d,ws):
    if not await exigir_admin(ws,uid):
        return

    alvo=str(d.get("id","")).strip()
    acao=str(d.get("acao","")).strip()

    u=await usuarios.find_one({"_id":alvo})

    if not u:
        return await error(ws,"Usuário não encontrado.")

    if alvo==uid and acao in ("banir","desbanir"):
        return await error(ws,"Você não pode alterar sua própria conta.")

    if acao=="verificar":
        await usuarios.update_one(
            {"_id":alvo},
            {"$set":{"verificado":True}}
        )

    elif acao=="remover_verificado":
        await usuarios.update_one(
            {"_id":alvo},
            {"$set":{"verificado":False}}
        )

    elif acao=="banir":
        await usuarios.update_one(
            {"_id":alvo},
            {"$set":{
                "banido":True,
                "verificado":False
            }}
        )

        alvo_ws=online.get(alvo)

        if alvo_ws:
            await send(alvo_ws,{
                "tipo":"erro",
                "mensagem":"Sua conta foi banida.",
                "codigo":"BANIDO"
            })

            try:await alvo_ws.close()
            except:pass

            online.pop(alvo,None)

    elif acao=="desbanir":
        await usuarios.update_one(
            {"_id":alvo},
            {"$set":{"banido":False}}
        )

    else:
        return await error(ws,"Ação administrativa inválida.")

    p=await perfil(alvo)

    await send(ws,{
        "tipo":"admin_ok",
        "acao":acao,
        "perfil":p
    })

    await transmitir_perfil(alvo)

    print(f"[ADMIN] {uid} -> {acao} -> {alvo}")


async def apagar_conta(uid,ws):
    if not await usuarios.find_one({"_id":uid}):
        return await error(ws,"Conta não encontrada.")

    await usuarios.delete_one({"_id":uid})

    await mensagens.delete_many({
        "usuarios":uid
    })

    online.pop(uid,None)

    await send(ws,{
        "tipo":"conta_apagada",
        "id":uid
    })

    for x in list(online):
        await sendto(x,{
            "tipo":"usuario_removido",
            "id":uid
        })

    try:await ws.close()
    except:pass

    print(f"[CONTA APAGADA] {uid}")


async def handler(ws):
    uid=None

    try:
        async for raw in ws:

            try:
                d=json.loads(raw)
            except:
                await error(ws,"JSON inválido.")
                continue

            tipo=d.get("tipo")

            if tipo=="registro":
                if not uid:
                    uid=await registro(ws,d)

            elif tipo=="login":
                if not uid:
                    uid=await login(ws,d)

            elif not uid:
                await error(ws,"Faça login primeiro.","NAO_AUTENTICADO")

            elif tipo=="usuarios":
                await usuarios_lista(ws)

            elif tipo in ("foto","atualizar_foto","perfil_foto"):
                await foto(uid,d,ws)

            elif tipo=="perfil":
                await perfil_req(ws,d)

            elif tipo=="historico":
                await historico(ws,uid,d)

            elif tipo=="mensagem":
                await mensagem(uid,d)

            elif tipo=="admin":
                await admin_acao(uid,d,ws)

            elif tipo in ("apagar_conta","excluir_conta","deletar_conta"):
                await apagar_conta(uid,ws)
                uid=None
                break

    except websockets.exceptions.ConnectionClosed:
        pass

    except Exception as e:
        print("[SERVER]",e)

    finally:
        if uid and online.get(uid)==ws:
            online.pop(uid,None)
            await transmitir_perfil(uid)
            print(f"[OFFLINE] {uid}")


async def main():
    try:
        await mongo.admin.command("ping")
        print("[MONGO] Conectado com sucesso!")
    except Exception as e:
        print("[MONGO] ERRO:",e)
        return

    await usuarios.create_index("id")
    await mensagens.create_index("usuarios")

    print(f"NEXUS iniciado em {HOST}:{PORT}")

    async with websockets.serve(
        handler,
        HOST,
        PORT,
        max_size=20*1024*1024,
        ping_interval=20,
        ping_timeout=30
    ):
        await asyncio.Future()


if __name__=="__main__":
    asyncio.run(main())
