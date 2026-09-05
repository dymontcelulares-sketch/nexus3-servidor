import asyncio,json,os,time,secrets,hashlib
import websockets

HOST="0.0.0.0"
PORT=int(os.getenv("PORT","10000"))
BASE=os.path.dirname(os.path.abspath(__file__))
DATA=os.path.join(BASE,"data")
USERS=os.path.join(DATA,"usuarios.json")
MSGS=os.path.join(DATA,"mensagens.json")
CONFIG=os.path.join(DATA,"config.json")
ADMIN_PASSWORD=os.getenv("ADMIN_PASSWORD","41914598")
MAX_FOTO=3*1024*1024
MAX_MSG=10*1024*1024

os.makedirs(DATA,exist_ok=True)
lock=asyncio.Lock()
online={}

def load(path,default):
    try:
        if os.path.exists(path):
            with open(path,"r",encoding="utf-8") as f:return json.load(f)
    except Exception as e:print("[JSON]",e)
    return default

def save(path,data):
    tmp=path+".tmp"
    try:
        with open(tmp,"w",encoding="utf-8") as f:
            json.dump(data,f,ensure_ascii=False,separators=(",",":"))
        os.replace(tmp,path)
        return True
    except Exception as e:
        print("[JSON]",e)
        try:os.remove(tmp)
        except:pass
        return False

usuarios=load(USERS,{})
mensagens=load(MSGS,{})
config=load(CONFIG,{"ultimo_id":0})

def senha_hash(s):
    return hashlib.sha256(s.encode()).hexdigest()

def gerar_id():
    n=int(config.get("ultimo_id",0))
    while True:
        n+=1
        uid=f"{n//10000:03d}.{n%10000:04d}"
        if uid not in usuarios:break
    config["ultimo_id"]=n
    save(CONFIG,config)
    return uid

def perfil(uid):
    u=usuarios.get(str(uid))
    if not u:return None
    return {
        "id":u["id"],
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

def chave(a,b):
    return "_".join(sorted([str(a),str(b)]))

async def transmitir_perfil(uid):
    p=perfil(uid)
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
        uid=gerar_id()
        admin=senha==ADMIN_PASSWORD

        usuarios[uid]={
            "id":uid,
            "nome":nome,
            "sobrenome":sobrenome,
            "senha":senha_hash(senha),
            "foto":"",
            "verificado":admin,
            "admin":admin,
            "banido":False,
            "criado_em":int(time.time())
        }

        if not save(USERS,usuarios):
            usuarios.pop(uid,None)
            return await error(ws,"Não foi possível criar sua conta.")

    online[uid]=ws
    p=perfil(uid)

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
    u=usuarios.get(uid)

    if not uid or not u or senha_hash(senha)!=u.get("senha",""):
        return await error(ws,"Nexus ID ou senha incorretos.")

    if u.get("banido",False):
        return await error(ws,"Esta conta foi banida.","BANIDO")

    old=online.get(uid)

    if old and old!=ws:
        try:
            await old.close()
        except:
            pass

    online[uid]=ws
    p=perfil(uid)

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

    for x in usuarios:
        p=perfil(x)
        if p:
            lista.append(p)

    await send(ws,{
        "tipo":"usuarios",
        "usuarios":lista
    })

async def foto(uid,d,ws):
    foto=d.get("foto","")

    if not isinstance(foto,str):
        return await error(ws,"Foto inválida.")

    if len(foto)>MAX_FOTO:
        return await error(ws,"A foto é muito grande.","FOTO_GRANDE")

    if foto and not foto.startswith("data:image/"):
        return await error(ws,"Formato de foto inválido.")

    async with lock:
        if uid not in usuarios:
            return

        usuarios[uid]["foto"]=foto

        if not save(USERS,usuarios):
            return await error(ws,"Não foi possível salvar a foto.")

    p=perfil(uid)

    await send(ws,{
        "tipo":"perfil_atualizado",
        "perfil":p
    })

    await transmitir_perfil(uid)

async def perfil_req(ws,d):
    uid=str(d.get("id","")).strip()

    if uid not in usuarios:
        return await error(ws,"Usuário não encontrado.")

    await send(ws,{
        "tipo":"perfil",
        "perfil":perfil(uid)
    })

async def historico(ws,uid,d):
    outro=str(d.get("para","")).strip()

    if outro not in usuarios:
        return await send(ws,{
            "tipo":"historico",
            "para":outro,
            "mensagens":[]
        })

    lista=mensagens.get(chave(uid,outro),[])[-1000:]

    await send(ws,{
        "tipo":"historico",
        "para":outro,
        "mensagens":lista
    })

async def mensagem(uid,d):
    u=usuarios.get(uid)

    if not u:
        return

    if u.get("banido",False):
        return await error(online.get(uid),"Esta conta foi banida.","BANIDO") if online.get(uid) else None

    para=str(d.get("para","")).strip()

    if para not in usuarios:
        return await sendto(uid,{
            "tipo":"erro",
            "mensagem":"Usuário não encontrado."
        })

    if usuarios.get(para,{}).get("banido",False):
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
        "perfil_remetente":perfil(uid),
        "nome_remetente":u.get("nome",""),
        "sobrenome_remetente":u.get("sobrenome",""),
        "foto_perfil_remetente":u.get("foto",""),
        "verificado_remetente":bool(u.get("verificado",False)),
        "admin_remetente":bool(u.get("admin",False))
    }

    k=chave(uid,para)

    async with lock:
        mensagens.setdefault(k,[])

        if not any(x.get("id")==mid for x in mensagens[k]):
            mensagens[k].append(msg)

        mensagens[k]=mensagens[k][-2000:]

        if not save(MSGS,mensagens):
            return await sendto(uid,{
                "tipo":"erro",
                "mensagem":"Não foi possível salvar a mensagem."
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

def eh_admin(uid):
    u=usuarios.get(uid)

    return bool(
        u and
        u.get("admin",False) and
        not u.get("banido",False)
    )

async def exigir_admin(ws,uid):
    if not eh_admin(uid):
        await error(ws,"Acesso negado.","SEM_PERMISSAO")
        return False

    return True

async def admin_acao(uid,d,ws):
    if not await exigir_admin(ws,uid):
        return

    alvo=str(d.get("id","")).strip()
    acao=str(d.get("acao","")).strip()
    u=usuarios.get(alvo)

    if not u:
        return await error(ws,"Usuário não encontrado.")

    if alvo==uid and acao in ("banir","desbanir"):
        return await error(ws,"Você não pode alterar sua própria conta.")

    if acao=="verificar":
        u["verificado"]=True

    elif acao=="remover_verificado":
        u["verificado"]=False

    elif acao=="banir":
        u["banido"]=True
        u["verificado"]=False

        alvo_ws=online.get(alvo)

        if alvo_ws:
            await send(alvo_ws,{
                "tipo":"erro",
                "mensagem":"Sua conta foi banida.",
                "codigo":"BANIDO"
            })

            try:
                await alvo_ws.close()
            except:
                pass

            online.pop(alvo,None)

    elif acao=="desbanir":
        u["banido"]=False

    else:
        return await error(ws,"Ação administrativa inválida.")

    if not save(USERS,usuarios):
        return await error(ws,"Não foi possível salvar a alteração.")

    p=perfil(alvo)

    await send(ws,{
        "tipo":"admin_ok",
        "acao":acao,
        "perfil":p
    })

    await transmitir_perfil(alvo)

    print(f"[ADMIN] {uid} -> {acao} -> {alvo}")

async def apagar_conta(uid,ws):
    if uid not in usuarios:
        return await error(ws,"Conta não encontrada.")

    async with lock:
        usuarios.pop(uid,None)

        apagar_chaves=[]

        for k in mensagens:
            partes=k.split("_")

            if uid in partes:
                apagar_chaves.append(k)

        for k in apagar_chaves:
            mensagens.pop(k,None)

        if not save(USERS,usuarios):
            return await error(ws,"Não foi possível apagar a conta.")

        save(MSGS,mensagens)

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

    try:
        await ws.close()
    except:
        pass

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

                if uid:
                    continue

                uid=await registro(ws,d)

            elif tipo=="login":

                if uid:
                    continue

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
