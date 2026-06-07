import asyncio
import json
import os
import aiohttp
from aiohttp import web
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, GiftEvent, LikeEvent, JoinEvent, ConnectEvent, DisconnectEvent

salas_ativas = {}


# 📸 O EXTRATOR DE FOTOS HÍBRIDO (Nativo + API Russa TikWM)
async def get_avatar(user):
    # 1. Tenta o método nativo rápido
    try:
        urls = getattr(user.avatar_thumb, 'url_list', [])
        if not urls:
            urls = getattr(user.avatar_thumb, 'urls', [])
        if urls and "dicebear" not in urls[0]:
            return urls[0]
    except Exception:
        pass

    # 2. Se falhar, usa a API TikWM (O truque do Mestre)
    nick = getattr(user, 'unique_id', str(user))
    if not nick: return ""

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://www.tikwm.com/api/user/info?unique_id={nick}",
                                   headers={'User-Agent': 'Mozilla/5.0'},
                                   timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    foto = data.get("data", {}).get("user", {}).get("avatarLarger", "")
                    if foto:
                        return foto
    except Exception:
        pass

    return ""


async def broadcast_para_sala(tiktok_user, event_type, data):
    sala = salas_ativas.get(tiktok_user)
    if not sala or not sala["clientes_ws"]: return

    msg = json.dumps({"event": event_type, "data": data})

    clientes_para_remover = set()
    for ws in sala["clientes_ws"]:
        try:
            await ws.send_str(msg)
        except Exception:
            clientes_para_remover.add(ws)

    for ws in clientes_para_remover:
        sala["clientes_ws"].discard(ws)


async def iniciar_tiktok_client(tiktok_user):
    client = TikTokLiveClient(unique_id=tiktok_user)
    salas_ativas[tiktok_user]["client"] = client

    @client.on(ConnectEvent)
    async def on_connect(event):
        print(f"✅ Conectado na live de: {tiktok_user}", flush=True)

    @client.on(DisconnectEvent)
    async def on_disconnect(event):
        print(f"❌ Desconectado da live de: {tiktok_user}", flush=True)

    @client.on(CommentEvent)
    async def on_chat(e):
        pic = await get_avatar(e.user)
        await broadcast_para_sala(tiktok_user, "chat", {
            "nickname": e.user.nickname, "uniqueId": getattr(e.user, 'unique_id', ''), "comment": e.comment,
            "profilePictureUrl": pic
        })

    @client.on(GiftEvent)
    async def on_gift(e):
        pic = await get_avatar(e.user)
        g_name = getattr(e.gift, 'name', 'Gift')
        g_count = getattr(e.gift, 'count', getattr(e, 'repeat_count', 1))
        moedas = getattr(e.gift.info, 'diamond_count', 1) if hasattr(e.gift, 'info') else 1
        await broadcast_para_sala(tiktok_user, "gift", {
            "nickname": e.user.nickname, "uniqueId": getattr(e.user, 'unique_id', ''), "giftName": g_name,
            "giftCount": g_count, "diamondCount": moedas, "profilePictureUrl": pic
        })

    @client.on(LikeEvent)
    async def on_like(e):
        pic = await get_avatar(e.user)
        await broadcast_para_sala(tiktok_user, "like", {
            "nickname": e.user.nickname, "uniqueId": getattr(e.user, 'unique_id', ''), "likeCount": e.count,
            "profilePictureUrl": pic
        })

    @client.on(JoinEvent)
    async def on_join(e):
        pic = await get_avatar(e.user)
        await broadcast_para_sala(tiktok_user, "join", {
            "nickname": e.user.nickname, "uniqueId": getattr(e.user, 'unique_id', ''), "profilePictureUrl": pic
        })

    try:
        await client.start()
    except Exception as e:
        print(f"⚠️ Erro ao conectar no TikTok de {tiktok_user}: {e}", flush=True)


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    tiktok_user_atual = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                dados = json.loads(msg.data)

                if dados.get("action") == "connect":
                    tiktok_user_atual = dados.get("tiktok_user", "").strip().replace("@", "")
                    if not tiktok_user_atual: continue

                    if tiktok_user_atual not in salas_ativas:
                        salas_ativas[tiktok_user_atual] = {"clientes_ws": set(), "tiktok_task": None, "client": None}
                        task = asyncio.create_task(iniciar_tiktok_client(tiktok_user_atual))
                        salas_ativas[tiktok_user_atual]["tiktok_task"] = task

                    salas_ativas[tiktok_user_atual]["clientes_ws"].add(ws)
                    print(f"🌐 Novo jogador entrou na sala: {tiktok_user_atual}", flush=True)

    except Exception as e:
        pass
    finally:
        if tiktok_user_atual and tiktok_user_atual in salas_ativas:
            salas_ativas[tiktok_user_atual]["clientes_ws"].discard(ws)

            if len(salas_ativas[tiktok_user_atual]["clientes_ws"]) == 0:
                print(f"🧹 Sala {tiktok_user_atual} vazia. Desligando motor.", flush=True)
                client = salas_ativas[tiktok_user_atual]["client"]
                if client:
                    try:
                        asyncio.create_task(client.disconnect())
                    except:
                        pass

                task = salas_ativas[tiktok_user_atual]["tiktok_task"]
                if task: task.cancel()

                del salas_ativas[tiktok_user_atual]

    return ws


async def health_check(request):
    return web.Response(text="Torre de Controle 100% Operacional!", status=200)


app = web.Application()
app.router.add_get('/', websocket_handler)
app.router.add_route('*', '/{tail:.*}', health_check)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Torre de Controle Iniciada na porta {port}...", flush=True)
    web.run_app(app, host="0.0.0.0", port=port)