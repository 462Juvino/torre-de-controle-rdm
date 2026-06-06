import asyncio
import json
import os
import websockets
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, GiftEvent, LikeEvent, JoinEvent, ConnectEvent, DisconnectEvent

# Dicionário de salas: {"nome_do_tiktoker": {"clientes_ws": set(), "tiktok_task": Task, "client": TikTokLiveClient}}
salas_ativas = {}

async def broadcast_para_sala(tiktok_user, event_type, data):
    """Envia o evento apenas para os jogadores que estão conectados na sala desse TikToker."""
    sala = salas_ativas.get(tiktok_user)
    if not sala or not sala["clientes_ws"]: return
    
    msg = json.dumps({"event": event_type, "data": data})
    
    # Envia para todos os navegadores abertos nesta sala
    clientes_para_remover = set()
    for ws in sala["clientes_ws"]:
        try:
            await ws.send(msg)
        except websockets.exceptions.ConnectionClosed:
            clientes_para_remover.add(ws)
            
    # Limpa conexões mortas
    for ws in clientes_para_remover:
        sala["clientes_ws"].discard(ws)

async def iniciar_tiktok_client(tiktok_user):
    """Inicia a escuta da live de um usuário específico."""
    client = TikTokLiveClient(unique_id=tiktok_user)
    salas_ativas[tiktok_user]["client"] = client

    @client.on(ConnectEvent)
    async def on_connect(event):
        print(f"✅ Conectado na live de: {tiktok_user}")

    @client.on(DisconnectEvent)
    async def on_disconnect(event):
        print(f"❌ Desconectado da live de: {tiktok_user}")

    @client.on(CommentEvent)
    async def on_chat(e):
        pic = getattr(e.user, 'avatar_thumb', {}).get('url_list', [""])[0] if hasattr(e.user, 'avatar_thumb') else ""
        await broadcast_para_sala(tiktok_user, "chat", {
            "nickname": e.user.nickname, "uniqueId": getattr(e.user, 'unique_id', ''), "comment": e.comment, "profilePictureUrl": pic
        })

    @client.on(GiftEvent)
    async def on_gift(e):
        pic = getattr(e.user, 'avatar_thumb', {}).get('url_list', [""])[0] if hasattr(e.user, 'avatar_thumb') else ""
        g_name = getattr(e.gift, 'name', 'Gift')
        g_count = getattr(e.gift, 'count', getattr(e, 'repeat_count', 1))
        moedas = getattr(e.gift.info, 'diamond_count', 1) if hasattr(e.gift, 'info') else 1
        await broadcast_para_sala(tiktok_user, "gift", {
            "nickname": e.user.nickname, "uniqueId": getattr(e.user, 'unique_id', ''), "giftName": g_name, "giftCount": g_count, "diamondCount": moedas, "profilePictureUrl": pic
        })

    @client.on(LikeEvent)
    async def on_like(e):
        pic = getattr(e.user, 'avatar_thumb', {}).get('url_list', [""])[0] if hasattr(e.user, 'avatar_thumb') else ""
        await broadcast_para_sala(tiktok_user, "like", {
            "nickname": e.user.nickname, "uniqueId": getattr(e.user, 'unique_id', ''), "likeCount": e.count, "profilePictureUrl": pic
        })

    @client.on(JoinEvent)
    async def on_join(e):
        pic = getattr(e.user, 'avatar_thumb', {}).get('url_list', [""])[0] if hasattr(e.user, 'avatar_thumb') else ""
        await broadcast_para_sala(tiktok_user, "join", {
            "nickname": e.user.nickname, "uniqueId": getattr(e.user, 'unique_id', ''), "profilePictureUrl": pic
        })

    try:
        await client.start()
    except Exception as e:
        print(f"⚠️ Erro ao conectar no TikTok de {tiktok_user}: {e}")

async def handle_websocket(websocket):
    """Gerencia a conexão do jogo (HTML) com o servidor na nuvem."""
    tiktok_user_atual = None

    try:
        async for message in websocket:
            dados = json.loads(message)
            
            # O jogo HTML envia: {"action": "connect", "tiktok_user": "nome_do_tiktoker"}
            if dados.get("action") == "connect":
                tiktok_user_atual = dados.get("tiktok_user", "").strip().replace("@", "")
                if not tiktok_user_atual: continue

                # Se a sala não existe, cria a sala e liga o motor do TikTok para ela
                if tiktok_user_atual not in salas_ativas:
                    salas_ativas[tiktok_user_atual] = {"clientes_ws": set(), "tiktok_task": None, "client": None}
                    task = asyncio.create_task(iniciar_tiktok_client(tiktok_user_atual))
                    salas_ativas[tiktok_user_atual]["tiktok_task"] = task

                # Adiciona este jogador (celular/navegador) à sala
                salas_ativas[tiktok_user_atual]["clientes_ws"].add(websocket)
                print(f"🌐 Novo jogador entrou na sala: {tiktok_user_atual}")

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # Quando o jogador fechar o jogo no celular, remove ele da sala
        if tiktok_user_atual and tiktok_user_atual in salas_ativas:
            salas_ativas[tiktok_user_atual]["clientes_ws"].discard(websocket)
            
            # Se não sobrar ninguém na sala, desliga a conexão com o TikTok para poupar servidor!
            if len(salas_ativas[tiktok_user_atual]["clientes_ws"]) == 0:
                print(f"🧹 Sala {tiktok_user_atual} vazia. Desligando motor do TikTok para poupar RAM.")
                client = salas_ativas[tiktok_user_atual]["client"]
                if client:
                    try:
                        asyncio.create_task(client.disconnect())
                    except: pass
                
                task = salas_ativas[tiktok_user_atual]["tiktok_task"]
                if task: task.cancel()
                
                del salas_ativas[tiktok_user_atual]

async def main():
    # O Render atribui a porta automaticamente através da variável de ambiente "PORT"
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Torre de Controle Iniciada na porta {port}...")
    async with websockets.serve(handle_websocket, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())