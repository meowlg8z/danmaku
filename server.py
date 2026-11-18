"""
KOOK 弹幕助手 — C/S 架构服务端（多用户模式）

职责：
  1. 独占一个 Bot Token，唯一一条 WebSocket 连接 KOOK Gateway
  2. 启一个 WebSocket Server（默认 :8765），所有 Client 通过它发命令 + 收事件
  3. Client 注册时带上自己的 user_id，Server 按 user_id 分桶隔离事件

启动方式：
  python server.py --token "xxxxxxxx" --host 0.0.0.0 --port 8765

也可以把 token 写进 config.json，直接 python server.py 启动。
"""

import asyncio
import json
import signal
import sys
import uuid
from typing import Optional

import websockets
from websockets.asyncio.server import ServerConnection

from kook_ws import KookGateway, RouteRecord
from logger import get_logger

log = get_logger()


class GatewayServer:
    """KookGateway + WebSocket Server 组合体"""

    def __init__(
            self,
            token: str,
            host: str = "0.0.0.0",
            port: int = 8765,
    ):
        self.token = token
        self.host = host
        self.port = port

        self.gw = KookGateway(token)

        # client_id -> ws
        self.clients: dict[str, ServerConnection] = {}
        # client_id -> user_id（记录谁是谁）
        self.client_users: dict[str, str] = {}
        # client_id -> 当前监听的 channel_id（按此过滤 voice/message 事件）
        self.client_channels: dict[str, str] = {}

        self._shutdown = asyncio.Event()

    # ------------------------------------------------------------------
    # Client 连接管理
    # ------------------------------------------------------------------
    async def _on_connect(self, ws: ServerConnection):
        client_id = uuid.uuid4().hex[:8]
        self.clients[client_id] = ws
        log.info(f"Client 连接: {client_id} (共 {len(self.clients)} 个)")

        unsubscribe = self.gw.add_event_listener(
            lambda ev, _ws=ws, _cid=client_id: self._push_to_client(_ws, _cid, ev)
        )

        try:
            async for raw in ws:
                try:
                    req = json.loads(raw)
                except Exception:
                    continue
                await self._handle_client_cmd(client_id, ws, req)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            log.warning(f"Client {client_id} 异常: {e}")
        finally:
            unsubscribe()
            self.clients.pop(client_id, None)
            self.client_users.pop(client_id, None)
            # 通知 Gateway 清理该 Client 的所有路由
            self.gw.on_client_disconnect(client_id)
            log.info(f"Client 断开: {client_id} (剩 {len(self.clients)} 个)")

    async def _push_to_client(self, ws: ServerConnection, client_id: str, event_dict: dict):
        """带过滤的事件推送：只把属于该 client 的事件发给它，同时同步 server 端 client_channels"""
        try:
            if event_dict.get("type") != "event":
                await ws.send(json.dumps(event_dict, ensure_ascii=False, default=str))
                return

            event_type = event_dict.get("event_type", "")
            data = event_dict.get("data", {})

            # track 事件：同步更新 server 端 client_channels（所有同 user_id 的 client）
            if event_type == "track":
                ev_user_id = str(data.get("user_id", ""))
                ev_ch = str(data.get("channel_id", ""))
                for cid, uid in self.client_users.items():
                    if uid == ev_user_id:
                        if ev_ch:
                            self.client_channels[cid] = ev_ch
                        else:
                            self.client_channels.pop(cid, None)

                # 只发给 data.user_id 匹配的 client
                my_user_id = self.client_users.get(client_id, "")
                if ev_user_id and my_user_id and ev_user_id != my_user_id:
                    return

            elif event_type in ("voice", "message"):
                # voice / message 事件只发给当前监听该 channel 的 client
                ev_ch = str(data.get("channel_id", ""))
                my_ch = self.client_channels.get(client_id, "")
                if ev_ch and my_ch and ev_ch != my_ch:
                    return

            await ws.send(json.dumps(event_dict, ensure_ascii=False, default=str))
        except Exception:
            pass

    async def _handle_client_cmd(self, client_id: str, ws: ServerConnection, req: dict):
        cmd_type = req.get("type")
        req_id = req.get("req_id")
        args = req.get("args", {})

        def reply(data=None, error=None):
            resp = {"type": "resp", "req_id": req_id}
            if error is not None:
                resp["ok"] = False
                resp["error"] = str(error)
            else:
                resp["ok"] = True
                resp["data"] = data
            asyncio.create_task(
                ws.send(json.dumps(resp, ensure_ascii=False, default=str))
            )

        try:
            if cmd_type == "register":
                user_id = str(args.get("user_id", ""))
                if user_id:
                    self.client_users[client_id] = user_id

                ch_id = str(args.get("channel_id", ""))
                if ch_id:
                    self.client_channels[client_id] = ch_id
                elif args.get("track_own_join"):
                    # track_own_join 模式下初始 channel_id 可能为空，等事件触发后更新
                    self.client_channels.pop(client_id, None)

                self.gw.register_channel(
                    channel_id=ch_id,
                    q=None,
                    voice_event_callback=None,
                    track_change_callback=None,
                    channel_name=args.get("channel_name", ""),
                    guild_id=args.get("guild_id", ""),
                    guild_name=args.get("guild_name", ""),
                    track_own_join=args.get("track_own_join", False),
                    user_id=user_id,
                    client_id=client_id,
                    _internal=True,
                )
                reply(data={"status": "registered"})

            elif cmd_type == "switch":
                new_ch = str(args.get("new_channel_id", ""))
                if new_ch:
                    self.client_channels[client_id] = new_ch
                else:
                    self.client_channels.pop(client_id, None)

                self.gw.switch_channel(
                    new_channel_id=new_ch,
                    channel_name=args.get("channel_name", ""),
                    guild_id=args.get("guild_id", ""),
                    guild_name=args.get("guild_name", ""),
                    user_id=str(args.get("user_id", "")),
                    _internal=True,
                )
                reply(data={"status": "switched"})

            elif cmd_type == "unregister":
                self.gw.unregister_channel(client_id=client_id, _internal=True)
                self.client_users.pop(client_id, None)
                self.client_channels.pop(client_id, None)
                reply(data={"status": "unregistered"})

            elif cmd_type == "get_current_route":
                r = self.gw.get_current_route(
                    user_id=str(args.get("user_id", "")), _internal=True
                )
                reply(data=_route_to_dict(r) if r else None)

            elif cmd_type == "ping":
                reply(data={"pong": True})

            else:
                reply(error=f"未知命令: {cmd_type}")

        except Exception as e:
            log.warning(f"命令 {cmd_type} 处理失败: {e}")
            reply(error=e)

    # ------------------------------------------------------------------
    # HTTP 健康检查（供外部 cron ping 保活 Render）
    # ------------------------------------------------------------------
    async def _process_http_request(self, path, headers):
        """拦截非 WebSocket 升级的 HTTP 请求，返回健康检查响应"""
        if path in ("/healthz", "/health", "/"):
            body = b'{"status":"ok","service":"kook-gateway"}'
            return (
                200,
                [
                    (b"Content-Type", b"application/json"),
                    (b"Content-Length", str(len(body)).encode()),
                    (b"Access-Control-Allow-Origin", b"*"),
                ],
                body,
            )
        return None

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------
    async def run(self):
        async with websockets.serve(
            self._on_connect, self.host, self.port,
            max_size=10 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=20,
            process_request=self._process_http_request,
        ):
            log.info(f"WebSocket Server 已启动: ws://{self.host}:{self.port}")
            log.info(f"健康检查端点: http(s)://<域名>/healthz")
            log.info("等待 Client 连接...")
            await self.gw.start()


def _route_to_dict(r: Optional[RouteRecord]) -> Optional[dict]:
    if not r:
        return None
    return {
        "channel_id": r.channel_id,
        "channel_name": r.channel_name,
        "guild_id": r.guild_id,
        "guild_name": r.guild_name,
        "track_own_join": r.track_own_join,
        "user_id": r.user_id,
    }


def _load_token_from_config() -> str:
    try:
        from config import load_config
        cfg = load_config()
        return cfg.get("token", "")
    except Exception:
        return ""


def _load_token_from_txt() -> str:
    try:
        with open("token.txt", "r", encoding="utf-8") as f:
            t = f.read().strip()
            if t:
                log.info("从 token.txt 读取到 Token")
            return t
    except FileNotFoundError:
        return ""
    except Exception as e:
        log.warning(f"读取 token.txt 失败: {e}")
        return ""


def _prompt_token() -> str:
    try:
        t = input("请输入 KOOK Bot Token（回车确认）: ").strip()
        if t:
            try:
                with open("token.txt", "w", encoding="utf-8") as f:
                    f.write(t)
                log.info("Token 已保存到 token.txt，下次启动自动读取")
            except Exception:
                pass
        return t
    except Exception:
        return ""


async def _serve_forever(srv: GatewayServer):
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: srv._shutdown.set())
        except NotImplementedError:
            pass

    gw_task = asyncio.create_task(srv.run())
    shutdown_task = asyncio.create_task(srv._shutdown.wait())

    done, pending = await asyncio.wait(
        [gw_task, shutdown_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    if shutdown_task in done:
        log.info("收到退出信号，正在关闭...")
        gw_task.cancel()
        try:
            await gw_task
        except asyncio.CancelledError:
            pass

    for t in pending:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description="KOOK 弹幕网关服务端（多用户）")
    parser.add_argument("--token", default="", help="KOOK Bot Token")
    parser.add_argument("--host", default="0.0.0.0", help="WebSocket Server 监听地址")
    parser.add_argument("--port", type=int, default=8765, help="WebSocket Server 端口")
    args = parser.parse_args()

    token = (
        args.token
        or _load_token_from_config()
        or _load_token_from_txt()
        or _prompt_token()
    )
    if not token:
        print("错误：未获取到 Bot Token，无法启动")
        sys.exit(1)

    srv = GatewayServer(
        token=token,
        host=args.host,
        port=args.port,
    )

    try:
        asyncio.run(_serve_forever(srv))
    except KeyboardInterrupt:
        log.info("收到 Ctrl+C，退出")


if __name__ == "__main__":
    main()