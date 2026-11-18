"""
KookGateway — KOOK Gateway 单连接 + 事件路由 服务端（多用户模式）

架构（C/S）：
    整个进程里只有 1 条 WebSocket 连接到 KOOK Gateway，
    所有事件都收到后，按 channel_id 桶路由给所有关心该频道的 Client。
    Client 注册时带 user_id，Server 按 user_id 隔离 track_own_join 事件。

对外 API：
    gateway = KookGateway(token)
    gateway.start()
    gateway.register_channel(channel_id, q,
                             voice_cb, track_change_cb,
                             user_id="张三的ID", client_id="Client生成的ID")
    gateway.switch_channel(new_channel_id, user_id="张三的ID")
    gateway.unregister_channel(client_id="xxx")
    gateway.on_client_disconnect(client_id)      # Client 断开时清理
    gateway.stop()

设计要点：
    - 路由表 self._routes: {channel_id: [RouteRecord, ...]} 多用户桶
    - 用户路由 self._user_routes: {user_id: RouteRecord} 每个用户独立追踪
    - 已注册用户 self._registered_user_ids: set 判断事件是不是"自己的"
    - 心跳、会话恢复、重连逻辑复用原来的实现
"""

import json
import re
import zlib
import asyncio
import time
import queue
import threading
import aiohttp
import websockets
from dataclasses import dataclass, field
from typing import Optional, Callable, Dict, Any, List

from logger import get_logger

log = get_logger()


@dataclass
class RouteRecord:
    """一个被客户端注册的频道路由"""
    channel_id: str
    channel_name: str = ""
    guild_id: str = ""
    guild_name: str = ""
    q: Any = None                            # queue.Queue 用于弹幕文字
    voice_event_callback: Optional[Callable] = None
    track_change_callback: Optional[Callable] = None
    track_own_join: bool = False
    user_id: str = ""                        # 这个路由属于哪个用户
    client_id: str = ""                      # 来自哪个 Client 连接

    def __repr__(self):
        return (
            f"Route(ch={self.channel_name or self.channel_id}, "
            f"guild={self.guild_name or self.guild_id}, "
            f"user={self.user_id})"
        )


class KookGateway:
    """KOOK Gateway 单连接 + 事件路由器（全局唯一实例）"""

    # ------------------------------------------------------------------
    # 构造 & 初始化
    # ------------------------------------------------------------------
    def __init__(self, token):
        self.token = token

        self._stop_event = asyncio.Event()
        self._bg_task: Optional[asyncio.Task] = None

        # ── Server 内部状态（只允许在 Server 线程读写）────────
        # channel_id → [RouteRecord]：一个频道可以被多个 Client 同时监听
        self._routes: Dict[str, list] = {}
        # user_id → [RouteRecord]：同一账号可有多个 Client 实例（多标签/多PC）
        self._user_routes: Dict[str, List[RouteRecord]] = {}
        # 所有已注册 user_id 集合（判断事件是不是"自己的"）
        self._registered_user_ids: set = set()

        # Gateway 连接状态
        self._last_msg_timestamp = int(time.time() * 1000)
        self._session_id: Optional[str] = None
        self.heartbeat_interval = 25000
        self._last_msg_time: float = 0
        self._conn_id: int = 0
        self._heartbeat_task: Optional[asyncio.Task] = None

        # 用户信息缓存（按 user_id → nickname）
        self._user_cache: Dict[str, str] = {}

        # ── C/S 命令通道 ──────────────────────────────────────
        self._cmd_queue: "queue.Queue[dict]" = queue.Queue()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._started = threading.Event()

        # ── 事件监听器列表（Server 端用来广播给 WebSocket Client）──
        # 每个 listener 是 async callable: async def fn(event_dict)
        self._event_listeners: list = []

        self.TYPE_LABELS = {
            1: "文字", 2: "图片", 3: "视频", 4: "文件",
            8: "音频", 9: "KMarkdown", 10: "卡片",
        }

    # ------------------------------------------------------------------
    # 对外 API — Client 代理（通过命令通道提交，不直碰内部状态）
    # ------------------------------------------------------------------
    def _submit(self, cmd_type, **args) -> Any:
        """往命令队列塞一个请求，阻塞等 Server 处理完成后返回结果"""
        if not self._started.is_set():
            raise RuntimeError("KookGateway 尚未启动，请先调用 start()")

        done = threading.Event()
        holder: Dict[str, Any] = {"value": None, "error": None}
        self._cmd_queue.put({
            "type": cmd_type,
            "args": args,
            "done": done,
            "holder": holder,
        })

        if not done.wait(timeout=5):
            raise TimeoutError(f"KookGateway 命令 [{cmd_type}] 超时")
        if holder["error"] is not None:
            raise holder["error"]
        return holder["value"]

    def register_channel(
            self,
            channel_id,
            q,
            voice_event_callback=None,
            track_change_callback=None,
            channel_name="",
            guild_id="",
            guild_name="",
            track_own_join=False,
            user_id="",
            client_id="",
            _internal=False,
    ):
        """注册一个频道路由。多用户模式下 user_id/client_id 必填。"""
        if _internal:
            return self._do_register(
                channel_id=channel_id, q=q,
                voice_event_callback=voice_event_callback,
                track_change_callback=track_change_callback,
                channel_name=channel_name, guild_id=guild_id,
                guild_name=guild_name, track_own_join=track_own_join,
                user_id=user_id, client_id=client_id,
            )
        return self._submit(
            "register",
            channel_id=channel_id, q=q,
            voice_event_callback=voice_event_callback,
            track_change_callback=track_change_callback,
            channel_name=channel_name, guild_id=guild_id,
            guild_name=guild_name, track_own_join=track_own_join,
            user_id=user_id, client_id=client_id,
        )

    def switch_channel(
            self,
            new_channel_id,
            channel_name="",
            guild_id="",
            guild_name="",
            user_id="",
            _internal=False,
    ):
        """切换指定用户的监听目标频道。"""
        if _internal:
            return self._do_switch(
                new_channel_id=new_channel_id,
                channel_name=channel_name, guild_id=guild_id, guild_name=guild_name,
                user_id=user_id,
            )
        return self._submit(
            "switch",
            new_channel_id=new_channel_id,
            channel_name=channel_name, guild_id=guild_id, guild_name=guild_name,
            user_id=user_id,
        )

    def unregister_channel(self, client_id="", _internal=False):
        """注销指定 Client 的所有路由。"""
        if _internal:
            self._do_unregister(client_id=client_id)
            return None
        return self._submit("unregister", client_id=client_id)

    def _get_first_route(self, user_id: str) -> Optional[RouteRecord]:
        """取指定 user_id 路由列表中的第一条（通常是 track_own_join 的那条）"""
        routes = self._user_routes.get(str(user_id), [])
        return routes[0] if routes else None

    def get_current_route(self, user_id="", _internal=False):
        """获取指定用户的当前路由（取列表中第一个）。"""
        if _internal:
            if user_id:
                return self._get_first_route(user_id)
            for uid in self._user_routes.keys():
                return self._get_first_route(uid)
            return None
        return self._submit("get_current_route", user_id=user_id)

    # ------------------------------------------------------------------
    # Client 连接管理辅助
    # ------------------------------------------------------------------
    def on_client_disconnect(self, client_id: str):
        """Client 断开时调用，清理其注册的所有路由"""
        if not client_id:
            return
        removed = 0
        # 从频道桶里移除
        for ch_id, records in list(self._routes.items()):
            keep = [r for r in records if r.client_id != client_id]
            if keep:
                self._routes[ch_id] = keep
            else:
                self._routes.pop(ch_id, None)
            removed += len(records) - len(keep)
        # 从 user_routes 里移除（list 里的某一条）
        for uid in list(self._user_routes.keys()):
            old_list = self._user_routes[uid]
            new_list = [r for r in old_list if r.client_id != client_id]
            if new_list:
                self._user_routes[uid] = new_list
            else:
                self._user_routes.pop(uid, None)
                self._registered_user_ids.discard(uid)
            removed += len(old_list) - len(new_list)
        if removed:
            log.info(f"Client {client_id} 断开，清理了 {removed} 条路由")

    # ------------------------------------------------------------------
    # 事件广播（Server 端给 WebSocket Client 推送用）
    # ------------------------------------------------------------------
    def add_event_listener(self, listener):
        """注册一个 async listener: async def fn(event_dict). 返回 unsubscribe"""
        self._event_listeners.append(listener)
        return lambda: self._event_listeners.remove(listener) if listener in self._event_listeners else None

    async def _broadcast_event(self, event_dict):
        """异步广播事件给所有 listener（吞异常避免一个 listener 挂了连累全部）"""
        dead = []
        for fn in self._event_listeners:
            try:
                await fn(event_dict)
            except Exception as e:
                log.warning(f"事件 listener 异常: {e}")
                dead.append(fn)
        for fn in dead:
            if fn in self._event_listeners:
                self._event_listeners.remove(fn)

    # ------------------------------------------------------------------
    # 启动 / 停止
    # ------------------------------------------------------------------
    async def start(self):
        """启动网关（阻塞直到 stop 被调用）"""
        self._loop = asyncio.get_running_loop()
        self._started.set()
        log.info("KookGateway Server 已就绪，开始命令轮询")

        # HTTP 预查询：如果用户已经在语音频道，自动锁上去
        await self._prelock_via_http()

        # 启动命令轮询后台任务
        poll_task = asyncio.create_task(self._poll_commands_loop())

        log.info("KookGateway 启动：开始 WebSocket 主循环")
        try:
            await self._ws_main_loop()
        finally:
            poll_task.cancel()
            try:
                await poll_task
            except asyncio.CancelledError:
                pass
        log.info("KookGateway 停止")

    def start_in_background(self, loop=None):
        """在指定事件循环里后台启动（方便从其他线程调用）"""
        if loop is None:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
        self._bg_task = loop.create_task(self.start())
        return self._bg_task

    def stop(self):
        """关闭网关（Client 侧代理，走命令通道）"""
        if self._started.is_set():
            try:
                self._submit("stop")
            except Exception:
                pass
        self._started.clear()

    # ------------------------------------------------------------------
    # Server 内部 —— 命令处理（只在 Server 线程里运行）
    # ------------------------------------------------------------------
    async def _poll_commands_loop(self):
        """后台任务：每 100ms poll 一次命令队列"""
        while not self._stop_event.is_set():
            try:
                await self._process_pending_commands()
            except Exception as e:
                log.error(f"命令处理异常: {e}")
            await asyncio.sleep(0.1)

    async def _process_pending_commands(self):
        while not self._cmd_queue.empty():
            cmd = self._cmd_queue.get_nowait()
            await self._execute_command(cmd)

    async def _execute_command(self, cmd: dict):
        cmd_type = cmd["type"]
        args = cmd.get("args", {})
        holder = cmd.get("holder", {})
        done = cmd.get("done")
        try:
            if cmd_type == "register":
                holder["value"] = self._do_register(**args)
            elif cmd_type == "switch":
                holder["value"] = self._do_switch(**args)
            elif cmd_type == "unregister":
                self._do_unregister(client_id=args.get("client_id", ""))
                holder["value"] = None
            elif cmd_type == "get_current_route":
                holder["value"] = self._get_first_route(args.get("user_id", ""))
            elif cmd_type == "stop":
                self._stop_event.set()
                holder["value"] = None
            else:
                raise ValueError(f"未知命令: {cmd_type}")
        except Exception as e:
            holder["error"] = e
        finally:
            if done:
                done.set()

    # ------------------------------------------------------------------
    # Server 内部 —— 真正操作 _routes 的逻辑
    # ------------------------------------------------------------------
    def _do_register(
            self,
            channel_id,
            q,
            voice_event_callback=None,
            track_change_callback=None,
            channel_name="",
            guild_id="",
            guild_name="",
            track_own_join=False,
            user_id="",
            client_id="",
    ) -> RouteRecord:
        ch_id = str(channel_id) if channel_id else ""
        user_id = str(user_id) if user_id else ""

        record = RouteRecord(
            channel_id=ch_id,
            channel_name=channel_name or ch_id,
            guild_id=str(guild_id) if guild_id else "",
            guild_name=guild_name or "",
            q=q,
            voice_event_callback=voice_event_callback,
            track_change_callback=track_change_callback,
            track_own_join=track_own_join and bool(user_id),
            user_id=user_id,
            client_id=client_id,
        )

        # 从路由表里移除该 Client 之前注册的（同一频道或不同频道）
        for old_ch, old_records in list(self._routes.items()):
            self._routes[old_ch] = [r for r in old_records if r.client_id != client_id]
            if not self._routes[old_ch]:
                self._routes.pop(old_ch, None)

        # 从 _user_routes 里也清掉该 client_id 的旧记录
        if user_id and user_id in self._user_routes:
            self._user_routes[user_id] = [
                r for r in self._user_routes[user_id] if r.client_id != client_id
            ]
            if not self._user_routes[user_id]:
                self._user_routes.pop(user_id, None)

        # 加入新频道的桶里
        if ch_id:
            self._routes.setdefault(ch_id, []).append(record)

        # 如果带 user_id，追加到该用户的 route list
        if user_id:
            self._user_routes.setdefault(user_id, []).append(record)
            if record.track_own_join:
                self._registered_user_ids.add(user_id)

        # track_own_join + channel_id 为空：异步预查询定位用户当前频道
        if record.track_own_join and not ch_id:
            try:
                loop = asyncio.get_running_loop()
                asyncio.ensure_future(self._prelock_via_http())
                log.info(f"🔍 触发 HTTP 预查询: user={user_id}")
            except RuntimeError:
                pass

        log.info(f"注册路由: ch_id='{ch_id}' track_own_join={record.track_own_join} user_id={user_id} client_id={client_id}")
        log.info(f"  当前 _routes keys: {list(self._routes.keys())}")
        log.info(f"  当前 _user_routes keys: {list(self._user_routes.keys())}")
        log.info(f"  当前 _registered_user_ids: {list(self._registered_user_ids)}")
        return record

    def _do_switch(
            self,
            new_channel_id,
            channel_name="",
            guild_id="",
            guild_name="",
            user_id="",
    ) -> List[RouteRecord]:
        user_id = str(user_id) if user_id else ""
        if not user_id:
            return []

        routes = self._user_routes.get(user_id, [])
        if not routes:
            return []

        new_id = str(new_channel_id)
        switched = []

        for old in routes:
            # 从旧频道桶里移除
            if old.channel_id and old.channel_id in self._routes:
                self._routes[old.channel_id] = [
                    r for r in self._routes[old.channel_id]
                    if r.client_id != old.client_id
                ]
                if not self._routes[old.channel_id]:
                    self._routes.pop(old.channel_id, None)

            # 更新 record
            old.channel_id = new_id
            old.channel_name = channel_name or new_id
            old.guild_id = str(guild_id) if guild_id else ""
            old.guild_name = guild_name or ""

            # 加入新频道桶
            if new_id:
                self._routes.setdefault(new_id, []).append(old)

            switched.append(old)

        log.info(f"切换路由 user={user_id} ({len(routes)} 个 client) → ch={new_id}")
        return switched

    def _do_unregister(self, client_id=""):
        """注销指定 Client 的所有路由"""
        self.on_client_disconnect(client_id)

    # ------------------------------------------------------------------
    # Gateway HTTP API — 获取 WebSocket URL
    # ------------------------------------------------------------------
    async def gateway(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://www.kookapp.cn/api/v3/gateway/index",
                headers={
                    "Authorization": f"Bot {self.token}",
                    "Content-Type": "application/json",
                },
                json={},
            ) as r:
                data = await r.json()
                if data["code"] != 0:
                    raise Exception(data)
                return data["data"]["url"]

    # ------------------------------------------------------------------
    # 心跳
    # ------------------------------------------------------------------
    async def heartbeat(self, ws, conn_id):
        while not self._stop_event.is_set():
            await asyncio.sleep(self.heartbeat_interval / 1000)
            if self._stop_event.is_set() or self._conn_id != conn_id:
                break
            try:
                await ws.send(
                    json.dumps({"s": 2, "d": {"sn": self._last_msg_timestamp}})
                )
            except Exception as e:
                log.error(f"心跳发送失败: {e}")
                break

    async def _check_heartbeat_timeout(self, ws, conn_id):
        while not self._stop_event.is_set():
            await asyncio.sleep(10)
            if self._stop_event.is_set() or self._conn_id != conn_id:
                break
            now = asyncio.get_event_loop().time()
            if self._last_msg_time > 0:
                elapsed = now - self._last_msg_time
                threshold = self.heartbeat_interval / 1000 + 10
                if elapsed > threshold:
                    log.error(
                        f"心跳超时 ({elapsed:.1f}s > {threshold:.1f}s)，强制关闭"
                    )
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    break

    # ------------------------------------------------------------------
    # 用户查询
    # ------------------------------------------------------------------
    async def _fetch_user_nickname(self, user_id, guild_id=None):
        cache_key = f"{guild_id}:{user_id}" if guild_id else user_id
        if cache_key in self._user_cache:
            return self._user_cache[cache_key]

        def _do():
            import requests
            headers = {"Authorization": f"Bot {self.token}"}
            if guild_id:
                try:
                    r = requests.get(
                        "https://www.kookapp.cn/api/v3/guild/user-list",
                        headers=headers,
                        params={
                            "guild_id": guild_id,
                            "filter_user_id": user_id,
                            "page_size": 50,
                        },
                        timeout=5,
                    )
                    data = r.json()
                    if data.get("code") == 0:
                        items = data.get("data", {}).get("items", [])
                        if items:
                            u = items[0]
                            return u.get("nickname") or u.get("username") or None
                except Exception as e:
                    log.warning(f"guild/user-list 异常 user={user_id}: {e}")
            try:
                r = requests.get(
                    "https://www.kookapp.cn/api/v3/user/view",
                    headers=headers,
                    params={"user_id": user_id},
                    timeout=5,
                )
                data = r.json()
                if data.get("code") == 0:
                    user = data.get("data", {})
                    return user.get("username") or None
            except Exception as e:
                log.warning(f"user/view 异常 user={user_id}: {e}")
            return None

        loop = asyncio.get_running_loop()
        nickname = await loop.run_in_executor(None, _do)
        if nickname:
            self._user_cache[cache_key] = nickname
            return nickname
        self._user_cache[cache_key] = user_id
        return user_id

    # ------------------------------------------------------------------
    # HTTP 预查询：启动前定位每个 track_own_join 用户的语音频道
    # ------------------------------------------------------------------
    async def _prelock_via_http(self):
        if not self._registered_user_ids:
            log.info("🔍 HTTP 预查询跳过：_registered_user_ids 为空")
            return

        targets = []
        for uid in self._registered_user_ids:
            for r in self._user_routes.get(uid, []):
                if r.track_own_join:
                    targets.append((uid, r))
                    break
        if not targets:
            log.info("🔍 HTTP 预查询跳过：没有 track_own_join 的 route")
            return

        log.info(f"🔍 HTTP 预查询：遍历 {len(targets)} 个 track_own_join 用户...")
        try:
            from kook_api import KookAPI
            api = KookAPI(self.token)
            loop = asyncio.get_running_loop()
            guilds = await loop.run_in_executor(None, api.get_guilds)
            log.info(f"机器人共加入 {len(guilds)} 个服务器")

            for uid, route in targets:
                log.info(f"🔍 查询 user={uid} 是否在语音频道...")
                found = None
                for g in guilds:
                    try:
                        channels = await loop.run_in_executor(
                            None, api.get_joined_channel, uid, str(g["id"])
                        )
                        if channels:
                            found = (g, channels[0])
                            log.info(
                                f"  ✅ 在 guild '{g.get('name','')}' 找到: "
                                f"ch_id={channels[0]['id']} ch_name={channels[0]['name']}"
                            )
                            break
                    except Exception as e:
                        log.warning(f"  guild {g.get('name','')} 查询失败: {e}")
                        continue

                if not found:
                    log.info(f"  ❌ user={uid} 当前不在任何语音频道")
                    continue

                guild, ch = found
                self.switch_channel(
                    new_channel_id=ch["id"],
                    channel_name=ch["name"],
                    guild_id=str(guild["id"]),
                    guild_name=guild.get("name", ""),
                    user_id=uid,
                    _internal=True,
                )
                log.info(f"  🔒 switch 后 _routes keys: {list(self._routes.keys())}")

                # 本地回调：对该 user_id 下所有 track_change_callback 都调一次
                routes = self._user_routes.get(uid, [])
                for r in routes:
                    if r and r.track_change_callback:
                        try:
                            r.track_change_callback(
                                r.channel_id, r.channel_name, "joined",
                                guild_id=r.guild_id, guild_name=r.guild_name,
                            )
                        except Exception as e:
                            log.warning(f"track_change_callback 异常: {e}")
                # 广播 track 事件
                first = self._get_first_route(uid)
                asyncio.create_task(self._broadcast_event({
                    "type": "event", "event_type": "track",
                    "data": {
                        "channel_id": first.channel_id if first else ch["id"],
                        "channel_name": first.channel_name if first else ch["name"],
                        "action": "joined",
                        "guild_id": str(guild["id"]),
                        "guild_name": guild.get("name", ""),
                        "user_id": uid,
                    },
                }))

        except Exception as e:
            log.warning(f"HTTP 预查询失败: {e}")

    async def _fetch_channel_and_guild_names(self):
        """异步补全所有路由的频道名/服务器名"""
        for uid in list(self._user_routes.keys()):
            await self._fetch_channel_and_guild_names_for(uid)

    async def _fetch_channel_and_guild_names_for(self, user_id: str):
        """补全指定用户所有路由的频道名/服务器名"""
        routes = self._user_routes.get(str(user_id), [])
        if not routes:
            return
        try:
            from kook_api import KookAPI
            api = KookAPI(self.token)
            loop = asyncio.get_running_loop()

            first = True
            for r in routes:
                if not r.channel_id:
                    continue
                channel_name = await loop.run_in_executor(
                    None, api.get_channel_name, r.channel_id
                )
                if channel_name and channel_name != r.channel_id:
                    r.channel_name = channel_name

                if not r.guild_name and r.guild_id:
                    guilds = await loop.run_in_executor(None, api.get_guilds)
                    for g in guilds:
                        if str(g["id"]) == r.guild_id:
                            r.guild_name = g.get("name", "")
                            break

                if channel_name and channel_name != r.channel_id:
                    log.info(f"✅ 频道名称补全 user={user_id}: {channel_name}")
                    if r.track_change_callback:
                        try:
                            r.track_change_callback(
                                r.channel_id, r.channel_name, "joined",
                                guild_id=r.guild_id, guild_name=r.guild_name,
                            )
                        except Exception as e:
                            log.warning(f"track_change_callback 异常: {e}")
                    if first:
                        asyncio.create_task(self._broadcast_event({
                            "type": "event", "event_type": "track",
                            "data": {
                                "channel_id": r.channel_id,
                                "channel_name": r.channel_name,
                                "action": "joined",
                                "guild_id": r.guild_id,
                                "guild_name": r.guild_name,
                                "user_id": user_id,
                            },
                        }))
                        first = False
        except Exception as e:
            log.warning(f"查询频道/服务器名称失败 user={user_id}: {e}")

    # ------------------------------------------------------------------
    # 事件处理 — 语音事件
    # ------------------------------------------------------------------
    async def _handle_voice_event(self, event):
        """
        所有语音事件都进来，做两件事：
        1. 如果是 _registered_user_ids 中某人的事件 + track_own_join → 只切这个用户的路由
        2. 他人事件 → 按 channel_id 桶派发给所有关心该频道的路由
        """
        extra = event.get("extra", {})
        extra_type = extra.get("type", "")
        body = extra.get("body", {})
        log.info(f"▶ [voice入口] extra_type='{extra_type}' body={json.dumps(body, ensure_ascii=False)[:300]} event_keys={list(event.keys())}")

        channel_id = str(body.get("channel_id", ""))
        user_id = str(body.get("user_id", ""))
        guild_id = str(event.get("target_id", ""))

        if not channel_id or not user_id:
            return
        if extra_type not in ("joined_channel", "exited_channel", "left_channel"):
            log.warning(
                f"⚠️ 未知 voice extra.type='{extra_type}' "
                f"channel_id='{channel_id}' user_id='{user_id}' "
                f"完整 extra={extra}"
            )
            return

        action_label = "来了" if extra_type == "joined_channel" else "走了"
        action_enum = "joined" if extra_type == "joined_channel" else "exited"

        # ── 1) 某个注册用户加入/退出 → 先切该用户自己的路由 + 广播 track ─
        is_self = user_id in self._registered_user_ids
        if is_self:
            routes = self._user_routes.get(str(user_id), [])
            has_track_own = any(r.track_own_join for r in routes)
            if has_track_own:
                if extra_type == "joined_channel":
                    self.switch_channel(
                        new_channel_id=channel_id,
                        channel_name=channel_id,  # 先占位
                        guild_id=guild_id,
                        user_id=user_id,
                        _internal=True,
                    )
                    log.info(f"🔒 用户 {user_id} 加入频道，自动锁定: {channel_id}")
                    asyncio.create_task(self._fetch_channel_and_guild_names_for(user_id))
                    asyncio.create_task(self._broadcast_event({
                        "type": "event", "event_type": "track",
                        "data": {"channel_id": channel_id, "channel_name": channel_id,
                                 "action": "joined", "guild_id": guild_id,
                                 "user_id": user_id},
                    }))
                elif extra_type in ("exited_channel", "left_channel"):
                    log.info(f"🔓 用户 {user_id} 退出频道")
                    for route in routes:
                        if route.track_change_callback:
                            try:
                                route.track_change_callback(
                                    channel_id, "", "exited",
                                    guild_id=guild_id, guild_name=route.guild_name,
                                )
                            except Exception as e:
                                log.warning(f"track_change_callback 异常: {e}")
                        old_ch = route.channel_id
                        route.channel_id = ""
                        route.channel_name = ""
                        route.guild_id = ""
                        route.guild_name = ""
                        if old_ch and old_ch in self._routes:
                            self._routes[old_ch] = [
                                r for r in self._routes[old_ch]
                                if r.client_id != route.client_id
                            ]
                            if not self._routes[old_ch]:
                                self._routes.pop(old_ch, None)
                    asyncio.create_task(self._broadcast_event({
                        "type": "event", "event_type": "track",
                        "data": {"channel_id": "", "channel_name": "",
                                 "action": "exited", "guild_id": guild_id,
                                 "user_id": user_id},
                    }))

        # ── 2) 无论是不是自己的事件 → 都要派发 voice 事件给该频道所有观察者 ─
        # （自己的路由在 step 1 已经切好加入了 _routes[channel_id]，所以这里也包含给自己派发）
        routes = self._routes.get(channel_id, [])
        log.info(
            f"🔔 voice事件: channel_id='{channel_id}' user_id='{user_id}' "
            f"extra_type='{extra_type}' is_self={is_self} "
            f"_routes.keys={list(self._routes.keys())} routes_count={len(routes)}"
        )

        try:
            nickname = await self._fetch_user_nickname(user_id, guild_id)
        except Exception as e:
            log.warning(f"_fetch_user_nickname 异常 user={user_id}: {e}")
            nickname = user_id
        msg = f"{nickname} {action_label}"
        log.info(msg)

        # 本地派发：跳过"自己"的事件（同一 user_id 不需要给自己显示"我来了/我走了"）
        for route in routes:
            if route.user_id and route.user_id == user_id:
                continue

            if route.q:
                route.q.put(msg)

            if route.voice_event_callback:
                try:
                    route.voice_event_callback({
                        "action": action_enum,
                        "user_id": user_id,
                        "nickname": nickname,
                        "channel_id": channel_id,
                        "channel_name": route.channel_name or channel_id,
                        "guild_id": guild_id,
                        "raw": event,
                    })
                except Exception as e:
                    log.warning(f"voice_event_callback 异常: {e}")

        # 远程广播始终发（过滤由 server.py 的 client_channels 做）
        asyncio.create_task(self._broadcast_event({
            "type": "event",
            "event_type": "voice",
            "data": {
                "msg": msg,
                "action": action_enum,
                "user_id": user_id,
                "nickname": nickname,
                "channel_id": channel_id,
                "guild_id": guild_id,
            },
        }))

    # ------------------------------------------------------------------
    # 事件处理 — 文字消息
    # ------------------------------------------------------------------
    async def _handle_message_event(self, event, event_type):
        target = str(event.get("target_id"))
        routes = self._routes.get(target, [])

        author = event.get("extra", {}).get("author", {})
        name = (
            author.get("nickname")
            or author.get("username")
            or "未知用户"
        )
        type_label = self.TYPE_LABELS.get(event_type, "未知")

        if event_type == 9:
            content = event.get("extra", {}).get("kmarkdown", {}).get("raw_content", "")
            if not content:
                return
            content = re.sub(r'\\(.)', r'\1', content)
            msg = f"{name}：{content}"
            log.info(f"收到{type_label}消息: {msg}")
        else:
            content = ""
            msg = f"{name} 发了一条{type_label}消息"
            log.info(f"收到{type_label}消息: user={name}")

        # 本地派发
        for route in routes:
            if route.q:
                route.q.put(msg)

        # 远程广播始终发（过滤由 server.py 的 client_channels 做）
        asyncio.create_task(self._broadcast_event({
            "type": "event",
            "event_type": "message",
            "data": {
                "msg": msg,
                "channel_id": target,
                "author": name,
                "content": content,
                "type_label": type_label,
            },
        }))

    # ------------------------------------------------------------------
    # 事件总分发
    # ------------------------------------------------------------------
    async def _process_event_dispatch(self, data):
        event = data.get("d", {})
        if not isinstance(event, dict):
            return

        event_timestamp = (
            event.get("created_at")
            or event.get("msg_timestamp")
            or 0
        )
        if (
            self._last_msg_timestamp > 0
            and event_timestamp > 0
            and event_timestamp <= self._last_msg_timestamp
        ):
            return
        if event_timestamp > self._last_msg_timestamp:
            self._last_msg_timestamp = event_timestamp

        event_type = event.get("type")
        extra = event.get("extra", {})
        extra_type = extra.get("type", "")

        # 系统通知事件（type=255）
        if event_type == 255:
            if extra_type in ("joined_channel", "exited_channel", "left_channel"):
                await self._handle_voice_event(event)
            else:
                log.warning(
                    f"⚠️ 系统通知但非语音进出: extra.type='{extra_type}' "
                    f"完整事件={json.dumps(event, ensure_ascii=False)[:300]}"
                )
            return

        # 未识别的消息类型
        if event_type not in self.TYPE_LABELS:
            if event_type is not None:
                log.info(
                    f"未知事件类型: type={event_type}, extra.type={extra_type}"
                )
            return

        # 业务消息 — 派发到对应频道路由
        await self._handle_message_event(event, event_type)

    # ------------------------------------------------------------------
    # 会话恢复 & 主循环
    # ------------------------------------------------------------------
    async def _cleanup_old_tasks(self):
        for t in [self._heartbeat_task]:
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        self._heartbeat_task = None

    def _reset_session_state(self):
        log.info("重置会话状态")
        self._last_msg_timestamp = int(time.time() * 1000)
        self._session_id = None

    async def _connect_with_resume(self, base_url):
        if not self._session_id or self._last_msg_timestamp <= 0:
            return None

        resume_url = (
            f"{base_url}&resume=1"
            f"&sn={self._last_msg_timestamp}"
            f"&session_id={self._session_id}"
        )
        log.info(
            f"尝试恢复会话: sn={self._last_msg_timestamp}, "
            f"session_id={self._session_id}"
        )

        try:
            ws = await websockets.connect(resume_url, ping_interval=None)
            log.info("会话恢复连接已建立")

            first_msg = await asyncio.wait_for(ws.recv(), timeout=10)
            if isinstance(first_msg, bytes):
                try:
                    first_msg = zlib.decompress(first_msg).decode("utf-8")
                except Exception:
                    first_msg = first_msg.decode("utf-8")
            data = json.loads(first_msg)
            log.info(f"恢复后首条消息: s={data.get('s')}")

            sig = data.get("s")
            if sig == 1:
                session_data = data.get("d", {})
                new_sid = session_data.get("session_id")
                if new_sid and new_sid != self._session_id:
                    log.info("服务端返回新 session_id")
                    self._session_id = new_sid
                interval = session_data.get("heartbeat_interval")
                if interval:
                    self.heartbeat_interval = interval
                self._last_msg_time = asyncio.get_event_loop().time()
                return ws
            elif sig == 5:
                log.warning("服务端拒绝恢复 (RECONNECT)")
                self._reset_session_state()
            elif sig == 3:
                log.info("恢复成功，接收补发事件...")
                self._last_msg_time = asyncio.get_event_loop().time()
                await self._process_event_dispatch(data)
                return ws
            else:
                log.warning(f"恢复异常，收到 s={sig}")
                self._reset_session_state()

            try:
                await ws.close()
            except Exception:
                pass
            return None

        except asyncio.TimeoutError:
            log.warning("恢复连接超时")
            self._reset_session_state()
            return None
        except Exception as e:
            log.warning(f"恢复连接失败: {e}")
            self._reset_session_state()
            return None

    async def _receive_loop(self, ws, conn_id):
        self._last_msg_time = asyncio.get_event_loop().time()
        timeout_task = asyncio.create_task(
            self._check_heartbeat_timeout(ws, conn_id)
        )

        try:
            async for raw in ws:
                if self._stop_event.is_set() or self._conn_id != conn_id:
                    break

                try:
                    if isinstance(raw, bytes):
                        try:
                            raw = zlib.decompress(raw).decode("utf-8")
                        except Exception:
                            raw = raw.decode("utf-8")

                    data = json.loads(raw)
                    self._last_msg_time = asyncio.get_event_loop().time()
                    sig = data.get("s")

                    if sig == 1:
                        session_data = data.get("d", {})
                        new_sid = session_data.get("session_id")
                        if new_sid:
                            self._session_id = new_sid
                            log.info(f"获取到 session_id: {self._session_id}")
                        interval = session_data.get("heartbeat_interval")
                        if interval:
                            self.heartbeat_interval = interval
                            log.info(f"心跳间隔: {self.heartbeat_interval}ms")
                        if self._heartbeat_task and not self._heartbeat_task.done():
                            self._heartbeat_task.cancel()
                        self._heartbeat_task = asyncio.create_task(
                            self.heartbeat(ws, conn_id)
                        )
                        self._last_msg_time = asyncio.get_event_loop().time()
                        continue

                    if sig == 2:
                        try:
                            await ws.send(json.dumps({"s": 3}))
                        except Exception:
                            pass
                        self._last_msg_time = asyncio.get_event_loop().time()
                        continue

                    if sig == 3:
                        d = data.get("d")
                        if d and isinstance(d, dict) and d.get("type"):
                            await self._process_event_dispatch(data)
                            continue
                        continue

                    if sig == 5:
                        log.warning("收到 RECONNECT 信令")
                        self._reset_session_state()
                        try:
                            await ws.close()
                        except Exception:
                            pass
                        break

                    if sig is None:
                        continue

                    await self._process_event_dispatch(data)

                except Exception as e:
                    log.error(f"消息处理错误: {e}")

        except (
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK,
            ConnectionResetError,
            OSError,
            asyncio.CancelledError,
        ):
            log.info("WebSocket 连接已关闭")
        except Exception as e:
            log.warning(f"接收消息异常: {e}")
        finally:
            timeout_task.cancel()
            try:
                await timeout_task
            except asyncio.CancelledError:
                pass

    async def _ws_main_loop(self):
        retry_delay = 1
        max_delay = 30
        max_retries = 100

        while not self._stop_event.is_set() and max_retries > 0:
            ws = None
            try:
                await self._cleanup_old_tasks()

                url = await self.gateway()
                self._conn_id += 1
                current_conn_id = self._conn_id

                log.info("KOOK WebSocket 连接（网关模式，接收所有事件）")

                ws = await self._connect_with_resume(url)
                if ws is None:
                    log.info("全新连接（无可用会话恢复）")
                    self._last_msg_timestamp = int(time.time() * 1000)
                    ws = await websockets.connect(url, ping_interval=None)

                await self._receive_loop(ws, current_conn_id)
                retry_delay = 1
                max_retries -= 1

            except Exception as e:
                err_msg = str(e).strip()
                if err_msg:
                    log.warning(f"WebSocket 断开: {e}")
                if self._session_id:
                    log.info(
                        f"保留会话状态用于恢复: "
                        f"session_id={self._session_id}, last_ts={self._last_msg_timestamp}"
                    )
                else:
                    log.info("无会话状态，下次为全新连接")
            finally:
                if ws:
                    try:
                        await ws.close()
                    except Exception:
                        pass

            if not self._stop_event.is_set():
                log.info(f"{retry_delay}秒后重连...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_delay)

        if max_retries <= 0:
            log.error("达到最大重连次数")