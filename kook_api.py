import requests


class KookAPI:

    def __init__(
            self,
            token
    ):

        self.headers = {

            "Authorization":
                f"Bot {token}"

        }

        self.base = (
            "https://www.kookapp.cn/api/v3"
        )

    def get_guilds(self):

        r = requests.get(
            self.base + "/guild/list",
            headers=self.headers
        )

        data = r.json()

        if data["code"] != 0:
            raise Exception(data)

        return data["data"]["items"]

    def get_channels(
            self,
            guild_id
    ):

        result = []

        page = 1

        while True:

            r = requests.get(
                self.base + "/channel/list",
                headers=self.headers,
                params={
                    "guild_id": guild_id,
                    "type": 2,
                    "page": page,
                    "page_size": 50
                }
            )

            data = r.json()

            if data["code"] != 0:
                raise Exception(data)

            items = data["data"]["items"]

            if not items:
                break

            result.extend(
                items
            )

            # 最后一页

            if len(items) < 50:
                break

            page += 1

        return result

    def get_channel_name(self, channel_id):
        """
        根据频道 ID 查询频道名称
        GET /channel/view?target_id=xxx
        返回频道名称，失败返回空字符串
        """
        try:
            r = requests.get(
                self.base + "/channel/view",
                headers=self.headers,
                params={"target_id": str(channel_id)},
                timeout=10
            )
            data = r.json()
            if data.get("code") != 0:
                return ""
            info = data.get("data", {}) or {}
            return info.get("name") or info.get("channel_name") or ""
        except Exception:
            return ""

    def get_joined_channel(self, user_id, guild_id=None):
        """
        根据 user_id 查询该用户当前所在的语音频道
        GET /channel-user/get-joined-channel
        guild_id 可用于限定只查指定服务器内的频道
        不在语音频道时返回 []
        """
        params = {"user_id": str(user_id)}
        if guild_id:
            params["guild_id"] = str(guild_id)

        r = requests.get(
            self.base + "/channel-user/get-joined-channel",
            headers=self.headers,
            params=params,
            timeout=10
        )
        data = r.json()
        if data["code"] != 0:
            raise Exception(data)

        items = data.get("data", {}).get("items", []) or []
        return [
            {
                "id": str(c["id"]),
                "name": c.get("name") or c.get("channel_name") or c["id"],
            }
            for c in items
            if isinstance(c, dict) and c.get("id")
        ]