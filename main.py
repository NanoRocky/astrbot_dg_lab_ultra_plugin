import asyncio
import json
from typing import Any

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.config.default import VERSION
from astrbot.core.message.components import At


@register(
    "astrbot_dg_lab_ultra_plugin", "RC-CHN & NanoRocky", "郊狼API控制插件", "3.1.1"
)
class MyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        game_api_config = config.get("game_api", {})
        target_info = config.get("target_info", {})
        authorized_settings = config.get("authorized_settings", {})
        group_settings = config.get("group_settings", {})
        self.session = None
        self.base_url = game_api_config.get("base_url", "")
        self.default_client_id = game_api_config.get("default_client_id", "all")
        self.verify_ssl = game_api_config.get("verify_ssl", True)
        self.target_user_id = target_info.get("user_id", "未设置")
        self.target_user_name = target_info.get("user_name", "未设置")
        self.allow_all_users = authorized_settings.get("allow_all_users", False)
        self.authorized_users = set(authorized_settings.get("authorized_users", []))
        self.allow_group_chat = group_settings.get("allow_group_chat", False)
        self.allowed_groups = set(group_settings.get("allowed_groups", []))

    def _save_config_updates(self):
        if "authorized_settings" not in self.config:
            self.config["authorized_settings"] = {}
        self.config["authorized_settings"]["authorized_users"] = list(
            self.authorized_users
        )
        self.config["authorized_settings"]["allow_all_users"] = self.allow_all_users

        if "group_settings" not in self.config:
            self.config["group_settings"] = {}
        self.config["group_settings"]["allowed_groups"] = list(self.allowed_groups)
        self.config["group_settings"]["allow_group_chat"] = self.allow_group_chat

        try:
            self.config.save_config()
        except Exception as e:
            logger.error(f"保存配置失败: {e}")

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        if not self.base_url:
            return {"error": "API基础URL未配置"}
        target_client_id = self.default_client_id
        if not target_client_id:
            return {"error": "客户端ID未指定且未配置默认值"}

        import re

        if not re.match(r"^[A-Za-z0-9_-]+$", target_client_id):
            return {"error": "客户端ID格式不合法"}

        url = f"{self.base_url.rstrip('/')}/api/v2/game/{target_client_id}{path}"
        logger.debug(f"Calling API: {url} with {kwargs}")

        headers = {
            "Accept": "application/json",
            "Referer": "https://astrbot.app/",
            "User-Agent": f"AstrBot/{VERSION}",
            "UAK": "AstrBot/plugin_dglab",
        }
        if "json" in kwargs:
            headers["Content-Type"] = "application/json"
            logger.debug(f"Payload: {json.dumps(kwargs['json'])}")

        try:
            if getattr(self, "session", None) is None or getattr(
                self.session, "closed", True
            ):
                timeout = aiohttp.ClientTimeout(total=10)
                self.session = aiohttp.ClientSession(trust_env=True, timeout=timeout)

            if self.session is None:
                return {"error": "内部错误：无法初始化 HTTP Session。"}

            async with self.session.request(
                method, url, ssl=self.verify_ssl, headers=headers, **kwargs
            ) as response:
                try:
                    res_json = await response.json(content_type=None)
                    logger.debug(
                        f"API response JSON: {json.dumps(res_json, ensure_ascii=False)}"
                    )

                    is_offline = False
                    if isinstance(res_json, dict):
                        if res_json.get("status") in [0, "0"] and "NO_CLIENT" in str(
                            res_json.get("code", "")
                        ):
                            is_offline = True
                        elif (
                            method == "GET"
                            and path == ""
                            and res_json.get("clientStrength") is None
                        ):
                            is_offline = True
                        elif (
                            method == "GET"
                            and path == "/strength"
                            and res_json.get("strengthConfig") is None
                        ):
                            is_offline = True

                    if is_offline:
                        return {
                            "status": 0,
                            "code": "DEVICE_NOT_CONNECTED",
                            "message": "警告：检测到郊狼终端未开启或未成功连接。",
                            "error": "设备未开启或连接丢失，控制无效～",
                            "raw_response": res_json,
                        }

                    return res_json
                except asyncio.CancelledError:
                    raise
                except Exception:
                    text = await response.text()
                    logger.error(f"非正常JSON响应: {text[:200]}")
                    return {
                        "error": f"非正常JSON响应 (状态码 {response.status}): {text[:200]}",
                        "status_code": response.status,
                    }
        except asyncio.CancelledError:
            raise
        except aiohttp.ClientConnectorError as e:
            logger.error(f"连接错误: {e}")
            return {"error": f"连接到API服务器失败: {e}"}
        except Exception as e:
            logger.error(f"API请求出错: {e}")
            return {"error": str(e)}

    async def _update_game_config_ws(self, new_config_fields: dict) -> dict:
        """通过 WebSocket 增量更新 gameConfig，绕过不存在的 HTTP POST /config 接口"""
        if not self.base_url:
            return {"status": 0, "error": "API基础URL未配置"}

        current_state = await self._request("GET", "")
        if current_state.get("status") in [0, "0"] or current_state.get("error"):
            return current_state

        current_config = current_state.get("gameConfig", {})
        current_config.update(new_config_fields)

        ws_url = (
            self.base_url.replace("http://", "ws://")
            .replace("https://", "wss://")
            .rstrip("/")
            + "/ws"
        )
        target_client_id = self.default_client_id

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(ws_url, ssl=self.verify_ssl) as ws:
                    await ws.send_json(
                        {
                            "action": "bindClient",
                            "clientId": target_client_id,
                            "requestId": "sys_bind",
                        }
                    )

                    async def wait_for_bind():
                        while True:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if (
                                    data.get("event") == "response"
                                    and data.get("requestId") == "sys_bind"
                                ):
                                    return data.get("data", {}).get("status") == 1

                    try:
                        bind_success = await asyncio.wait_for(
                            wait_for_bind(), timeout=3.0
                        )
                        if not bind_success:
                            return {"status": 0, "error": "WebSocket客户端绑定失败"}
                    except asyncio.TimeoutError:
                        return {"status": 0, "error": "WebSocket绑定超时"}

                    await ws.send_json(
                        {
                            "action": "updateConfig",
                            "type": "main-game",
                            "config": current_config,
                            "requestId": "sys_update_config",
                        }
                    )

                    try:

                        async def recv_update_response():
                            while True:
                                msg = await ws.receive()
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if (
                                        data.get("event") == "response"
                                        and data.get("requestId") == "sys_update_config"
                                    ):
                                        resp = data.get("data", {})
                                        if resp.get("status") == 0:
                                            err_msg = resp.get("message", "更新失败")
                                            if "detail" in resp:
                                                err_msg += f" | {resp.get('detail')}"
                                            return {"status": 0, "error": err_msg}
                                    elif (
                                        data.get("event") == "gameConfigUpdated"
                                        and data.get("data", {}).get("type")
                                        == "main-game"
                                    ):
                                        return {
                                            "status": 1,
                                            "code": "OK",
                                            "message": "跨通道：游戏高级配置已成功更新！",
                                        }
                                elif msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.ERROR,
                                ):
                                    return {"status": 0, "error": "WebSocket 提前关闭"}

                        return await asyncio.wait_for(
                            recv_update_response(), timeout=3.0
                        )
                    except asyncio.TimeoutError:
                        return {
                            "status": 0,
                            "error": "修改游戏高级配置超时（服务器未响应）",
                        }
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"WS更新游戏配置出错: {e}")
            return {"status": 0, "error": f"通过WebSocket修改设置失败: {e}"}

    @filter.llm_tool(name="dglab_get_target_info")
    async def dglab_get_target_info(self, event: AstrMessageEvent) -> str:
        """获取当前郊狼插件控制的目标受控人员信息以及可用指令说明。当你想知道你在控制谁，或需要了解有哪些控制能力时，请调用此函数。"""
        help_text = f"当前郊狼设备的佩戴者/受控者是：{self.target_user_name} ({self.target_user_id})。\n\n"
        help_text += "作为助手，你可以通过 API 对该受控者进行操作。\n"
        help_text += "操作前，可以先试着了解受控者使用了什么电极，是否使用了 B 通道，A 通道和 B 通道分别接在什么部位。\n"
        help_text += "操作时，除了基础强度外，还可以通过设置随机强度、波形、B 通道强度等，丰富体验。\n\n"
        help_text += "**重要状态提示**：如果你在调用任意游戏/强度获取接口时，收到返回码为 `DEVICE_NOT_CONNECTED`，说明郊狼未开启，或掉线，在此状态下任何电击指令等均无效。\n\n"
        help_text += "以下是你可以调用的主要函数说明：\n"
        help_text += "- dglab_get_game_info: 获取受控者的设备状态与最大强度限制（强烈建议操作前调用）。\n"
        help_text += "- dglab_get_strength: 查看当前的基础与随机强度。\n"
        help_text += "- dglab_set_strength: 修改当前的基础与随机强度（包含“增加、减少、设为一定值”等逻辑），这是**基础的使用和控制方式**。\n"
        help_text += "- dglab_get_pulse_list: 查看完整电击波形列表。\n"
        help_text += "- dglab_get_pulse: 查看当前启用的电击波形。\n"
        help_text += "- dglab_set_pulse: 修改电击波形（可传入多波形ID以按模式混合）。\n"
        help_text += "- dglab_set_game_config: 修改游戏高阶设置（随机强度波动时间、波形播放模式如顺序/随机、波形切换时间、B通道开关(是否启用控制B通道)、B通道强度的倍率(必须是正整数)、开火专属波形等）。\n"
        help_text += "- dglab_action_fire: 对该受控者进行一次短时间的强刺激电击（需要给出强度，可附加时间），这是**偏重的惩罚方式**。\n"
        return help_text

    @filter.llm_tool(name="dglab_get_game_info")
    async def dglab_get_game_info(self, event: AstrMessageEvent) -> str:
        """获取郊狼游戏终端的整体信息。包含当前设定的基础和随机强度、波形列表、波形播放模式，以及客户端被限制的最大强度等。建议调整参数或开火前先查询。

        Returns:
            JSON 字符串。包含以下完整字段：
            - `status` (int): 请求状态码，1 表示成功，0 表示失败。
            - `code` (str): 状态码信息，如 "OK"。
            - `strengthConfig` (object): 强度设置信息：
                - `strength` (number): 基础强度，只能为正整数。
                - `randomStrength` (number): 随机波动强度，只能为正整数，实际强度范围：[strength, strength + randomStrength]。随机数会从 0 到 randomStrength 之间随机生成，然后与基础强度相加。例如，如果 strength=5，randomStrength=15，则实际强度会在 5 到 20 之间波动。
            - `gameConfig` (object): 游戏配置信息：
                - `strengthChangeInterval` (array): 随机强度变化间隔的时间范围，如 [15, 30]，单位：秒。
                - `enableBChannel` (boolean): 是否启用了 B 通道。
                - `bChannelStrengthMultiplier` (number): B 通道的强度倍数（A 通道正在运行的实际强度*倍数=B 通道正在运行的实际强度）。
                - `pulseId` (string 或 array): 当前使用的波形列表。
                - `pulseMode` (string): 波形播放模式，支持 "single"（单个波形）, "sequence"（列表顺序播放）, "random"（随机播放）。
                - `pulseChangeInterval` (number): 波形切换间隔，单位：秒。
                - `fireStrengthLimit` (number): 一键开火的强度限制。
            - `clientStrength` (object): 客户端实际设备的强度状态：
                - `strength` (number): 客户端当前正在运行的实际强度（A 通道）。
                - `limit` (number): 客户端被限制的最大强度上限。**重要提示：对受控者的所有操作绝不可超过此数值，否则可能造成事故**。
            - `currentPulseId` (string): 当前实际正在播放播放和输出的波形ID。
        """
        res = await self._request("GET", "")
        return json.dumps(res, ensure_ascii=False)

    @filter.llm_tool(name="dglab_get_pulse_list")
    async def dglab_get_pulse_list(self, event: AstrMessageEvent) -> str:
        """获取郊狼所有可用的波形列表及其ID。可以使用这些波形ID来设置当前波形或者进行一键开火。

        Returns:
            JSON 字符串。包含以下完整字段：
            - `status` (int): 请求状态码，1 表示成功，0 表示失败。
            - `code` (str): 状态码信息，如 "OK"。
            - `pulseList` (array): 波形对象列表，数组中每个对象包含：
                - `id` (string): 独一无二的波形ID（如 "d6f83af0"）。
                - `name` (string): 具体的波形名称（如 "呼吸"、"跳跃" 等），可以通过分析该字段判断哪种波形更适合当下的情境。
        """
        res = await self._request("GET", "/pulse_list")
        return json.dumps(res, ensure_ascii=False)

    @filter.llm_tool(name="dglab_get_strength")
    async def dglab_get_strength(self, event: AstrMessageEvent) -> str:
        """获取当前的游戏强度配置，包括基础强度(strength)和随机强度(randomStrength)。

        Returns:
            JSON 字符串。包含以下完整字段：
            - `status` (int): 请求状态码，1 表示成功，0 表示失败。
            - `code` (str): 状态码信息，如 "OK"。
            - `strengthConfig` (object): 强度设置信息：
                - `strength` (number): 当前设置的基础强度数值。
                - `randomStrength` (number): 当前设置的随机波动强度。实际运行时强度将在 [strength, strength + randomStrength] 之间动态波动。随机数会从 0 到 randomStrength 之间随机生成，然后与基础强度相加。例如，如果 strength=5，randomStrength=15，则实际强度会在 5 到 20 之间波动。
        """
        res = await self._request("GET", "/strength")

        return json.dumps(res, ensure_ascii=False)

    @filter.llm_tool(name="dglab_set_strength")
    async def dglab_set_strength(
        self,
        event: AstrMessageEvent,
        strength_add: int | None = None,
        strength_sub: int | None = None,
        strength_set: int | None = None,
        random_strength_add: int | None = None,
        random_strength_sub: int | None = None,
        random_strength_set: int | None = None,
    ) -> str:
        """设置或修改郊狼的基础强度与随机强度配置，这是控制郊狼的**基础使用方式**。提供增、减或直接设定的功能。未指定参数则不修改相关属性。

        Args:
            strength_add (number): 可选，增加的基础强度值。
            strength_sub (number): 可选，减少的基础强度值。
            strength_set (number): 可选，直接设置的基础强度值。请避免超过客户端配置的最大限制。
            random_strength_add (number): 可选，增加的随机强度值。随机数会从 0 到 randomStrength 之间随机生成，然后与基础强度相加。例如，如果 strength=5，randomStrength=15，则实际强度会在 5 到 20 之间波动。
            random_strength_sub (number): 可选，减少的随机强度值。随机数会从 0 到 randomStrength 之间随机生成，然后与基础强度相加。例如，如果 strength=5，randomStrength=15，则实际强度会在 5 到 20 之间波动。
            random_strength_set (number): 可选，直接设置的随机强度值。随机数会从 0 到 randomStrength 之间随机生成，然后与基础强度相加。例如，如果 strength=5，randomStrength=15，则实际强度会在 5 到 20 之间波动。

        Returns:
            JSON 字符串。包含以下完整字段：
            - `status` (int): 请求状态码，1 表示成功，0 表示失败。
            - `code` (str): 状态码信息，如 "OK"。
            - `message` (str): 成功说明，例如 "成功设置了 1 个游戏的强度配置"。
            - `successClientIds` (array): 成功应用该设置的客户端ID列表。
        """
        if sum(x is not None for x in [strength_add, strength_sub, strength_set]) > 1:
            return "错误：由于参数冲突，不能同时指定多个操作喔。"

        if (
            sum(
                x is not None
                for x in [random_strength_add, random_strength_sub, random_strength_set]
            )
            > 1
        ):
            return "错误：由于参数冲突，不能同时指定多个操作喔。"

        payload = {}
        if any(x is not None for x in [strength_add, strength_sub, strength_set]):
            payload["strength"] = {}
            if strength_add is not None:
                payload["strength"]["add"] = strength_add
            if strength_sub is not None:
                payload["strength"]["sub"] = strength_sub
            if strength_set is not None:
                payload["strength"]["set"] = strength_set
        if any(
            x is not None
            for x in [random_strength_add, random_strength_sub, random_strength_set]
        ):
            payload["randomStrength"] = {}
            if random_strength_add is not None:
                payload["randomStrength"]["add"] = random_strength_add
            if random_strength_sub is not None:
                payload["randomStrength"]["sub"] = random_strength_sub
            if random_strength_set is not None:
                payload["randomStrength"]["set"] = random_strength_set

        if not payload:
            return "没有提供需要修改的参数配置。"

        info = await self._request("GET", "")
        if info.get("status") == 1:
            client_strength = info.get("clientStrength") or {}
            limit = client_strength.get("limit")
            strength_config = info.get("strengthConfig") or {}

            new_str = strength_config.get("strength", 0)
            if strength_add is not None:
                new_str += strength_add
            elif strength_sub is not None:
                new_str -= strength_sub
            elif strength_set is not None:
                new_str = strength_set
            if new_str < 0:
                return json.dumps(
                    {"status": 0, "error": "哎呀，基础强度不可以变负数啦～"},
                    ensure_ascii=False,
                )

            new_rnd = strength_config.get("randomStrength", 0)
            if random_strength_add is not None:
                new_rnd += random_strength_add
            elif random_strength_sub is not None:
                new_rnd -= random_strength_sub
            elif random_strength_set is not None:
                new_rnd = random_strength_set
            if new_rnd < 0:
                return json.dumps(
                    {
                        "status": 0,
                        "error": "哼，随机强度的波动怎么能是负数呢～是往回倒吸快感吗？",
                    },
                    ensure_ascii=False,
                )

            if limit is not None and (new_str + new_rnd) > limit:
                return json.dumps(
                    {
                        "status": 0,
                        "error": f"哎呀，强度设定得太高啦！基础强度与随机强度之和({new_str} + {new_rnd} = {new_str + new_rnd})超过了受控者设置的安全上限({limit})。请调低一点点再试吧～",
                    },
                    ensure_ascii=False,
                )

        res = await self._request("POST", "/strength", json=payload)
        return json.dumps(res, ensure_ascii=False)

    @filter.llm_tool(name="dglab_get_pulse")
    async def dglab_get_pulse(self, event: AstrMessageEvent) -> str:
        """获取当前已启用的波形ID。可能是单个(string)也可能是数组(array)。

        Returns:
            JSON 字符串。包含以下完整字段：
            - `status` (int): 请求状态码，1 表示成功，0 表示失败。
            - `code` (str): 状态码信息，如 "OK"。
            - `pulseId` (string 或 array): 当前生效的波形ID。可能是单个波形ID，也可能是包含多个波形ID名称的数组。
        """
        res = await self._request("GET", "/pulse")

        return json.dumps(res, ensure_ascii=False)

    @filter.llm_tool(name="dglab_set_pulse")
    async def dglab_set_pulse(self, event: AstrMessageEvent, pulse_id: str) -> str:
        """设置当前郊狼使用的波形ID。

        Args:
            pulse_id (string): 必需，需要设置的波形ID。可使用 `dglab_get_pulse_list` 获得的有效波形ID进行替换。如果有多个波形ID请使用英文逗号拼接。

        Returns:
            JSON 字符串。返回对象包含以下完整字段：
            - `status` (int): 请求状态码，1 表示成功，0 表示失败。
            - `code` (str): 状态码信息，如 "OK" 或 "ERR::INVALID_REQUEST"。
            - `message` (str): 成功或失败说明，例如 "成功设置了 1 个游戏的波形ID"。
            - `successClientIds` (array): 成功应用该设置的客户端ID列表，数组内为对应的客户端ID字符串。
        """
        payload = {}
        if "," in pulse_id:
            payload["pulseId"] = [p.strip() for p in pulse_id.split(",") if p.strip()]
        else:
            payload["pulseId"] = pulse_id.strip()

        res = await self._request("POST", "/pulse", json=payload)
        return json.dumps(res, ensure_ascii=False)

    @filter.llm_tool(name="dglab_set_game_config")
    async def dglab_set_game_config(
        self,
        event: AstrMessageEvent,
        strength_change_interval_min: int | None = None,
        strength_change_interval_max: int | None = None,
        pulse_mode: str | None = None,
        pulse_change_interval: int | None = None,
        enable_b_channel: bool | None = None,
        b_channel_multiplier: int | None = None,
        fire_pulse_id: str | None = None,
    ) -> str:
        """设置或修改郊狼的高级配置（除了基础强度与波形外的游戏配置），包括随机时间、波形播放顺序、波形切换时间、B通道开关、B通道倍数、开火专属波形等。

        Args:
            strength_change_interval_min (number): 可选，随机强度变化间隔的最小秒数。
            strength_change_interval_max (number): 可选，随机强度变化间隔的最大秒数。此参数通常需与min一起提供。
            pulse_mode (string): 可选，波形播放模式，必须是 "single"（单个且不切换）、"sequence"（按给定列表顺序播放）、"random"（随机播放其中一个）之一。
            pulse_change_interval (number): 可选，波形自动切换的时间间隔（秒）。
            enable_b_channel (boolean): 可选，是否启用控制B通道（B通道开关）。
            b_channel_multiplier (number): 可选，B通道强度的倍率（B通道倍数）。注意：必须是正整数（不能为0）。
            fire_pulse_id (string): 可选，一键开火指定的专属波形ID。可为空字符串 "null" 或 "clear" 表示清空。

        Returns:
            JSON 字符串。如果无相关参数则提示，否则返回修改请求结果。
        """
        new_fields = {}

        if (
            strength_change_interval_min is not None
            and strength_change_interval_max is not None
        ):
            if strength_change_interval_min < 0:
                return '{"error": "最小间隔不能小于 0"}'
            if strength_change_interval_max <= 0:
                return '{"error": "最大间隔必须大于 0"}'
            if strength_change_interval_min > strength_change_interval_max:
                return '{"error": "最大间隔不能小于最小间隔"}'
            new_fields["strengthChangeInterval"] = [
                int(strength_change_interval_min),
                int(strength_change_interval_max),
            ]

        if pulse_mode is not None:
            if pulse_mode in ["single", "sequence", "random"]:
                new_fields["pulseMode"] = pulse_mode
            else:
                return '{"error": "pulse_mode 必须是 single, sequence 或 random"}'

        if pulse_change_interval is not None:
            if pulse_change_interval < 1:
                pulse_change_interval = 1
            new_fields["pulseChangeInterval"] = int(pulse_change_interval)

        if enable_b_channel is not None:
            new_fields["enableBChannel"] = enable_b_channel

        if b_channel_multiplier is not None:
            if (
                not isinstance(b_channel_multiplier, int)
                and not b_channel_multiplier.is_integer()
            ):
                return '{"error": "b_channel_multiplier 必须是整数"}'
            if b_channel_multiplier < 1:
                return '{"error": "b_channel_multiplier 必须大于或等于1"}'

            if enable_b_channel is False:
                return '{"error": "无法在关闭B通道的同时设置B通道倍数"}'
            elif enable_b_channel is None:
                state_res = await self._request("GET", "")
                if state_res.get("status") == 1:
                    game_config = state_res.get("gameConfig", {})
                    if not game_config.get("enableBChannel", False):
                        return '{"error": "B通道当前未开启，必须先确认用户已连接B通道电极，再开启B通道 (enable_b_channel=true) 才能设置B通道倍数"}'

            new_fields["bChannelStrengthMultiplier"] = int(b_channel_multiplier)

        if fire_pulse_id is not None:
            if fire_pulse_id.lower() in ["null", "clear", "None", ""]:
                new_fields["firePulseId"] = None
            else:
                new_fields["firePulseId"] = str(fire_pulse_id)

        if not new_fields:
            return '{"error": "没有提供任何需要修改的配置参数"}'

        res = await self._update_game_config_ws(new_fields)
        return json.dumps(res, ensure_ascii=False)

    @filter.llm_tool(name="dglab_action_fire")
    async def dglab_action_fire(
        self,
        event: AstrMessageEvent,
        strength: int,
        time: int | None = None,
        override: bool | None = None,
        pulse_id: str | None = None,
    ) -> str:
        """使用郊狼进行一键开火电击，对该受控者进行一次短时间的强刺激电击，这是对受控者**偏重的惩罚方式**。请保证该开火强度不会超过游戏设置或客户端限制上限。

        Args:
            strength (number): 必需，开火电击强度。建议开火前判断或通过 `dglab_get_game_info` 查询 `gameConfig.fireStrengthLimit` 以及 `clientStrength.limit`，防止强度越界或对用户造成惊吓。一键开火的强度是叠加在当前强度上的，例如当前强度是12，一键开火强度设置为20，则实际用户受到的强度为32，并在设定的开火时间结束后回到当前强度。
            time (number): 可选，电击时间，单位：毫秒。最高30000（30秒），不传默认为5000。
            override (boolean): 可选，多次一键开火时，是否重置时间 (true为重置, false为叠加)，不传默认为false。
            pulse_id (string): 可选，一键开火期望指定使用的专属波形ID。

        Returns:
            JSON 字符串。返回对象包含以下完整字段：
            - `status` (int): 请求状态码，1 表示成功，0 表示失败。
            - `code` (str): 状态码信息，如 "OK"。
            - `message` (str): 成功或失败说明，例如 "成功向 1 个游戏发送了一键开火指令"。
            - `successClientIds` (array): 成功应用开火指令的客户端ID列表。
        """
        if strength <= 0:
            return json.dumps(
                {"status": 0, "error": "开火强度必须是正整数（不能为0）"},
                ensure_ascii=False,
            )
        if time is not None and time > 30000:
            return json.dumps(
                {"status": 0, "error": "开火时间不能超过30000毫秒"}, ensure_ascii=False
            )

        info = await self._request("GET", "")
        if info.get("status") == 1:
            client_strength = info.get("clientStrength") or {}
            limit = client_strength.get("limit")
            game_config = info.get("gameConfig") or {}
            fire_limit = game_config.get("fireStrengthLimit")
            strength_config = info.get("strengthConfig") or {}
            curr_str = strength_config.get("strength", 0)
            curr_rnd = strength_config.get("randomStrength", 0)

            if fire_limit is not None and strength > fire_limit:
                return json.dumps(
                    {
                        "status": 0,
                        "error": f"开火强度({strength})超过了 TA 的一键开火限制({fire_limit})，太刺激了可不行噢，请调低一点点再试吧～",
                    },
                    ensure_ascii=False,
                )

            if limit is not None and (curr_str + curr_rnd + strength) > limit:
                return json.dumps(
                    {
                        "status": 0,
                        "error": f"基础+随机+开火强度的总和({curr_str}+{curr_rnd}+{strength}={curr_str + curr_rnd + strength})超过了客户端上限({limit})。太狠了会坏掉的，请降低开火强度后再试～",
                    },
                    ensure_ascii=False,
                )

        payload: dict[str, Any] = {"strength": strength}
        if time is not None:
            payload["time"] = time
        if override is not None:
            payload["override"] = override
        if pulse_id is not None:
            payload["pulseId"] = pulse_id.strip()

        res = await self._request("POST", "/action/fire", json=payload)
        return json.dumps(res, ensure_ascii=False)

    @filter.command("郊狼授权")
    async def dglab_auth(
        self, event: AstrMessageEvent, target_type: str = "", target_id: str = ""
    ):
        """授权用户或群聊可以使用郊狼指令。仅受控者本人或管理员可用。
        用法:
            /郊狼授权 用户 <用户ID> (切换用户授权)
            /郊狼授权 群聊 <群号> (切换特定群聊授权)
            /郊狼授权 群聊开关 (全局启用或禁用群聊功能)
        """
        event.stop_event()
        if not event.is_private_chat():
            return

        sender_id = str(event.get_sender_id())
        is_owner_or_admin = (sender_id == str(self.target_user_id)) or event.is_admin()
        if not is_owner_or_admin:
            return

        if not target_type:
            auth_users = (
                ", ".join(self.authorized_users) if self.authorized_users else "无"
            )
            allow_all = "已开启" if self.allow_all_users else "已关闭"
            group_status = "已开启" if self.allow_group_chat else "已关闭"
            auth_groups = (
                ", ".join(self.allowed_groups) if self.allowed_groups else "无"
            )
            yield event.plain_result(
                f"【当前郊狼授权状态】\n"
                f"允许所有用户: {allow_all}\n"
                f"已授权用户: {auth_users}\n"
                f"全局群聊允许状态: {group_status}\n"
                f"已授权群聊: {auth_groups}\n\n"
                f"用法指南:\n"
                f"/郊狼授权 用户 <用户ID>\n"
                f"/郊狼授权 用户开关 (允许所有用户控制)\n"
                f"/郊狼授权 群聊 <群号>\n"
                f"/郊狼授权 群聊开关"
            )
            return

        if target_type == "用户":
            if not target_id:
                yield event.plain_result("❌ 请提供要授权的用户ID！")
                return
            if target_id in self.authorized_users:
                self.authorized_users.remove(target_id)
                self._save_config_updates()
                yield event.plain_result(f"✅ 已取消用户 {target_id} 的郊狼控制权限。")
            else:
                self.authorized_users.add(target_id)
                self._save_config_updates()
                yield event.plain_result(
                    f"✅ 已授权用户 {target_id} 可以使用郊狼的控制指令！"
                )
        elif target_type == "用户开关":
            self.allow_all_users = not self.allow_all_users
            self._save_config_updates()
            status = "已开启" if self.allow_all_users else "已关闭"
            yield event.plain_result(f"✅ 允许所有用户控制状态已切换为: {status}")
        elif target_type == "群聊开关":
            self.allow_group_chat = not self.allow_group_chat
            self._save_config_updates()
            status = "已开启" if self.allow_group_chat else "已关闭"
            yield event.plain_result(f"✅ 全局群聊控制状态已切换为: {status}")
        elif target_type == "群聊":
            if not target_id:
                yield event.plain_result("❌ 请提供要授权的群号！")
                return
            if target_id in self.allowed_groups:
                self.allowed_groups.remove(target_id)
                self._save_config_updates()
                yield event.plain_result(f"✅ 已移出群聊 {target_id} 的控制权限。")
            else:
                self.allowed_groups.add(target_id)
                self._save_config_updates()
                yield event.plain_result(f"✅ 已添加群聊 {target_id} 的控制权限！")
        else:
            yield event.plain_result(
                "❌ 用法错误！请使用：用户、群聊 或 群聊开关 作为第二个参数"
            )

    @filter.command("郊狼客户端")
    async def dglab_set_client_id(self, event: AstrMessageEvent, client_id: str = ""):
        """修改郊狼控制目标的 client_id。仅受控者本人或管理员可用。
        用法: /郊狼客户端 <新的client_id>
        """
        event.stop_event()
        if not event.is_private_chat():
            return

        sender_id = str(event.get_sender_id())
        is_owner_or_admin = (sender_id == str(self.target_user_id)) or event.is_admin()
        if not is_owner_or_admin:
            return

        if not client_id:
            yield event.plain_result(
                f"当前绑定的 client_id 为: {self.default_client_id}\n"
                "请提供新的 client_id，例如: /郊狼客户端 all\n"
            )
            return

        import re

        if not re.match(r"^[A-Za-z0-9_-]+$", client_id):
            yield event.plain_result(
                "❌ 客户端ID格式错误，只能包含字母、数字、短横线和下划线！"
            )
            return

        self.default_client_id = client_id
        if "game_api" not in self.config:
            self.config["game_api"] = {}
        self.config["game_api"]["default_client_id"] = client_id
        try:
            self.config.save_config()
            yield event.plain_result(f"✅ 已成功将目标客户端修改为: {client_id}")
        except Exception as e:
            logger.error(f"保存配置失败: {e}")
            yield event.plain_result(
                f"⚠️ 客户端 ID 已修改为 {client_id}，但配置文件保存失败: {e}"
            )

    @filter.command("郊狼指令")
    async def dglab_command(
        self,
        event: AstrMessageEvent,
        action: str = "",
        target: str = "",
        arg1: str = "",
        arg2: str = "",
    ):
        """用户直接控制郊狼设备的指令
        【查看】 /郊狼指令 查看 状态/波形列表/当前波形/强度
        【修改】 /郊狼指令 修改 强度/随机强度 <增/减/设为> <数值>
        【修改】 /郊狼指令 修改 波形/波形模式/波形时间/随机时间 <参数>
        【修改】 /郊狼指令 修改 B通道开关/B通道倍数/开火限制/开火波形 <参数>
        【操作】 /郊狼指令 开火 <强度> [时间毫秒]
        """
        event.stop_event()
        if not event.is_private_chat():
            if not self.allow_group_chat:
                return
            if (
                self.allowed_groups
                and str(event.get_group_id()) not in self.allowed_groups
            ):
                return

            is_at_bot = any(
                isinstance(c, At) and str(c.qq) == str(event.get_self_id())
                for c in event.message_obj.message
            )
            if not is_at_bot:
                return

        sender_id = str(event.get_sender_id())
        has_perm = (
            self.allow_all_users
            or sender_id == str(self.target_user_id)
            or event.is_admin()
            or sender_id in self.authorized_users
        )

        if action in ["修改", "开火"] and not has_perm:
            yield event.plain_result(
                "❌ 权限不足！只有受控者本人、管理员或被授权用户才能使用这些指令哦~"
            )
            return

        if not action or action not in ["查看", "修改", "开火"]:
            yield event.plain_result(
                f"🐾 请告诉我你想对{self.target_user_name}的郊狼做什么呀～\n"
                "✨ 可用指令：\n"
                "⚡ /郊狼指令 查看 <状态|波单|当前|强度>\n"
                "⚡ /郊狼指令 修改 强度 <增|减|设为> <数值>\n"
                "⚡ /郊狼指令 修改 随机强度 <增|减|设为> <数值>\n"
                "⚡ /郊狼指令 修改 波形 <波形ID>\n"
                "⚡ /郊狼指令 修改 波形模式 <单|顺序|随机>\n"
                "⚡ /郊狼指令 修改 波形时间 <秒数>\n"
                "⚡ /郊狼指令 修改 随机时间 <最小秒> <最大秒>\n"
                "⚡ /郊狼指令 修改 B通道开关 <开|关>\n"
                "⚡ /郊狼指令 修改 B通道倍数 <数值>\n"
                "⚡ /郊狼指令 修改 开火限制 <数值>\n"
                "⚡ /郊狼指令 修改 开火波形 <波形ID|空>\n"
                "🔥 /郊狼指令 开火 <强度> [时间毫秒]"
            )
            return

        if action == "查看":
            if target in ["状态", "当前"]:
                res = await self._request("GET", "")
                if res.get("status") == 1:
                    conf = res.get("strengthConfig") or {}
                    client_str = res.get("clientStrength") or {}
                    limit = client_str.get("limit", "未知")
                    curr = client_str.get("strength", "未知")
                    game = res.get("gameConfig") or {}
                    msg = (
                        f"🌸 **{self.target_user_name} 当前的郊狼状态报告** 🌸\n"
                        f"🔌 **基础强度**: {conf.get('strength')} | 🎲 **随机强度**: {conf.get('randomStrength')}\n"
                        f"⚠️ **设备运行强度**: {curr} / **最大限制**: {limit}\n"
                        f"🌊 **当前波形ID**: {res.get('currentPulseId')}\n"
                        f"🔁 **波形模式**: {game.get('pulseMode')} (切换间隔: {game.get('pulseChangeInterval')}秒)\n"
                        f"⏱️ **随机强度变化间隔**: {game.get('strengthChangeInterval', '未知')}秒\n"
                        f"🔀 **B通道启用**: {'是' if game.get('enableBChannel') else '否'} (倍率: {game.get('bChannelStrengthMultiplier')})\n"
                        "💬 这就是目前的设备状态啦～快尽情吩咐吧！😈"
                    )
                    yield event.plain_result(msg)
                else:
                    yield event.plain_result(
                        f"呜呜，联系不到 {self.target_user_name} 的郊狼了呢... 错误原因：{res.get('message', res.get('error', '未知'))}"
                    )

            elif target in ["波形列表", "波单"]:
                res = await self._request("GET", "/pulse_list")
                if res.get("status") == 1:
                    pulses: list[dict] = res.get("pulseList") or []
                    if pulses:
                        pulse_str = "\n".join(
                            [
                                f"✨ [{p.get('id', '未知')}] {p.get('name', '未命名')}"
                                for p in pulses
                            ]
                        )
                        yield event.plain_result(
                            f"🎼 **这是所发现的可用波形列表哟：**\n{pulse_str}"
                        )
                    else:
                        yield event.plain_result("咦？居然没有找到任何可用的波形喵？")
                else:
                    yield event.plain_result(
                        f"呜呜，联系不到 {self.target_user_name} 的郊狼了呢... 错误原因：{res.get('message', res.get('error', '未知'))}"
                    )

            elif target in ["波形", "当前波形"]:
                res = await self._request("GET", "/pulse")
                if res.get("status") == 1:
                    pulse_id = res.get("pulseId")
                    if isinstance(pulse_id, list):
                        pulse_id = ", ".join(pulse_id)
                    yield event.plain_result(
                        f"🌊 **当前正在冲刷 {self.target_user_name} 的波形是**：{pulse_id} 哟～"
                    )
                else:
                    yield event.plain_result(
                        f"呜呜，联系不到 {self.target_user_name} 的郊狼了呢... 错误原因：{res.get('message', res.get('error', '未知'))}"
                    )

            elif target == "强度":
                res = await self._request("GET", "/strength")
                if res.get("status") == 1:
                    conf = res.get("strengthConfig") or {}
                    yield event.plain_result(
                        f"⚡ **{self.target_user_name} 的强度揭秘时间** ⚡\n"
                        f"📍 基础强度：{conf.get('strength')}\n"
                        f"🎲 随机浮动：{conf.get('randomStrength')}\n"
                        "💬 要不要考虑再调高一点呢？坏笑～"
                    )
                else:
                    yield event.plain_result(
                        f"获取 {self.target_user_name} 的强度失败喵... 错误原因：{res.get('message', res.get('error', '未知'))}"
                    )

            else:
                yield event.plain_result("唔...查看的目标我不认识呢！")

        elif action == "修改":
            if not has_perm:
                yield event.plain_result(
                    "❌ 权限不足！只有受控者本人、管理员或被授权用户才能使用这些指令哦~"
                )
                return
            elif target == "强度":
                mode = arg1
                try:
                    val = int(arg2)
                except ValueError:
                    yield event.plain_result("别闹啦，强度数值必须是个有效整数哦！😠")
                    return

                if val < 0:
                    yield event.plain_result(
                        "别闹，强度不能是负数哦！你这已经是想用爱发电了吧？👿"
                    )
                    return

                info = await self._request("GET", "")
                if info.get("status") == 1:
                    client_strength = info.get("clientStrength") or {}
                    limit = client_strength.get("limit")
                    strength_config = info.get("strengthConfig") or {}
                    curr_str = strength_config.get("strength", 0)
                    curr_rnd = strength_config.get("randomStrength", 0)

                    new_str = curr_str
                    if mode in ["增", "增加"]:
                        new_str += val
                    elif mode in ["减", "减少"]:
                        new_str -= val
                    elif mode in ["设为", "设置", "设定"]:
                        new_str = val

                    if new_str < 0:
                        yield event.plain_result(
                            "❌ 哎呀，你这样减来减去，最终的强度都要变成负数啦，这样一点感觉也没有的哦！"
                        )
                        return

                    if limit is not None and (new_str + curr_rnd) > limit:
                        yield event.plain_result(
                            "❌ 哎呀，强度设定得太高啦！这会超过 TA 的强度安全上限，被设备拦截了哦。请调低一点点再试吧～"
                        )
                        return

                payload = {"strength": {}}
                if mode in ["增", "增加"]:
                    payload["strength"]["add"] = val
                    action_desc = f"增加了 {val} 点"
                elif mode in ["减", "减少"]:
                    payload["strength"]["sub"] = val
                    action_desc = f"减少了 {val} 点"
                elif mode in ["设为", "设置", "设定"]:
                    payload["strength"]["set"] = val
                    action_desc = f"设为了 {val}"
                else:
                    yield event.plain_result(
                        "哎呀，参数填错啦，强度的修改模式只能是 [增/减/设为] 哟！"
                    )
                    return

                res = await self._request("POST", "/strength", json=payload)
                if res.get("status") == 1:
                    yield event.plain_result(
                        f"✅ 嘿嘿，已经成功把 {self.target_user_name} 的基础强度 {action_desc} 啦~"
                    )
                else:
                    yield event.plain_result(
                        f"❌ 呜呜，强度修改失败了：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target == "随机强度":
                mode = arg1
                try:
                    val = int(arg2)
                except ValueError:
                    yield event.plain_result(
                        "别闹啦，随机强度数值必须是个有效整数哦！😠"
                    )
                    return

                if val < 0:
                    yield event.plain_result(
                        "随机强度不能是负数哦！哪有把波动减掉变成负数的道理呢~👿"
                    )
                    return

                info = await self._request("GET", "")
                if info.get("status") == 1:
                    client_strength = info.get("clientStrength") or {}
                    limit = client_strength.get("limit")
                    strength_config = info.get("strengthConfig") or {}
                    curr_str = strength_config.get("strength", 0)
                    curr_rnd = strength_config.get("randomStrength", 0)

                    new_rnd = curr_rnd
                    if mode in ["增", "增加"]:
                        new_rnd += val
                    elif mode in ["减", "减少"]:
                        new_rnd -= val
                    elif mode in ["设为", "设置", "设定"]:
                        new_rnd = val

                    if new_rnd < 0:
                        yield event.plain_result(
                            "❌ 哎呀，你这样减来减去，随机波动的幅度都变成负数啦... 这样就没惊喜了嘛！"
                        )
                        return

                    if limit is not None and (curr_str + new_rnd) > limit:
                        yield event.plain_result(
                            "❌ 哎呀，随机强度设定得太高啦！这会超过 TA 的强度安全上限，被设备拦截了哦。请调低一点点再试吧～"
                        )
                        return

                payload = {"randomStrength": {}}
                if mode in ["增", "增加"]:
                    payload["randomStrength"]["add"] = val
                    action_desc = f"增加了 {val} 点"
                elif mode in ["减", "减少"]:
                    payload["randomStrength"]["sub"] = val
                    action_desc = f"减少了 {val} 点"
                elif mode in ["设为", "设置", "设定"]:
                    payload["randomStrength"]["set"] = val
                    action_desc = f"设为了 {val}"
                else:
                    yield event.plain_result(
                        "哎呀，参数填错啦，随机强度的修改模式只能是 [增/减/设为] 哟！"
                    )
                    return

                res = await self._request("POST", "/strength", json=payload)
                if res.get("status") == 1:
                    yield event.plain_result(
                        f"✅ 嘿嘿，已经成功把 {self.target_user_name} 的随机强度 {action_desc} 啦~\n现在的刺激感更加未知了呢！"
                    )
                else:
                    yield event.plain_result(
                        f"❌ 呜呜，随机强度修改失败了：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target == "波形":
                if not arg1:
                    yield event.plain_result(
                        "想要换成什么波形呢？不告诉我波形ID我可没法帮你配置哟！😈"
                    )
                    return
                pulse_ids = [p.strip() for p in arg1.split(",") if p.strip()]
                payload = {"pulseId": pulse_ids if len(pulse_ids) > 1 else pulse_ids[0]}
                res = await self._request("POST", "/pulse", json=payload)
                if res.get("status") == 1:
                    yield event.plain_result(
                        f"✅ 成功切换到新波形：{arg1}！\n新的波浪要打过来啦，{self.target_user_name} 准备好了吗？🌊"
                    )
                else:
                    yield event.plain_result(
                        f"❌ 波形切换失败了呜喵：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target == "波形模式":
                mode_map = {
                    "单": "single",
                    "单个": "single",
                    "顺序": "sequence",
                    "随机": "random",
                }
                mode = mode_map.get(arg1, arg1)
                if mode not in ["single", "sequence", "random"]:
                    yield event.plain_result(
                        "哎呀，波形模式填错啦，只能是 [单/顺序/随机] 哦！"
                    )
                    return

                payload = {"pulseMode": mode}
                res = await self._update_game_config_ws(payload)
                if res.get("status") == 1:
                    yield event.plain_result(
                        f"✅ 已将波形播放顺序修改为：{arg1}！\n(现在的电击节奏变成了 {arg1} 模式啦~)"
                    )
                else:
                    yield event.plain_result(
                        f"❌ 修改波形模式失败：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target == "波形时间":
                try:
                    val = int(arg1)
                except ValueError:
                    yield event.plain_result("波形切换时间必须得是个数字才行呀，笨笨！")
                    return

                if val <= 0:
                    yield event.plain_result(
                        "别闹，波形切换时间怎么能小于等于 0 呢！至少给设备一点反应的时间嘛~"
                    )
                    return

                payload = {"pulseChangeInterval": val}
                res = await self._update_game_config_ws(payload)
                if res.get("status") == 1:
                    yield event.plain_result(
                        f"✅ 已将波形切换时间修改为：{val}秒！\n(现在的波浪每 {val} 秒就会变一次呢~🌊)"
                    )
                else:
                    yield event.plain_result(
                        f"❌ 修改波形时间失败：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target == "随机时间":
                try:
                    min_t = int(arg1)
                    max_t = int(arg2)
                except ValueError:
                    yield event.plain_result("随机时间必须是两个数字哦~")
                    return

                if min_t < 0:
                    yield event.plain_result(
                        "哎呀，最小时间怎么能是负数呢！你在试图让时间倒流吗？"
                    )
                    return
                if max_t <= 0:
                    yield event.plain_result(
                        "最大时间必须大于 0 哟！不可能一瞬间波澜都没有吧？"
                    )
                    return
                if min_t > max_t:
                    yield event.plain_result(
                        "哎呀，最小时间怎么能大于最大时间呢！笨笨的呢~"
                    )
                    return

                payload = {"strengthChangeInterval": [min_t, max_t]}
                res = await self._update_game_config_ws(payload)
                if res.get("status") == 1:
                    yield event.plain_result(
                        f"✅ 已将随机强度变化间隔修改为：{min_t}-{max_t}秒！\n(现在的惊喜频率变成 {min_t} 到 {max_t} 秒之间啦~)"
                    )
                else:
                    yield event.plain_result(
                        f"❌ 修改随机时间失败：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target in ["B通道开关", "通道开关"]:
                mode = arg1
                if mode in ["开", "开启", "打开", "true", "1"]:
                    enable = True
                elif mode in ["关", "关闭", "关闭", "false", "0"]:
                    enable = False
                else:
                    yield event.plain_result("别闹啦，开关状态只能是 [开/关] 哦！")
                    return

                payload = {"enableBChannel": enable}
                res = await self._update_game_config_ws(payload)
                if res.get("status") == 1:
                    state_str = "打开" if enable else "关闭"
                    msg = f"✅ 已成功将 B通道 的状态设为：{state_str}！"
                    if enable:
                        msg += "\n双通道的快乐已经准备就绪了哟~😈"
                    yield event.plain_result(msg)
                else:
                    yield event.plain_result(
                        f"❌ 修改 B通道开关 失败：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target in ["B通道倍数", "通道倍数", "倍数"]:
                try:
                    val = int(arg1)
                except ValueError:
                    yield event.plain_result("别闹啦，B通道倍数必须是个整数哦！😠")
                    return

                if val <= 0:
                    yield event.plain_result(
                        "B通道倍数必须是大于 0 的正整数哦，要不然怎么超级加倍呢？😈"
                    )
                    return

                info = await self._request("GET", "")
                if info.get("status") == 1:
                    game_config = info.get("gameConfig") or {}
                    if not game_config.get("enableBChannel", False):
                        yield event.plain_result(
                            "哎呀，B通道还没开启呢！请先开启它，再来设置倍数享受双倍快感吧~😈"
                        )
                        return

                payload = {"bChannelStrengthMultiplier": val}
                res = await self._update_game_config_ws(payload)
                if res.get("status") == 1:
                    yield event.plain_result(
                        f"✅ 已将 B通道倍数 修改为：{val}倍！\n(双通道起飞啦~😈)"
                    )
                else:
                    yield event.plain_result(
                        f"❌ 修改 B通道倍数 失败：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target in ["开火限制", "开火强度限制"]:
                if str(event.get_sender_id()) != str(self.target_user_id):
                    yield event.plain_result(
                        "❌ 权限不足！开火强度限制是非常危险的设置，只有被控者本人才可以修改哦~"
                    )
                    return
                try:
                    val = int(arg1)
                except ValueError:
                    yield event.plain_result("别闹啦，开火强度限制必须是个整数哦！👿")
                    return
                if val <= 0:
                    yield event.plain_result(
                        "限制必须是大于 0 的正整数哦，设成 0 或负数算什么惩罚上限呢？怎么，想逃避惩罚吗？😈"
                    )
                    return
                payload = {"fireStrengthLimit": val}
                res = await self._update_game_config_ws(payload)
                if res.get("status") == 1:
                    yield event.plain_result(
                        f"✅ 已将一键开火的强度限制修改为：{val}！\n再惹我生气，惩罚可是很疼的哦~🔥"
                    )
                else:
                    yield event.plain_result(
                        f"❌ 修改开火强度限制失败：{res.get('message', res.get('error', '未知错误'))}"
                    )

            elif target == "开火波形":
                pulse_id = arg1.strip()
                if pulse_id.lower() in ["空", "无", "清空", "null", "clear"]:
                    payload = {"firePulseId": None}
                    pulse_name_str = "空"
                else:
                    payload = {"firePulseId": pulse_id}
                    pulse_name_str = pulse_id

                res = await self._update_game_config_ws(payload)
                if res.get("status") == 1:
                    msg = f"✅ 已将开火专属波形修改为：{pulse_name_str}！\n"
                    if payload["firePulseId"] is None:
                        msg += "开火波形清空啦，接下来开火就用当前波形了哦~"
                    else:
                        msg += "下次开火，你就能体验到这个绝妙的专属波形啦~⚡"
                    yield event.plain_result(msg)
                else:
                    yield event.plain_result(
                        f"❌ 修改开火波形失败：{res.get('message', res.get('error', '未知错误'))}"
                    )

            else:
                yield event.plain_result("咦，您的修改目标我不认识呢！")

        elif action == "开火":
            if not has_perm:
                yield event.plain_result(
                    "❌ 权限不足！只有受控者本人、管理员或被授权用户才能使用这些指令哦~"
                )
                return
            try:
                strength = int(target)
                time_ms = int(arg1) if arg1 else 5000
            except ValueError:
                yield event.plain_result(
                    "咦，填写的惩罚强度或时间不对哦，必须得是明确的数字才行！👿"
                )
                return

            if strength <= 0:
                yield event.plain_result("别闹，开火强度必须得大于 0 哟！这么心软嘛~🔥")
                return
            if time_ms > 30000:
                yield event.plain_result(
                    "惩罚时间太长会坏掉的，不能超过30000毫秒（30秒）哦！"
                )
                return

            info = await self._request("GET", "")
            if info.get("status") == 1:
                client_strength = info.get("clientStrength") or {}
                limit = client_strength.get("limit")
                game_config = info.get("gameConfig") or {}
                fire_limit = game_config.get("fireStrengthLimit")
                strength_config = info.get("strengthConfig") or {}
                curr_str = strength_config.get("strength", 0)
                curr_rnd = strength_config.get("randomStrength", 0)

                if fire_limit is not None and strength > fire_limit:
                    yield event.plain_result(
                        "❌ 哎呀，开火强度太高啦！这超过了 TA 自己设定的一键开火限制哦，调低一点点再试吧～"
                    )
                    return

                if limit is not None and (curr_str + curr_rnd + strength) > limit:
                    yield event.plain_result(
                        "❌ 开火操作被拦截了喵！这个强度再加上原本的强度波动会超过 TA 的绝对安全上限的，太狠了会坏掉的～"
                    )
                    return

            payload = {"strength": strength, "time": time_ms}
            res = await self._request("POST", "/action/fire", json=payload)
            if res.get("status") == 1:
                yield event.plain_result(
                    f"🔥 **BINGO! 惩罚降临!** 🔥\n"
                    f"向 {self.target_user_name} 发射了强度为 {strength} 的强力电击，将持续 {time_ms} 毫秒！\n"
                    "好刺激呀，太美妙啦～😈"
                )
            else:
                yield event.plain_result(
                    f"❌ 开火操作失败...\n错误原因：{res.get('message', res.get('error', '未知'))}"
                )

    async def terminate(self):
        """插件被卸载/停用时调用，用于清理资源。"""
        session = getattr(self, "session", None)
        if session is not None and not getattr(session, "closed", True):
            try:
                await session.close()
            except Exception as e:
                logger.error(f"Error closing HTTP session: {e}")
            logger.info(f"{self.__class__.__name__}: HTTP session closed.")
