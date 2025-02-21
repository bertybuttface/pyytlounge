import asyncio
import json
import logging
from typing import Any, AsyncIterator, Callable, Dict, List, Optional

from aiohttp import ClientPayloadError, ClientSession, ClientTimeout, TCPConnector

from .api import api_base
from .exceptions import NotConnectedException, NotLinkedException, NotPairedException
from .models import AuthState, PlaybackState, _Device, _DeviceInfo, _LoungeStatus
from .util import as_aiter, iter_response_lines


def get_thumbnail_url(video_id: str, thumbnail_idx=0) -> str:
    """Return thumbnail URL for a given video. Use thumbnail idx to get different thumbnails."""
    return f"https://img.youtube.com/vi/{video_id}/{thumbnail_idx}.jpg"


class YtLoungeApi:
    def __init__(self, device_name: str, logger: Optional[logging.Logger] = None):
        self.device_name = device_name
        self.auth = AuthState()
        self._sid = None
        self._gsession = None
        self._last_event_id = None
        self.state = PlaybackState(logger)
        self.state_update = 0
        self._command_offset = 1
        self._screen_name: str = None
        self._device_info: Optional[_DeviceInfo] = None
        self._logger = logger or logging.Logger(__package__, logging.DEBUG)
        # Initialize these as None - they'll be set up in __aenter__
        self.conn: Optional[TCPConnector] = None
        self.session: Optional[ClientSession] = None

    async def __aenter__(self):
        self.conn = TCPConnector(ttl_dns_cache=300)
        self.session = ClientSession(
            connector=self.conn, json_serialize=lambda x: json.dumps(x).decode()
        )
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def close(self):
        if self.session:
            await self.session.close()
        if self.conn:
            await self.conn.close()

    def paired(self) -> bool:
        """Returns true if screen id is known."""
        return self.auth.screen_id is not None

    def linked(self) -> bool:
        """Returns true if paired and lounge id token is known."""
        return self.paired() and self.auth.lounge_id_token is not None

    def connected(self) -> bool:
        """Returns true if the screen's session is connected."""
        return self._sid is not None and self._gsession is not None

    def __repr__(self):
        return (
            f"screen_id: {self.auth.screen_id}\n"
            f"lounge_id_token: {self.auth.lounge_id_token}\n"
            f"sid: {self._sid}\n"
            f"gsession: {self._gsession}\n"
            f"last_event_id: {self._last_event_id}\n"
        )

    @property
    def screen_name(self) -> str:
        """Returns screen name as returned by YouTube"""
        if not self.linked():
            raise NotLinkedException("Not linked")
        return self._screen_name

    @property
    def screen_device_name(self) -> Optional[str]:
        """Returns device name built from device info returned by YouTube.
        Returns None if not yet initialized or information was not sent."""
        if not self.connected():
            raise NotConnectedException("Not connected")
        if not self._device_info:
            return None
        brand = self._device_info["brand"]
        model = self._device_info["model"]
        return f"{brand} {model}"

    async def pair_with_screen_id(
        self, screen_id: str, screen_name: Optional[str] = None
    ) -> bool:
        """Pair with device using a known screen id, optionally specify the screen name if known"""
        self.auth.screen_id = screen_id
        self._screen_name = screen_name
        return await self.refresh_auth()

    async def pair(self, pairing_code: str) -> bool:
        """Pair with a device using a manual input pairing code"""
        pair_url = f"{api_base}/pairing/get_screen"
        pair_data = {"pairing_code": pairing_code}
        async with self.session.post(pair_url, data=pair_data) as resp:
            resp.raise_for_status()
            screens = await resp.json(loads=json.loads)
            screen = screens["screen"]
            self._screen_name = screen["name"]
            self.auth.screen_id = screen["screenId"]
            self.auth.lounge_id_token = screen["loungeToken"]
            return self.linked()

    async def refresh_auth(self) -> bool:
        """Refresh lounge token using stored refresh token."""
        if not self.paired():
            raise NotPairedException("Must be paired")
        refresh_url = f"{api_base}/pairing/get_lounge_token_batch"
        refresh_data = {"screen_ids": self.auth.screen_id}
        async with self.session.post(refresh_url, data=refresh_data) as resp:
            resp.raise_for_status()
            screens = await resp.json(loads=json.loads)
            screen = screens["screens"][0]
            self.auth.screen_id = screen["screenId"]
            self.auth.lounge_id_token = screen["loungeToken"]
            self._logger.info(
                "Refreshed auth, lounge id token %s", self.auth.lounge_id_token
            )
            return self.linked()

    def store_auth_state(self) -> dict:
        """Return auth parameters as dict which can be serialized for later use"""
        return {
            "screenId": self.auth.screen_id,
            "lounge_id_token": self.auth.lounge_id_token,
            "refresh_token": self.auth.refresh_token,
        }

    def load_auth_state(self, data: dict):
        """Use deserialized auth parameters"""
        self.auth = AuthState()
        self.auth.deserialize(data)

    def _update_state(self):
        self.state_update += 1

    def _lounge_token_expired(self):
        self.auth.lounge_id_token = None

    def _connection_lost(self):
        self._sid = None
        self._gsession = None
        self._last_event_id = None

    def _process_event(self, event_type: str, args: List[Any]):
        if event_type == "onStateChange":
            self.state.apply_state(args[0])
            self._update_state()
        elif event_type == "nowPlaying":
            self.state = PlaybackState(self._logger, args[0])
            self._update_state()
        elif event_type == "loungeStatus":
            data: _LoungeStatus = args[0]
            devices: List[_Device] = json.loads(data["devices"])
            for device in devices:
                if device["type"] == "LOUNGE_SCREEN":
                    self._screen_name = device["name"]
                    self._device_info = json.loads(device.get("deviceInfo", "null"))
                    break
        elif event_type == "loungeScreenDisconnected":
            self.state = PlaybackState(self._logger)
            self._update_state()
            self._connection_lost()
            self._lounge_token_expired()
        elif event_type == "noop":
            pass
        else:
            self._logger.debug("Unprocessed event %s %s", event_type, args)

    def _process_events(self, events):
        for event_id, (event_type, *args) in events:
            if event_type == "c":
                self._sid = args[0]
            elif event_type == "S":
                self._gsession = args[0]
            else:
                self._process_event(event_type, args)
        self._last_event_id = events[-1][0]

    async def _parse_event_chunks(self, lines: AsyncIterator[str]):
        current_chunk = ""
        chunk_remaining = 0
        async for line in lines:
            line = line.replace("\n", "")
            if not chunk_remaining:
                chunk_remaining = int(line)
                current_chunk = ""
            else:
                current_chunk += line
                chunk_remaining -= len(line) + 1
                if chunk_remaining <= 0:
                    yield json.loads(current_chunk)

    async def is_available(self) -> bool:
        """Asks YouTube API if the screen is available. Must be linked prior to this."""
        if not self.linked():
            raise NotConnectedException("Not connected")
        body = {"lounge_token": self.auth.lounge_id_token}
        url = f"{api_base}/pairing/get_screen_availability"
        async with self.session.post(url, data=body) as result:
            result.raise_for_status()
            status = await result.json(loads=json.loads)
            screens = status.get("screens", [])
            return screens and screens[0].get("status") == "online"

    def get_thumbnail_url(self, thumbnail_idx=0) -> Optional[str]:
        """Returns thumbnail for current video. Use thumbnail idx to get different thumbnails.
        Returns None if no video is set."""
        if not self.state.videoId:
            return None
        return get_thumbnail_url(self.state.videoId, thumbnail_idx)

    async def connect(self) -> bool:
        """Attempt to connect using the previously set tokens"""
        if not self.linked():
            raise NotLinkedException("Not linked")
        connect_body = {
            "app": "web",
            "mdx-version": "3",
            "name": self.device_name,
            "id": self.auth.screen_id,
            "device": "REMOTE_CONTROL",
            "capabilities": "que,dsdtr,atp",
            "method": "setPlaylist",
            "magnaKey": "cloudPairedDevice",
            "ui": "false",
            "deviceContext": "user_agent=dunno&window_width_points=&window_height_points=&os_name=android&ms=",
            "theme": "cl",
            "loungeIdToken": self.auth.lounge_id_token,
        }
        connect_url = (
            f"{api_base}/bc/bind?RID=1&VER=8&CVER=1&auth_failure_option=send_error"
        )
        async with self.session.post(connect_url, data=connect_body) as resp:
            text = await resp.text()
            if resp.status == 401:
                self._lounge_token_expired()
                return False
            if resp.status != 200:
                self._logger.warning(
                    "Unknown reply to connect %i %s", resp.status, resp.reason
                )
                return False
            lines = text.splitlines()
            async for events in self._parse_event_chunks(as_aiter(lines)):
                self._process_events(events)
            self._command_offset = 1
            return self.connected()

    def _handle_session_result(self, status_code: int, reason: str) -> bool:
        if (status_code == 400 and "Unknown SID" in reason) or (
            status_code == 410 and "Gone" in reason
        ):
            self._connection_lost()
            return False
        if status_code == 401 and "Expired" in reason:
            self._connection_lost()
            self._lounge_token_expired()
            return False
        return True

    def _common_connection_parameters(self) -> Dict[str, Any]:
        return {
            "name": self.device_name,
            "loungeIdToken": self.auth.lounge_id_token,
            "SID": self._sid,
            "AID": self._last_event_id,
            "gsessionid": self._gsession,
            "device": "REMOTE_CONTROL",
            "app": "youtube-desktop",
            "VER": "8",
            "v": "2",
        }

    async def subscribe(self, callback: Callable[[PlaybackState], Any]) -> None:
        """Start listening for events"""
        if not self.connected():
            raise NotConnectedException("Not connected")
        params = {
            **self._common_connection_parameters(),
            "RID": "rpc",
            "CI": "0",
            "TYPE": "xmlhttp",
        }
        url = f"{api_base}/bc/bind"
        self._logger.info("Subscribing to lounge id %s", self.auth.lounge_id_token)
        async with self.session.get(
            url, params=params, timeout=ClientTimeout()
        ) as resp:
            if not self._handle_session_result(resp.status, resp.reason):
                return
            try:
                async for events in self._parse_event_chunks(
                    iter_response_lines(resp.content)
                ):
                    pre_state_update = self.state_update
                    self._process_events(events)
                    if pre_state_update != self.state_update:
                        await callback(self.state)
                    if not self.connected():
                        break
                self._logger.info(
                    "Subscribe completed, status %i %s", resp.status, resp.reason
                )
            except ClientPayloadError:
                self._logger.exception(
                    "Subscribe payload error, status %s reason %s",
                    resp.status,
                    resp.reason,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.exception(
                    "Subscribe failed, status %s reason %s", resp.status, resp.reason
                )
                raise

    async def disconnect(self) -> bool:
        """Disconnect from the current session"""
        if not self.connected():
            raise NotConnectedException("Not connected")
        command_body = {
            "ui": "",
            "TYPE": "terminate",
            "clientDisconnectReason": "MDX_SESSION_DISCONNECT_REASON_DISCONNECTED_BY_USER",
        }
        params = {
            **self._common_connection_parameters(),
            "CVER": "1",
            "RID": self._command_offset,
            "auth_failure_option": "send_error",
        }
        url = f"{api_base}/bc/bind"
        async with self.session.post(url, data=command_body, params=params) as resp:
            text = await resp.text()
            if not self._handle_session_result(resp.status, text):
                return False
            resp.raise_for_status()
            return True

    async def _command(
        self, command: str, command_parameters: Optional[dict] = None
    ) -> bool:
        if not self.connected():
            raise NotConnectedException("Not connected")
        command_body = {"count": 1, "ofs": self._command_offset, "req0__sc": command}
        if command_parameters:
            command_body.update({f"req0_{k}": v for k, v in command_parameters.items()})
        self._command_offset += 1
        params = {**self._common_connection_parameters(), "RID": self._command_offset}
        url = f"{api_base}/bc/bind"
        async with self.session.post(url, data=command_body, params=params) as resp:
            text = await resp.text()
            if not self._handle_session_result(resp.status, text):
                return False
            resp.raise_for_status()
            return True

    async def play(self) -> bool:
        return await self._command("play")

    async def pause(self) -> bool:
        return await self._command("pause")

    async def next(self) -> bool:
        return await self._command("next")

    async def previous(self) -> bool:
        return await self._command("previous")

    async def skip_ad(self) -> bool:
        return await self._command("skipAd")

    async def play_video(self, video_id: str) -> bool:
        return await self._command("setPlaylist", {"videoId": video_id})

    async def seek_to(self, time: float) -> bool:
        return await self._command("seekTo", {"newTime": time})

    async def set_auto_play_mode(self, enabled: bool) -> None:
        await self._command(
            "setAutoplayMode", {"autoplayMode": "ENABLED" if enabled else "DISABLED"}
        )

    async def set_volume(self, volume: int) -> bool:
        return await self._command("setVolume", {"volume": volume})
