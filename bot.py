"""FiveM Shift Management Discord Bot — single-file entry point.

Run from the project root:  python bot.py

Everything lives in this one file: the bot itself, the /shift, /check,
/list and /reset commands, JSON persistence with atomic writes and
automatic backups, the FiveM client, the permission system and the
disconnect monitor with its grace period.

Key design decisions (see README.md for full docs):
  - player.id in FiveM is a temporary session id and is never used as a
    permanent key; matching is done via stable license:/license2: identifiers.
  - All timestamps are stored in UTC and only converted to the configured
    display timezone when shown. Durations come from aware datetimes only.
  - Every read-modify-write on the JSON database runs under an asyncio lock
    and is persisted atomically (temp file -> os.replace).
  - A corrupted data file is preserved, never silently deleted.
  - Active shifts survive restarts: they are restored from JSON.
"""

import asyncio
import json
import logging
import math
import os
import shutil
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

# ======================================================================
# توکن ربات
# ----------------------------------------------------------------------
# توکن فقط از همین فایل (EMBEDDED_TOKEN) خوانده می‌شود — برای تعویض توکن
# فقط مقدار زیر را عوض کنید.
# ======================================================================

EMBEDDED_TOKEN = "ADD_Discord_Token"

TOKEN = EMBEDDED_TOKEN

BASE_DIR = Path(__file__).resolve().parent

log = logging.getLogger("shift_bot")

# ======================================================================
# تنظیمات ربات (جایگزین config.json — همه‌چیز داخل همین فایل)
# ----------------------------------------------------------------------
# شناسه سرور، کانال، نقش‌ها و تنظیمات FiveM را اینجا تنظیم کنید.
# ======================================================================

CONFIG = {
    "guild_id": "",
    "channel_id": "1537151050686140611",
    "roles": {
        "shift": ["1399406163967086752","1538579361689509969"],
        "userplus": ["1399406163967086752","1538579361689509969"],
        "moderator": ["1032342911058202804","1032342960379006987","1264838860077011064","1538579361689509969"],
        "manager": ["1032342911058202804","1032342960379006987","1264838860077011064","1538579361689509969"],
    },
    "settings": {
        "disconnect_grace_minutes": 15,
        "timezone": "Asia/Tehran",
        "players_url": "http://domainORip:30120/players.json",
        "check_interval_seconds": 10,
    },
}

# ======================================================================
# Time helpers
# ======================================================================


def utc_now() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """Serialize a datetime to ISO 8601 (keeps the UTC offset)."""
    return dt.isoformat()


def parse_iso(value):
    """Parse an ISO 8601 string into an aware datetime.

    Returns ``None`` instead of raising on bad input so callers never crash
    on corrupted stored data. Naive strings are assumed to be UTC.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        log.warning("Invalid ISO timestamp: %r", value)
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_timezone(name):
    """Return a tzinfo for the given IANA name, falling back to UTC."""
    if not name:
        return timezone.utc
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        log.warning("Unknown timezone %r, falling back to UTC", name)
        return timezone.utc


def to_local(dt: datetime, tz) -> datetime:
    """Convert an aware datetime to the display timezone."""
    return dt.astimezone(tz)


def format_time(dt: datetime, tz) -> str:
    """Format as HH:MM in the display timezone (24h clock)."""
    return to_local(dt, tz).strftime("%H:%M")


def format_date_time(dt: datetime, tz) -> str:
    """Format as YYYY-MM-DD HH:MM in the display timezone."""
    return to_local(dt, tz).strftime("%Y-%m-%d %H:%M")


def format_duration(seconds) -> str:
    """Format a duration as 45m / 2h 15m / 12h 42m / 127h 05m."""
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def sessions_total_in_period(sessions, period: str, tz) -> int:
    """Sum ``duration_seconds`` of sessions whose *end* falls inside the period.

    Periods are computed in the display timezone:
      - "day"   -> since local midnight
      - "week"  -> since local Monday 00:00 (ISO week)
      - "month" -> since the 1st of the current local month
    """
    now_local = utc_now().astimezone(tz)
    if period == "day":
        start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "week":
        start = (now_local - timedelta(days=now_local.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    elif period == "month":
        start = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Unknown period: {period}")

    total = 0
    for session in sessions or []:
        end = parse_iso(session.get("end"))
        if end is None:
            continue
        if end.astimezone(tz) >= start:
            total += int(session.get("duration_seconds") or 0)
    return total


def _tz(bot):
    return get_timezone((bot.config.get("settings") or {}).get("timezone"))


# ======================================================================
# Database (JSON persistence)
# ======================================================================

# Number of automatic backups to keep.
BACKUP_KEEP = 20

ACTIVE_DEFAULTS = {
    "started_at": None,
    "channel_id": None,
    "message_id": None,
    "disconnect_at": None,
    "fiveM_identifier": None,
    "fiveM_name": None,
}

USER_DEFAULTS = {
    "discord_id": None,
    "fiveM_identifier": None,
    "fiveM_name": None,
    "total_shift_seconds": 0,
    "total_shift_hours": 0.0,
    "active_shift": dict(ACTIVE_DEFAULTS),
    "sessions": [],
}


def _to_int(value, default=0):
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


class Database:
    def __init__(self, data_file="data/shift_data.json", backup_dir="data/backups"):
        self.data_file = Path(data_file)
        self.backup_dir = Path(backup_dir)
        self._lock = asyncio.Lock()
        self.data: dict = {}
        self._load()

    # ------------------------------------------------------------------
    # Loading / validation
    # ------------------------------------------------------------------
    def _load(self):
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        if not self.data_file.exists():
            self.data = {}
            self._write_now({})
            log.info("Created new shift data file at %s", self.data_file)
            return

        try:
            raw = json.loads(self.data_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
            # Preserve the corrupted file for manual recovery.
            corrupt_path = self.backup_dir / f"shift_data_corrupt_{datetime.now():%Y-%m-%d_%H-%M-%S}.json"
            try:
                shutil.copy2(self.data_file, corrupt_path)
                log.error("Shift data is corrupted (%s); original preserved at %s", exc, corrupt_path)
            except OSError:
                log.exception("Could not back up corrupted shift data file")
            self.data = {}
            self._write_now({})
            return

        if not isinstance(raw, dict):
            log.error("Shift data file is not a JSON object; starting fresh")
            self.data = {}
            self._write_now({})
            return

        normalized = {}
        for key, value in raw.items():
            user = self._normalize_user(value)
            if user is None:
                log.warning("Dropping malformed record for user %r", key)
                continue
            normalized[key] = user
        self.data = normalized
        log.info("Loaded %d registered user(s) from %s", len(self.data), self.data_file)

    def _normalize_user(self, raw):
        """Merge a raw record with defaults so missing keys never crash the bot."""
        if not isinstance(raw, dict):
            return None
        user = deepcopy(USER_DEFAULTS)
        user["discord_id"] = str(raw.get("discord_id") or "")
        user["fiveM_identifier"] = raw.get("fiveM_identifier") or None
        user["fiveM_name"] = raw.get("fiveM_name") or None
        user["total_shift_seconds"] = _to_int(raw.get("total_shift_seconds"))
        try:
            user["total_shift_hours"] = float(raw.get("total_shift_hours") or 0.0)
        except (TypeError, ValueError):
            user["total_shift_hours"] = round(user["total_shift_seconds"] / 3600, 2)

        active = raw.get("active_shift")
        if isinstance(active, dict):
            for key in ACTIVE_DEFAULTS:
                if key in active and active[key] is not None:
                    user["active_shift"][key] = active[key]

        sessions = raw.get("sessions")
        if isinstance(sessions, list):
            clean = []
            for session in sessions:
                if not isinstance(session, dict):
                    continue
                start = parse_iso(session.get("start"))
                end = parse_iso(session.get("end"))
                if start is None or end is None:
                    continue
                clean.append(
                    {
                        "start": iso(start),
                        "end": iso(end),
                        "duration_seconds": _to_int(session.get("duration_seconds")),
                    }
                )
            user["sessions"] = clean
        return user

    # ------------------------------------------------------------------
    # Atomic persistence
    # ------------------------------------------------------------------
    async def update(self, fn):
        """Run ``fn(data)`` under a lock, persist the result, return fn's value.

        ``fn`` must be a plain synchronous function (no awaits) so the whole
        modify-and-save happens without yielding to the event loop.
        """
        async with self._lock:
            result = fn(self.data)
            self._write_now(self.data)
            return result

    def _write_now(self, data):
        """Atomic write: temp file -> os.replace. Then snapshot a backup."""
        tmp_path = self.data_file.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(data, indent=4, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp_path, self.data_file)
        self._backup()

    def _backup(self):
        try:
            stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            dest = self.backup_dir / f"shift_data_{stamp}.json"
            shutil.copy2(self.data_file, dest)
            old = sorted(self.backup_dir.glob("shift_data_*.json"))
            for path in old[:-BACKUP_KEEP]:
                path.unlink(missing_ok=True)
        except OSError:
            log.exception("Automatic backup failed")

    # ------------------------------------------------------------------
    # Reads (return deep copies so callers can't corrupt live state)
    # ------------------------------------------------------------------
    def get_user(self, discord_id):
        return deepcopy(self.data.get(str(discord_id)))

    def all_users(self):
        return [(key, deepcopy(user)) for key, user in self.data.items()]

    def active_users(self):
        return [
            (key, deepcopy(user))
            for key, user in self.data.items()
            if user.get("active_shift", {}).get("started_at")
        ]

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------
    async def register_user(self, discord_id, identifier, name):
        key = str(discord_id)

        def fn(data):
            if key in data:
                return False
            user = deepcopy(USER_DEFAULTS)
            user["discord_id"] = key
            user["fiveM_identifier"] = identifier
            user["fiveM_name"] = name
            data[key] = user
            return True

        return await self.update(fn)

    async def start_shift(self, discord_id, started_at, channel_id, message_id, identifier, name):
        key = str(discord_id)

        def fn(data):
            user = data.get(key)
            if not user or user["active_shift"].get("started_at"):
                return False  # duplicate start rejected atomically
            active = user["active_shift"]
            active["started_at"] = iso(started_at)
            active["channel_id"] = str(channel_id) if channel_id else None
            active["message_id"] = str(message_id) if message_id else None
            active["disconnect_at"] = None
            active["fiveM_identifier"] = identifier
            active["fiveM_name"] = name
            return True

        return await self.update(fn)

    async def end_shift(self, discord_id, end_at):
        """End the active shift, record the session, update totals.

        Returns the session dict, or None if there was nothing to end
        (e.g. already ended by another path). Idempotent by design.
        """
        key = str(discord_id)

        def fn(data):
            user = data.get(key)
            if not user:
                return None
            started_at = user["active_shift"].get("started_at")
            if not started_at:
                return None
            start = parse_iso(started_at)
            if start is None:
                log.error("Cannot end shift for %s: invalid started_at %r", key, started_at)
                return None
            duration = max(0, int((end_at - start).total_seconds()))
            session = {
                "start": iso(start),
                "end": iso(end_at),
                "duration_seconds": duration,
            }
            user["sessions"].append(session)
            user["total_shift_seconds"] = user.get("total_shift_seconds", 0) + duration
            user["total_shift_hours"] = round(user["total_shift_seconds"] / 3600, 2)
            user["active_shift"] = deepcopy(ACTIVE_DEFAULTS)
            return session

        return await self.update(fn)

    async def begin_grace(self, discord_id, detected_at):
        """Start the 15-minute grace period after a detected disconnect."""
        key = str(discord_id)

        def fn(data):
            user = data.get(key)
            if not user or not user["active_shift"].get("started_at"):
                return False
            if user["active_shift"].get("disconnect_at"):
                return False
            user["active_shift"]["disconnect_at"] = iso(detected_at)
            return True

        return await self.update(fn)

    async def clear_grace(self, discord_id):
        """Player came back: cancel the grace period, keep the shift running."""
        key = str(discord_id)

        def fn(data):
            user = data.get(key)
            if not user or not user["active_shift"].get("started_at"):
                return False
            if not user["active_shift"].get("disconnect_at"):
                return False
            user["active_shift"]["disconnect_at"] = None
            return True

        return await self.update(fn)

    async def reset_all_shift_data(self):
        """Wipe every shift statistic while keeping player registrations."""
        def fn(data):
            count = 0
            for user in data.values():
                user["total_shift_seconds"] = 0
                user["total_shift_hours"] = 0.0
                user["sessions"] = []
                user["active_shift"] = deepcopy(ACTIVE_DEFAULTS)
                count += 1
            return count

        return await self.update(fn)

    async def delete_user(self, discord_id):
        """Completely remove a user's record (registration + shifts).

        Returns True if the user existed and was removed.
        """
        key = str(discord_id)

        def fn(data):
            return data.pop(key, None) is not None

        return await self.update(fn)

    async def wipe_all(self):
        """Remove every user record from the database.

        Returns how many users were removed.
        """
        def fn(data):
            count = len(data)
            data.clear()
            return count

        return await self.update(fn)


# ======================================================================
# FiveM client
# ======================================================================


class FiveMError(Exception):
    """Raised internally for expected failure modes of the FiveM endpoint."""


class FiveMClient:
    def __init__(self, players_url: str, timeout: float = 10.0):
        self.url = players_url
        self.timeout = timeout
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def fetch_players(self):
        """Fetch and parse players.json.

        Returns a normalized list of player dicts, or ``None`` on any failure.
        Accepts both a plain JSON list of players and dict-style payloads
        (e.g. ``{\"players\": [...]}`` or ``{\"id\": {...}}``).
        ``player[\"id\"]`` is a temporary session id and is never used as a
        permanent key.
        """
        try:
            session = await self._get_session()
            async with session.get(self.url) as response:
                if response.status != 200:
                    raise FiveMError(f"HTTP {response.status}")
                try:
                    data = await response.json(content_type=None)
                except (json.JSONDecodeError, aiohttp.ContentTypeError) as exc:
                    raise FiveMError("invalid JSON payload") from exc
            players = normalize_players(data)
            if players is None:
                raise FiveMError("unexpected payload (expected a JSON list of players)")
            return players
        except (asyncio.TimeoutError, aiohttp.ClientError, FiveMError) as exc:
            log.warning("Failed to fetch players from %s: %s", self.url, exc)
            return None

    async def close(self):
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None


# ----------------------------------------------------------------------
# Player payload / identifier helpers
# ----------------------------------------------------------------------
def normalize_players(data):
    """Turn various players.json shapes into a plain list of player dicts.

    Supported shapes:
      * ``[{\"id\": 1, \"name\": \"A\", ...}, ...]``   (standard FiveM)
      * ``{\"players\": [...]}`` / ``{\"data\": [...]}`` / ``{\"result\": [...]}``
      * ``{\"1\": {\"id\": 1, \"name\": \"A\"}, ...}``   (keyed by id)
    Returns ``None`` if the payload cannot be interpreted.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("players", "data", "result"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        values = [
            v for v in data.values()
            if isinstance(v, dict) and ("id" in v or "name" in v)
        ]
        if values:
            return values
    return None


def extract_license(identifiers) -> str | None:
    """Return the first stable license identifier: license: > license2:."""
    if not identifiers:
        return None
    for prefix in ("license:", "license2:"):
        for ident in identifiers:
            if str(ident).startswith(prefix):
                return str(ident)
    return None


def get_player_key(player) -> str | None:
    """Return the best permanent key for a player.

    Prefers a license identifier; falls back to a name-based key
    (``name:<in-game name>``, case-insensitive) for servers whose
    players.json reports empty ``identifiers``. The session ``id`` is never
    used as a permanent key.
    """
    if not isinstance(player, dict):
        return None
    license_id = extract_license(player.get("identifiers") or [])
    if license_id:
        return license_id
    name = (player.get("name") or "").strip()
    if name:
        return f"name:{name.casefold()}"
    return None


def find_player_by_key(players, key):
    """Find an online player matching a stored permanent key.

    ``name:...`` keys are matched against the in-game name
    (case-insensitive); everything else is matched against the player's
    ``identifiers`` list.
    """
    if not key:
        return None
    key = str(key)
    if key.startswith("name:"):
        needle = key[len("name:"):].casefold()
        for player in players or []:
            if (player.get("name") or "").strip().casefold() == needle:
                return player
        return None
    for player in players or []:
        if key in (player.get("identifiers") or []):
            return player
    return None


def find_player_by_session_id(players, session_id):
    """Find an online player by the FiveM session ``id`` shown in players.json.

    The session id is only used as a lookup key for the currently-online
    player; it is never stored as the permanent identifier.
    """
    needle = str(session_id or "").strip()
    if not needle:
        return None
    for player in players or []:
        if str(player.get("id")) == needle:
            return player
    return None


# ======================================================================
# Permissions (roles from config.json, never hard-coded)
# ======================================================================

SHIFT_LEVEL = 1
USERPLUS_LEVEL = 2
MODERATOR_LEVEL = 3
MANAGER_LEVEL = 4

_LEVEL_KEYS = {
    SHIFT_LEVEL: "shift",
    USERPLUS_LEVEL: "userplus",
    MODERATOR_LEVEL: "moderator",
    MANAGER_LEVEL: "manager",
}


def get_level(member, config) -> int:
    """Return the highest permission level for a member (0 = no access)."""
    if member is None:
        return 0
    roles = config.get("roles") or {}
    member_role_ids = {str(role.id) for role in member.roles}
    for level in (MANAGER_LEVEL, MODERATOR_LEVEL, USERPLUS_LEVEL, SHIFT_LEVEL):
        allowed = roles.get(_LEVEL_KEYS[level]) or []
        if any(str(role_id) in member_role_ids for role_id in allowed):
            return level
    return 0


def has_level(member, config, required_level: int) -> bool:
    """True if the member has at least ``required_level`` access."""
    return get_level(member, config) >= required_level


# ======================================================================
# Shift management (/shift, buttons, registration, disconnect monitor)
# ======================================================================


class ShiftError(Exception):
    """A user-facing rejection with a friendly Persian message."""


def build_shift_embed(bot, user_id, note=None) -> discord.Embed:
    user = bot.db.get_user(str(user_id))
    tz = _tz(bot)
    active = bool(user and user["active_shift"].get("started_at"))
    embed = discord.Embed(
        title="SHIFT MANAGEMENT",
        color=discord.Color.green() if active else discord.Color.dark_grey(),
    )
    embed.add_field(name="Player", value=f"<@{user_id}>", inline=True)
    embed.add_field(name="Status", value="**ON SHIFT**" if active else "**OFF SHIFT**", inline=True)
    if active:
        started = parse_iso(user["active_shift"]["started_at"])
        if started:
            elapsed = max(0, int((utc_now() - started).total_seconds()))
            embed.add_field(name="Started", value=format_time(started, tz), inline=True)
            embed.add_field(name="Elapsed", value=format_duration(elapsed), inline=True)
        if user["active_shift"].get("disconnect_at"):
            embed.add_field(name="Grace", value="⚠️ بازیکن از سرور خارج شده — منتظر بازگشت", inline=False)
    if note:
        embed.add_field(name="Note", value=note, inline=False)
    return embed


class RegistrationModal(discord.ui.Modal, title="ثبت نام در سیستم شیفت"):
    """First-time registration: the user types their FiveM session id (the
    number shown next to their in-game name in the online players list) and
    the bot finds the player, saves the name + permanent key, and shows the
    shift panel. No name has to be typed."""

    fivem_id = discord.ui.TextInput(
        label="ایدی خود رو وارد کنید (مثلاً 42)",
        placeholder="ایدی بالای کاراکتر شما",
        max_length=10,
        required=True,
    )

    def __init__(self, bot):
        super().__init__()
        self.bot = bot

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        players = await self.bot.fivem.fetch_players()
        if players is None:
            await interaction.followup.send(
                "سرور FiveM در دسترس نیست. لطفاً کمی بعد دوباره تلاش کنید.", ephemeral=True
            )
            return
        player = find_player_by_session_id(players, self.fivem_id.value)
        if player is None:
            await interaction.followup.send(
                "کد واردشده با هیچ بازیکن آنلاینی مطابقت نداره.\n"
                "مطمئن شو داخل بازی آنلاین هستی، کدت رو درست بنویس و دوباره تلاش کن.",
                ephemeral=True,
            )
            return
        identifier = get_player_key(player)
        if not identifier:
            await interaction.followup.send(
                "نام داخل بازی برای این بازیکن پیدا نشد. لطفاً با ادمین سرور هماهنگ کن.", ephemeral=True
            )
            return
        name = player.get("name") or "Unknown"
        ok = await self.bot.db.register_user(str(interaction.user.id), identifier, name)
        if ok:
            log.info("Registered user %s as FiveM %r (%s)", interaction.user.id, name, identifier)
        embed = build_shift_embed(
            self.bot, interaction.user.id,
            note=f"✅ ثبت‌نام انجام شد — نام داخل بازی: **{name}**\nحالا می‌توانید شیفت خود را شروع کنید.",
        )
        await interaction.followup.send(
            embed=embed, view=ShiftView(self.bot, interaction.user.id), ephemeral=True
        )


class ShiftView(discord.ui.View):
    def __init__(self, bot, user_id):
        super().__init__(timeout=None)
        self.bot = bot
        self.user_id = user_id
        user = bot.db.get_user(str(user_id))
        active = bool(user and user["active_shift"].get("started_at"))
        self.on_start.disabled = active
        self.on_end.disabled = not active

    async def _preflight(self, interaction: discord.Interaction) -> bool:
        cfg = self.bot.config
        if str(interaction.channel_id) != str(cfg.get("channel_id")):
            await interaction.response.send_message(
                "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
            )
            return False
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "شما نمی‌توانید پنل شخص دیگری را کنترل کنید.", ephemeral=True
            )
            return False
        if not has_level(interaction.user, cfg, SHIFT_LEVEL):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این بخش را ندارید.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Start Shift", style=discord.ButtonStyle.green, row=0)
    async def on_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._preflight(interaction):
            return
        await interaction.response.defer()
        try:
            user = self.bot.db.get_user(str(self.user_id))
            if not user or user["active_shift"].get("started_at"):
                raise ShiftError("شما در حال حاضر یک شیفت فعال دارید. ابتدا شیفت فعلی را تمام کنید.")
            players = await self.bot.fivem.fetch_players()
            if players is None:
                raise ShiftError("سرور FiveM در دسترس نیست. لطفاً کمی بعد دوباره تلاش کنید.")
            identifier = user.get("fiveM_identifier")
            player = find_player_by_key(players, identifier)
            if player is None:
                raise ShiftError("شما در حال حاضر در سرور FiveM آنلاین نیستید. برای شروع شیفت باید آنلاین باشید.")
            started_at = utc_now()
            channel_id, message_id = await _send_shift_message(self.bot, self.user_id, started_at)
            ok = await self.bot.db.start_shift(
                str(self.user_id),
                started_at,
                channel_id,
                message_id,
                identifier,
                player.get("name") or user.get("fiveM_name"),
            )
            if not ok:
                raise ShiftError("شما در حال حاضر یک شیفت فعال دارید. ابتدا شیفت فعلی را تمام کنید.")
            note = None
            if channel_id is None:
                note = "⚠️ پیام شیفت در Channel ارسال نشد. لطفاً channel_id را در بخش تنظیمات داخل bot.py بررسی کنید."
            await interaction.edit_original_response(
                embed=build_shift_embed(self.bot, self.user_id, note=note),
                view=ShiftView(self.bot, self.user_id),
            )
        except ShiftError as exc:
            log.info("Start shift rejected for %s: %s", self.user_id, exc)
            await interaction.edit_original_response(
                embed=build_shift_embed(self.bot, self.user_id, note=str(exc)),
                view=ShiftView(self.bot, self.user_id),
            )
        except Exception:
            log.exception("Unexpected error while starting shift for %s", self.user_id)
            await interaction.edit_original_response(
                embed=build_shift_embed(self.bot, self.user_id, note="خطای غیرمنتظره رخ داد. لطفاً دوباره تلاش کنید."),
                view=ShiftView(self.bot, self.user_id),
            )

    @discord.ui.button(label="End Shift", style=discord.ButtonStyle.red, row=0)
    async def on_end(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._preflight(interaction):
            return
        await interaction.response.defer()
        try:
            user = self.bot.db.get_user(str(self.user_id))
            if not user or not user["active_shift"].get("started_at"):
                raise ShiftError("شما هیچ شیفت فعالی ندارید.")
            active = user["active_shift"]
            session = await self.bot.db.end_shift(str(self.user_id), utc_now())
            if session is None:
                raise ShiftError("شیفت فعالی برای پایان دادن وجود ندارد.")
            await _update_shift_message(
                self.bot, self.user_id, active.get("channel_id"), active.get("message_id"),
                session["start"], session["end"],
            )
            await interaction.edit_original_response(
                embed=build_shift_embed(self.bot, self.user_id, note="شیفت با موفقیت پایان یافت ✅"),
                view=ShiftView(self.bot, self.user_id),
            )
        except ShiftError as exc:
            log.info("End shift rejected for %s: %s", self.user_id, exc)
            await interaction.edit_original_response(
                embed=build_shift_embed(self.bot, self.user_id, note=str(exc)),
                view=ShiftView(self.bot, self.user_id),
            )
        except Exception:
            log.exception("Unexpected error while ending shift for %s", self.user_id)
            await interaction.edit_original_response(
                embed=build_shift_embed(self.bot, self.user_id, note="خطای غیرمنتظره رخ داد. لطفاً دوباره تلاش کنید."),
                view=ShiftView(self.bot, self.user_id),
            )


# ------------------------------------------------------------------
# Shift channel message helpers (also used by the disconnect monitor)
# ------------------------------------------------------------------


async def _resolve_channel(bot, channel_id):
    if not channel_id:
        return None
    try:
        channel = bot.get_channel(int(channel_id))
        if channel is None:
            channel = await bot.fetch_channel(int(channel_id))
        return channel
    except (discord.HTTPException, ValueError, TypeError):
        return None


async def _resolve_display_name(bot, channel, user_id):
    """Live Discord tag (nickname or username) for the user — plain text,
    no mention, no @."""
    guild = getattr(channel, "guild", None)
    if guild is not None:
        member = guild.get_member(user_id)
        if member is None:
            try:
                member = await guild.fetch_member(user_id)
            except discord.NotFound:
                member = None
        if member is not None:
            return member.display_name
    user = bot.get_user(user_id)
    if user is None:
        try:
            user = await bot.fetch_user(user_id)
        except discord.NotFound:
            return str(user_id)
    return user.display_name


async def _send_shift_message(bot, user_id, started_at):
    """
    Send the public shift tracking message to the configured channel.

    Returns:
        (channel_id, message_id) on success
        (None, None) on failure
    """
    cfg = bot.config
    channel_id = cfg.get("channel_id")

    channel = await _resolve_channel(bot, channel_id)
    if channel is None:
        log.error("Shift channel %s not found", channel_id)
        return None, None

    tz = _tz(bot)

    try:
        guild = channel.guild

        # دریافت Member (همیشه آخرین Nickname سرور)
        member = guild.get_member(user_id)
        if member is None:
            try:
                member = await guild.fetch_member(user_id)
            except discord.NotFound:
                member = None

        if member:
            mention = member.mention
            display_name = member.display_name
        else:
            # اگر Member پیدا نشد
            user = bot.get_user(user_id)
            if user is None:
                user = await bot.fetch_user(user_id)

            mention = user.mention
            display_name = user.display_name

        content = (
            f"{mention}\n"
            f"**{display_name}**\n\n"
            f"On : {format_time(started_at, tz)}\n"
            f"Off : -- \n"
            f"----------------"
        )

        message = await channel.send(content)

        return message.channel.id, message.id

    except discord.HTTPException:
        log.exception("Failed to send shift message.")
        return None, None

    except Exception:
        log.exception("Unexpected error while sending shift message.")
        return None, None


async def _update_shift_message(bot, user_id, channel_id, message_id, started_iso, end_iso):
    """Edit the shift message to show the end time; resend if it was deleted."""

    tz = _tz(bot)

    start = parse_iso(started_iso)
    end = parse_iso(end_iso)

    if not start or not end:
        log.error(
            "Cannot update shift message: bad timestamps (%r -> %r)",
            started_iso,
            end_iso,
        )
        return

    channel = await _resolve_channel(bot, channel_id)
    if channel is None:
        log.error("Cannot update shift message: channel %s not found", channel_id)
        return

    try:
        guild = channel.guild

        # همیشه آخرین Nickname کاربر را بگیر
        member = guild.get_member(user_id)
        if member is None:
            try:
                member = await guild.fetch_member(user_id)
            except discord.NotFound:
                member = None

        if member:
            mention = member.mention
            display_name = member.display_name
        else:
            user = bot.get_user(user_id)
            if user is None:
                user = await bot.fetch_user(user_id)

            mention = user.mention
            display_name = user.display_name

        content = (
            f"{mention}\n"
            f"**{display_name}**\n\n"
            f"On : {format_time(start, tz)}\n"
            f"Off : {format_time(end, tz)}\n"
            f"----------------"
        )

        if message_id:
            try:
                message = await channel.fetch_message(int(message_id))
                await message.edit(content=content)
                return

            except discord.NotFound:
                log.info(
                    "Shift message %s was deleted; sending a new one",
                    message_id,
                )

            except discord.HTTPException:
                log.exception(
                    "Failed to edit shift message %s; sending a new one",
                    message_id,
                )

        new_message = await channel.send(content)
        return channel.id, new_message.id

    except discord.HTTPException:
        log.exception("Failed to send replacement shift message")

    except Exception:
        log.exception("Unexpected error while updating shift message")


class ShiftCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.monitor_task = None

    async def cog_load(self):
        self.monitor_task = asyncio.create_task(self._monitor_loop())

    async def cog_unload(self):
        if self.monitor_task:
            self.monitor_task.cancel()

    # ------------------------------------------------------------------
    # /shift command
    # ------------------------------------------------------------------
    @app_commands.command(name="shift", description="پنل مدیریت شیفت (شروع / پایان شیفت)")
    @app_commands.guild_only()
    async def shift_command(self, interaction: discord.Interaction):
        cfg = self.bot.config
        if str(interaction.channel_id) != str(cfg.get("channel_id")):
            await interaction.response.send_message(
                "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
            )
            return
        if not has_level(interaction.user, cfg, SHIFT_LEVEL):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این دستور را ندارید.", ephemeral=True
            )
            return
        user = self.bot.db.get_user(str(interaction.user.id))
        if user is None:
            # First use: ask only for the player's code (the FiveM session id
            # shown next to their in-game name in the online players list).
            await interaction.response.send_modal(RegistrationModal(self.bot))
            return
        await interaction.response.defer(ephemeral=True)
        embed = build_shift_embed(self.bot, interaction.user.id)
        await interaction.followup.send(
            embed=embed, view=ShiftView(self.bot, interaction.user.id), ephemeral=True
        )

    # ------------------------------------------------------------------
    # Disconnect detection / grace period monitor
    # ------------------------------------------------------------------
    async def _monitor_loop(self):
        await self.bot.wait_until_ready()
        settings = self.bot.config.get("settings") or {}
        try:
            interval = max(5, int(settings.get("check_interval_seconds", 10)))
        except (TypeError, ValueError):
            interval = 10
        try:
            grace_minutes = max(1.0, float(settings.get("disconnect_grace_minutes", 15)))
        except (TypeError, ValueError):
            grace_minutes = 15.0
        grace_seconds = grace_minutes * 60
        log.info("Shift monitor started (interval=%ss, grace=%smin)", interval, grace_minutes)
        while True:
            try:
                await self._check_disconnects(grace_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Monitor cycle failed")
            await asyncio.sleep(interval)

    async def _check_disconnects(self, grace_seconds):
        active = self.bot.db.active_users()
        if not active:
            return
        players = await self.bot.fivem.fetch_players()
        if players is None:
            log.warning("players.json unavailable during monitor cycle; skipping this round")
            return
        online = set()
        for player in players:
            key = get_player_key(player)
            if key:
                online.add(key)
        now = utc_now()
        for uid, user in active:
            active_rec = user["active_shift"]
            identifier = active_rec.get("fiveM_identifier") or user.get("fiveM_identifier")
            if not identifier:
                log.warning("Active shift for %s has no identifier; skipping", uid)
                continue
            online_now = identifier in online
            disconnect_at = parse_iso(active_rec.get("disconnect_at"))
            if online_now:
                if disconnect_at is not None:
                    log.info("Player %s came back within the grace period; shift continues", uid)
                    await self.bot.db.clear_grace(uid)
                continue
            if disconnect_at is None:
                log.info("Player %s left the server; grace period started", uid)
                await self.bot.db.begin_grace(uid, now)
                continue
            if (now - disconnect_at).total_seconds() >= grace_seconds:
                end_at = disconnect_at + timedelta(seconds=grace_seconds)
                log.info("Grace period expired for %s; auto-ending shift at %s", uid, end_at)
                session = await self.bot.db.end_shift(uid, end_at)
                if session:
                    await _update_shift_message(
                        self.bot, uid, active_rec.get("channel_id"), active_rec.get("message_id"),
                        session["start"], session["end"],
                    )


# ======================================================================
# Statistics commands (/check, /list)
# ======================================================================

PER_PAGE = 10


def build_check_embed(bot, member, record) -> discord.Embed:
    tz = _tz(bot)
    sessions = record.get("sessions") or []
    active = bool(record.get("active_shift", {}).get("started_at"))
    total = int(record.get("total_shift_seconds") or 0)
    today = sessions_total_in_period(sessions, "day", tz)
    week = sessions_total_in_period(sessions, "week", tz)
    month = sessions_total_in_period(sessions, "month", tz)

    status = "ON SHIFT" if active else "OFF SHIFT"
    embed = discord.Embed(
        title="Shift Statistics",
        description=f"User: {member.mention}\n\n**Status:** {status}",
        color=discord.Color.green() if active else discord.Color.dark_grey(),
    )
    embed.add_field(name="Total Shift", value=f"**{format_duration(total)}**", inline=False)
    embed.add_field(name="Today", value=format_duration(today), inline=True)
    embed.add_field(name="This Week", value=format_duration(week), inline=True)
    embed.add_field(name="This Month", value=format_duration(month), inline=True)
    embed.add_field(name="Sessions", value=str(len(sessions)), inline=True)
    if sessions:
        last = sessions[-1]
        start = parse_iso(last.get("start"))
        end = parse_iso(last.get("end"))
        if start and end:
            line = f"{format_date_time(start, tz)} → {format_date_time(end, tz)}"
        else:
            line = "—"
        embed.add_field(
            name="Last Shift",
            value=f"`{line}` ({format_duration(last.get('duration_seconds') or 0)})",
            inline=False,
        )
    else:
        embed.add_field(name="Last Shift", value="No sessions yet", inline=False)
    if active:
        started = parse_iso(record["active_shift"].get("started_at"))
        if started:
            embed.add_field(name="Active Since", value=format_date_time(started, tz), inline=False)
    return embed


def build_leaderboard_embed(bot, users, page, guild) -> discord.Embed:
    per_page = PER_PAGE
    pages = max(1, math.ceil(len(users) / per_page))
    page = max(0, min(page, pages - 1))
    start = page * per_page
    chunk = users[start : start + per_page]

    lines = []
    for index, (uid, user) in enumerate(chunk, start=start + 1):
        # The database key is the Discord user id, so a raw mention always
        # tags the player — no member cache required.
        display = f"<@{uid}>"
        duration = format_duration(int(user.get("total_shift_seconds") or 0))
        lines.append(f"#{index}  {display}  **{duration}**")

    embed = discord.Embed(
        title="Shift Leaderboard",
        description="\n".join(lines) if lines else "هنوز بازیکنی در سیستم ثبت نشده است.",
        color=discord.Color.gold(),
    )
    embed.set_footer(text=f"Page {page + 1}/{pages}")
    return embed


def build_onlist_embed(bot, users, page, guild) -> discord.Embed:
    """Leaderboard-style embed of users who are online in FiveM with an active shift."""
    tz = _tz(bot)
    per_page = PER_PAGE
    pages = max(1, math.ceil(len(users) / per_page))
    page = max(0, min(page, pages - 1))
    start = page * per_page
    chunk = users[start : start + per_page]

    lines = []
    for index, (uid, user) in enumerate(chunk, start=start + 1):
        display = f"<@{uid}>"
        total = format_duration(int(user.get("total_shift_seconds") or 0))
        started = parse_iso(user.get("active_shift", {}).get("started_at"))
        started_str = f" — since {format_time(started, tz)}" if started else ""
        lines.append(f"#{index}  {display}  **{total}**{started_str}")

    embed = discord.Embed(
        title="On Shift — Online Players",
        description="\n".join(lines) if lines else "در حال حاضر هیچ بازیکنی با شیفت فعال آنلاین نیست.",
        color=discord.Color.green(),
    )
    embed.set_footer(text=f"Page {page + 1}/{pages}")
    return embed


class LeaderboardView(discord.ui.View):
    def __init__(self, bot, users, page=0, required_level=MODERATOR_LEVEL, embed_builder=None):
        super().__init__(timeout=None)
        self.bot = bot
        self.users = users
        self.page = page
        self.per_page = PER_PAGE
        self.pages = max(1, math.ceil(len(users) / self.per_page))
        self.required_level = required_level
        self.embed_builder = embed_builder or build_leaderboard_embed
        self._update_buttons()

    def _embed(self, guild):
        return self.embed_builder(self.bot, self.users, self.page, guild)

    def _update_buttons(self):
        self.prev.disabled = self.page <= 0
        self.next.disabled = self.page >= self.pages - 1

    async def _check(self, interaction):
        if str(interaction.channel_id) != str(self.bot.config.get("channel_id")):
            await interaction.response.send_message(
                "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
            )
            return False
        if not has_level(interaction.user, self.bot.config, self.required_level):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این دستور را ندارید.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary, row=0)
    async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check(interaction):
            return
        self.page = max(0, self.page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self._embed(interaction.guild), view=self)

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary, row=0)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check(interaction):
            return
        self.page = min(self.pages - 1, self.page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self._embed(interaction.guild), view=self)


class StatisticsCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def _channel_ok(self, interaction) -> bool:
        if str(interaction.channel_id) == str(self.bot.config.get("channel_id")):
            return True
        await interaction.response.send_message(
            "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
        )
        return False

    @app_commands.command(name="check", description="مشاهده آمار شیفت یک بازیکن")
    @app_commands.describe(user="بازیکنی که می‌خواهید آمار شیفتش را ببینید")
    @app_commands.guild_only()
    async def check(self, interaction: discord.Interaction, user: discord.Member):
        if not await self._channel_ok(interaction):
            return
        if not has_level(interaction.user, self.bot.config, MODERATOR_LEVEL):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این دستور را ندارید.", ephemeral=True
            )
            return
        record = self.bot.db.get_user(str(user.id))
        if record is None:
            await interaction.response.send_message(
                "این بازیکن هنوز در سیستم شیفت ثبت نشده است.", ephemeral=True
            )
            return
        embed = build_check_embed(self.bot, user, record)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="list", description="لیست بازیکنان بر اساس کل زمان شیفت")
    @app_commands.guild_only()
    async def list_command(self, interaction: discord.Interaction):
        if not await self._channel_ok(interaction):
            return
        if not has_level(interaction.user, self.bot.config, MODERATOR_LEVEL):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این دستور را ندارید.", ephemeral=True
            )
            return
        users = self.bot.db.all_users()
        users.sort(key=lambda item: int(item[1].get("total_shift_seconds") or 0), reverse=True)
        await interaction.response.defer(ephemeral=True)
        embed = build_leaderboard_embed(self.bot, users, 0, interaction.guild)
        await interaction.followup.send(
            embed=embed, view=LeaderboardView(self.bot, users, page=0), ephemeral=True
        )

    @app_commands.command(name="onlist", description="لیست بازیکنانی که آنلاین هستند و شیفت فعال دارند")
    @app_commands.guild_only()
    async def onlist(self, interaction: discord.Interaction):
        if not await self._channel_ok(interaction):
            return
        if not has_level(interaction.user, self.bot.config, USERPLUS_LEVEL):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این دستور را ندارید.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        players = await self.bot.fivem.fetch_players()
        if players is None:
            await interaction.followup.send(
                "سرور FiveM در دسترس نیست. لطفاً بعداً دوباره تلاش کنید.", ephemeral=True
            )
            return
        users = []
        for uid, user in self.bot.db.active_users():
            identifier = (
                user.get("fiveM_identifier")
                or user.get("active_shift", {}).get("fiveM_identifier")
            )
            if identifier and find_player_by_key(players, identifier):
                users.append((uid, user))
        users.sort(
            key=lambda item: int(item[1].get("total_shift_seconds") or 0), reverse=True
        )
        embed = build_onlist_embed(self.bot, users, 0, interaction.guild)
        view = LeaderboardView(
            self.bot, users, page=0,
            required_level=USERPLUS_LEVEL,
            embed_builder=build_onlist_embed,
        )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


# ======================================================================
# Admin commands (/reset with confirmation)
# ======================================================================


class ResetConfirmView(discord.ui.View):
    def __init__(self, bot, invoker_id):
        super().__init__(timeout=None)
        self.bot = bot
        self.invoker_id = invoker_id

    async def _check(self, interaction) -> bool:
        if str(interaction.channel_id) != str(self.bot.config.get("channel_id")):
            await interaction.response.send_message(
                "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
            )
            return False
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "فقط شخصی که دستور /reset را اجرا کرده می‌تواند تأیید کند.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Confirm Reset", style=discord.ButtonStyle.danger, row=0)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check(interaction):
            return
        count = await self.bot.db.reset_all_shift_data()
        log.warning("Shift data for %d user(s) was reset by %s", count, interaction.user.id)
        embed = discord.Embed(
            title="Reset Complete",
            description=f"تمام اطلاعات شیفت **{count}** بازیکن پاک شد.",
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=0)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check(interaction):
            return
        embed = discord.Embed(
            title="Reset Cancelled",
            description="عملیات Reset لغو شد. هیچ داده‌ای پاک نشد.",
            color=discord.Color.dark_grey(),
        )
        await interaction.response.edit_message(embed=embed, view=None)


class AdminCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="reset", description="پاک‌سازی کامل تمام اطلاعات شیفت (فقط Manager)")
    @app_commands.guild_only()
    async def reset(self, interaction: discord.Interaction):
        if str(interaction.channel_id) != str(self.bot.config.get("channel_id")):
            await interaction.response.send_message(
                "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
            )
            return
        if not has_level(interaction.user, self.bot.config, MANAGER_LEVEL):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این دستور را ندارید.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        embed = discord.Embed(
            title="⚠️ Reset All Shift Data",
            description=(
                "آیا مطمئن هستید؟ تمام زمان‌های شیفت، سشن‌ها و شیفت‌های فعال "
                "**برای همیشه** پاک خواهند شد.\n\nاین عملیات قابل بازگشت نیست."
            ),
            color=discord.Color.red(),
        )
        view = ResetConfirmView(self.bot, interaction.user.id)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="wipe", description="حذف کامل یک بازیکن از سیستم شیفت (فقط Manager)")
    @app_commands.describe(user="بازیکنی که می‌خواهید کاملاً از دیتابیس حذف کنید")
    @app_commands.guild_only()
    async def wipe(self, interaction: discord.Interaction, user: discord.Member):
        if str(interaction.channel_id) != str(self.bot.config.get("channel_id")):
            await interaction.response.send_message(
                "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
            )
            return
        if not has_level(interaction.user, self.bot.config, MANAGER_LEVEL):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این دستور را ندارید.", ephemeral=True
            )
            return
        removed = await self.bot.db.delete_user(str(user.id))
        if not removed:
            await interaction.response.send_message(
                f"{user.mention} در سیستم شیفت ثبت نشده است.", ephemeral=True
            )
            return
        log.warning(
            "User %s was completely wiped from shift data by %s", user.id, interaction.user.id
        )
        await interaction.response.send_message(
            f"✅ کاربر {user.mention} به‌طور کامل از سیستم شیفت حذف شد.", ephemeral=True
        )

    @app_commands.command(name="wipeall", description="پاک‌سازی کامل دیتابیس شیفت — حذف همه بازیکنان (فقط Manager)")
    @app_commands.guild_only()
    async def wipeall(self, interaction: discord.Interaction):
        if str(interaction.channel_id) != str(self.bot.config.get("channel_id")):
            await interaction.response.send_message(
                "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
            )
            return
        if not has_level(interaction.user, self.bot.config, MANAGER_LEVEL):
            await interaction.response.send_message(
                "شما دسترسی لازم برای استفاده از این دستور را ندارید.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        embed = discord.Embed(
            title="⚠️ Wipe ALL Shift Data",
            description=(
                "آیا مطمئن هستید؟ **تمام بازیکنان** به همراه ثبت‌نام‌ها، زمان‌های شیفت، "
                "سشن‌ها و شیفت‌های فعال **برای همیشه** از دیتابیس حذف خواهند شد.\n\n"
                "این عملیات قابل بازگشت نیست."
            ),
            color=discord.Color.red(),
        )
        view = WipeAllConfirmView(self.bot, interaction.user.id)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

class WipeAllConfirmView(discord.ui.View):
    def __init__(self, bot, invoker_id):
        super().__init__(timeout=None)
        self.bot = bot
        self.invoker_id = invoker_id

    async def _check(self, interaction) -> bool:
        if str(interaction.channel_id) != str(self.bot.config.get("channel_id")):
            await interaction.response.send_message(
                "این دستور فقط در Channel مخصوص Shift قابل استفاده است.", ephemeral=True
            )
            return False
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "فقط شخصی که دستور /wipeall را اجرا کرده می‌تواند تأیید کند.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Confirm Wipe All", style=discord.ButtonStyle.danger, row=0)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check(interaction):
            return
        count = await self.bot.db.wipe_all()
        log.warning("ALL shift data (%d user(s)) was wiped by %s", count, interaction.user.id)
        embed = discord.Embed(
            title="Wipe Complete",
            description=f"تمام اطلاعات **{count}** بازیکن به‌طور کامل از دیتابیس حذف شد.",
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=0)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._check(interaction):
            return
        embed = discord.Embed(
            title="Wipe Cancelled",
            description="عملیات لغو شد. هیچ داده‌ای پاک نشد.",
            color=discord.Color.dark_grey(),
        )
        await interaction.response.edit_message(embed=embed, view=None)


# ======================================================================
# Bot entry point
# ======================================================================


class ShiftBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.config = CONFIG
        self.logger = logging.getLogger("shift_bot")
        settings = self.config.get("settings") or {}
        self.fivem = FiveMClient(settings.get("players_url", ""))
        self.db = Database(
            str(BASE_DIR / "data" / "shift_data.json"),
            str(BASE_DIR / "data" / "backups"),
        )
        self._validate_config()

    def _validate_config(self):
        cfg = self.config
        settings = cfg.get("settings") or {}
        missing = []
        if not cfg.get("channel_id"):
            missing.append("channel_id")
        for role_key in ("shift", "moderator", "manager"):
            if not cfg.get("roles", {}).get(role_key):
                missing.append(f"roles.{role_key}")
        if not settings.get("players_url"):
            missing.append("settings.players_url")
        if missing:
            self.logger.warning(
                "CONFIG is missing/empty for: %s — fill them in (top of bot.py) before going live.",
                ", ".join(missing),
            )

    async def setup_hook(self):
        await self.add_cog(ShiftCog(self))
        await self.add_cog(StatisticsCog(self))
        await self.add_cog(AdminCog(self))
        command_names = [cmd.name for cmd in self.tree.get_commands()]
        self.logger.info("Registered %d command(s): %s", len(command_names), ", ".join(command_names))
        await self._sync_global_commands()
        guild_id = str(self.config.get("guild_id") or "").strip()
        if guild_id:
            await self._sync_to_guild(int(guild_id), label="configured guild")

    async def _sync_global_commands(self):
        """Register the commands GLOBALLY so the bot shows up in Discord's
        \"Supports Commands\" list / badge and works in every server.
        (Global propagation can take up to an hour; guild sync below covers
        the main servers instantly.)"""
        try:
            await self.tree.sync()
        except discord.HTTPException:
            self.logger.exception("[SYNC] Global command sync failed")
            return
        self.logger.info(
            "[SYNC OK] Global commands registered: %s",
            ", ".join(sorted(cmd.name for cmd in self.tree.get_commands())),
        )

    async def _sync_to_guild(self, guild_id: int, label: str, retries: int = 3):
        """Sync the command tree to one guild with retry + loud logging."""
        for attempt in range(1, retries + 1):
            try:
                await self.tree.sync(guild=discord.Object(id=guild_id))
                names = ", ".join(sorted(cmd.name for cmd in self.tree.get_commands()))
                self.logger.info(
                    "[SYNC OK] Commands sent to %s (guild %s): %s", label, guild_id, names
                )
                print(f"[SYNC OK] دستورات به سرور ارسال شد: {names}")
                return True
            except discord.Forbidden:
                self.logger.error(
                    "[SYNC FAIL] Bot lacks 'applications.commands' scope on guild %s. "
                    "Re-invite the bot with BOTH 'bot' and 'applications.commands' scopes.",
                    guild_id,
                )
                print(
                    "[SYNC FAIL] ربات دسترسی سینک دستورات را ندارد — لینک دعوت باید "
                    "هم bot و هم applications.commands را داشته باشد."
                )
                return False
            except discord.HTTPException:
                if attempt < retries:
                    self.logger.warning(
                        "[SYNC RETRY] Sync to guild %s failed (attempt %d/%d); retrying...",
                        guild_id, attempt, retries,
                    )
                    await asyncio.sleep(attempt * 2)
                    continue
                self.logger.exception("Sync to guild %s failed after %d attempts", guild_id, retries)
                print(f"[SYNC FAIL] سینک دستورات به سرور {guild_id} شکست خورد.")
                return False
        return False

    async def _ensure_command_sync(self):
        """After login, push the command list to every guild the bot is in so
        newly added commands (e.g. /wipe, /wipeall) appear instantly on restart
        instead of waiting up to an hour for global propagation."""
        guild_id = str(self.config.get("guild_id") or "").strip()
        if guild_id:
            await self._sync_to_guild(int(guild_id), label="configured guild")
            return
        if not self.guilds:
            self.logger.warning("[SYNC] Bot is not in any guild yet — nothing to sync to.")
            return
        for guild in self.guilds:
            await self._sync_to_guild(guild.id, label=f"guild {guild.name}")

    async def on_ready(self):
        self.logger.info("Logged in as %s (id=%s)", self.user, self.user.id)
        active = self.db.active_users()
        self.logger.info("Restored %d active shift(s) from disk", len(active))
        for key, user in active:
            active_rec = user["active_shift"]
            self.logger.info(
                "  Active shift: user=%s started=%s channel=%s message=%s disconnect=%s",
                key,
                active_rec.get("started_at"),
                active_rec.get("channel_id"),
                active_rec.get("message_id"),
                active_rec.get("disconnect_at"),
            )
        await self._ensure_command_sync()

    async def close(self):
        await self.fivem.close()
        await super().close()


def setup_logging() -> logging.Logger:
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(
        log_dir / "bot.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)
    return root


def main():
    setup_logging()
    if not TOKEN or TOKEN == "YOUR_BOT_TOKEN_HERE":
        log.error("DISCORD_TOKEN is not set. Copy .env.example to .env and fill in your token.")
        raise SystemExit(1)
    bot = ShiftBot()
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
