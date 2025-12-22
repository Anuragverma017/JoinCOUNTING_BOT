# --------------------------------------------------- 
# Telegram Private Join-Link Counter Bot
# Uses: Telethon + Supabase
# Focus:
#   - Login user account
#   - Create & track invite links (ONLY groups/channels, no user chats)
#   - Show per-link join stats (with selection + pagination)
#   - /remove_link also deletes join data for that link (with confirmation)
#   - Subscription system shared with AutoForward bot (Razorpay + Supabase)
# ---------------------------------------------------

import os
import re
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Set, Any, Optional

from dotenv import load_dotenv
from supabase import create_client, Client
import aiohttp, json, base64  # HTTP Razorpay client

from telethon import TelegramClient, events, errors, Button
from telethon import types as tl_types  # for User/Chat/Channel/UpdateBotChatInviteRequester, InputUserEmpty
from telethon.tl import functions, types
from telethon.utils import get_peer_id

# ---------------- ENV & GLOBALS ----------------

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
# allow either SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

SESSION_DIR = os.getenv("SESSION_DIR", "sessions")
TOP_N = 14  # how many pinned chats to show in selection

# Razorpay env (same as AutoForward bot)
RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")

# Plan config
PLAN_DURATION_DAYS = int(os.getenv("PLAN_DURATION_DAYS", "30"))
BASIC_PRICE_PAISE = int(os.getenv("BASIC_PRICE_PAISE", "69900"))
PRO_PRICE_PAISE = int(os.getenv("PRO_PRICE_PAISE", "149900"))
PREMIUM_PRICE_PAISE = int(os.getenv("PREMIUM_PRICE_PAISE", "249900"))

assert API_ID and API_HASH and BOT_TOKEN, "Set API_ID, API_HASH, BOT_TOKEN in .env"
assert SUPABASE_URL and SUPABASE_KEY, "Set SUPABASE_URL and SUPABASE_KEY / SUPABASE_SERVICE_ROLE_KEY in .env"

os.makedirs(SESSION_DIR, exist_ok=True)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Razorpay (HTTP flow same as AutoForward bot) ---
RP_BASE = "https://api.razorpay.com/v1"
# yahan RAZORPAY_KEY_ID / SECRET upar already defined hain
assert RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET, "Set RAZORPAY_KEY_ID / RAZORPAY_KEY_SECRET in .env"

def _rp_auth_headers():
    token = base64.b64encode(f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }

def sp_log_payment_link(uid: int, plan_id: str, plan_label: str, price_paise: int,
                        duration_days: int, plink_id: str, plink_url: str, raw: dict):
    supabase.table("user_payment_links").upsert({
        "user_id": uid,
        "plan_id": plan_id,
        "plan_label": plan_label,
        "price_paise": int(price_paise),
        "duration_days": int(duration_days),
        "paymentlink_id": plink_id,
        "paymentlink_url": plink_url,
        "status": "created",
        "raw": raw,
    }, on_conflict="paymentlink_id").execute()

def sp_upsert_subscription_plan_meta(uid: int, plan_id: str, plan_label: str,
                                     price_paise: int, duration_days: int,
                                     plink_id: str, plink_url: str):
    now = datetime.now(timezone.utc).isoformat()
    supabase.table("user_subscriptions").upsert({
        "user_id": uid,
        "plan_id": plan_id,
        "plan_label": plan_label,
        "plan_price_paise": int(price_paise),
        "plan_duration_days": int(duration_days),
        "last_paymentlink_id": plink_id,
        "last_paymentlink_url": plink_url,
        "last_payment_status": "created",
        "updated_at": now,
    }, on_conflict="user_id").execute()

bot = TelegramClient("join_counter_bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# ---- in-memory state ----
login_state: Dict[int, Dict[str, Any]] = {}
select_state: Dict[int, Dict[str, Any]] = {}   # selection flows (create/remove links/confirm)
USER_CLIENT_CACHE: Dict[int, TelegramClient] = {}
stats_state: Dict[int, Dict[str, Any]] = {}    # stats link selection context

# per-message stats pagination state
stats_pages: Dict[Tuple[int, int], Dict[str, Any]] = {}

PHONE_RE = re.compile(r"^\+\d{6,15}$", re.IGNORECASE)
OTP_RE = re.compile(r"^(?:HELLO\s*)?(\d{4,8})$", re.IGNORECASE)


# ---------------- TELEGRAM HELPERS ----------------

def session_path(uid: int, phone: str) -> str:
    digits = "".join([c for c in phone if c.isdigit()])
    return os.path.join(SESSION_DIR, f"{uid}_{digits}.session")


async def safe_connect(client: TelegramClient, retries: int = 3, delay: int = 2):
    """Safer connect with retries."""
    try:
        if client.is_connected():
            return
    except Exception:
        pass
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            await client.connect()
            if client.is_connected():
                return
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                await asyncio.sleep(delay * attempt)
    raise last_exc or RuntimeError("safe_connect: failed to connect")


def title_of(ent) -> str:
    if getattr(ent, "title", None):
        return ent.title
    fn = getattr(ent, "first_name", "") or ""
    ln = getattr(ent, "last_name", "") or ""
    if fn or ln:
        return (fn + " " + ln).strip()
    if getattr(ent, "username", None):
        return "@" + ent.username
    return f"id:{getattr(ent, 'id', '')}"


async def top_dialog_pairs(client: TelegramClient, limit: int = TOP_N) -> List[Tuple[int, str]]:
    """
    Return only PRIVATE groups/channels (no 1-1 chats),
    AND only those where the logged-in user is admin/creator.
    Private ka matlab:
      - Normal groups (types.Chat) hamesha private
      - Channels jinka username None ho (no @publicname)
    """
    dialogs = await client.get_dialogs(limit=200)
    pairs: List[Tuple[int, str]] = []

    for d in dialogs:
        # 1-1 user chats skip
        if d.is_user:
            continue

        ent = d.entity

        # Sirf groups / channels hi allow
        if not isinstance(ent, (types.Channel, types.Chat)):
            continue

        # -------- PRIVATE FILTER --------
        # Channel / supergroup agar public @username hai toh skip
        if isinstance(ent, types.Channel):
            # Agar username hai -> public channel/supergroup
            if getattr(ent, "username", None):
                continue
        # types.Chat normal group hai – ye waise hi private hota hai, so ok

        # -------- ADMIN CHECK --------
        is_admin = False

        # creator ho ya admin_rights ho
        if getattr(ent, "creator", False):
            is_admin = True

        rights = getattr(ent, "admin_rights", None)
        if rights:
            is_admin = True

        if not is_admin:
            continue

        # Yaha tak aaya matlab:
        # - private group/channel
        # - jisme user admin/creator hai
        pairs.append((int(get_peer_id(ent)), title_of(ent)))

        if len(pairs) >= limit:
            break

    return pairs


def numbered_list_from_pairs(pairs: List[Tuple[int, str]]) -> str:
    return "\n".join([f"{i + 1}. {pairs[i][1]}" for i in range(len(pairs))])


def multi_kb(n: int, selected: Set[int]) -> List[List[Button]]:
    """Generic multi-select keyboard."""
    rows, row = [], []
    for i in range(1, n + 1):
        label = f"{'✅ ' if (i - 1) in selected else ''}{i}"
        row.append(Button.inline(label, data=f"msel:{i}".encode()))
        if len(row) == 7:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([Button.inline("✅ Done", data=b"msel_done"),
                 Button.inline("✖ Cancel", data=b"msel_cancel")])
    return rows


# ---------------- SUPABASE + SUBSCRIPTION HELPERS ----------------

def sp_get_subscription(uid: int) -> Optional[dict]:
    """
    Fetch user's subscription row from user_subscriptions table.
    """
    try:
        res = (
            supabase.table("user_subscriptions")
            .select("*")
            .eq("user_id", uid)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as ex:
        print("sp_get_subscription error:", ex)
        return None


def is_plan_active(sub: dict) -> bool:
    """
    Check if subscription is active based on expires_at timestamp.
    """
    try:
        exp = sub.get("expires_at")
        if not exp:
            return False
        exp_dt = datetime.fromisoformat(str(exp).replace("Z", "")).astimezone(timezone.utc)
        return datetime.now(timezone.utc) < exp_dt
    except Exception:
        return False


def format_plan_status(uid: int) -> str:
    """
    Returns human readable string for /upgrade_status based on subscription.
    """
    sub = sp_get_subscription(uid)
    if not sub:
        return "🔴 **No active plan found.**\nUse /upgrade to purchase any plan."

    if not is_plan_active(sub):
        return "🟠 **Your plan has expired.**\nUse /upgrade to renew."

    expires = str(sub.get("expires_at")).replace("Z", "")[:19]
    label = sub.get("plan_label") or "Unknown Plan"

    return (
        f"🟢 **Active Plan: {label}**\n"
        f"📅 **Expires at:** `{expires}`\n\n"
        "Thank you for being a premium user! 🎉"
    )


async def require_active_plan(e) -> bool:
    """
    Used to protect premium commands (same behaviour as AutoForward bot).
    """
    uid = e.sender_id
    sub = sp_get_subscription(uid)
    if not sub or not is_plan_active(sub):
        await e.respond(
            "🔴 **You do not have an active plan.**\n\n"
            "Use `/upgrade` to purchase a plan.",
            parse_mode="md",
        )
        return False
    return True


def sp_get_session(uid: int) -> Optional[dict]:
    res = supabase.table("user_sessions").select("*").eq("user_id", uid).limit(1).execute()
    return res.data[0] if res.data else None


def sp_upsert_session(uid: int, phone: str, session_file: str):
    payload = {
        "user_id": uid,
        "phone": phone,
        "session_file": session_file,
        "is_active": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        supabase.table("user_sessions").upsert(payload, on_conflict="user_id").execute()
    except Exception:
        existing = supabase.table("user_sessions").select("user_id").eq("user_id", uid).limit(1).execute()
        if existing and existing.data:
            supabase.table("user_sessions").update(payload).eq("user_id", uid).execute()
        else:
            supabase.table("user_sessions").insert(payload).execute()


def sp_delete_session(uid: int):
    supabase.table("user_sessions").delete().eq("user_id", uid).execute()


def sp_save_invite_link(uid: int, chat_id: int, chat_title: str, link: str):
    supabase.table("invite_links").upsert(
        {
            "user_id": uid,
            "chat_id": chat_id,
            "chat_title": chat_title,
            "invite_link": link,
            "is_active": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        on_conflict="user_id,chat_id,invite_link",
    ).execute()


def sp_list_invite_links(uid: int) -> List[dict]:
    res = (
        supabase.table("invite_links")
        .select("*")
        .eq("user_id", uid)
        .eq("is_active", True)
        .order("created_at", desc=True)
        .execute()
    )
    return res.data or []


def sp_soft_delete_links(uid: int, link_ids: List[int]):
    """
    Remove selected invite_links and all their join rows from the DB.
    """
    if not link_ids:
        return

    # 1) delete join rows
    supabase.table("joins") \
        .delete() \
        .eq("user_id", uid) \
        .in_("invite_link_id", link_ids) \
        .execute()

    # 2) delete invite_links rows
    supabase.table("invite_links") \
        .delete() \
        .eq("user_id", uid) \
        .in_("id", link_ids) \
        .execute()


def sp_replace_joins_for_link(uid: int, invite_link_id: int, rows: List[dict]):
    supabase.table("joins").delete().eq("user_id", uid).eq("invite_link_id", invite_link_id).execute()
    if rows:
        supabase.table("joins").insert(rows).execute()


def _count_from_response(res) -> int:
    try:
        if hasattr(res, "count") and res.count is not None:
            return int(res.count)
    except Exception:
        pass
    try:
        return len(res.data or [])
    except Exception:
        return 0


def sp_count_joins_for_link(
    uid: int,
    invite_link_id: int,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> int:
    """Count joins for a specific invite_link."""
    q = (
        supabase.table("joins")
        .select("id", count="exact")
        .eq("user_id", uid)
        .eq("invite_link_id", invite_link_id)
    )
    if since:
        q = q.gte("joined_at", since.isoformat())
    if until:
        q = q.lte("joined_at", until.isoformat())
    res = q.execute()
    return _count_from_response(res)


def sp_fetch_joins_for_link(
    uid: int,
    invite_link_id: int,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    offset: int = 0,
    limit: int = 20,
) -> List[dict]:
    """
    Fetch a page of join rows for one invite_link, newest first.
    """
    q = (
        supabase.table("joins")
        .select("*")
        .eq("user_id", uid)
        .eq("invite_link_id", invite_link_id)
    )
    if since:
        q = q.gte("joined_at", since.isoformat())
    if until:
        q = q.lte("joined_at", until.isoformat())

    # newest first
    q = q.order("joined_at", desc=True).range(offset, offset + limit - 1)

    res = q.execute()
    return res.data or []


def _safe_ascii(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\x20-\x7E]+", " ", s)  # remove emojis/unicode
    s = re.sub(r"\s+", " ", s).strip()
    return s[:200]

async def create_payment_link(uid: int, amount_paise: int, plan_id: str, plan_label: str) -> Optional[str]:
    safe_label = _safe_ascii(plan_label) or plan_id.upper()

    payload = {
        "amount": int(amount_paise),
        "currency": "INR",
        "description": f"GetAIPilot {safe_label} plan for user {uid}",
        "notify": {"email": False, "sms": False},
        "reminder_enable": True,
        "expire_by": int((datetime.now(timezone.utc) + timedelta(minutes=30)).timestamp()),
        "notes": {
            "telegram_user_id": str(uid),
            "plan": plan_id,
            "plan_label": safe_label,
        },
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                f"{RP_BASE}/payment_links",
                headers=_rp_auth_headers(),
                json=payload,   # ✅ IMPORTANT FIX
            ) as resp:
                txt = await resp.text()
                if resp.status >= 300:
                    print("Razorpay create failed:", resp.status, txt)
                    return None

                data = json.loads(txt)

        p_id = data.get("id")
        p_url = data.get("short_url") or data.get("url")

        if not p_id or not p_url:
            print("Razorpay response missing id/url:", data)
            return None

        sp_log_payment_link(
            uid=uid,
            plan_id=plan_id,
            plan_label=safe_label,
            price_paise=amount_paise,
            duration_days=PLAN_DURATION_DAYS,
            plink_id=p_id,
            plink_url=p_url,
            raw=data,
        )

        sp_upsert_subscription_plan_meta(
            uid=uid,
            plan_id=plan_id,
            plan_label=safe_label,
            price_paise=amount_paise,
            duration_days=PLAN_DURATION_DAYS,
            plink_id=p_id,
            plink_url=p_url,
        )

        return p_url

    except Exception as ex:
        print("create_payment_link error:", ex)
        return None


# ---------------- STATS PAGE RENDER HELPER ----------------

async def render_stats_page(event, uid: int, ctx: Dict[str, Any]):
    """
    Render one page of stats (joins list) with pagination.
    Newest first (joined_at DESC).
    """
    link_id = ctx["link_id"]
    label = ctx["label"]
    since = ctx["since"]
    until = ctx["until"]
    offset = ctx["offset"]
    page_size = ctx["page_size"]
    total = ctx["total"]
    title = ctx["title"]
    link = ctx["link"]
    created_at = ctx.get("created_at")

    # India timezone (IST)
    IST = timezone(timedelta(hours=5, minutes=30))

    # Fetch page of join rows (newest first)
    rows = sp_fetch_joins_for_link(
        uid,
        link_id,
        since=since,
        until=until,
        offset=offset,
        limit=page_size,
    )

    # We need user client to resolve usernames
    try:
        uc = await get_user_client(uid)
    except Exception as ex:
        return await event.edit(
            f"❌ Error loading user session for stats:\n`{ex}`",
            parse_mode="md",
            buttons=None,
        )

    # Header
    lines: List[str] = []

    lines.append(f"📊 **{label} stats for:**")
    lines.append(f"`{title}`")
    lines.append("")
    lines.append(f"🔗 `{link}`")

    # Link created time -> show in IST
    if created_at:
        try:
            clean = str(created_at).replace("Z", "")
            dt = datetime.fromisoformat(clean)
            dt_ist = dt.astimezone(IST)
            created_str = dt_ist.strftime("%Y-%m-%d %H:%M IST")
        except Exception:
            created_str = str(created_at)
        lines.append(f"📅 Link created at: `{created_str}`")

    lines.append(f"👥 Total joins ({label}): `{total}`")
    lines.append("")

    if not rows:
        lines.append("ℹ️ No joins found in this range yet.")
    else:
        start_idx = offset + 1
        end_idx = offset + len(rows)
        lines.append(
            f"📌 Showing `{start_idx}-{end_idx}` of `{total}` joins (newest first):\n"
        )

        for i, r in enumerate(rows):
            joined_user_id = r.get("joined_user_id")

            # Default display name
            line_index = offset + i + 1
            display_name = f"User {line_index}"

            # Try to resolve real Telegram name (for real users)
            try:
                if joined_user_id:
                    ent = await uc.get_entity(int(joined_user_id))
                    display_name = title_of(ent)
            except Exception:
                # For IDs jinke liye entity nahi milti (dummy / deleted), fallback hi rahega
                pass

            lines.append(f"{line_index}. **{display_name}**")

    # Build pagination buttons
    buttons: List[List[Button]] = []
    nav_row: List[Button] = []

    # Prev button
    if offset > 0:
        nav_row.append(Button.inline("⬅️ Prev", data=b"stats_page:prev"))

    # Next button
    if offset + page_size < total:
        nav_row.append(Button.inline("➡️ Next", data=b"stats_page:next"))

    if nav_row:
        buttons.append(nav_row)

    # Close button
    buttons.append([Button.inline("✖ Close", data=b"stats_page:close")])

    await event.edit("\n".join(lines), parse_mode="md", buttons=buttons)


# ---------------- USER CLIENT HANDLING ----------------

async def get_user_client(uid: int) -> TelegramClient:
    """Return cached TelegramClient for this user (login session)."""
    client = USER_CLIENT_CACHE.get(uid)
    if client:
        try:
            if client.is_connected():
                return client
        except Exception:
            pass

    sess = sp_get_session(uid)
    if not sess:
        raise RuntimeError("No saved session. Use /login first.")

    local = os.path.join(SESSION_DIR, sess["session_file"])
    client = TelegramClient(local, API_ID, API_HASH)
    await safe_connect(client)

    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("Session exists but not authorized. /login again.")

    USER_CLIENT_CACHE[uid] = client
    return client


async def is_logged_in(uid: int) -> bool:
    try:
        _ = await get_user_client(uid)
        return True
    except Exception:
        return False


# ---------------- COMMANDS TEXT ----------------
def commands_text() -> str:
    lines = [
        "👋 Welcome to Join Counter Bot!",
        "",
        "ℹ️ This bot helps you **track how many users joined** via your private invite links.",
        "",
        "📌 If you log in and create invite links, the bot will:",
        "   • Save your private invite links in the database",
        "   • Track how many users joined via each link",
        "   • When you remove a link, all join data for that link will also be removed",
        "",
        "📖 Commands:",
        "",
        "▶️ /start — Show this help & all commands",
        "🎯 /start_demo — Try limited demo mode",
        "ℹ️ /help — Short usage guide",
        "🧷 /create_link — Create invite links for pinned private groups/channels",
        "📋 /links — List your active tracked invite links",
        "🗑️ /remove_link — Remove invite links (and their join data)",
        "",
        "📊 /stats — Select link & view all-time joins",
        "⏱️ /hour_status — Joins in last 1 hour",
        "📅 /today_status — Joins today",
        "📆 /week_status — Joins in last 7 days",
        "🗓️ /month_status — Joins in last 30 days",
        "📈 /year_status — Joins in last 365 days",
        "",
        "🔐 /login — Login your Telegram account",
        "🛑 /stoplogin — Cancel login process",
        "✅ /status — Check login status",
        "🚪 /logout — Delete session & stop tracking",
        "",
        "💳 /upgrade — View and buy premium plans",
        "📡 /upgrade_status — Check your plan status",
        "",
        "_Flow: `/login` → `/create_link` → share invite link → `/stats` for join tracking._",
    ]
    return "\n".join(lines)

# 👆 Button import already hoga, agar nahi hai to ye line ensure karo

# ... commands_text() function ke neeche yeh add karo:



# ---------------- /start /help /status /demo ----------------

@bot.on(events.NewMessage(pattern=r"^/start$"))
async def cmd_start(event):
    await event.respond(
        commands_text().replace("{BOT}", "Join Counter Bot"),
        parse_mode="html"
    )



@bot.on(events.NewMessage(pattern=r"^/help$"))
async def help_cmd(e):
    txt = (
        "ℹ️ **How to use this bot (simple flow)**\n\n"
        "1️⃣ Use /login and follow OTP / 2FA steps.\n"
        "2️⃣ In your Telegram app (same account), *pin* the private channel/group\n"
        "   where you want to generate an invite link.\n"
        "3️⃣ Come back here, type /create_link and select the chat using buttons.\n"
        "4️⃣ The invite link you get from the bot is the one you should share.\n"
        "   Everyone who joins using that invite link will be counted by the bot\n"
        "   (per link, not per channel only).\n"
        "5️⃣ Use /stats, /hour_status, /today_status, etc. — first you select which invite link,\n"
        "   then the bot will show join counts for that specific link.\n\n"
        "❗ When you use /remove_link and delete a link, **all join data for that link**\n"
        "   will also be permanently deleted from the database.\n"
    )
    await e.respond(txt, parse_mode="md")


@bot.on(events.NewMessage(pattern=r"^/start_demo$"))
async def start_demo_cmd(e):
    txt = (
        "🎉 **DEMO MODE ACTIVATED**\n\n"
        "This is a limited demo version of Join Counting Bot.\n"
        "You can:\n"
        "• Login\n"
        "• Create invite links\n"
        "• See stats\n\n"
        "⚠️ **Full features require a plan.**\n"
        "Use `/upgrade` to unlock unlimited counting + AutoApproval bot + Full suite."
    )
    await e.respond(txt, parse_mode="md")


@bot.on(events.NewMessage(pattern=r"^/status$"))
async def status_cmd(e):
    data = sp_get_session(e.sender_id)
    if not data:
        return await e.respond("🔴 Not logged in. Use **/login** first.", parse_mode="md")
    if not await is_logged_in(e.sender_id):
        return await e.respond(
            "🟠 Session found but not authorized locally. Please **/login** again.",
            parse_mode="md",
        )
    await e.respond(
        f"🟢 Logged In\n**Phone:** `{data['phone']}`\n`{data['session_file']}`",
        parse_mode="md",
    )


# ---------------- LOGIN / LOGOUT FLOW ----------------

@bot.on(events.NewMessage(pattern=r"^/login$"))
async def login_cmd(e):
    uid = e.sender_id
    if await is_logged_in(uid):
        return await e.respond(
            "✅ Already logged in.\nNow use `/create_link` to generate invite links.",
            parse_mode="md",
        )
    login_state[uid] = {"step": "phone", "phone": None}
    await e.respond(
        "📲 Send your phone number in this format:\n\n"
        "`+919876543210`\n\n"
        "You can cancel anytime with `/stoplogin`.",
        parse_mode="md",
    )


@bot.on(events.NewMessage(pattern=r"^/stoplogin$"))
async def stoplogin_cmd(e):
    uid = e.sender_id
    if uid in login_state:
        login_state.pop(uid, None)
        await e.respond("✖ Login cancelled. You can start again with `/login`.")
        return
    await e.respond("ℹ️ No login in progress.")


@bot.on(events.NewMessage)
async def login_flow(e):
    """Handles /login steps (phone → OTP → 2FA password)."""
    uid = e.sender_id
    msg = (e.raw_text or "").strip()
    if uid not in login_state:
        return
    st = login_state[uid]

    # STEP: phone number
    if st["step"] == "phone":
        if not PHONE_RE.match(msg):
            return await e.respond(
                "⚠️ Please send a valid number like `+919876543210`.",
                parse_mode="md",
            )
        phone = msg
        local = session_path(uid, phone)
        client = TelegramClient(local, API_ID, API_HASH)
        st["phone"] = phone
        try:
            await client.connect()
            if await client.is_user_authorized():
                me = await client.get_me()
                sp_upsert_session(uid, phone, os.path.basename(local))
                await e.respond(
                    f"✅ Already logged in as **{me.first_name}**.\n"
                    "Use `/create_link` to generate invite links.",
                    parse_mode="md",
                )
                login_state.pop(uid, None)
                return
            res = await client.send_code_request(phone)
            st["phone_code_hash"] = getattr(res, "phone_code_hash", None)
            await e.respond(
                "📩 Send OTP in this format: `HELLO123456` .\n\n"
                "You can also tap *Resend OTP* if needed.",
                parse_mode="md",
                buttons=[[Button.inline("🔁 Resend OTP", data=b"resend_otp")]],
            )
            st["step"] = "otp"
        except Exception as ex:
            await e.respond(
                f"❌ OTP send error: `{ex}`\nStart again with `/login`.",
                parse_mode="md",
            )
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
        return

    # STEP: OTP
    if st["step"] == "otp":
        m = OTP_RE.match(msg)
        if not m:
            return await e.respond(
                "⚠️ Send OTP in `HELLO123456` format.",
                parse_mode="md",
            )
        otp = m.group(1)
        phone = st.get("phone")
        if not phone:
            login_state.pop(uid, None)
            return await e.respond(
                "⚠️ Phone missing. Start `/login` again.",
                parse_mode="md",
            )
        code_hash = st.get("phone_code_hash")
        if not code_hash:
            login_state.pop(uid, None)
            return await e.respond(
                "❌ Code session expired. `/login` again.",
                parse_mode="md",
            )

        local = session_path(uid, phone)
        client = TelegramClient(local, API_ID, API_HASH)
        try:
            await client.connect()
            try:
                await client.sign_in(phone, otp, phone_code_hash=code_hash)
                me = await client.get_me()
                sp_upsert_session(uid, phone, os.path.basename(local))
                await e.respond(
                    f"✅ Logged in as **{me.first_name}**.\n"
                    "Now use `/create_link` to generate invite links.",
                    parse_mode="md",
                )
                login_state.pop(uid, None)
                return
            except errors.SessionPasswordNeededError:
                try:
                    pwd = await client(functions.account.GetPasswordRequest())
                    hint = getattr(pwd, "hint", "") or ""
                except Exception:
                    hint = ""
                st["step"] = "2fa"
                st["twofa_session_path"] = local
                msg_hint = f" (hint: `{hint}`)" if hint else ""
                await e.respond(
                    f"🔐 2FA enabled. Please enter your **Telegram password**{msg_hint}.\n\n"
                    "We do not store your password; it is only used once to finish login.",
                    parse_mode="md",
                )
                return
        except errors.PhoneCodeInvalidError:
            await e.respond("❌ Wrong OTP. `/login` try again.", parse_mode="md")
        except Exception as ex:
            await e.respond(
                f"❌ Login failed: `{ex}`\nStart again with `/login`.",
                parse_mode="md",
            )
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
        return

    # STEP: 2FA password
    if st["step"] == "2fa":
        password = msg
        phone = st.get("phone")
        local = st.get("twofa_session_path") or session_path(uid, phone or "")
        if not phone or not local:
            login_state.pop(uid, None)
            return await e.respond(
                "⚠️ Session expired. Start `/login` again.",
                parse_mode="md",
            )
        client = TelegramClient(local, API_ID, API_HASH)
        try:
            await client.connect()
            await client.sign_in(password=password)
            me = await client.get_me()
            sp_upsert_session(uid, phone, os.path.basename(local))
            await e.respond(
                f"✅ 2FA verified. Logged in as **{me.first_name}**.\n"
                "Now use `/create_link` to generate invite links.",
                parse_mode="md",
            )
        except errors.PasswordHashInvalidError:
            return await e.respond(
                "❌ Wrong password, try again (or `/login` to restart).",
                parse_mode="md",
            )
        except Exception as ex:
            await e.respond(
                f"❌ 2FA login failed: `{ex}`\nUse `/login` to reset.",
                parse_mode="md",
            )
        finally:
            login_state.pop(uid, None)
            try:
                await client.disconnect()
            except Exception:
                pass

# ---------- START MENU INLINE BUTTON HANDLERS ----------

@bot.on(events.CallbackQuery(pattern=b"menu_login"))
async def cb_menu_login(event):
    await event.answer()
    await event.respond("👤 Login ke liye command use karo:\n\n`/login`\n\nYahi se tum apna Telegram account connect karoge.", parse_mode="md")

@bot.on(events.CallbackQuery(pattern=b"menu_create_link"))
async def cb_menu_create_link(event):
    await event.answer()
    await event.respond("🧷 Naya invite link banane ke liye:\n\n`/create_link`\n\nYeh command sirf un groups/channels ke liye kaam karegi jahan tum admin ho.", parse_mode="md")

@bot.on(events.CallbackQuery(pattern=b"menu_links"))
async def cb_menu_links(event):
    await event.answer()
    await event.respond("📋 Apne sab active links dekhne ke liye:\n\n`/links`", parse_mode="md")

@bot.on(events.CallbackQuery(pattern=b"menu_stats"))
async def cb_menu_stats(event):
    await event.answer()
    await event.respond("📊 Joins ka stats dekhne ke liye:\n\n`/stats`\n\nYa jo bhi tumne stats wali command rakhi hai (agar naam alag hai to yahan update kar lena).", parse_mode="md")

@bot.on(events.CallbackQuery(pattern=b"menu_upgrade"))
async def cb_menu_upgrade(event):
    await event.answer()
    await event.respond("💳 Plan details aur payment link ke liye command use karo:\n\n`/upgrade`", parse_mode="md")

@bot.on(events.CallbackQuery(pattern=b"menu_upgrade_status"))
async def cb_menu_upgrade_status(event):
    await event.answer()
    await event.respond("📡 Apna current plan / expiry dekhne ke liye:\n\n`/upgrade_status`", parse_mode="md")


@bot.on(events.CallbackQuery(pattern=b"resend_otp"))
async def cb_resend_otp(event):
    uid = event.sender_id
    st = login_state.get(uid)
    if not st or not st.get("phone"):
        return await event.answer("No login in progress. Use /login.", alert=True)
    phone = st["phone"]
    local = session_path(uid, phone)
    client = TelegramClient(local, API_ID, API_HASH)
    try:
        await safe_connect(client)
        res = await client.send_code_request(phone)
        st["phone_code_hash"] = getattr(res, "phone_code_hash", None)
        await event.edit(
            "📩 OTP resent. Send like `HELLO123456`.\n\n"
            "If you still do not receive it, try again after a minute.",
            buttons=[[Button.inline("🔁 Resend OTP", data=b"resend_otp")]],
        )
    except Exception as ex:
        await event.edit(f"❌ Resend failed: `{ex}`\nStart `/login` again.")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


@bot.on(events.NewMessage(pattern=r"^/logout$"))
async def logout_cmd(e):
    if not await is_logged_in(e.sender_id):
        return await e.respond("ℹ️ Not logged in.", parse_mode="md")
    await e.respond(
        "⚠️ Are you sure you want to logout?\n\n"
        "Your session file and all tracked links will be removed.",
        parse_mode="md",
        buttons=[
            [
                Button.inline("✅ Yes, logout", data=b"logout_confirm"),
                Button.inline("✖ Cancel", data=b"logout_cancel"),
            ]
        ],
    )


@bot.on(events.CallbackQuery(pattern=b"logout_confirm"))
async def logout_confirm_cb(event):
    uid = event.sender_id
    data = sp_get_session(uid)
    if not data:
        return await event.edit("ℹ️ No session found.")
    # disconnect cached client
    client = USER_CLIENT_CACHE.pop(uid, None)
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass
    # delete session file
    path = os.path.join(SESSION_DIR, data["session_file"])
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception as ex:
            print("remove session file err:", ex)
    sp_delete_session(uid)
    await event.edit("👋 Logged out. You can `/login` again anytime.", buttons=None)


@bot.on(events.CallbackQuery(pattern=b"logout_cancel"))
async def logout_cancel_cb(event):
    await event.edit("✖ Logout cancelled. You are still logged in.", buttons=None)


# ---------------- CREATE LINK FLOW (GROUPS/CHANNELS ONLY) ----------------

@bot.on(events.NewMessage(pattern=r"^/create_link$"))
async def create_link_cmd(e):
    if not await is_logged_in(e.sender_id):
        return await e.respond("🔒 Please `/login` first.", parse_mode="md")
    # premium check
    if not await require_active_plan(e):
        return
    await e.respond(
        "🔗 **Create Invite Links (Private Groups/Channels)**\n\n"
        "1️⃣ In your Telegram app, *pin* the private channels/groups where you want invite links.\n"
        "2️⃣ Come back here and tap the button below.\n\n"
        "_Only groups/channels are shown (no 1-1 user chats). Max top 14 pinned chats._",
        parse_mode="md",
        buttons=[[Button.inline("📌 I have pinned the chats", data=b"pin_create_links")]],
    )


@bot.on(events.CallbackQuery(pattern=b"^pin_create_links$"))
async def cb_pin_create_links(event):
    uid = event.sender_id
    try:
        uc = await get_user_client(uid)
    except Exception as ex:
        return await event.edit(f"❌ {ex}\nUse `/login` again.", buttons=None)

    pairs = await top_dialog_pairs(uc, TOP_N)
    if not pairs:
        return await event.edit(
            "ℹ️ No eligible group/channel dialogs found.\n"
            "Pin your private channel/group and try again.",
            buttons=None,
        )

    select_state[uid] = {
        "mode": "create_links",
        "pairs": pairs,
        "selected": set(),  # indexes
    }

    await event.edit(
        "🔗 Select chats for which you want new invite links (multi-select).\n"
        "Tap numbers to toggle, then **Done**.\n\n" + numbered_list_from_pairs(pairs),
        buttons=multi_kb(len(pairs), set()),
    )


@bot.on(events.CallbackQuery(pattern=b"^msel:"))
async def cb_toggle(event):
    uid = event.sender_id
    st = select_state.get(uid)
    if not st:
        return await event.answer(
            "Session expired. Use /create_link or /remove_link again.",
            alert=True,
        )
    try:
        idx = int(event.data.decode().split(":")[1]) - 1
    except Exception:
        return await event.answer("Invalid.", alert=True)
    if idx < 0 or idx >= len(st["pairs"]):
        return await event.answer("Out of range.", alert=True)

    sel: Set[int] = st["selected"]
    if idx in sel:
        sel.remove(idx)
    else:
        sel.add(idx)

    mode = st["mode"]
    if mode == "create_links":
        header = "🔗 Select chats to create invite links"
    elif mode == "remove_links":
        header = "🗑️ Select links to remove"
    else:
        header = "Select items"

    await event.edit(
        f"{header} (multi-select). Tap numbers, then **Done**.\n\n"
        + numbered_list_from_pairs(st["pairs"]),
        buttons=multi_kb(len(st["pairs"]), sel),
    )


@bot.on(events.CallbackQuery(pattern=b"^msel_done$"))
async def cb_msel_done(event):
    uid = event.sender_id
    st = select_state.get(uid)
    if not st:
        return await event.answer("Session expired. Start again.", alert=True)

    mode = st["mode"]

    # --- create_links mode ---
    if mode == "create_links":
        chosen = sorted(st["selected"])
        if not chosen:
            select_state.pop(uid, None)
            return await event.edit(
                "ℹ️ No chats selected. Use `/create_link` again.",
                buttons=None,
            )

        try:
            uc = await get_user_client(uid)
        except Exception as ex:
            select_state.pop(uid, None)
            return await event.edit(f"❌ {ex}", buttons=None)

        created_lines = []
        for idx in chosen:
            chat_id, title = st["pairs"][idx]
            try:
                # Export private invite link
                res = await uc(
                    functions.messages.ExportChatInviteRequest(
                        peer=chat_id,
                        legacy_revoke_permanent=False,
                    )
                )
                link = res.link if hasattr(res, "link") else str(res)
                sp_save_invite_link(uid, int(chat_id), title, link)
                created_lines.append(f"• `{title}` → {link}")
            except Exception as ex:
                created_lines.append(f"• `{title}` → ❌ `{ex}`")

        select_state.pop(uid, None)
        txt = "✅ **Invite links created / saved:**\n\n" + "\n".join(created_lines)
        return await event.edit(txt, parse_mode="md", buttons=None)

    # --- remove_links mode → move to confirmation ---
    if mode == "remove_links":
        chosen = sorted(st["selected"])
        rows: List[dict] = st.get("rows", [])
        link_ids: List[int] = []
        selected_labels: List[str] = []

        for i in chosen:
            if 0 <= i < len(rows):
                rid = rows[i].get("id")
                if rid is not None:
                    link_ids.append(int(rid))
                    title = rows[i].get("chat_title") or f"id:{rows[i].get('chat_id')}"
                    link = rows[i].get("invite_link") or "-"
                    selected_labels.append(f"- `{title}` → {link}")

        if not link_ids:
            select_state.pop(uid, None)
            return await event.edit(
                "ℹ️ Nothing selected. Use `/remove_link` again.",
                buttons=None,
            )

        # save pending deletion state for confirmation
        select_state[uid] = {
            "mode": "remove_confirm",
            "link_ids": link_ids,
            "labels": selected_labels,
        }

        text = (
            "⚠️ **Delete confirmation**\n\n"
            "You are about to delete the following invite link(s):\n\n"
            + "\n".join(selected_labels)
            + "\n\n"
            "If you continue, **all join data associated with these invite links** "
            "will be permanently removed from the database.\n\n"
            "Are you sure you want to delete?"
        )

        return await event.edit(
            text,
            parse_mode="md",
            buttons=[
                [
                    Button.inline("✅ Yes, delete", data=b"rem_confirm"),
                    Button.inline("✖ Cancel", data=b"rem_cancel"),
                ]
            ],
        )

    # any other mode
    select_state.pop(uid, None)
    return await event.edit("ℹ️ Nothing to do.", buttons=None)


@bot.on(events.CallbackQuery(pattern=b"^rem_confirm$"))
async def cb_rem_confirm(event):
    uid = event.sender_id
    st = select_state.get(uid)
    if not st or st.get("mode") != "remove_confirm":
        return await event.answer("Nothing pending to delete.", alert=True)

    link_ids: List[int] = st.get("link_ids", [])
    count = len(link_ids)
    select_state.pop(uid, None)

    if not link_ids:
        return await event.edit("ℹ️ No links to delete.", buttons=None)

    sp_soft_delete_links(uid, link_ids)
    await event.edit(
        f"🗑️ Deleted **{count}** invite link(s) and all associated join data.",
        buttons=None,
    )


@bot.on(events.CallbackQuery(pattern=b"^rem_cancel$"))
async def cb_rem_cancel(event):
    uid = event.sender_id
    st = select_state.get(uid)
    if st and st.get("mode") == "remove_confirm":
        select_state.pop(uid, None)
    await event.edit("✖ Deletion cancelled. No links were removed.", buttons=None)


@bot.on(events.CallbackQuery(pattern=b"^msel_cancel$"))
async def cb_msel_cancel(event):
    select_state.pop(event.sender_id, None)
    await event.edit("✖ Selection cancelled.", buttons=None)


# ---------------- LIST / REMOVE LINKS ----------------

@bot.on(events.NewMessage(pattern=r"^/links$"))
async def links_cmd(e):
    uid = e.sender_id
    if not await is_logged_in(uid):
        return await e.respond("🔒 Please `/login` first.", parse_mode="md")
    if not await require_active_plan(e):
        return
    rows = sp_list_invite_links(uid)
    if not rows:
        return await e.respond(
            "ℹ️ No active invite links yet. Use `/create_link`.",
            parse_mode="md",
        )
    lines = ["📋 **Your tracked invite links:**", ""]
    for r in rows:
        title = r.get("chat_title") or f"id:{r.get('chat_id')}"
        link = r.get("invite_link") or "-"
        created = str(r.get("created_at") or "")[:19]
        lines.append(f"- `{title}`\n  {link}\n  _created: {created}_\n")

    await e.respond("\n".join(lines), parse_mode="md")


@bot.on(events.NewMessage(pattern=r"^/remove_link$"))
async def remove_link_cmd(e):
    uid = e.sender_id
    if not await is_logged_in(uid):
        return await e.respond("🔒 Please `/login` first.", parse_mode="md")
    if not await require_active_plan(e):
        return
    rows = sp_list_invite_links(uid)
    if not rows:
        return await e.respond("ℹ️ No active links to remove.", parse_mode="md")
    pairs = []
    for i, r in enumerate(rows):
        title = r.get("chat_title") or f"id:{r.get('chat_id')}"
        link = r.get("invite_link") or "-"
        label = f"{title} — {link}"
        pairs.append((i, label))
    select_state[uid] = {
        "mode": "remove_links",
        "pairs": pairs,
        "selected": set(),
        "rows": rows,
    }
    await e.respond(
        "🗑️ **Remove Links** — select links to remove (multi-select) then **Done**.\n\n"
        "Note: Deleting a link will also permanently delete all join data stored for that link.\n\n"
        + numbered_list_from_pairs(pairs),
        buttons=multi_kb(len(pairs), set()),
    )


# ---------------- SYNC IMPORTERS → JOINS TABLE ----------------

async def sync_importers_to_db(uid: int):
    """
    For each invite_link: fetch Telegram invite importers and refresh joins table.
    Uses GetChatInviteImporters with proper peer + link hash.
    """
    rows = sp_list_invite_links(uid)
    if not rows:
        return

    try:
        uc = await get_user_client(uid)
    except Exception as ex:
        print("sync_importers_to_db error (get_user_client):", ex)
        return

    for r in rows:
        invite_link_id = int(r["id"])
        chat_id = int(r["chat_id"])
        full_link = r["invite_link"]

        # 1) extract hash part
        link_part = full_link.rsplit("/", 1)[-1]
        link_part = link_part.lstrip("+").replace("joinchat/", "")

        try:
            # 2) convert chat_id to InputPeer
            peer = await uc.get_input_entity(chat_id)
        except Exception as ex:
            print(f"sync_importers_to_db get_input_entity error for {chat_id}:", ex)
            continue

        try:
            # 3) GetChatInviteImporters call
            result = await uc(
                functions.messages.GetChatInviteImportersRequest(
                    peer=peer,
                    link=link_part,
                    offset_date=None,
                    offset_user=tl_types.InputUserEmpty(),
                    limit=1000,
                    requested=False,
                )
            )
        except Exception as ex:
            print("GetChatInviteImporters error:", ex)
            continue

        importers = getattr(result, "importers", []) or []
        join_rows: List[dict] = []

        for imp in importers:
            try:
                user_id = int(getattr(imp, "user_id", 0) or 0)
                if not user_id:
                    continue

                # -------- NEW: resolve joined_username --------
                try:
                    ent = await uc.get_entity(user_id)

                    if getattr(ent, "username", None):
                        username = "@" + ent.username
                    elif getattr(ent, "first_name", None) or getattr(ent, "last_name", None):
                        username = f"{(ent.first_name or '')} {(ent.last_name or '')}".strip()
                    else:
                        username = f"id:{user_id}"
                except Exception:
                    # if we can't fetch entity, just store id
                    username = f"id:{user_id}"

                # joined_at time
                join_date = getattr(imp, "date", None)
                if isinstance(join_date, datetime):
                    joined_at = join_date.astimezone(timezone.utc)
                else:
                    joined_at = datetime.now(timezone.utc)

                # row to insert
                join_rows.append(
                    {
                        "user_id": uid,  # owner of this link
                        "chat_id": chat_id,
                        "invite_link_id": invite_link_id,
                        "joined_user_id": user_id,
                        "joined_username": username,  # <-- NEW COLUMN
                        "joined_at": joined_at.isoformat(),
                    }
                )

            except Exception as ex:
                print("importer parse err:", ex)
                continue

        sp_replace_joins_for_link(uid, invite_link_id, join_rows)


# ---------------- STATS COMMANDS (PER LINK, WITH SELECTION) ----------------

async def _stats_template(
    e,
    label: str,
    since: Optional[datetime],
    until: Optional[datetime] = None,
):
    """Ask user which invite_link they want stats for."""
    uid = e.sender_id
    if not await is_logged_in(uid):
        return await e.respond("🔒 Please `/login` first.", parse_mode="md")
    if not await require_active_plan(e):
        return

    rows = sp_list_invite_links(uid)
    if not rows:
        return await e.respond(
            "ℹ️ No active invite links yet. Use `/create_link` first.",
            parse_mode="md",
        )

    # save context for callback
    stats_state[uid] = {
        "label": label,
        "since": since,
        "until": until,
    }

    btn_rows: List[List[Button]] = []
    for r in rows[:10]:
        title = r.get("chat_title") or f"id:{r.get('chat_id')}"
        short = (title[:40] + "…") if len(title) > 40 else title
        data = f"stats:{r['id']}".encode()
        btn_rows.append([Button.inline(short, data=data)])

    btn_rows.append([Button.inline("✖ Cancel", data=b"stats_cancel")])

    await e.respond(
        "📊 **Select which invite link you want stats for:**",
        parse_mode="md",
        buttons=btn_rows,
    )


@bot.on(events.CallbackQuery(pattern=b"^stats:"))
async def cb_stats_link(event):
    """User selected a specific invite_link for stats (first page)."""
    uid = event.sender_id
    st = stats_state.get(uid)
    if not st:
        return await event.answer("Session expired. Run /stats again.", alert=True)

    try:
        link_id = int(event.data.decode().split(":")[1])
    except Exception:
        return await event.answer("Invalid selection.", alert=True)

    label = st["label"]
    since = st["since"]
    until = st["until"]
    stats_state.pop(uid, None)

    # Sync latest importers from Telegram to DB
    await event.edit(
        "⏳ Syncing join data from Telegram for this link...\n"
        "Please wait 2–3 seconds.",
        buttons=None,
    )
    await sync_importers_to_db(uid)

    # Total joins for this link
    total = sp_count_joins_for_link(uid, link_id, since=since, until=until)

    # Fetch link info once and store in context
    rows = sp_list_invite_links(uid)
    chosen = None
    for r in rows:
        if int(r["id"]) == link_id:
            chosen = r
            break

    if chosen:
        title = chosen.get("chat_title") or f"id:{chosen.get('chat_id')}"
        link = chosen.get("invite_link") or "-"
        created_at = chosen.get("created_at")
    else:
        title = "(link not found / removed)"
        link = "-"
        created_at = None

    # Normal page size
    page_size = 15

    msg = await event.get_message()
    key = (uid, msg.id)
    stats_pages[key] = {
        "link_id": link_id,
        "label": label,
        "since": since,
        "until": until,
        "offset": 0,
        "page_size": page_size,
        "total": total,
        "title": title,
        "link": link,
        "created_at": created_at,
    }

    # Render first page
    await render_stats_page(event, uid, stats_pages[key])


@bot.on(events.CallbackQuery(pattern=b"^stats_page:"))
async def cb_stats_page(event):
    """
    Handle pagination buttons for stats user list.
    """
    uid = event.sender_id
    try:
        action = event.data.decode().split(":")[1]
    except Exception:
        return await event.answer("Invalid action.", alert=True)

    msg = await event.get_message()
    key = (uid, msg.id)
    ctx = stats_pages.get(key)

    if not ctx:
        return await event.answer("Session expired. Run /stats again.", alert=True)

    if action == "close":
        stats_pages.pop(key, None)
        await event.edit("✖ Stats closed.", buttons=None)
        return

    if action == "next":
        ctx["offset"] += ctx["page_size"]
    elif action == "prev":
        ctx["offset"] -= ctx["page_size"]

    await render_stats_page(event, uid, ctx)


@bot.on(events.CallbackQuery(pattern=b"^stats_cancel$"))
async def cb_stats_cancel(event):
    stats_state.pop(event.sender_id, None)
    await event.edit("✖ Stats selection cancelled.", buttons=None)


@bot.on(events.NewMessage(pattern=r"^/stats$"))
async def stats_all_cmd(e):
    await _stats_template(e, "All time", since=None)


@bot.on(events.NewMessage(pattern=r"^/hour_status$"))
async def stats_hour_cmd(e):
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=1)
    await _stats_template(e, "Last 1 hour", since=start)


@bot.on(events.NewMessage(pattern=r"^/today_status$"))
async def stats_today_cmd(e):
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    await _stats_template(e, "Today", since=start)


@bot.on(events.NewMessage(pattern=r"^/week_status$"))
async def stats_week_cmd(e):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=7)
    await _stats_template(e, "Last 7 days", since=start)


@bot.on(events.NewMessage(pattern=r"^/month_status$"))
async def stats_month_cmd(e):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)
    await _stats_template(e, "Last 30 days", since=start)


@bot.on(events.NewMessage(pattern=r"^/year_status$"))
async def stats_year_cmd(e):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=365)
    await _stats_template(e, "Last 365 days", since=start)


# ---------------- UPGRADE & PLAN CALLBACKS ----------------

# ---------------- UPGRADE & PLAN CALLBACKS ----------------
#  GetAIPilot plans UI + Razorpay Pay/Verify flow
#  (AutoForward bot = main product, Join + AutoApprove free with any plan)
# ---------------------------------------------------------

PLANS_HEADER_TEXT = """
✨ **GetAIPilot — Plans**

🎁 **Free with any plan:**
• Auto approval bot — @Getai_approvedbot
• Join Tracking bot — @Getai_joincountbot


💠 **BASIC — ₹699 / 30 days**
• Unlimited auto-forwarding between your selected chats
• Choose sources & targets easily
• Start/Stop forwarding anytime
• Manage mappings (remove sources/targets)
• ⚠️ High-size file sending is NOT included

────────────────────────

⚡️ **PRO — ₹1499 / 30 days**
• Everything in BASIC
• Text replacement filters (@old → @new)
• Show / delete one / delete all filters
• Custom delay control between forwards
• ✅ High-size media & file sending supported

────────────────────────

💎 **PREMIUM — ₹2499 / 30 days**
• Everything in PRO
• Add custom text at the START of every forward
• Add custom text at the END of every forward
• Blacklist words (auto-remove from text)
• ✅ High-size media & file sending supported

_Every payment extends your expiry by +30 days._
"""

BASIC_PLAN_TEXT = """
💠 **BASIC — ₹699 / 30 days**

🎁 **Free with this plan:**
• Auto approval bot — @Getai_approvedbot
• Join Tracking bot — @Getai_joincountbot

**Features:**
• Unlimited auto-forwarding between your selected chats
• Choose sources & targets easily
• Start/Stop forwarding anytime
• Manage mappings (remove sources/targets)
• ⚠️ High-size file sending is NOT included

**Commands in this plan:**
• `/incoming` — Select source chats
• `/outgoing` — Select target chats
• `/work` — Start auto-forward
• `/stop` — Stop auto-forward
• `/remove_incoming` — Remove saved sources
• `/remove_outgoing` — Remove saved targets

⚠️ High-size files not supported

_Validity: 30 days • Every renewal adds +30 days._
"""

PRO_PLAN_TEXT = """
⚡️ **PRO — ₹1499 / 30 days**

🎁 **Free with this plan:**
• Auto approval bot — @Getai_approvedbot
• Join Tracking bot — @Getai_joincountbot

**Features:**
• Everything in BASIC
• Text replacement filters (@old → @new)
• Show / delete one / delete all filters
• Custom delay control between forwards
• ✅ High-size media & file sending supported

**Commands in this plan:**
• `/incoming`, `/outgoing`, `/work`, `/stop`, `/remove_incoming`, `/remove_outgoing`
• `/addfilter` — Replace @left with @right
• `/showfilter` — List filters
• `/removefilter` — Delete a specific filter
• `/deleteallfilters` — Clear all filters
• `/delay` — Set delay (0–999s)

✅ High-size files supported

🎁 **Free with this plan:**
• Auto approval bot — @Getai_approvedbot
• Join Tracking bot — @Getai_joincountbot

_Validity: 30 days • Every renewal adds +30 days._
"""

PREMIUM_PLAN_TEXT = """
💎 **PREMIUM — ₹2499 / 30 days**

🎁 **Free with this plan:**
• Auto approval bot — @Getai_approvedbot
• Join Tracking bot — @Getai_joincountbot

**Features:**
• Everything in PRO
• Add custom text at the START of every forward
• Add custom text at the END of every forward
• Blacklist words (auto-remove from text)
• ✅ High-size media & file sending supported

**Commands in this plan:**
• `/incoming`, `/outgoing`, `/work`, `/stop`, `/remove_incoming`, `/remove_outgoing`
• `/addfilter`, `/showfilter`, `/removefilter`, `/deleteallfilters`, `/delay`
• `/start_text` — Set prefix
• `/end_text` — Set suffix
• `/remove_text` — Clear prefix/suffix
• `/blacklist_word` — Add a word to block
• `/remove_blacklist` — Remove blocked words

✅ High-size files supported

_Validity: 30 days • Every renewal adds +30 days._
"""


def sp_get_latest_payment_link(uid: int, plan_id: str) -> Optional[dict]:
    """
    Latest payment link row for this user+plan (from user_payment_links).
    """
    try:
        res = (
            supabase.table("user_payment_links")
            .select("*")
            .eq("user_id", uid)
            .eq("plan_id", plan_id)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as ex:
        print("sp_get_latest_payment_link error:", ex)
        return None


def sp_apply_successful_payment(uid: int, payment_row: dict) -> datetime:
    """
    Mark payment row as paid + extend/activate subscription in user_subscriptions.
    Returns new expiry datetime (UTC).
    """
    now = datetime.now(timezone.utc)
    plan_id = payment_row.get("plan_id") or "unknown"
    plan_label = payment_row.get("plan_label") or plan_id.upper()
    duration_days = int(payment_row.get("duration_days") or PLAN_DURATION_DAYS)

    # Base date = existing active expiry (if any) else now
    sub = sp_get_subscription(uid)
    if sub and is_plan_active(sub):
        try:
            base = datetime.fromisoformat(str(sub["expires_at"]).replace("Z", "")).astimezone(timezone.utc)
        except Exception:
            base = now
    else:
        base = now

    new_exp = base + timedelta(days=duration_days)

    payload = {
        "user_id": uid,
        "plan_id": plan_id,
        "plan_label": plan_label,
        "expires_at": new_exp.isoformat(),
        "updated_at": now.isoformat(),
    }

    try:
        supabase.table("user_subscriptions").upsert(payload, on_conflict="user_id").execute()
    except Exception as ex:
        print("sp_apply_successful_payment upsert sub error:", ex)

    # Mark payment row as paid
    try:
        supabase.table("user_payment_links").update({"status": "paid"}).eq("paymentlink_id", payment_row["paymentlink_id"]).execute()
    except Exception as ex:
        print("sp_apply_successful_payment update payment error:", ex)

    return new_exp


async def _show_plans_root(event_or_msg):
    """
    Common helper: show main plans list + 3 'View' buttons.
    Also shows active plan status on top if any.
    """
    uid = event_or_msg.sender_id
    sub = sp_get_subscription(uid)
    if sub and is_plan_active(sub):
        status = format_plan_status(uid)
        txt = status + "\n\n" + PLANS_HEADER_TEXT
    else:
        txt = PLANS_HEADER_TEXT

    buttons = [
        [Button.inline("💠 View BASIC", data=b"plans_basic")],
        [Button.inline("⚡ View PRO", data=b"plans_pro")],
        [Button.inline("💎 View PREMIUM", data=b"plans_premium")],
    ]

    # event_or_msg can be NewMessage event or CallbackQuery
    if isinstance(event_or_msg, events.CallbackQuery.Event):
        await event_or_msg.edit(txt, parse_mode="md", buttons=buttons)
    else:
        await event_or_msg.respond(txt, parse_mode="md", buttons=buttons)


@bot.on(events.NewMessage(pattern=r"^/upgrade$"))
async def upgrade_cmd(e):
    """
    /upgrade -> show GetAIPilot plans + "View BASIC/PRO/PREMIUM" buttons
    (same style as autoforward bot).
    """
    await _show_plans_root(e)


@bot.on(events.CallbackQuery(pattern=b"plans_root"))
async def cb_plans_root(event):
    await _show_plans_root(event)


@bot.on(events.CallbackQuery(pattern=b"plans_basic"))
async def cb_plans_basic(event):
    buttons = [
        [Button.inline("⬅️ Back to Plans", data=b"plans_root")],
        [Button.inline("💳 Buy ₹699 / 30 days", data=b"buy_basic")],
    ]
    await event.edit(BASIC_PLAN_TEXT, parse_mode="md", buttons=buttons)


@bot.on(events.CallbackQuery(pattern=b"plans_pro"))
async def cb_plans_pro(event):
    buttons = [
        [Button.inline("⬅️ Back to Plans", data=b"plans_root")],
        [Button.inline("💳 Buy ₹1499 / 30 days", data=b"buy_pro")],
    ]
    await event.edit(PRO_PLAN_TEXT, parse_mode="md", buttons=buttons)


@bot.on(events.CallbackQuery(pattern=b"plans_premium"))
async def cb_plans_premium(event):
    buttons = [
        [Button.inline("⬅️ Back to Plans", data=b"plans_root")],
        [Button.inline("💳 Buy ₹2499 / 30 days", data=b"buy_premium")],
    ]
    await event.edit(PREMIUM_PLAN_TEXT, parse_mode="md", buttons=buttons)

async def _show_payment_created(event, plan_id: str, plan_label: str, amount_paise: int):
    uid = event.sender_id

    # 1) Pehle latest payment link dekho
    existing = sp_get_latest_payment_link(uid, plan_id)
    if existing and (existing.get("status") or "").lower() == "created":
        # Purana link hi use kar lo
        link_url = existing.get("paymentlink_url")
    else:
        # 2) Naya link banao
        link_url = await create_payment_link(uid, amount_paise, plan_id, plan_label)

    if not link_url:
        # Yaha aane ka matlab: Razorpay ne error diya (jaise Too many requests)
        return await event.edit(
            "❌ Unable to create payment link right now.\n"
            "❌ Unable to create payment link.\nPlease try again in a minute."

            "⏳ sorry for this problem please try again.",
            parse_mode="md",
            buttons=[[Button.inline("⬅️ Back to Plans", data=b"plans_root")]],
        )

    txt = (
        "🔗 **Payment Link Created**\n"
        f"Plan: {plan_label} (₹{amount_paise/100:.0f} / 30 days)\n\n"
        "After payment, press **Verify**."
    )

    buttons = [
        [Button.url("💳 Pay Now", link_url)],
        [Button.inline("✅ I have paid — Verify", data=f"verify_{plan_id}".encode())],
        [Button.inline("⬅️ Back to Plans", data=b"plans_root")],
    ]

    await event.edit(txt, parse_mode="md", buttons=buttons)


@bot.on(events.CallbackQuery(pattern=b"buy_basic"))
async def cb_buy_basic(event):
    await _show_payment_created(event, "basic", " BASIC", BASIC_PRICE_PAISE)


@bot.on(events.CallbackQuery(pattern=b"buy_pro"))
async def cb_buy_pro(event):
    await _show_payment_created(event, "pro", " PRO", PRO_PRICE_PAISE)


@bot.on(events.CallbackQuery(pattern=b"buy_premium"))
async def cb_buy_premium(event):
    await _show_payment_created(event, "premium", " PREMIUM", PREMIUM_PRICE_PAISE)


async def _handle_verify(event, plan_id: str):
    """
    Callback for "I have paid — Verify" buttons.
    Razorpay HTTP API se payment_link status check karta hai
    (AutoForward bot ke flow ke jaisa).
    """
    uid = event.sender_id

    row = sp_get_latest_payment_link(uid, plan_id)
    if not row:
        return await event.edit(
            "❌ No recent payment link found for this plan.\n"
            "Use /upgrade → Buy again.",
            parse_mode="md",
            buttons=[[Button.inline("⬅️ Back to Plans", data=b"plans_root")]],
        )

    # Agar pehle se paid mark hai to direct status dikha do
    if (row.get("status") or "").lower() == "paid":
        msg = "✅ Payment already verified.\n\n" + format_plan_status(uid)
        return await event.edit(
            msg,
            parse_mode="md",
            buttons=[[Button.inline("⬅️ Back to Plans", data=b"plans_root")]],
        )

    plink_id = row.get("paymentlink_id")
    if not plink_id:
        return await event.edit(
            "❌ This payment link record is missing an id.\n"
            "Please create a new one via /upgrade.",
            parse_mode="md",
            buttons=[[Button.inline("⬅️ Back to Plans", data=b"plans_root")]],
        )

    # ---- Razorpay payment_link fetch (HTTP) ----
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                f"{RP_BASE}/payment_links/{plink_id}",
                headers=_rp_auth_headers(),
            ) as resp:
                if resp.status >= 300:
                    try:
                        txt = await resp.text()
                    except Exception:
                        txt = "unknown error body"
                    return await event.edit(
                        "❌ Failed to verify payment (HTTP error).\n"
                        f"Status: `{resp.status}`\n"
                        f"Body: `{txt}`",
                        parse_mode="md",
                        buttons=[[Button.inline("⬅️ Back to Plans", data=b"plans_root")]],
                    )
                info = await resp.json()
    except Exception as ex:
        return await event.edit(
            f"❌ Failed to verify payment:\n`{ex}`",
            parse_mode="md",
            buttons=[[Button.inline("⬅️ Back to Plans", data=b"plans_root")]],
        )

    status = (info or {}).get("status", "").lower()

    if status != "paid":
        # Not paid yet
        purl = row.get("paymentlink_url")
        buttons: List[List[Button]] = []
        if purl:
            buttons.append([Button.url("💳 Pay Now", purl)])
        buttons.append([Button.inline("⬅️ Back to Plans", data=b"plans_root")])

        return await event.edit(
            f"⚠️ Payment is not completed yet.\n"
            f"Current status: `{status or 'unknown'}`.\n\n"
            "Please finish payment using *Pay Now*, then press **Verify** again.",
            parse_mode="md",
            buttons=buttons,
        )

    # ---- Mark paid + extend subscription ----
    new_exp = sp_apply_successful_payment(uid, row)
    new_exp_str = new_exp.strftime("%Y-%m-%d %H:%M UTC")

    msg = (
        "✅ **Payment verified successfully!**\n\n"
        f"Your plan is now active until: `{new_exp_str}`\n\n"
        "You can now use **GetAIPilot** (autoforward bot) aur\n"
        "**Join Tracking bot + Auto approval bot** is subscription ke sath."
    )

    await event.edit(
        msg,
        parse_mode="md",
        buttons=[[Button.inline("⬅️ Back to Plans", data=b"plans_root")]],
    )


@bot.on(events.CallbackQuery(pattern=b"verify_basic"))
async def cb_verify_basic(event):
    await _handle_verify(event, "basic")


@bot.on(events.CallbackQuery(pattern=b"verify_pro"))
async def cb_verify_pro(event):
    await _handle_verify(event, "pro")


@bot.on(events.CallbackQuery(pattern=b"verify_premium"))
async def cb_verify_premium(event):
    await _handle_verify(event, "premium")


@bot.on(events.NewMessage(pattern=r"^/upgrade_status$"))
async def upgrade_status_cmd(e):
    """
    Show current subscription status (shared across GetAIPilot + all helper bots).
    """
    uid = e.sender_id
    await e.respond(format_plan_status(uid), parse_mode="md")

@bot.on(events.CallbackQuery(pattern=b"plan_basic"))
async def cb_plan_basic(event):
    uid = event.sender_id
    amt = BASIC_PRICE_PAISE
    link = await create_payment_link(uid, amt, "basic", "BASIC")
    if not link:
        return await event.edit("❌ Unable to create payment link. Please try again later.", buttons=None)
    await event.edit(f"🧾 **Basic Plan**\n\nPay using link below:\n{link}", buttons=None)


@bot.on(events.CallbackQuery(pattern=b"plan_pro"))
async def cb_plan_pro(event):
    uid = event.sender_id
    amt = PRO_PRICE_PAISE
    link = await create_payment_link(uid, amt, "pro", "PRO")
    if not link:
        return await event.edit("❌ Unable to create payment link. Please try again later.", buttons=None)
    await event.edit(f"🧾 **Pro Plan**\n\nPay using link below:\n{link}", buttons=None)


@bot.on(events.CallbackQuery(pattern=b"plan_premium"))
async def cb_plan_premium(event):
    uid = event.sender_id
    amt = PREMIUM_PRICE_PAISE
    link = await create_payment_link(uid, amt, "premium", "PREMIUM")
    if not link:
        return await event.edit("❌ Unable to create payment link. Please try again later.", buttons=None)
    await event.edit(f"🧾 **Premium Plan**\n\nPay using link below:\n{link}", buttons=None)

# ---------------- BOT PROFILE (DESCRIPTION + COMMANDS) ----------------

async def setup_bot_profile():
    """Set bot description + commands list."""
    try:
        me = await bot.get_me()
        await bot(
            functions.bots.SetBotInfoRequest(
                bot=me,
                lang_code="en",
                name="Join Counter Bot",
                about="Track private invite-link joins for your groups/channels.",
                description=(
                    "Log in with your own account, create unique private invite links for your "
                    "groups/channels, and track how many users joined via each link."
                ),
            )
        )
        cmds = [
            ("start", "Show help & all commands"),
            ("help", "Short usage guide"),
            ("start_demo", "Try limited demo mode"),
            ("login", "Login your Telegram account"),
            ("status", "Check login status"),
            ("logout", "Delete session & stop tracking"),
            ("create_link", "Create invite links for pinned chats"),
            ("links", "List your invite links"),
            ("remove_link", "Remove links & join data (with confirmation)"),
            ("stats", "Select link & show total joins"),
            ("hour_status", "Select link & joins last 1 hour"),
            ("today_status", "Select link & joins today"),
            ("week_status", "Select link & joins last 7 days"),
            ("month_status", "Select link & joins last 30 days"),
            ("year_status", "Select link & joins last 365 days"),
            ("upgrade", "View subscription plans"),
            ("upgrade_status", "Check your current plan status"),
        ]
        await bot(
            functions.bots.SetBotCommandsRequest(
                scope=types.BotCommandScopeDefault(),
                lang_code="en",
                commands=[types.BotCommand(c, d) for c, d in cmds],
            )
        )
    except Exception as e:
        print("Profile/commands set error:", e)


# ---------- DEBUG IMPORTERS (optional manual test) ----------

@bot.on(events.NewMessage(pattern=r"^/debug_importers$"))
async def debug_importers_cmd(e):
    uid = e.sender_id

    try:
        uc = await get_user_client(uid)
    except Exception as ex:
        return await e.respond(
            f"⚠️ Not logged in (user client error): `{ex}`",
            parse_mode="md",
        )

    try:
        res = (
            supabase.table("invite_links")
            .select("*")
            .eq("user_id", uid)
            .eq("is_active", True)
            .limit(1)
            .execute()
        )
    except Exception as ex:
        return await e.respond(
            f"❌ DB error reading invite_links:\n`{ex}`",
            parse_mode="md",
        )

    if not res.data:
        return await e.respond(
            "ℹ️ No active invite_links found in DB for this user.",
            parse_mode="md",
        )

    row = res.data[0]
    chat_id = int(row["chat_id"])
    full_link = row["invite_link"]

    link_part = full_link.rsplit("/", 1)[-1]
    link_part = link_part.lstrip("+").replace("joinchat/", "")

    try:
        peer = await uc.get_input_entity(chat_id)
    except Exception as ex:
        return await e.respond(
            f"❌ get_input_entity failed for chat_id {chat_id}:\n`{ex}`",
            parse_mode="md",
        )

    await e.respond(
        "🔍 Testing importers for:\n"
        f"- chat_id: `{chat_id}`\n"
        f"- link: `{link_part}`\n\n"
        "Please wait a few seconds...",
        parse_mode="md",
    )

    try:
        result = await uc(
            functions.messages.GetChatInviteImportersRequest(
                peer=peer,
                link=link_part,
                offset_date=None,
                offset_user=tl_types.InputUserEmpty(),
                limit=1000,
                requested=False,
            )
        )
    except Exception as ex:
        return await e.respond(
            f"❌ Telegram API error:\n`{ex}`",
            parse_mode="md",
        )

    importers = getattr(result, "importers", []) or []
    total = len(importers)

    if not total:
        return await e.respond(
            "✅ API call succeeded but **0 importers** were returned.\n\n"
            "Possible reasons:\n"
            "• No one has joined using this specific invite link yet\n"
            "• Users joined using a different link or via a public @username\n"
            "• Your logged-in account is not admin in this channel/group\n",
            parse_mode="md",
        )

    lines = [f"✅ `GetChatInviteImporters` OK — found **{total}** user(s).\n"]
    for imp in importers[:5]:
        dt = getattr(imp, "date", None)
        dt_str = dt.isoformat() if dt else "?"
        lines.append(f"- joined_user_id `{imp.user_id}` at `{dt_str}`")

    await e.respond("\n".join(lines), parse_mode="md")


# ---------------- RUN ----------------

if __name__ == "__main__":
    print("🤖 Join Counter Bot ready!")
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(setup_bot_profile())
    except Exception as e:
        print("setup_bot_profile error:", e)
    bot.run_until_disconnected()
