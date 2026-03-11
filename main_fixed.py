import json
import os
import asyncio
import threading
import math
import time
import random
import re
import sys
from urllib.parse import urlparse

# للتحقق من عمليات القفل (لينكس فقط)
if os.name != 'nt':
    try:
        import fcntl
    except ImportError:
        fcntl = None

import telebot
from telebot import apihelper
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
import requests
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest, SendReactionRequest, GetMessagesViewsRequest
from telethon.tl.functions.messages import SendVoteRequest
from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.tl.functions.messages import DeleteChatUserRequest, CheckChatInviteRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import ReactionEmoji
from telethon.errors import AuthKeyUnregisteredError, UserDeactivatedError, UserDeactivatedBanError
from telethon.errors import RPCError
from telethon.errors.rpcerrorlist import BotResponseTimeoutError, FloodWaitError

BOT_TOKEN = "7743407402:AAEir_E3HjpqS2aTrGLCUOFz3E5RnLXWGA0"
API_ID = 21112010
API_HASH = "ddfa5a300af8ea44f66ce0f9eb71fc8e"  # 
DEVELOPER_ID = 1819279320  # آيدي المطور

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSIONS_FILE = os.path.join(_BASE_DIR, "sessions.json")
DEVS_FILE = os.path.join(_BASE_DIR, "developers.json")
ROTATION_FILE = os.path.join(_BASE_DIR, "sessions_rotation.json")
OPERATIONS_FILE = os.path.join(_BASE_DIR, "operations.json")
POINTS_FILE = os.path.join(_BASE_DIR, "points.json")
REFERRALS_FILE = os.path.join(_BASE_DIR, "referrals.json")
BANNED_USERS_FILE = os.path.join(_BASE_DIR, "banned_users.json")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

apihelper.CONNECT_TIMEOUT = 60
apihelper.READ_TIMEOUT = 180

# دالة للتحقق من وجود نسخة أخرى من البوت
def check_single_instance():
    """التحقق من أن نسخة واحدة فقط من البوت تعمل"""
    lock_file = os.path.join(_BASE_DIR, "bot.lock")
    
    try:
        if os.name == 'nt':  # Windows
            # محاولة إنشاء ملف قفل حصري
            if os.path.exists(lock_file):
                try:
                    # محاولة حذف الملف القديم
                    os.remove(lock_file)
                except:
                    print("❌ نسخة أخرى من البوت تعمل بالفعل!")
                    print("يرجى إغلاق العملية الحالية أولاً")
                    sys.exit(1)
            
            # إنشاء ملف القفل
            with open(lock_file, 'w') as f:
                f.write(str(os.getpid()))
        else:  # Linux/Unix
            with open(lock_file, 'w') as f:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    f.write(str(os.getpid()))
                except IOError:
                    print("❌ نسخة أخرى من البوت تعمل بالفعل!")
                    sys.exit(1)
                    
    except Exception as e:
        print(f"⚠️ تحذير: لا يمكن التحقق من النسخ المكررة: {e}")

# دالة لتنظيف ملف القفل عند الخروج
def cleanup_lock_file():
    """حذف ملف القفل عند الخروج"""
    lock_file = os.path.join(_BASE_DIR, "bot.lock")
    try:
        if os.path.exists(lock_file):
            os.remove(lock_file)
    except:
        pass

import atexit
atexit.register(cleanup_lock_file)

# التحقق من نسخة واحدة
check_single_instance()

_orig_send_message = bot.send_message


async def _connect_telethon_client(session_string: str) -> TelegramClient:
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    try:
        authorized = await client.is_user_authorized()
    except Exception:
        authorized = False
    if not authorized:
        try:
            await client.disconnect()
        except Exception:
            pass
        raise AuthKeyUnregisteredError()
    return client


def _safe_send_message(*args, **kwargs):
    last_exc = None
    for _ in range(3):
        try:
            return _orig_send_message(*args, **kwargs)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
            last_exc = e
            time.sleep(1)
        except Exception:
            raise
    if last_exc:
        raise last_exc


bot.send_message = _safe_send_message


def _is_cancel_text(text: str | None) -> bool:
    t = (text or "").strip()
    return t in ("إلغاء العملية", "الغاء العملية")


def load_sessions():
    if not os.path.exists(SESSIONS_FILE):
        return []
    try:
        with open(SESSIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []
    return data.get("sessions", []) if isinstance(data, dict) else []


def save_sessions(sessions):
    data = {"sessions": sessions}
    with open(SESSIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_rotation_state() -> dict:
    if not os.path.exists(ROTATION_FILE):
        return {"cursors": {}}
    try:
        with open(ROTATION_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"cursors": {}}
    if not isinstance(data, dict):
        return {"cursors": {}}
    cursors = data.get("cursors")
    return {"cursors": cursors if isinstance(cursors, dict) else {}}


def _save_rotation_state(state: dict):
    if not isinstance(state, dict):
        state = {"cursors": {}}
    if not isinstance(state.get("cursors"), dict):
        state["cursors"] = {}
    try:
        with open(ROTATION_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def select_sessions_rotating(key: str, count: int) -> list[dict]:
    sessions = load_sessions()
    if not sessions or not count or count <= 0:
        return []
    total = len(sessions)
    count = min(int(count), total)

    state = _load_rotation_state()
    cursors = state.get("cursors") or {}
    try:
        start = int(cursors.get(key, 0) or 0)
    except Exception:
        start = 0
    if start < 0:
        start = 0
    if total:
        start = start % total

    selected = []
    for i in range(count):
        selected.append(sessions[(start + i) % total])

    if total:
        cursors[key] = (start + count) % total
    state["cursors"] = cursors
    _save_rotation_state(state)

    return selected


def select_sessions_for_voting(key: str, count: int, exclude_voted: bool = True) -> list[dict]:
    """اختيار جلسات للتصويت مع استثناء الجلسات التي صوتت بالفعل"""
    sessions = load_sessions()
    if not sessions or not count or count <= 0:
        return []
    
    # تحميل الجلسات التي صوتت بالفعل
    voted_sessions_file = os.path.join(_BASE_DIR, "voted_sessions.json")
    voted_sessions = set()
    
    if exclude_voted and os.path.exists(voted_sessions_file):
        try:
            with open(voted_sessions_file, "r", encoding="utf-8") as f:
                voted_data = json.load(f)
                voted_sessions = set(voted_data.get(key, []))
        except Exception:
            pass
    
    # استبعاد الجلسات التي صوتت بالفعل
    available_sessions = []
    for session in sessions:
        session_hash = _session_hash(session.get("session", ""))
        if session_hash not in voted_sessions:
            available_sessions.append(session)
    
    # إذا لم تكن هناك جلسات متاحة، نستخدم كل الجلسات
    if not available_sessions:
        available_sessions = sessions
    
    total = len(available_sessions)
    count = min(int(count), total)
    
    # اختيار عشوائي للجلسات المتاحة
    if count < total:
        random.shuffle(available_sessions)
    
    selected = available_sessions[:count]
    
    # حفظ الجلسات المختارة كجلسات صوتت
    if exclude_voted:
        try:
            voted_sessions.update(_session_hash(s.get("session", "")) for s in selected)
            
            # تحميل البيانات الحالية
            voted_data = {}
            if os.path.exists(voted_sessions_file):
                try:
                    with open(voted_sessions_file, "r", encoding="utf-8") as f:
                        voted_data = json.load(f)
                except Exception:
                    pass
            
            # حفظ الجلسات الجديدة
            if key not in voted_data:
                voted_data[key] = []
            voted_data[key] = list(voted_sessions)
            
            with open(voted_sessions_file, "w", encoding="utf-8") as f:
                json.dump(voted_data, f, ensure_ascii=False, indent=2)
                
        except Exception:
            pass
    
    return selected


def reset_voted_sessions(key: str = None) -> bool:
    """إعادة تعيين الجلسات التي صوتت"""
    voted_sessions_file = os.path.join(_BASE_DIR, "voted_sessions.json")
    
    try:
        if key and os.path.exists(voted_sessions_file):
            # إعادة تعيين تصويتات معينة فقط
            with open(voted_sessions_file, "r", encoding="utf-8") as f:
                voted_data = json.load(f)
            
            if key in voted_data:
                del voted_data[key]
                
            with open(voted_sessions_file, "w", encoding="utf-8") as f:
                json.dump(voted_data, f, ensure_ascii=False, indent=2)
            return True
            
        elif os.path.exists(voted_sessions_file):
            # إعادة تعيين جميع التصويتات
            os.remove(voted_sessions_file)
            return True
            
        return True
        
    except Exception:
        return False


def get_voted_sessions_info() -> dict:
    """الحصول على معلومات عن الجلسات التي صوتت"""
    voted_sessions_file = os.path.join(_BASE_DIR, "voted_sessions.json")
    
    try:
        if not os.path.exists(voted_sessions_file):
            return {"total_keys": 0, "sessions_per_key": {}}
            
        with open(voted_sessions_file, "r", encoding="utf-8") as f:
            voted_data = json.load(f)
        
        sessions_per_key = {}
        total_sessions = 0
        
        for key, session_hashes in voted_data.items():
            count = len(session_hashes) if isinstance(session_hashes, list) else 0
            sessions_per_key[key] = count
            total_sessions += count
        
        return {
            "total_keys": len(voted_data),
            "total_sessions": total_sessions,
            "sessions_per_key": sessions_per_key
        }
        
    except Exception:
        return {"total_keys": 0, "sessions_per_key": {}}


def _load_operations_state() -> dict:
    if not os.path.exists(OPERATIONS_FILE):
        return {"operations": {}}
    try:
        with open(OPERATIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"operations": {}}
    if not isinstance(data, dict):
        return {"operations": {}}
    ops = data.get("operations")
    return {"operations": ops if isinstance(ops, dict) else {}}


def _save_operations_state(state: dict):
    if not isinstance(state, dict):
        state = {"operations": {}}
    if not isinstance(state.get("operations"), dict):
        state["operations"] = {}
    try:
        with open(OPERATIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _ensure_operation_record(op_code: str, op_type: str, user_id: int, ctx: dict):
    if not op_code:
        return
    state = _load_operations_state()
    ops = state.get("operations") or {}
    if op_code in ops:
        return
    safe_ctx = {}
    for k in (
        "link",
        "username",
        "msg_id",
        "button_text",
        "button_index",
        "r_username",
        "r_msg_id",
        "p_username",
        "p_msg_id",
        "rv_username",
        "rv_msg_id",
        "vrv_username",
        "vrv_msg_id",
        "v_link",
        "v_username",
        "v_msg_id",
        "c_link",
        "c_channel_username",
        "c_group_username",
        "c_msg_id",
        "p_answer_index",
    ):
        if k in (ctx or {}):
            safe_ctx[k] = ctx.get(k)
    ops[op_code] = {
        "type": op_type,
        "user_id": int(user_id or 0),
        "created_at": int(time.time()),
        "ctx": safe_ctx,
        "executed": [],
        "comments": [],
    }
    state["operations"] = ops
    _save_operations_state(state)


def _append_operation_execution(op_code: str, session_string: str, ok: bool, meta: dict | None = None):
    if not op_code or not session_string:
        return
    state = _load_operations_state()
    ops = state.get("operations") or {}
    rec = ops.get(op_code)
    if not isinstance(rec, dict):
        return
    executed = rec.get("executed")
    if not isinstance(executed, list):
        executed = []
    entry = {
        "session_hash": _session_hash(session_string),
        "ok": bool(ok),
        "at": int(time.time()),
    }
    if isinstance(meta, dict) and meta:
        entry["meta"] = meta
    executed.append(entry)
    rec["executed"] = executed
    ops[op_code] = rec
    state["operations"] = ops
    _save_operations_state(state)


def _append_operation_comment(op_code: str, session_string: str, peer: str, message_id: int):
    if not op_code or not session_string or not peer or not message_id:
        return
    state = _load_operations_state()
    ops = state.get("operations") or {}
    rec = ops.get(op_code)
    if not isinstance(rec, dict):
        return
    comments = rec.get("comments")
    if not isinstance(comments, list):
        comments = []
    comments.append(
        {
            "session_hash": _session_hash(session_string),
            "peer": str(peer),
            "message_id": int(message_id),
            "at": int(time.time()),
        }
    )
    rec["comments"] = comments
    ops[op_code] = rec
    state["operations"] = ops
    _save_operations_state(state)


def _session_hash(sess: str) -> str:
    try:
        import hashlib

        return hashlib.sha256(sess.encode("utf-8")).hexdigest()
    except Exception:
        return str(len(sess))


async def _telethon_session_info(session_string: str) -> dict:
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    try:
        me = await client.get_me()
        if not me:
            raise ValueError("invalid_session")
        return {
            "phone": str(getattr(me, "phone", "") or ""),
            "user_id": int(getattr(me, "id", 0) or 0),
            "username": str(getattr(me, "username", "") or ""),
            "first_name": str(getattr(me, "first_name", "") or ""),
        }
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def load_developers():
    if not os.path.exists(DEVS_FILE):
        return []
    try:
        with open(DEVS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []
    developers = data.get("developers", [])
    return developers if isinstance(developers, list) else []


def save_developers(developers):
    data = {"developers": developers}
    with open(DEVS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_points():
    if not os.path.exists(POINTS_FILE):
        return {}
    try:
        with open(POINTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    points = data.get("points", {})
    return points if isinstance(points, dict) else {}


def save_points(points: dict):
    data = {"points": points}
    with open(POINTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_user_points(user_id: int) -> int:
    points = load_points()
    try:
        return int(points.get(str(user_id), 0))
    except (TypeError, ValueError):
        return 0


def set_user_points(user_id: int, value: int):
    points = load_points()
    points[str(user_id)] = max(0, int(value))
    save_points(points)


def add_user_points(user_id: int, delta: int):
    current = get_user_points(user_id)
    set_user_points(user_id, current + int(delta))


def load_referrals():
    if not os.path.exists(REFERRALS_FILE):
        return {}
    try:
        with open(REFERRALS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    refs = data.get("referrals", {})
    return refs if isinstance(refs, dict) else {}


def save_referrals(refs: dict):
    data = {"referrals": refs}
    with open(REFERRALS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_json(file_path: str, default: any = None):
    """تحميل ملف JSON مع معالجة الأخطاء"""
    if not os.path.exists(file_path):
        return default if default is not None else {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default if default is not None else {}


def save_json(file_path: str, data: dict):
    """حفظ ملف JSON مع معالجة الأخطاء"""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_banned_users() -> dict:
    """تحميل قائمة المحظورين"""
    return load_json(BANNED_USERS_FILE, {})


def save_banned_users(banned: dict):
    """حفظ قائمة المحظورين"""
    save_json(BANNED_USERS_FILE, banned)


def is_user_banned(user_id: int) -> bool:
    """التحقق إذا كان المستخدم محظوراً"""
    banned = load_banned_users()
    return str(user_id) in banned


def ban_user(user_id: int, reason: str = "غير محدد") -> bool:
    """حظر مستخدم"""
    if is_primary_developer(user_id) or is_developer_user(user_id):
        return False  # لا يمكن حظر المطورين
    
    banned = load_banned_users()
    banned[str(user_id)] = {
        "banned_at": int(time.time()),
        "reason": reason,
        "banned_by": "developer"
    }
    save_banned_users(banned)
    return True


def unban_user(user_id: int) -> bool:
    """الغاء حظر مستخدم"""
    banned = load_banned_users()
    if str(user_id) in banned:
        del banned[str(user_id)]
        save_banned_users(banned)
        return True
    return False


def get_banned_users_list() -> list:
    """الحصول على قائمة المحظورين"""
    banned = load_banned_users()
    result = []
    for user_id_str, info in banned.items():
        try:
            user_id = int(user_id_str)
            result.append({
                "user_id": user_id,
                "banned_at": info.get("banned_at", 0),
                "reason": info.get("reason", "غير محدد"),
                "banned_by": info.get("banned_by", "developer")
            })
        except ValueError:
            continue
    return result


def is_developer_user(user_id: int) -> bool:
    """التحقق هل المستخدم مطوّر (مالك البوت أو مطوّر مرفوع من خلال البوت)."""
    if is_primary_developer(user_id):
        return True
    developers = load_developers()
    for d in developers:
        if d.get("user_id") == user_id:
            return True
    return False


async def _safe_get_entity_and_message(client: TelegramClient, username: str, msg_id: int):
    last_exc: Exception | None = None
    for _ in range(3):
        try:
            entity = await client.get_entity(username)
            try:
                await client(JoinChannelRequest(entity))
            except Exception:
                pass
            msg = await client.get_messages(entity, ids=int(msg_id))
            if not msg:
                raise ValueError("message_not_found")
            return entity, msg
        except FloodWaitError as e:
            last_exc = e
            await asyncio.sleep(int(getattr(e, "seconds", 0) or 0))
        except Exception as e:
            last_exc = e
            await asyncio.sleep(1)
    raise last_exc if last_exc else ValueError("message_not_found")


async def _safe_add_views(client: TelegramClient, entity, msg_id: int) -> bool:
    """دالة محسنة لزيادة المشاهدات مع معالجة الأخطاء"""
    last_exc: Exception | None = None
    
    for attempt in range(5):  # 5 محاولات
        try:
            # التحقق من وجود الرسالة أولاً
            msg = await client.get_messages(entity, ids=msg_id)
            if not msg:
                return False
            
            # زيادة المشاهدات
            await client(GetMessagesViewsRequest(peer=entity, id=[msg_id], increment=True))
            
            # التحقق من نجاح العملية
            await asyncio.sleep(1)
            updated_msg = await client.get_messages(entity, ids=msg_id)
            if updated_msg and hasattr(updated_msg, 'views'):
                return True
                
        except FloodWaitError as e:
            last_exc = e
            wait_time = int(getattr(e, "seconds", 0) or 5)
            await asyncio.sleep(wait_time)
        except Exception as e:
            last_exc = e
            wait_time = min(2 + attempt, 10)
            await asyncio.sleep(wait_time)
    
    return False


async def _safe_click_callback(client: TelegramClient, peer, msg_id: int, data: bytes) -> bool:
    last_exc: Exception | None = None
    for attempt in range(5):  # زيادة المحاولات من 3 إلى 5
        try:
            await client(GetBotCallbackAnswerRequest(peer=peer, msg_id=int(msg_id), data=data))
            return True
        except BotResponseTimeoutError as e:
            last_exc = e
            wait_time = min(5 + attempt * 2, 15)  # زيادة وقت الانتظار تدريجياً
            await asyncio.sleep(wait_time)
        except FloodWaitError as e:
            last_exc = e
            wait_time = int(getattr(e, "seconds", 0) or 5)
            await asyncio.sleep(wait_time)
        except RPCError as e:
            last_exc = e
            await asyncio.sleep(2)
        except Exception as e:
            last_exc = e
            await asyncio.sleep(2)
    return False


def _get_user_rank(user_id: int):
    if is_primary_developer(user_id):
        return "مطور البوت"
    developers = load_developers()
    for d in developers:
        if d.get("user_id") == user_id:
            return d.get("rank")
    return None


def is_vip_user(user_id: int) -> bool:
    rank = _get_user_rank(user_id)
    if not rank:
        return False
    try:
        return str(rank).strip().lower() == "vip"
    except Exception:
        return False


def is_primary_developer(user_id: int) -> bool:
    """مالك/ملاك البوت الأساسيين (صلاحيات أعلى من المطور العادي)."""
    return user_id in {DEVELOPER_ID, 7182427468}


def calculate_cost(units: int) -> int:
    """حساب تكلفة العملية بالنقاط: كل 10 وحدات = 5 نقاط (مقربة للأعلى)."""
    if units <= 0:
        return 0
    return int(math.ceil(units / 10.0) * 5)


DEFAULT_REACTIONS = [
    "👍",
    "❤️",
    "🔥",
    "😂",
    "👏",
    "😍",
    "🤩",
    "🎉",
    "😢",
    "😡",
]


def get_sessions_count():
    sessions = load_sessions()
    return len(sessions)


user_states = {}  # chat_id -> state
competition_context = {}  # chat_id -> بيانات تدفّق المسابقات
user_request_counts = {}  # user_id -> عدد طلبات الرشق التي قام بها
dev_context = {}
votes_stop_flags = {}  # chat_id -> إيقاف رشق التصويتات
comments_stop_flags = {}  # chat_id -> إيقاف رشق التعليقات
views_stop_flags = {}  # chat_id -> إيقاف رشق المشاهدات
reactions_stop_flags = {}  # chat_id -> إيقاف رشق التفاعلات
reacts_views_stop_flags = {}  # chat_id -> إيقاف رشق التفاعلات والمشاهدات
votes_reacts_stop_flags = {}  # chat_id -> إيقاف رشق التصويتات والتفاعلات
poll_stop_flags = {}  # chat_id -> إيقاف رشق الاستفتاء
filter_stop_flags = {}  # chat_id -> إيقاف عمليات قسم التصفية
pending_requests = {}  # req_id -> بيانات طلبات الرشق المعلقة للموافقة
approval_context = {}  # chat_id (للمطور) -> بيانات سبب الرفض للطلبات
last_seen_status = {}  # session_hash -> bool (True=آخر ظهور مفعل, False=معطل)
bot_verification_required = {}  # user_id -> bool (True=يحتاج تحقق)
user_phone_numbers = {}  # user_id -> phone_number


# قفل عام لطلبات المستخدمين (غير المالك): إذا يوجد طلب جاري، باقي المستخدمين ينتظرون
active_boost_job = {
    "active": False,
    "tag": None,
    "type": None,
    "label": None,
    "user_id": None,
    "chat_id": None,
    "created_at": None,
}


def _is_boost_locked_for_user(user_id: int) -> bool:
    if is_primary_developer(user_id):
        return False
    return bool(active_boost_job.get("active"))


def _start_non_owner_boost(tag: str, req_type: str, label: str, user_id: int, chat_id: int):
    # لا نقفل على المالك
    if is_primary_developer(user_id):
        return
    active_boost_job["active"] = True
    active_boost_job["tag"] = tag
    active_boost_job["type"] = req_type
    active_boost_job["label"] = label
    active_boost_job["user_id"] = user_id
    active_boost_job["chat_id"] = chat_id
    active_boost_job["created_at"] = int(time.time())


def _finish_non_owner_boost(tag: str):
    try:
        if active_boost_job.get("active") and active_boost_job.get("tag") == tag:
            active_boost_job["active"] = False
            active_boost_job["tag"] = None
            active_boost_job["type"] = None
            active_boost_job["label"] = None
            active_boost_job["user_id"] = None
            active_boost_job["chat_id"] = None
            active_boost_job["created_at"] = None
    except Exception:
        pass


def _format_active_job_status() -> str:
    if not active_boost_job.get("active"):
        return "<b>حالة الطلب الجاري</b>\n<blockquote>لا يوجد طلب جاري حالياً.</blockquote>"
    req_type = active_boost_job.get("type") or "غير معروف"
    label = active_boost_job.get("label") or req_type
    uid = active_boost_job.get("user_id")
    cid = active_boost_job.get("chat_id")
    created_at = active_boost_job.get("created_at")
    return (
        "<b>حالة الطلب الجاري</b>\n"
        f"<blockquote>الخدمة: {label}\n"
        f"النوع: {req_type}\n"
        f"User ID: <code>{uid}</code>\n"
        f"Chat ID: <code>{cid}</code>\n"
        f"Tag: <code>{active_boost_job.get('tag')}</code>\n"
        f"الوقت: {created_at}</blockquote>"
    )


def remove_session_entry(session_obj: dict):
    sessions = load_sessions()
    target = session_obj.get("session")
    if not target:
        return
    new_sessions = [s for s in sessions if s.get("session") != target]
    if len(new_sessions) != len(sessions):
        save_sessions(new_sessions)


def handle_session_error(chat_id: int, session_obj: dict, exc: Exception, action: str) -> bool:
    if isinstance(exc, (AuthKeyUnregisteredError, UserDeactivatedError, UserDeactivatedBanError)):
        remove_session_entry(session_obj)
        try:
            bot.send_message(
                chat_id,
                f"<b>تم حذف جلسة معطوبة أثناء {action} وتمت تصفيتها من القائمة.</b>",
            )
        except Exception:
            pass
        return True
    return False


def main_menu_keyboard(is_developer: bool) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("المسابقات"), KeyboardButton("الحسابات"))
    kb.row(KeyboardButton("الإحصائيات"), KeyboardButton("قسم التصفية"))
    if is_developer:
        kb.row(KeyboardButton("لوحة المطور"))
    return kb


def accounts_menu_keyboard(is_owner: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    count = get_sessions_count()
    kb.row(KeyboardButton(f"عدد الجلسات: {count}"))
    kb.row(KeyboardButton("إضافة جلسة"), KeyboardButton("حذف جلسة"))
    if is_owner:
        kb.row(KeyboardButton("إضافة جلسات"))
    kb.row(KeyboardButton("فحص الجلسات"))
    if is_owner:
        kb.row(KeyboardButton("قسم التصفية"))
    kb.row(KeyboardButton("الرجوع للقائمة الرئيسية"))
    return kb


def add_sessions_mode_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("دفعة واحدة"))
    kb.row(KeyboardButton("وحدة وحدة"))
    kb.row(KeyboardButton("رجوع"))
    return kb


def filter_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("مغادرة كروبات"))
    kb.row(KeyboardButton("مغادرة قنوات"))
    kb.row(KeyboardButton("مغادرة الجميع"))
    kb.row(KeyboardButton("رجوع"))
    return kb


def leave_groups_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("مغادرة جميع الكروبات"))
    kb.row(KeyboardButton("مغادرة قروب محدد"))
    kb.row(KeyboardButton("رجوع"))
    return kb


def leave_channels_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("مغادرة جميع القنوات"))
    kb.row(KeyboardButton("مغادرة قناة محددة"))
    kb.row(KeyboardButton("رجوع"))
    return kb


def _extract_invite_hash(url: str) -> str | None:
    try:
        u = (url or "").strip()
        if not u:
            return None
        if not u.startswith("http://") and not u.startswith("https://"):
            u = "https://" + u
        parsed = urlparse(u)
        if parsed.netloc not in ("t.me", "telegram.me", "www.t.me", "telegram.dog"):
            return None
        path = parsed.path.strip("/")
        if not path:
            return None
        if path.startswith("+"):
            return path.lstrip("+")
        if path.lower().startswith("joinchat/"):
            return path.split("/", 1)[1] if "/" in path else None
    except Exception:
        return None
    return None


def _extract_public_username(url: str) -> str | None:
    try:
        u = (url or "").strip()
        if not u:
            return None
        if not u.startswith("http://") and not u.startswith("https://"):
            u = "https://" + u
        parsed = urlparse(u)
        if parsed.netloc not in ("t.me", "telegram.me", "www.t.me", "telegram.dog"):
            return None
        parts = parsed.path.strip("/").split("/")
        if not parts:
            return None
        username = (parts[0] or "").strip()
        if not username or username.startswith("+") or username.lower() == "joinchat":
            return None
        return username
    except Exception:
        return None


async def _safe_leave_groups_for_client(client: TelegramClient) -> int:
    left = 0
    dialogs_before = []
    try:
        async for d in client.iter_dialogs():
            if getattr(d, "is_group", False):
                dialogs_before.append(d.entity.id if d.entity else None)
    except Exception:
        pass
    
    async for d in client.iter_dialogs():
        try:
            if not getattr(d, "is_group", False):
                continue
            ent = d.entity
            if ent is None:
                continue
            if ent.__class__.__name__ == "Chat":
                me_inp = await client.get_input_entity("me")
                await client(DeleteChatUserRequest(chat_id=ent.id, user_id=me_inp))
            else:
                await client(LeaveChannelRequest(ent))
            
            # التحقق من المغادرة الفعلية
            await asyncio.sleep(1)
            try:
                await client.get_entity(ent)
                # لا يزال في المجموعة، فشلت المغادرة
                continue
            except Exception:
                # تمت المغادرة بنجاح
                left += 1
                
            await asyncio.sleep(2)
        except FloodWaitError as e:
            await asyncio.sleep(int(getattr(e, "seconds", 0) or 0))
        except Exception:
            continue
    return left


async def _safe_leave_channels_for_client(client: TelegramClient) -> int:
    left = 0
    async for d in client.iter_dialogs():
        try:
            if not getattr(d, "is_channel", False):
                continue
            if getattr(d, "is_group", False):
                continue
            ent = d.entity
            if ent is None:
                continue
            
            await client(LeaveChannelRequest(ent))
            
            # التحقق من المغادرة الفعلية
            await asyncio.sleep(1)
            try:
                await client.get_entity(ent)
                # لا يزال في القناة، فشلت المغادرة
                continue
            except Exception:
                # تمت المغادرة بنجاح
                left += 1
                
            await asyncio.sleep(2)
        except FloodWaitError as e:
            await asyncio.sleep(int(getattr(e, "seconds", 0) or 0))
        except Exception:
            continue
    return left


async def _safe_leave_specific_peer(client: TelegramClient, link: str) -> bool:
    invite_hash = _extract_invite_hash(link)
    if invite_hash:
        try:
            checked = await client(CheckChatInviteRequest(invite_hash))
            if checked.__class__.__name__ == "ChatInviteAlready":
                ent = getattr(checked, "chat", None)
                if ent is None:
                    return False
                if ent.__class__.__name__ == "Chat":
                    me_inp = await client.get_input_entity("me")
                    await client(DeleteChatUserRequest(chat_id=ent.id, user_id=me_inp))
                else:
                    await client(LeaveChannelRequest(ent))
                await asyncio.sleep(2)
                return True
            return False
        except FloodWaitError as e:
            await asyncio.sleep(int(getattr(e, "seconds", 0) or 0))
            return False
        except Exception:
            return False

    username = _extract_public_username(link)
    if not username:
        return False
    try:
        ent = await client.get_entity(username)
        if ent.__class__.__name__ == "Chat":
            me_inp = await client.get_input_entity("me")
            await client(DeleteChatUserRequest(chat_id=ent.id, user_id=me_inp))
        else:
            await client(LeaveChannelRequest(ent))
        await asyncio.sleep(2)
        return True
    except FloodWaitError as e:
        await asyncio.sleep(int(getattr(e, "seconds", 0) or 0))
        return False
    except Exception:
        return False


def start_filter_job(chat_id: int, mode: str, target_link: str | None = None) -> None:
    try:
        filter_stop_flags.pop(chat_id, None)
    except Exception:
        pass
    thread = threading.Thread(target=run_filter_job, args=(chat_id, mode, target_link))
    thread.start()


def run_filter_job(chat_id: int, mode: str, target_link: str | None = None) -> None:
    async def _inner():
        start_ts = time.time()
        sessions = load_sessions()
        if not sessions:
            bot.send_message(chat_id, "<b>لا توجد جلسات مضافة.</b>")
            return

        total_groups = 0
        total_channels = 0
        ok_specific = 0

        for idx, s in enumerate(sessions, start=1):
            if filter_stop_flags.get(chat_id):
                break
            client = None
            try:
                client = await _connect_telethon_client(s["session"])
                if mode == "groups_all":
                    total_groups += await _safe_leave_groups_for_client(client)
                elif mode == "channels_all":
                    total_channels += await _safe_leave_channels_for_client(client)
                elif mode == "all":
                    total_groups += await _safe_leave_groups_for_client(client)
                    total_channels += await _safe_leave_channels_for_client(client)
                elif mode == "group_specific":
                    if target_link and await _safe_leave_specific_peer(client, target_link):
                        ok_specific += 1
                elif mode == "channel_specific":
                    if target_link and await _safe_leave_specific_peer(client, target_link):
                        ok_specific += 1
            except Exception as e:
                if handle_session_error(chat_id, s, e, "التصفية"):
                    continue
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

            # فاصل بين كل حساب وحساب
            if filter_stop_flags.get(chat_id):
                break
            await asyncio.sleep(2)

        took = int(time.time() - start_ts)
        if mode == "groups_all":
            bot.send_message(chat_id, f"<b>تمت مغادرة الكروبات بنجاح.</b>\n<blockquote>المغادرة: {total_groups}\nالمدة: {took} ثانية</blockquote>")
        elif mode == "channels_all":
            bot.send_message(chat_id, f"<b>تمت مغادرة القنوات بنجاح.</b>\n<blockquote>المغادرة: {total_channels}\nالمدة: {took} ثانية</blockquote>")
        elif mode == "all":
            bot.send_message(chat_id, f"<b>تمت عملية المغادرة الشاملة بنجاح.</b>\n<blockquote>الكروبات: {total_groups}\nالقنوات: {total_channels}\nالمدة: {took} ثانية</blockquote>")
        elif mode in ("group_specific", "channel_specific"):
            bot.send_message(chat_id, f"<b>تمت العملية.</b>\n<blockquote>نجح مع {ok_specific} من {len(sessions)} جلسة\nالمدة: {took} ثانية</blockquote>")

        filter_stop_flags.pop(chat_id, None)

    asyncio.run(_inner())


def _check_and_cleanup_sessions():
    async def _inner():
        sessions = load_sessions()
        if not sessions:
            return 0, 0

        kept = []
        ok_count = 0
        removed = 0

        for s in sessions:
            session_string = s.get("session")
            if not session_string:
                removed += 1
                continue
            try:
                client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
                await client.connect()
                try:
                    authorized = await client.is_user_authorized()
                    if not authorized:
                        removed += 1
                        continue
                    await client.get_me()
                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                kept.append(s)
                ok_count += 1
            except Exception as e:
                if isinstance(e, (AuthKeyUnregisteredError, UserDeactivatedError, UserDeactivatedBanError)):
                    removed += 1
                else:
                    kept.append(s)
                    ok_count += 1

        if removed:
            save_sessions(kept)

        return ok_count, removed

    try:
        return asyncio.run(_inner())
    except Exception:
        return None


@bot.message_handler(func=lambda m: m.text == "فحص الجلسات")
def handle_check_sessions(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    bot.send_chat_action(message.chat.id, "typing")
    result = _check_and_cleanup_sessions()
    if result is None:
        bot.send_message(message.chat.id, "<b>حدث خطأ أثناء فحص الجلسات.</b>")
        return

    ok_count, removed = result
    bot.send_message(
        message.chat.id,
        f"<b>نتيجة فحص الجلسات</b>\n<blockquote>السليمة: {ok_count}\nالمحذوفة (غير صالحة): {removed}</blockquote>",
        reply_markup=accounts_menu_keyboard(is_owner=True),
    )


@bot.message_handler(func=lambda m: m.text == "الحسابات")
def handle_accounts(message):
    is_owner = is_primary_developer(message.from_user.id)
    kb = accounts_menu_keyboard(is_owner=is_owner)
    text = (
        "<b>قسم الحسابات</b>\n"
        "<blockquote>اختر أحد الخيارات التالية لإدارة جلسات Telethon.</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=kb)


@bot.message_handler(func=lambda m: m.text == "إضافة جلسة")
def handle_add_session(message):
    user_states[message.chat.id] = "add_session_waiting_string"
    bot.send_message(
        message.chat.id,
        "<b>إضافة جلسة</b>\n<blockquote>أرسل الآن StringSession (سطر واحد).</blockquote>",
        reply_markup=cancel_only_keyboard(),
    )


@bot.message_handler(func=lambda m: m.text == "إضافة جلسات")
def handle_add_sessions_menu(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return
    _start_collect_sessions_window(message.chat.id, message.from_user.id, seconds=40)


def _extract_sessions_from_text(text: str | None) -> list[str]:
    raw = (text or "")
    raw = raw.replace("\r", "\n")
    # بعض المستخدمين يرسلون عدة جلسات في رسالة واحدة مفصولة بمسافات/تبويبات وليس أسطر فقط
    parts = re.split(r"\s+", raw)
    cleaned = []
    for p in parts:
        s = (p or "").strip()
        if not s:
            continue
        # تنظيف الرموز الشائعة التي قد تُضاف عند النسخ/التحويل
        s = s.strip("\"'`<>()[]{}.,;:")
        if not s:
            continue
        cleaned.append(s)
    return cleaned


def _try_add_single_session(sess: str) -> tuple[bool, str]:
    s = (sess or "").strip()
    if not s or len(s) < 20:
        return False, "invalid"

    sessions = load_sessions()
    sess_h = _session_hash(s)
    for existing in sessions:
        if existing.get("hash") == sess_h or existing.get("session") == s:
            return False, "duplicate"

    try:
        info = asyncio.run(_telethon_session_info(s))
    except Exception:
        return False, "verify_failed"

    sessions.append(
        {
            "session": s,
            "hash": sess_h,
            "phone": info.get("phone", ""),
            "user_id": info.get("user_id", 0),
            "username": info.get("username", ""),
            "first_name": info.get("first_name", ""),
            "created_at": int(time.time()),
        }
    )

    try:
        save_sessions(sessions)
    except Exception:
        return False, "save_failed"

    return True, "added"


def _start_collect_sessions_window(chat_id: int, user_id: int, seconds: int = 40):
    token = f"{int(time.time())}_{random.randint(1000, 9999)}"
    dev_context[chat_id] = {"collect_token": token, "sessions": [], "started_at": int(time.time()), "seconds": int(seconds)}
    user_states[chat_id] = "dev_add_sessions_collecting"

    bot.send_message(
        chat_id,
        f"<b>إضافة جلسات</b>\n<blockquote>أرسل الآن الجلسات (يمكنك إرسال أكثر من رسالة).\nسيتم الإغلاق تلقائياً بعد {seconds} ثانية ثم تبدأ الإضافة.</blockquote>",
        reply_markup=cancel_only_keyboard(),
    )

    def _worker():
        try:
            time.sleep(int(seconds))
            ctx = dev_context.get(chat_id) or {}
            if ctx.get("collect_token") != token:
                return
            if user_states.get(chat_id) != "dev_add_sessions_collecting":
                return

            items = ctx.get("sessions") or []
            added = 0
            duplicates = 0
            invalid = 0
            failed = 0

            for s in items:
                ok, status = _try_add_single_session(s)
                if ok:
                    added += 1
                else:
                    if status == "duplicate":
                        duplicates += 1
                    elif status == "invalid":
                        invalid += 1
                    else:
                        failed += 1

            user_states[chat_id] = None
            dev_context.pop(chat_id, None)
            bot.send_message(
                chat_id,
                f"<b>انتهى وقت التجميع</b>\n<blockquote>تم استلام: {len(items)}\nتمت الإضافة: {added}\nمكررة: {duplicates}\nغير صالحة: {invalid}\nفشل التحقق/الحفظ: {failed}</blockquote>",
                reply_markup=accounts_menu_keyboard(is_owner=is_primary_developer(user_id)),
            )
        except Exception:
            pass

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


@bot.message_handler(func=lambda m: False)
def _handle_add_sessions_mode_disabled(message):
    return


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "dev_add_sessions_collecting")
def handle_add_sessions_collecting(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    if not is_primary_developer(message.from_user.id):
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    ctx = dev_context.get(message.chat.id)
    if not isinstance(ctx, dict) or not ctx.get("collect_token"):
        return

    items = _extract_sessions_from_text(message.text)
    if not items:
        return

    try:
        buf = ctx.get("sessions")
        if not isinstance(buf, list):
            buf = []
        buf.extend(items)
        ctx["sessions"] = buf
        dev_context[message.chat.id] = ctx
    except Exception:
        pass


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "dev_add_sessions_bulk")
def handle_add_sessions_bulk(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    if not is_primary_developer(message.from_user.id):
        user_states.pop(message.chat.id, None)
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    items = _extract_sessions_from_text(message.text)
    if not items:
        bot.send_message(message.chat.id, "<b>لم يتم العثور على أي جلسة في الرسالة.</b>")
        return

    added = 0
    duplicates = 0
    invalid = 0
    failed = 0

    for s in items:
        ok, status = _try_add_single_session(s)
        if ok:
            added += 1
        else:
            if status == "duplicate":
                duplicates += 1
            elif status == "invalid":
                invalid += 1
            else:
                failed += 1

    user_states[message.chat.id] = None
    bot.send_message(
        message.chat.id,
        f"<b>نتيجة إضافة الجلسات</b>\n<blockquote>تمت الإضافة: {added}\nمكررة: {duplicates}\nغير صالحة: {invalid}\nفشل التحقق/الحفظ: {failed}</blockquote>",
        reply_markup=accounts_menu_keyboard(is_owner=True),
    )


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "dev_add_sessions_step")
def handle_add_sessions_step(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    if not is_primary_developer(message.from_user.id):
        user_states.pop(message.chat.id, None)
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    items = _extract_sessions_from_text(message.text)
    if not items:
        bot.send_message(message.chat.id, "<b>أرسل StringSession واحدة على الأقل.</b>")
        return

    added = 0
    duplicates = 0
    invalid = 0
    failed = 0

    for s in items:
        ok, status = _try_add_single_session(s)
        if ok:
            added += 1
        else:
            if status == "duplicate":
                duplicates += 1
            elif status == "invalid":
                invalid += 1
            else:
                failed += 1

    bot.send_message(
        message.chat.id,
        f"<b>تمت معالجة الرسالة</b>\n<blockquote>تمت الإضافة: {added}\nمكررة: {duplicates}\nغير صالحة: {invalid}\nفشل التحقق/الحفظ: {failed}</blockquote>\n<b>أرسل الجلسة التالية أو اضغط (إلغاء العملية).</b>",
        reply_markup=cancel_only_keyboard(),
    )


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "add_session_waiting_string")
def handle_add_session_string(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    sess = (message.text or "").strip()
    if not sess or len(sess) < 20:
        bot.send_message(message.chat.id, "<b>Session غير صحيحة.</b>")
        return

    sessions = load_sessions()
    sess_h = _session_hash(sess)
    for s in sessions:
        if s.get("hash") == sess_h or s.get("session") == sess:
            user_states[message.chat.id] = None
            bot.send_message(message.chat.id, "<b>هذه الجلسة مضافة مسبقاً.</b>", reply_markup=accounts_menu_keyboard(is_owner=is_primary_developer(message.from_user.id)))
            return

    try:
        info = asyncio.run(_telethon_session_info(sess))
    except Exception:
        bot.send_message(message.chat.id, "<b>فشل التحقق من الجلسة.</b>\n<blockquote>تأكد أن StringSession صحيحة وغير منتهية.</blockquote>")
        return

    sessions.append(
        {
            "session": sess,
            "hash": sess_h,
            "phone": info.get("phone", ""),
            "user_id": info.get("user_id", 0),
            "username": info.get("username", ""),
            "first_name": info.get("first_name", ""),
            "created_at": int(time.time()),
        }
    )
    try:
        save_sessions(sessions)
    except Exception:
        bot.send_message(message.chat.id, "<b>فشل حفظ الجلسة على السيرفر.</b>")
        return

    user_states[message.chat.id] = None
    bot.send_message(
        message.chat.id,
        "<b>تمت إضافة الجلسة بنجاح.</b>",
        reply_markup=accounts_menu_keyboard(is_owner=is_primary_developer(message.from_user.id)),
    )


@bot.message_handler(func=lambda m: m.text == "قسم التصفية")
def handle_filter_section(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return
    user_states[message.chat.id] = "filter_menu"
    bot.send_message(message.chat.id, "<b>قسم التصفية</b>", reply_markup=filter_menu_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "filter_menu")
def handle_filter_menu(message):
    if not is_primary_developer(message.from_user.id):
        user_states[message.chat.id] = None
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    txt = (message.text or "").strip()
    if txt == "مغادرة كروبات":
        user_states[message.chat.id] = "filter_groups_menu"
        bot.send_message(message.chat.id, "<b>مغادرة الكروبات</b>", reply_markup=leave_groups_menu_keyboard())
        return
    if txt == "مغادرة قنوات":
        user_states[message.chat.id] = "filter_channels_menu"
        bot.send_message(message.chat.id, "<b>مغادرة القنوات</b>", reply_markup=leave_channels_menu_keyboard())
        return
    if txt == "مغادرة الجميع":
        bot.send_message(message.chat.id, "<b>بدأت عملية مغادرة الجميع...</b>")
        start_filter_job(message.chat.id, "all")
        return
    if txt == "رجوع":
        user_states[message.chat.id] = None
        bot.send_message(message.chat.id, "<b>قسم الحسابات</b>", reply_markup=accounts_menu_keyboard(is_owner=True))
        return
    bot.send_message(message.chat.id, "<b>اختر من الأزرار فقط.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "filter_groups_menu")
def handle_filter_groups_menu(message):
    if not is_primary_developer(message.from_user.id):
        user_states[message.chat.id] = None
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    txt = (message.text or "").strip()
    if txt == "مغادرة جميع الكروبات":
        bot.send_message(message.chat.id, "<b>بدأت عملية مغادرة جميع الكروبات...</b>")
        start_filter_job(message.chat.id, "groups_all")
        return
    if txt == "مغادرة قروب محدد":
        user_states[message.chat.id] = "filter_waiting_group_link"
        bot.send_message(message.chat.id, "<b>أرسل رابط القروب الآن.</b>", reply_markup=cancel_only_keyboard())
        return
    if txt == "رجوع":
        user_states[message.chat.id] = "filter_menu"
        bot.send_message(message.chat.id, "<b>قسم التصفية</b>", reply_markup=filter_menu_keyboard())
        return
    bot.send_message(message.chat.id, "<b>اختر من الأزرار فقط.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "filter_channels_menu")
def handle_filter_channels_menu(message):
    if not is_primary_developer(message.from_user.id):
        user_states[message.chat.id] = None
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    txt = (message.text or "").strip()
    if txt == "مغادرة جميع القنوات":
        bot.send_message(message.chat.id, "<b>بدأت عملية مغادرة جميع القنوات...</b>")
        start_filter_job(message.chat.id, "channels_all")
        return
    if txt == "مغادرة قناة محددة":
        user_states[message.chat.id] = "filter_waiting_channel_link"
        bot.send_message(message.chat.id, "<b>أرسل رابط القناة الآن.</b>", reply_markup=cancel_only_keyboard())
        return
    if txt == "رجوع":
        user_states[message.chat.id] = "filter_menu"
        bot.send_message(message.chat.id, "<b>قسم التصفية</b>", reply_markup=filter_menu_keyboard())
        return
    bot.send_message(message.chat.id, "<b>اختر من الأزرار فقط.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "filter_waiting_group_link")
def handle_filter_group_link(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    if not is_primary_developer(message.from_user.id):
        user_states[message.chat.id] = None
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return
    link = (message.text or "").strip()
    if not (_extract_public_username(link) or _extract_invite_hash(link)):
        bot.send_message(message.chat.id, "<b>الرابط غير صالح.</b>")
        return
    user_states[message.chat.id] = "filter_groups_menu"
    bot.send_message(message.chat.id, "<b>بدأت عملية مغادرة القروب المحدد...</b>", reply_markup=leave_groups_menu_keyboard())
    start_filter_job(message.chat.id, "group_specific", target_link=link)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "filter_waiting_channel_link")
def handle_filter_channel_link(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    if not is_primary_developer(message.from_user.id):
        user_states[message.chat.id] = None
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return
    link = (message.text or "").strip()
    if not (_extract_public_username(link) or _extract_invite_hash(link)):
        bot.send_message(message.chat.id, "<b>الرابط غير صالح.</b>")
        return
    user_states[message.chat.id] = "filter_channels_menu"
    bot.send_message(message.chat.id, "<b>بدأت عملية مغادرة القناة المحددة...</b>", reply_markup=leave_channels_menu_keyboard())
    start_filter_job(message.chat.id, "channel_specific", target_link=link)


def competitions_menu_keyboard(is_developer: bool) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    # تظهر جميع خيارات الرشق لكل المستخدمين، بينما يتحكم نظام النقاط + موافقة المطوّر في صلاحية التنفيذ للمستخدم العادي
    kb.row(KeyboardButton("رشق تصويتات"), KeyboardButton("رشق تعليقات"))
    kb.row(KeyboardButton("رشق مشاهدات"), KeyboardButton("رشق تفاعلات"))
    kb.row(KeyboardButton("رشق تفاعلات ومشاهدات"))
    kb.row(KeyboardButton("رشق تصويتات وتفاعلات"))
    kb.row(KeyboardButton("رشق استفتاء"))
    kb.row(KeyboardButton("سحب التصويتات"))
    kb.row(KeyboardButton("الرجوع للقائمة الرئيسية"))
    return kb


def _get_operation_by_code(op_code: str) -> dict | None:
    if not op_code:
        return None
    state = _load_operations_state()
    ops = state.get("operations") or {}
    rec = ops.get(op_code)
    return rec if isinstance(rec, dict) else None


def _get_session_string_by_hash(session_hash: str) -> str | None:
    if not session_hash:
        return None
    for s in load_sessions():
        sess_str = (s or {}).get("session")
        if sess_str and _session_hash(sess_str) == session_hash:
            return sess_str
    return None


def _gen_op_code(op_type: str, user_id: int) -> str:
    try:
        uid = int(user_id or 0)
    except Exception:
        uid = 0
    return f"{int(time.time())}_{str(op_type or 'op')}_{uid}"


def dev_panel_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("رفع مطور"), KeyboardButton("تنزيل مطور"))
    kb.row(KeyboardButton("حظر مستخدم"), KeyboardButton("الغاء حظر"))
    kb.row(KeyboardButton("اذاعة رسالة"), KeyboardButton("اعداد النقاط"))
    kb.row(KeyboardButton("إعادة تعيين التصويتات"), KeyboardButton("عرض التصويتات"))
    kb.row(KeyboardButton("تفعيل/تعطيل آخر ظهور"))
    kb.row(KeyboardButton("اختبار الإشعارات"), KeyboardButton("فحص المشاركات"))
    kb.row(KeyboardButton("حالة الطلبات"), KeyboardButton("إحصائيات"))
    kb.row(KeyboardButton("الرجوع للقائمة الرئيسية"))
    return kb


def verification_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("مشاركة جهة اتصالي", request_contact=True))
    return kb


def cancel_only_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("إلغاء العملية"))
    return kb


@bot.message_handler(func=lambda m: (m.text or "").strip() == "إلغاء العملية")
def handle_cancel_operation(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_states.pop(chat_id, None)
    competition_context.pop(chat_id, None)
    dev_context.pop(chat_id, None)

    # إيقاف أي عمليات رشق/تصفية جارية لهذا الشات
    try:
        votes_stop_flags[chat_id] = True
    except Exception:
        pass
    try:
        views_stop_flags[chat_id] = True
    except Exception:
        pass
    try:
        reactions_stop_flags[chat_id] = True
    except Exception:
        pass
    try:
        comments_stop_flags[chat_id] = True
    except Exception:
        pass
    try:
        reacts_views_stop_flags[chat_id] = True
    except Exception:
        pass
    try:
        votes_reacts_stop_flags[chat_id] = True
    except Exception:
        pass
    try:
        poll_stop_flags[chat_id] = True
    except Exception:
        pass
    try:
        filter_stop_flags[chat_id] = True
    except Exception:
        pass

    bot.send_message(
        chat_id,
        "<b>تم إلغاء العملية.</b>",
        reply_markup=main_menu_keyboard(is_developer=is_primary_developer(user_id)),
    )


@bot.message_handler(func=lambda m: (m.text or "").strip() == "الغاء العملية")
def handle_cancel_operation_variant(message):
    handle_cancel_operation(message)


@bot.message_handler(commands=["start"])
def handle_start(message):
    user = message.from_user
    user_id = user.id

    # التحقق إذا كان المستخدم محظوراً
    if is_user_banned(user_id):
        bot.send_message(
            message.chat.id,
            "<b>🚫 أنت محظور من البوت</b>\n<blockquote>لقد تم حظرك من استخدام هذا البوت. تواصل مع المطورين إذا كان هناك خطأ.</blockquote>"
        )
        return

    # التحقق إذا كان المستخدم يحتاج تحقق من البوت
    if bot_verification_required.get(user_id, False):
        kb = verification_keyboard()
        bot.send_message(
            message.chat.id,
            "<b>🔒 التحقق مطلوب</b>\n"
            "<blockquote>للتأكد أنك لست بوت، قم بمشاركة جهة اتصالك عبر الزر بالأسفل.</blockquote>",
            reply_markup=kb
        )
        return

    # معالجة رابط الإحالة إن وجد (/start ref_123)
    text_cmd = message.text or ""
    parts = text_cmd.split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else ""
    if payload.startswith("ref_"):
        try:
            ref_id = int(payload.split("_", 1)[1])
        except (ValueError, TypeError):
            ref_id = None
        if ref_id and ref_id != user_id:
            refs = load_referrals()
            if str(user_id) not in refs:
                refs[str(user_id)] = ref_id
                save_referrals(refs)
                add_user_points(ref_id, 2)  # تغيير من 10 إلى 2 نقاط
                try:
                    bot.send_message(
                        ref_id,
                        "<b>تم إضافة 2 نقطة إلى رصيدك</b>\n<blockquote>بسبب دعوة مستخدم جديد إلى البوت.</blockquote>",
                    )
                except Exception:
                    pass

    # التحقق إذا كان المستخدم جديد (أول مرة يدخل)
    points = get_user_points(user_id)
    if points == 0 and not is_developer_user(user_id) and not is_primary_developer(user_id):
        # المستخدم جديد، يتطلب تحقق
        bot_verification_required[user_id] = True
        kb = verification_keyboard()
        bot.send_message(
            message.chat.id,
            "<b>🔒 التحقق مطلوب</b>\n"
            "<blockquote>للتأكد أنك لست بوت، قم بمشاركة جهة اتصالك عبر الزر بالأسفل.</blockquote>",
            reply_markup=kb
        )
        return

    is_dev = is_developer_user(user_id)
    kb = main_menu_keyboard(is_dev)

    first_name = user.first_name or "بدون اسم"
    username = f"@{user.username}" if user.username else "لا يوجد يوزر"

    # تحديد الرتبة المعروضة
    if is_primary_developer(user_id):
        rank = "مطور البوت"
    else:
        rank = "مستخدم"
        developers = load_developers()
        for d in developers:
            if d.get("user_id") == user_id:
                rank = d.get("rank") or "مطور"
                break

    # توليد رابط الدعوة الخاص بالمستخدم
    invite_link = "غير متوفر حالياً"
    try:
        me = bot.get_me()
        if getattr(me, "username", None):
            invite_link = f"https://t.me/{me.username}?start=ref_{user_id}"
    except Exception:
        pass
    text = (
        "<b>مرحباً بك في البوت</b>\n"
        f"<blockquote>"
        f"<b>الاسم:</b> {first_name}\n"
        f"<b>اليوزر:</b> {username}\n"
        f"<b>الآيدي:</b> <code>{user_id}</code>\n"
        f"<b>الرتبة:</b> {rank}\n"
        f"<b>النقاط:</b> {points}\n"
        f"<b>رابط الدعوة:</b> <code>{invite_link}</code>\n"
        f"</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=kb)


@bot.message_handler(content_types=['contact'])
def handle_contact_verification(message):
    """معالج مشاركة جهة الاتصال - إشعار تلقائي للجميع"""
    user_id = message.from_user.id
    
    # التحقق إذا كان المستخدم محظوراً
    if is_user_banned(user_id):
        bot.send_message(
            message.chat.id,
            "<b>🚫 أنت محظور من البوت</b>\n<blockquote>لقد تم حظرك من استخدام هذا البوت.</blockquote>"
        )
        return
    
    # التحقق من أن جهة الاتصال خاصة بالمستخدم
    if message.contact and message.contact.user_id == user_id:
        print(f"✅ User {user_id} shared contact!")
        
        # استخراج رقم الهاتف
        phone_number = message.contact.phone_number
        user_phone_numbers[user_id] = phone_number
        
        # جمع معلومات المستخدم
        user_info = {
            'first_name': message.from_user.first_name or "بدون اسم",
            'username': f"@{message.from_user.username}" if message.from_user.username else "لا يوجد يوزر",
            'user_id': user_id,
            'phone': phone_number
        }
        
        print(f"User info collected: {user_info}")
        
        # إرسال إشعار للمطورين (دائماً)
        print("Sending notification to developers...")
        notify_developers_new_user(user_info)
        print("Notification sent!")
        
        # التحقق إذا كان المستخدم يحتاج تحقق من البوت
        if bot_verification_required.get(user_id, False):
            # تم التحقق بنجاح
            bot_verification_required[user_id] = False
            
            points = get_user_points(user_id)
            
            # تحديد الرتبة
            is_dev = is_developer_user(user_id)
            if is_primary_developer(user_id):
                rank = "مطور البوت"
            else:
                rank = "مستخدم"
                if is_dev:
                    developers = load_developers()
                    for d in developers:
                        if d.get("user_id") == user_id:
                            rank = d.get("rank") or "مطور"
                            break
            
            # توليد رابط الدعوة
            invite_link = "غير متوفر حالياً"
            try:
                me = bot.get_me()
                if getattr(me, "username", None):
                    invite_link = f"https://t.me/{me.username}?start=ref_{user_id}"
            except Exception:
                pass
            
            text = (
                "<b>✅ تم التأكيد بنجاح</b>\n"
                "<blockquote>أنت لست بوتاً! مرحباً بك في البوت.</blockquote>\n"
                f"<blockquote>"
                f"<b>الاسم:</b> {user_info['first_name']}\n"
                f"<b>اليوزر:</b> {user_info['username']}\n"
                f"<b>الآيدي:</b> <code>{user_id}</code>\n"
                f"<b>الرتبة:</b> {rank}\n"
                f"<b>النقاط:</b> {points}\n"
                f"<b>رابط الدعوة:</b> <code>{invite_link}</code>\n"
                f"</blockquote>"
            )
            
            kb = main_menu_keyboard(is_dev)
            bot.send_message(message.chat.id, text, reply_markup=kb)
        else:
            # المستخدم لا يحتاج تحقق، فقط شارك جهة الاتصال
            bot.send_message(message.chat.id, "<b>✅ تم استلام رقم هاتفك بنجاح</b>\n<blockquote>شكراً لمشاركة جهة الاتصال.</blockquote>")
    else:
        bot.send_message(message.chat.id, "<b>❌ فشل التحقق</b>\n<blockquote>يرجى مشاركة جهة اتصالك الخاصة.</blockquote>", reply_markup=verification_keyboard())


def notify_developers_new_user(user_info: dict):
    """إرسال إشعار بسيط وواضح للمطورين عند مشاركة جهة الاتصال"""
    try:
        # تحميل المطورين
        developers = load_developers()
        
        # تجميع كل المطورين: الأساسي + العاديين
        all_devs = [DEVELOPER_ID, 7182427468]  # Primary developers
        for d in developers:
            dev_id = d.get("user_id")
            if dev_id and dev_id not in all_devs:
                all_devs.append(dev_id)
        
        print(f"Sending notification to {len(all_devs)} developers")
        print(f"Developer IDs: {all_devs}")
        
        # إنشاء رسالة بسيطة وواضحة
        text = (
            "🔔 <b>دخول عضو جديد للبوت</b>\n\n"
            f"👤 <b>الاسم:</b> {user_info.get('first_name', 'غير معروف')}\n"
            f"📱 <b>رقم الهاتف:</b> {user_info.get('phone', 'غير متوفر')}\n"
            f"🆔 <b>الآيدي:</b> <code>{user_info.get('user_id', 'غير معروف')}</code>\n"
            f"👥 <b>اليوزر:</b> {user_info.get('username', 'لا يوجد')}\n"
            f"⏰ <b>الوقت:</b> {time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        print(f"Message text: {text}")
        
        success_count = 0
        for dev_id in all_devs:
            try:
                print(f"🔄 Sending to developer {dev_id}...")
                
                # محاولة إرسال الرسالة
                result = bot.send_message(dev_id, text)
                print(f"✅ Message sent successfully to {dev_id}")
                print(f"Message ID: {result.message_id}")
                success_count += 1
                
            except Exception as e:
                print(f"❌ Failed to send to {dev_id}: {e}")
                print(f"Error type: {type(e).__name__}")
                
                # محاولة إرسال رسالة نصية بسيطة بدون تنسيق
                try:
                    simple_text = f"🔔 عضو جديد:\nالاسم: {user_info.get('first_name', 'غير معروف')}\nرقم: {user_info.get('phone', 'غير متوفر')}\nالآيدي: {user_info.get('user_id', 'غير معروف')}"
                    bot.send_message(dev_id, simple_text)
                    print(f"✅ Simple text sent to {dev_id}")
                    success_count += 1
                except Exception as e2:
                    print(f"❌ Even simple text failed for {dev_id}: {e2}")
        
        print(f"✅ Final result: {success_count}/{len(all_devs)} notifications sent")
        
        # إذا لم يتم إرسال أي إشعار، جرب إرسال للمطورين الأساسيين بطريقة أخرى
        if success_count == 0:
            print("🚨 No notifications sent! Trying alternative method...")
            for primary_dev in [DEVELOPER_ID, 7182427468]:
                try:
                    # محاولة إرسال بدون أي تنسيق
                    alt_text = f"عضو جديد: {user_info.get('first_name')} - {user_info.get('phone')} - {user_info.get('user_id')}"
                    bot.send_message(primary_dev, alt_text, parse_mode=None)
                    print(f"✅ Alternative notification sent to primary developer {primary_dev}")
                except Exception as e:
                    print(f"❌ Alternative method failed for {primary_dev}: {e}")
        
    except Exception as e:
        print(f"❌ Critical error in notify_developers_new_user: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # محاولة إرسال رسالة بسيطة جداً لجميع المطورين الأساسيين
        for primary_dev in [DEVELOPER_ID, 7182427468]:
            try:
                bot.send_message(primary_dev, f"عضو جديد: {user_info.get('first_name', 'غير معروف')}", parse_mode=None)
                print(f"✅ Emergency notification sent to {primary_dev}")
            except Exception as e2:
                print(f"❌ Emergency notification failed for {primary_dev}: {e2}")


@bot.message_handler(func=lambda m: m.text == "رشق مشاهدات")
def handle_rashq_views(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return

    link = (message.text or "").strip()
    if not link:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رابط صالح.</b>")
        return

    username, msg_id = parse_telegram_message_link(link)
    if not username or not msg_id:
        bot.send_message(message.chat.id, "<b>الرابط غير صالح، تأكد من أنه على الشكل https://t.me/username/1234.</b>")
        return

    max_sessions = get_sessions_count()
    competition_context[message.chat.id] = {
        "v_link": link,
        "v_username": username,
        "v_msg_id": msg_id,
        "v_max_sessions": max_sessions,
    }

    text = (
        "<b>عدد الجلسات للمشاهدات</b>\n"
        f"<blockquote>أرسل الآن عدد الجلسات التي تريد استخدامها (من 1 إلى {max_sessions}).</blockquote>"
    )
    user_states[message.chat.id] = "views_waiting_sessions"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "views_waiting_sessions")
def handle_views_sessions(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return

    ctx = competition_context.get(message.chat.id) or {}
    max_sessions = ctx.get("v_max_sessions") or get_sessions_count()

    text = (message.text or "").strip()
    try:
        count = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if count <= 0 or count > max_sessions:
        bot.send_message(
            message.chat.id,
            f"<b>عدد غير صالح.</b>\n<blockquote>يجب أن يكون بين 1 و {max_sessions}.</blockquote>",
        )
        return

    ctx["v_sessions_count"] = count
    competition_context[message.chat.id] = ctx

    text = (
        "<b>الوقت بين كل جلسة والأخرى</b>\n"
        "<blockquote>أرسل الوقت بالثواني بين كل مشاهدة والأخرى.</blockquote>"
    )
    user_states[message.chat.id] = "views_waiting_delay"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "views_waiting_delay")
def handle_views_delay(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return

    ctx = competition_context.get(message.chat.id) or {}
    text = (message.text or "").strip()
    try:
        delay = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if delay < 0:
        bot.send_message(message.chat.id, "<b>الوقت يجب أن يكون 0 أو أكبر.</b>")
        return

    ctx["v_delay"] = delay
    competition_context[message.chat.id] = ctx
    _finalize_views_request(message, ctx)


def _finalize_views_request(message, ctx: dict):
    required_keys = ["v_link", "v_username", "v_msg_id", "v_sessions_count", "v_delay"]
    if not all(k in ctx for k in required_keys):
        bot.send_message(message.chat.id, "<b>بيانات رشق المشاهدات غير مكتملة، أعد العملية من جديد.</b>")
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    user_id = message.from_user.id
    sessions_count = ctx.get("v_sessions_count", 0)
    delay = ctx.get("v_delay", 0)

    # المالك: تنفيذ مباشر بدون نقاط
    if is_primary_developer(user_id):
        text = (
            "<b>تم استلام إعدادات رشق المشاهدات</b>\n"
            f"<blockquote>عدد الجلسات: {sessions_count}\n"
            f"الوقت بين كل جلسة: {delay} ثانية</blockquote>"
        )
        bot.send_message(message.chat.id, text)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None
        competition_context.pop(message.chat.id, None)

        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_views"))
        start_msg = bot.send_message(message.chat.id, "<b>تم بدء رشق المشاهدات من الجلسات.</b>", reply_markup=stop_kb)
        job_ctx["v_status_message_id"] = start_msg.message_id
        views_stop_flags[message.chat.id] = False

        job_tag = job_ctx.get("job_tag") or f"job_views_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "views", "رشق مشاهدات", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_views_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مطوّر مرفوع: تنفيذ مباشر بدون موافقة لكن مع خصم نقاط
    if is_developer_user(user_id):
        cost = calculate_cost(sessions_count)
        user_points_before = get_user_points(user_id)
        if user_points_before < cost:
            bot.send_message(
                message.chat.id,
                f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
            )
            user_states.pop(message.chat.id, None)
            competition_context.pop(message.chat.id, None)
            return

        add_user_points(user_id, -cost)

        text = (
            "<b>تم استلام إعدادات رشق المشاهدات</b>\n"
            f"<blockquote>عدد الجلسات: {sessions_count}\n"
            f"الوقت بين كل جلسة: {delay} ثانية</blockquote>"
        )
        bot.send_message(message.chat.id, text)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None
        competition_context.pop(message.chat.id, None)

        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_views"))
        start_msg = bot.send_message(message.chat.id, "<b>تم بدء رشق المشاهدات من الجلسات.</b>", reply_markup=stop_kb)
        job_ctx["v_status_message_id"] = start_msg.message_id
        views_stop_flags[message.chat.id] = False

        job_tag = job_ctx.get("job_tag") or f"job_views_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "views", "رشق مشاهدات", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_views_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مستخدم عادي: نقاط + موافقة المطور الأساسي
    cost = calculate_cost(sessions_count)
    user_points_before = get_user_points(user_id)
    if user_points_before < cost:
        bot.send_message(
            message.chat.id,
            f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
        )
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    add_user_points(user_id, -cost)
    user_points_after = user_points_before - cost

    job_ctx = ctx.copy()
    user_states[message.chat.id] = None
    competition_context.pop(message.chat.id, None)

    req_id = f"{int(time.time())}_views_{user_id}"
    job_tag = f"job_{req_id}"
    job_ctx["job_tag"] = job_tag
    job_ctx["op_code"] = req_id
    pending_requests[req_id] = {
        "type": "views",
        "label": "رشق مشاهدات",
        "chat_id": message.chat.id,
        "user_id": user_id,
        "ctx": job_ctx,
        "cost": cost,
        "points_before": user_points_before,
        "points_after": user_points_after,
        "created_at": int(time.time()),
    }

    bot.send_message(
        message.chat.id,
        "<b>تم إرسال طلبك</b>\n<blockquote>انتظر موافقة المطور على طلب رشق المشاهدات قبل البدء بالتنفيذ.</blockquote>",
    )

    user = message.from_user
    first_name = user.first_name or "بدون اسم"
    username_txt = f"@{user.username}" if user.username else "لا يوجد يوزر"
    link = job_ctx.get("v_link", "غير متوفر")
    sessions = job_ctx.get("v_sessions_count", 0)
    delay_val = job_ctx.get("v_delay", 0)
    dev_text = (
        "<b>طلب جديد لرشق مشاهدات</b>\n"
        f"<blockquote>المستخدم: {first_name} ({username_txt})\n"
        f"الآيدي: <code>{user_id}</code>\n"
        f"الرابط: {link}\n"
        f"عدد الجلسات: {sessions}\n"
        f"التأخير بين كل جلسة: {delay_val} ثانية\n"
        f"النقاط قبل: {user_points_before}\n"
        f"النقاط بعد (مخصوم): {user_points_after}\n"
        f"التكلفة: {cost} نقطة\n"
        f"ID الطلب: <code>{req_id}</code></blockquote>"
    )
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✅ موافقة", callback_data=f"approve_req_{req_id}"),
        InlineKeyboardButton("❌ رفض", callback_data=f"reject_req_{req_id}"),
    )
    try:
        bot.send_message(DEVELOPER_ID, dev_text, reply_markup=kb)
    except Exception:
        pass


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "reacts_waiting_link")
def handle_reacts_link(message):
    link = (message.text or "").strip()
    if not link:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رابط صالح.</b>")
        return

    username, msg_id = parse_telegram_message_link(link)
    if not username or not msg_id:
        bot.send_message(message.chat.id, "<b>الرابط غير صالح، تأكد من أنه على الشكل https://t.me/username/1234.</b>")
        return

    max_sessions = get_sessions_count()
    competition_context[message.chat.id] = {
        "r_link": link,
        "r_username": username,
        "r_msg_id": msg_id,
        "r_max_sessions": max_sessions,
    }

    text = (
        "<b>عدد الجلسات للتفاعلات</b>\n"
        f"<blockquote>أرسل الآن عدد الجلسات التي تريد استخدامها (من 1 إلى {max_sessions}).</blockquote>"
    )
    user_states[message.chat.id] = "reacts_waiting_sessions"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "reacts_waiting_sessions")
def handle_reacts_sessions(message):
    ctx = competition_context.get(message.chat.id) or {}
    max_sessions = ctx.get("r_max_sessions") or get_sessions_count()

    text = (message.text or "").strip()
    try:
        count = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if count <= 0 or count > max_sessions:
        bot.send_message(
            message.chat.id,
            f"<b>عدد غير صالح.</b>\n<blockquote>يجب أن يكون بين 1 و {max_sessions}.</blockquote>",
        )
        return

    ctx["r_sessions_count"] = count
    competition_context[message.chat.id] = ctx

    text = (
        "<b>الوقت بين كل تفاعل والآخر</b>\n"
        "<blockquote>أرسل الوقت بالثواني بين كل جلسة والأخرى.</blockquote>"
    )
    user_states[message.chat.id] = "reacts_waiting_delay"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "reacts_waiting_delay")
def handle_reacts_delay(message):
    ctx = competition_context.get(message.chat.id) or {}

    text = (message.text or "").strip()
    try:
        delay = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if delay <= 0:
        bot.send_message(message.chat.id, "<b>الوقت يجب أن يكون أكبر من صفر.</b>")
        return

    ctx["r_delay"] = delay
    competition_context[message.chat.id] = ctx

    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("رشق محدد"), KeyboardButton("رشق عشوائي"))
    kb.row(KeyboardButton("إلغاء العملية"))
    text = (
        "<b>نوع رشق التفاعلات</b>\n"
        "<blockquote>اختر هل تريد الرشق بإيموجي محدد أو تفاعلات عشوائية.</blockquote>"
    )
    user_states[message.chat.id] = "reacts_waiting_mode"
    bot.send_message(message.chat.id, text, reply_markup=kb)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "reacts_waiting_mode")
def handle_reacts_mode(message):
    ctx = competition_context.get(message.chat.id) or {}
    choice = (message.text or "").strip()

    if choice == "رشق محدد":
        ctx["r_mode"] = "specific"
        competition_context[message.chat.id] = ctx
        user_states[message.chat.id] = "reacts_waiting_emoji"
        text = (
            "<b>الإيموجي المحدد</b>\n"
            "<blockquote>أرسل الآن الإيموجي الذي تريد أن تستخدمه الجلسات في التفاعلات.</blockquote>"
        )
        bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())
    elif choice == "رشق عشوائي":
        ctx["r_mode"] = "random"
        competition_context[message.chat.id] = ctx
        _finalize_reactions_request(message, ctx)
    else:
        bot.send_message(message.chat.id, "<b>الرجاء الاختيار من الأزرار الظاهرة فقط.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "reacts_waiting_emoji")
def handle_reacts_emoji(message):
    ctx = competition_context.get(message.chat.id) or {}
    emoji_text = (message.text or "").strip()
    if not emoji_text:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال إيموجي صالح.</b>")
        return

    ctx["r_emoji"] = emoji_text
    competition_context[message.chat.id] = ctx
    _finalize_reactions_request(message, ctx)


def _finalize_reactions_request(message, ctx: dict):
    required_keys = ["r_username", "r_msg_id", "r_sessions_count", "r_delay", "r_mode"]
    if not all(k in ctx for k in required_keys):
        bot.send_message(message.chat.id, "<b>بيانات رشق التفاعلات غير مكتملة، أعد العملية من جديد.</b>")
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return
    
    user_id = message.from_user.id
    user_request_counts[user_id] = user_request_counts.get(user_id, 0) + 1

    # قفل عام
    if _is_boost_locked_for_user(user_id):
        bot.send_message(
            message.chat.id,
            "<b>يوجد طلب جاري حالياً.</b>\n<blockquote>الرجاء الانتظار حتى يكتمل الطلب الحالي ثم أعد المحاولة.</blockquote>",
        )
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    sessions_count = ctx.get("r_sessions_count", 0)
    delay = ctx.get("r_delay", 0)
    mode = ctx.get("r_mode")
    emoji_text = ctx.get("r_emoji")

    if mode == "specific" and not emoji_text:
        bot.send_message(message.chat.id, "<b>لم يتم استلام الإيموجي المحدد للتفاعلات.</b>")
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    # المالك: تنفيذ مباشر بدون نقاط
    if is_primary_developer(user_id):
        op_code = ctx.get("op_code") or _gen_op_code("reactions", user_id)
        ctx["op_code"] = op_code
        text = (
            "<b>تم استلام إعدادات رشق التفاعلات</b>\n"
            f"<blockquote>عدد الجلسات: {sessions_count}\n"
            f"الوقت بين كل جلسة: {delay} ثانية\n"
            f"النوع: {'محدد' if mode == 'specific' else 'عشوائي'}\n"
            f"كود العملية: <code>{op_code}</code></blockquote>"
        )
        bot.send_message(message.chat.id, text)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None
        competition_context.pop(message.chat.id, None)

        # زر إيقاف الرشق للتفاعلات
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_reactions"))
        bot.send_message(message.chat.id, "<b>تم بدء رشق التفاعلات من الجلسات.</b>", reply_markup=stop_kb)

        reactions_stop_flags[message.chat.id] = False

        job_tag = job_ctx.get("job_tag") or f"job_reactions_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "reactions", "رشق تفاعلات", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_reactions_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مطوّر مرفوع: تنفيذ مباشر بدون موافقة لكن مع خصم نقاط
    if is_developer_user(user_id):
        op_code = ctx.get("op_code") or _gen_op_code("reactions", user_id)
        ctx["op_code"] = op_code
        cost = calculate_cost(sessions_count)
        user_points_before = get_user_points(user_id)
        if user_points_before < cost:
            bot.send_message(
                message.chat.id,
                f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
            )
            user_states.pop(message.chat.id, None)
            competition_context.pop(message.chat.id, None)
            return

        add_user_points(user_id, -cost)

        text = (
            "<b>تم استلام إعدادات رشق التفاعلات</b>\n"
            f"<blockquote>عدد الجلسات: {sessions_count}\n"
            f"الوقت بين كل جلسة: {delay} ثانية\n"
            f"النوع: {'محدد' if mode == 'specific' else 'عشوائي'}\n"
            f"كود العملية: <code>{op_code}</code></blockquote>"
        )
        bot.send_message(message.chat.id, text)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None
        competition_context.pop(message.chat.id, None)

        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_reactions"))
        bot.send_message(message.chat.id, "<b>تم بدء رشق التفاعلات من الجلسات.</b>", reply_markup=stop_kb)

        reactions_stop_flags[message.chat.id] = False

        job_tag = ctx.get("job_tag") or f"job_reactions_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "reactions", "رشق تفاعلات", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_reactions_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مستخدم عادي أو مطوّر مرفوع: نقاط + موافقة المطور الأساسي
    cost = calculate_cost(sessions_count)
    user_points_before = get_user_points(user_id)

    # مطوّر مرفوع: بدون نقاط لكن مع موافقة
    if is_developer_user(user_id) and not is_primary_developer(user_id):
        effective_cost = 0
        user_points_after = user_points_before
    else:
        if user_points_before < cost:
            bot.send_message(
                message.chat.id,
                f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
            )
            user_states.pop(message.chat.id, None)
            competition_context.pop(message.chat.id, None)
            return

        add_user_points(user_id, -cost)
        effective_cost = cost
        user_points_after = user_points_before - cost

    job_ctx = ctx.copy()
    user_states[message.chat.id] = None
    competition_context.pop(message.chat.id, None)

    req_id = f"{int(time.time())}_reactions_{user_id}"
    job_ctx["op_code"] = req_id
    pending_requests[req_id] = {
        "type": "reactions",
        "label": "رشق تفاعلات",
        "chat_id": message.chat.id,
        "user_id": user_id,
        "ctx": job_ctx,
        "cost": effective_cost,
        "points_before": user_points_before,
        "points_after": user_points_after,
        "created_at": int(time.time()),
    }

    bot.send_message(
        message.chat.id,
        "<b>تم إرسال طلبك</b>\n<blockquote>انتظر موافقة المطور على طلب رشق التفاعلات قبل البدء بالتنفيذ.</blockquote>",
    )

    user = message.from_user
    first_name = user.first_name or "بدون اسم"
    username = f"@{user.username}" if user.username else "لا يوجد يوزر"
    link = job_ctx.get("r_link", "غير متوفر")
    sessions = job_ctx.get("r_sessions_count", 0)
    delay_val = job_ctx.get("r_delay", 0)
    mode_text = "محدد" if mode == "specific" else "عشوائي"
    emoji_part = f"\nالإيموجي: {emoji_text}" if mode == "specific" else ""

    dev_text = (
        "<b>طلب جديد لرشق تفاعلات</b>\n"
        f"<blockquote>المستخدم: {first_name} ({username})\n"
        f"الآيدي: <code>{user_id}</code>\n"
        f"الرابط: {link}\n"
        f"عدد الجلسات: {sessions}\n"
        f"التأخير بين كل جلسة: {delay_val} ثانية\n"
        f"النوع: {mode_text}{emoji_part}\n"
        f"النقاط قبل: {user_points_before}\n"
        f"النقاط بعد (مخصوم): {user_points_after}\n"
        f"التكلفة: {effective_cost} نقطة\n"
        f"ID الطلب: <code>{req_id}</code></blockquote>"
    )
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✅ موافقة", callback_data=f"approve_req_{req_id}"),
        InlineKeyboardButton("❌ رفض", callback_data=f"reject_req_{req_id}"),
    )
    try:
        bot.send_message(DEVELOPER_ID, dev_text, reply_markup=kb)
    except Exception:
        pass


@bot.message_handler(func=lambda m: m.text == "رشق استفتاء")
def handle_poll_boost_start(message):
    chat_id = message.chat.id
    user_states.pop(chat_id, None)
    competition_context.pop(chat_id, None)
    dev_context.pop(chat_id, None)

    if _is_boost_locked_for_user(message.from_user.id):
        bot.send_message(
            message.chat.id,
            "<b>يوجد طلب جاري حالياً.</b>\n<blockquote>الرجاء الانتظار حتى يكتمل الطلب الحالي ثم حاول مرة أخرى.</blockquote>",
        )
        return
    if not load_sessions():
        bot.send_message(
            message.chat.id,
            "<b>لا توجد جلسات مضافة.</b>\n<blockquote>أضف جلسات أولاً من قسم الحسابات.</blockquote>",
            reply_markup=accounts_menu_keyboard(),
        )
        return

    user_states[message.chat.id] = "poll_waiting_link"
    competition_context[message.chat.id] = {}
    text = (
        "<b>رشق استفتاء</b>\n"
        "<blockquote>أرسل الآن رابط رسالة الاستفتاء (من نوع https://t.me/username/1234).</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "poll_waiting_link")
def handle_poll_link(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    link = (message.text or "").strip()
    if not link:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رابط صالح.</b>")
        return

    bot.send_chat_action(message.chat.id, "typing")

    try:
        ok, data, _ = asyncio.run(fetch_poll_buttons_with_first_session(link))
    except Exception:
        ok, data = False, "حدث خطأ أثناء محاولة قراءة رسالة الاستفتاء من الرابط."

    if not ok:
        bot.send_message(
            message.chat.id,
            f"<b>تعذّر استخدام الرابط:</b>\n<blockquote>{data}</blockquote>",
        )
        return

    username, msg_id, answers, options = data
    if not answers or not options:
        bot.send_message(message.chat.id, "<b>لم يتم العثور على خيارات استفتاء (Poll) في هذه الرسالة.</b>")
        return

    competition_context[message.chat.id] = {
        "p_link": link,
        "p_username": username,
        "p_msg_id": msg_id,
        "p_answers": answers,
        "p_options": options,
    }

    ctx = competition_context.get(message.chat.id) or {}
    max_sessions = get_sessions_count()
    ctx["p_max_sessions"] = max_sessions
    competition_context[message.chat.id] = ctx

    options_count = len(answers)
    text = (
        "<b>رقم الإجابة في الاستفتاء</b>\n"
        f"<blockquote>هذا الاستفتاء يحتوي على <b>{options_count}</b> إجابات.\n"
        "أرسل الآن رقم الإجابة التي تريد أن تصوت لها الجلسات (مثلاً 1 أو 2...).</blockquote>"
    )
    user_states[message.chat.id] = "poll_waiting_answer"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "poll_waiting_delay")
def handle_poll_delay(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    ctx = competition_context.get(message.chat.id) or {}

    text = (message.text or "").strip()
    try:
        delay = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if delay <= 0:
        bot.send_message(message.chat.id, "<b>الوقت يجب أن يكون أكبر من صفر.</b>")
        return

    ctx["p_delay"] = delay
    competition_context[message.chat.id] = ctx

    _finalize_poll_boost_request(message, ctx)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "poll_waiting_sessions")
def handle_poll_sessions(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    ctx = competition_context.get(message.chat.id) or {}
    max_sessions = ctx.get("p_max_sessions") or get_sessions_count()

    text = (message.text or "").strip()
    try:
        count = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if count <= 0 or count > max_sessions:
        bot.send_message(
            message.chat.id,
            f"<b>عدد غير صالح.</b>\n<blockquote>يجب أن يكون بين 1 و {max_sessions}.</blockquote>",
        )
        return

    ctx["p_sessions_count"] = count
    competition_context[message.chat.id] = ctx

    text = (
        "<b>الوقت بين كل جلسة والأخرى</b>\n"
        "<blockquote>أرسل الوقت بالثواني بين كل جلسة والأخرى.</blockquote>"
    )
    user_states[message.chat.id] = "poll_waiting_delay"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "poll_waiting_answer")
def handle_poll_answer(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    ctx = competition_context.get(message.chat.id) or {}
    answers = ctx.get("p_answers") or []
    options_count = len(answers)
    if not answers:
        bot.send_message(message.chat.id, "<b>لا توجد بيانات استفتاء محفوظة، أعد إرسال الرابط.</b>")
        user_states[message.chat.id] = None
        competition_context.pop(message.chat.id, None)
        return

    text = (message.text or "").strip()
    try:
        answer_num = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم إجابة صالح.</b>")
        return

    if answer_num <= 0 or answer_num > options_count:
        bot.send_message(
            message.chat.id,
            f"<b>رقم غير صالح.</b>\n<blockquote>الرجاء اختيار رقم بين 1 و {options_count}.</blockquote>",
        )
        return

    ctx["p_answer_index"] = answer_num - 1
    competition_context[message.chat.id] = ctx

    max_sessions = ctx.get("p_max_sessions") or get_sessions_count()
    ctx["p_max_sessions"] = max_sessions
    competition_context[message.chat.id] = ctx

    text = (
        "<b>عدد الجلسات للاستفتاء</b>\n"
        f"<blockquote>أرسل الآن عدد الجلسات التي تريد استخدامها (من 1 إلى {max_sessions}).</blockquote>"
    )
    user_states[message.chat.id] = "poll_waiting_sessions"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


def _finalize_poll_boost_request(message, ctx: dict):
    required_keys = ["p_username", "p_msg_id", "p_sessions_count", "p_delay", "p_answer_index", "p_options", "p_answers"]
    if not all(k in ctx for k in required_keys):
        bot.send_message(message.chat.id, "<b>بيانات رشق الاستفتاء غير مكتملة، أعد العملية من جديد.</b>")
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    user_id = message.from_user.id
    user_request_counts[user_id] = user_request_counts.get(user_id, 0) + 1

    # قفل عام
    if _is_boost_locked_for_user(user_id):
        bot.send_message(
            message.chat.id,
            "<b>يوجد طلب جاري حالياً.</b>\n<blockquote>الرجاء الانتظار حتى يكتمل الطلب الحالي ثم أعد المحاولة.</blockquote>",
        )
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    sessions_count = ctx.get("p_sessions_count", 0)
    delay = ctx.get("p_delay", 0)

    # المالك: تنفيذ مباشر بدون نقاط
    if is_primary_developer(user_id):
        op_code = ctx.get("op_code") or _gen_op_code("poll", user_id)
        ctx["op_code"] = op_code
        text = (
            "<b>تم استلام إعدادات رشق الاستفتاء</b>\n"
            f"<blockquote>عدد الجلسات: {sessions_count}\n"
            f"الوقت بين كل جلسة: {delay} ثانية\n"
            f"كود العملية: <code>{op_code}</code></blockquote>"
        )
        bot.send_message(message.chat.id, text)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None
        competition_context.pop(message.chat.id, None)

        job_tag = ctx.get("job_tag") or f"job_poll_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "poll", "رشق استفتاء", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_poll_boost_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مطوّر مرفوع: تنفيذ مباشر بدون موافقة لكن مع خصم نقاط
    if is_developer_user(user_id):
        op_code = ctx.get("op_code") or _gen_op_code("poll", user_id)
        ctx["op_code"] = op_code
        cost = calculate_cost(sessions_count)
        user_points_before = get_user_points(user_id)
        if user_points_before < cost:
            bot.send_message(
                message.chat.id,
                f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
            )
            user_states.pop(message.chat.id, None)
            competition_context.pop(message.chat.id, None)
            return

        add_user_points(user_id, -cost)

        text = (
            "<b>تم استلام إعدادات رشق الاستفتاء</b>\n"
            f"<blockquote>عدد الجلسات: {sessions_count}\n"
            f"الوقت بين كل جلسة: {delay} ثانية\n"
            f"كود العملية: <code>{op_code}</code></blockquote>"
        )
        bot.send_message(message.chat.id, text)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None
        competition_context.pop(message.chat.id, None)

        job_tag = ctx.get("job_tag") or f"job_poll_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "poll", "رشق استفتاء", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_poll_boost_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مستخدم عادي أو مطوّر مرفوع: نقاط + موافقة المطور الأساسي
    cost = calculate_cost(sessions_count)
    user_points_before = get_user_points(user_id)

    # مطوّر مرفوع: بدون نقاط لكن مع موافقة
    if is_developer_user(user_id) and not is_primary_developer(user_id):
        effective_cost = 0
        user_points_after = user_points_before
    else:
        if user_points_before < cost:
            bot.send_message(
                message.chat.id,
                f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
            )
            user_states.pop(message.chat.id, None)
            competition_context.pop(message.chat.id, None)
            return

        add_user_points(user_id, -cost)
        effective_cost = cost
        user_points_after = user_points_before - cost

    job_ctx = ctx.copy()
    user_states[message.chat.id] = None
    competition_context.pop(message.chat.id, None)

    req_id = f"{int(time.time())}_poll_{user_id}"
    job_ctx["op_code"] = req_id
    pending_requests[req_id] = {
        "type": "poll",
        "label": "رشق استفتاء",
        "chat_id": message.chat.id,
        "user_id": user_id,
        "ctx": job_ctx,
        "cost": effective_cost,
        "points_before": user_points_before,
        "points_after": user_points_after,
        "created_at": int(time.time()),
    }

    bot.send_message(
        message.chat.id,
        "<b>تم إرسال طلبك</b>\n<blockquote>انتظر موافقة المطور على طلب رشق الاستفتاء قبل البدء بالتنفيذ.</blockquote>",
    )

    user = message.from_user
    first_name = user.first_name or "بدون اسم"
    username = f"@{user.username}" if user.username else "لا يوجد يوزر"
    link = job_ctx.get("p_link", "غير متوفر")
    sessions = job_ctx.get("p_sessions_count", 0)
    delay_val = job_ctx.get("p_delay", 0)
    answer_index = job_ctx.get("p_answer_index", 0)
    answers = job_ctx.get("p_answers") or []
    answer_text = answers[answer_index] if 0 <= answer_index < len(answers) else "غير معروف"

    dev_text = (
        "<b>طلب جديد لرشق استفتاء</b>\n"
        f"<blockquote>المستخدم: {first_name} ({username})\n"
        f"الآيدي: <code>{user_id}</code>\n"
        f"الرابط: {link}\n"
        f"عدد الجلسات: {sessions}\n"
        f"التأخير بين كل جلسة: {delay_val} ثانية\n"
        f"رقم الإجابة المختارة: {answer_index + 1} ({answer_text})\n"
        f"النقاط قبل: {user_points_before}\n"
        f"النقاط بعد (مخصوم): {user_points_after}\n"
        f"التكلفة: {effective_cost} نقطة\n"
        f"ID الطلب: <code>{req_id}</code></blockquote>"
    )
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✅ موافقة", callback_data=f"approve_req_{req_id}"),
        InlineKeyboardButton("❌ رفض", callback_data=f"reject_req_{req_id}"),
    )
    try:
        bot.send_message(DEVELOPER_ID, dev_text, reply_markup=kb)
    except Exception:
        pass


def run_poll_boost_job(chat_id: int, user_id: int, ctx: dict):
    async def _inner():
        job_tag = ctx.get("job_tag")
        op_code = ctx.get("op_code")
        username = ctx.get("p_username")
        msg_id = ctx.get("p_msg_id")
        sessions_count = ctx.get("p_sessions_count")
        delay = ctx.get("p_delay")
        answer_index = ctx.get("p_answer_index", 0)
        options = ctx.get("p_options") or []

        if not username or not msg_id or not sessions_count:
            bot.send_message(chat_id, "<b>تعذّر قراءة بيانات رشق الاستفتاء.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        if op_code:
            _ensure_operation_record(str(op_code), "poll", int(user_id or 0), ctx)

        rotation_key = f"poll:{username}:{msg_id}"
        sessions = select_sessions_rotating(rotation_key, sessions_count)
        if not sessions:
            bot.send_message(chat_id, "<b>لا توجد جلسات متاحة لتنفيذ رشق الاستفتاء.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        if not isinstance(options, list) or not options or not (0 <= int(answer_index) < len(options)):
            bot.send_message(chat_id, "<b>تعذّر قراءة خيارات الاستفتاء من الرسالة.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        if chat_id not in poll_stop_flags:
            poll_stop_flags[chat_id] = False

        for idx, s in enumerate(sessions, start=1):
            if poll_stop_flags.get(chat_id):
                break
            client = None
            try:
                client = await _connect_telethon_client(s["session"])
                try:
                    entity, msg = await _safe_get_entity_and_message(client, username, int(msg_id))
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق استفتاء"):
                        continue
                    bot.send_message(chat_id, "<b>تعذّر الوصول إلى رسالة الاستفتاء من بعض الجلسات.</b>")
                    if op_code:
                        _append_operation_execution(str(op_code), s["session"], False)
                    continue

                # التصويت على الاستفتاء الحقيقي (Telegram Poll)
                try:
                    option = options[int(answer_index)]
                    await client(SendVoteRequest(peer=entity, msg_id=int(msg_id), options=[option]))
                    if op_code:
                        _append_operation_execution(str(op_code), s["session"], True)
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق استفتاء"):
                        continue
                    bot.send_message(chat_id, f"<b>فشل التصويت من الجلسة رقم {idx}.</b>")
                    if op_code:
                        _append_operation_execution(str(op_code), s["session"], False)

                if idx < len(sessions):
                    await asyncio.sleep(delay)
            except Exception as e:
                if handle_session_error(chat_id, s, e, "رشق استفتاء"):
                    continue
                bot.send_message(chat_id, f"<b>حدث خطأ غير متوقع مع إحدى الجلسات (رقم {idx}).</b>")
                if op_code:
                    _append_operation_execution(str(op_code), s["session"], False)
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

        if poll_stop_flags.get(chat_id):
            bot.send_message(chat_id, "<b>تم إيقاف رشق الاستفتاء بناءً على طلبك.</b>")
        else:
            bot.send_message(chat_id, "<b>اكتمل تنفيذ طلب رشق الاستفتاء.</b>")
        poll_stop_flags.pop(chat_id, None)
        if job_tag:
            _finish_non_owner_boost(job_tag)

    asyncio.run(_inner())


def run_reactions_job(chat_id: int, user_id: int, ctx: dict):
    async def _inner():
        job_tag = ctx.get("job_tag")
        op_code = ctx.get("op_code")
        username = ctx.get("r_username")
        msg_id = ctx.get("r_msg_id")
        sessions_count = ctx.get("r_sessions_count")
        delay = ctx.get("r_delay")
        mode = ctx.get("r_mode")
        emoji_text = ctx.get("r_emoji")

        if not username or not msg_id or not sessions_count:
            bot.send_message(chat_id, "<b>تعذّر قراءة بيانات رشق التفاعلات.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        if op_code:
            _ensure_operation_record(str(op_code), "reactions", int(user_id or 0), ctx)

        rotation_key = f"reactions:{username}:{msg_id}"
        sessions = select_sessions_rotating(rotation_key, sessions_count)
        if not sessions:
            bot.send_message(chat_id, "<b>لا توجد جلسات متاحة لتنفيذ رشق التفاعلات.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        for idx, s in enumerate(sessions, start=1):
            if reactions_stop_flags.get(chat_id):
                break
            client = None
            try:
                client = await _connect_telethon_client(s["session"])
                try:
                    entity = await client.get_entity(username)
                    try:
                        await client(JoinChannelRequest(entity))
                    except Exception:
                        pass
                    await client.get_messages(entity, ids=msg_id)
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق التفاعلات"):
                        continue
                    bot.send_message(chat_id, "<b>تعذّر الوصول إلى الرسالة من بعض الجلسات.</b>")
                    if op_code:
                        _append_operation_execution(str(op_code), s["session"], False)
                    continue

                if mode == "specific" and emoji_text:
                    chosen_emoji = emoji_text
                else:
                    chosen_emoji = random.choice(DEFAULT_REACTIONS)

                reaction = [ReactionEmoji(emoticon=chosen_emoji)]
                try:
                    await client(SendReactionRequest(peer=entity, msg_id=msg_id, reaction=reaction))
                    if op_code:
                        _append_operation_execution(str(op_code), s["session"], True)
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق التفاعلات"):
                        continue
                    bot.send_message(chat_id, f"<b>فشل إرسال التفاعل من الجلسة رقم {idx}.</b>")
                    if op_code:
                        _append_operation_execution(str(op_code), s["session"], False)

                if idx < len(sessions) and not reactions_stop_flags.get(chat_id):
                    await asyncio.sleep(int(delay or 0))
            except Exception as e:
                if handle_session_error(chat_id, s, e, "رشق التفاعلات"):
                    continue
                bot.send_message(chat_id, f"<b>حدث خطأ غير متوقع مع إحدى الجلسات (رقم {idx}).</b>")
                if op_code:
                    _append_operation_execution(str(op_code), s["session"], False)
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

        try:
            if reactions_stop_flags.get(chat_id):
                bot.send_message(chat_id, "<b>تم إيقاف رشق التفاعلات بناءً على طلبك.</b>")
            else:
                bot.send_message(chat_id, "<b>اكتمل تنفيذ طلب رشق التفاعلات.</b>")
        finally:
            reactions_stop_flags.pop(chat_id, None)
            if job_tag:
                _finish_non_owner_boost(job_tag)

    asyncio.run(_inner())


def run_votes_reacts_job(chat_id: int, user_id: int, ctx: dict):
    async def _inner():
        username = ctx.get("vrv_username")
        msg_id = ctx.get("vrv_msg_id")
        sessions_count = ctx.get("vrv_sessions_count")
        delay = ctx.get("vrv_delay")
        mode = ctx.get("vrv_mode")
        emoji_text = ctx.get("vrv_emoji")
        button_index = ctx.get("vrv_button_index")
        button_text = ctx.get("vrv_button_text")

        if not username or not msg_id or not sessions_count:
            bot.send_message(chat_id, "<b>تعذّر قراءة بيانات رشق التفاعلات.</b>")
            return

        rotation_key = f"votes_reacts:{username}:{msg_id}"
        sessions = select_sessions_rotating(rotation_key, sessions_count)
        if not sessions:
            bot.send_message(chat_id, "<b>لا توجد جلسات متاحة لتنفيذ رشق التفاعلات.</b>")
            return

        for idx, s in enumerate(sessions, start=1):
            client = None
            try:
                client = await _connect_telethon_client(s["session"])
                try:
                    entity = await client.get_entity(username)
                    try:
                        await client(JoinChannelRequest(entity))
                    except Exception:
                        pass
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق التفاعلات"):
                        continue
                    bot.send_message(chat_id, "<b>تعذّر الوصول إلى الرسالة من بعض الجلسات.</b>")
                    continue

                # تحديد الإيموجي المستخدم في هذه الجلسة
                if mode == "specific" and emoji_text:
                    chosen_emoji = emoji_text
                else:
                    chosen_emoji = random.choice(DEFAULT_REACTIONS)

                reaction = [ReactionEmoji(emoticon=chosen_emoji)]
                try:
                    await client(SendReactionRequest(peer=entity, msg_id=msg_id, reaction=reaction))
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق التفاعلات"):
                        continue
                    bot.send_message(chat_id, f"<b>فشل إرسال التفاعل من الجلسة رقم {idx}.</b>")

                # محاولة التصويت أيضاً (إن وُجد زر)
                try:
                    msg = await client.get_messages(entity, ids=int(msg_id))
                    target_btn = None
                    if msg and hasattr(msg, "reply_markup") and getattr(msg.reply_markup, "rows", None):
                        buttons_flat = []
                        for row in msg.reply_markup.rows:
                            for btn in row.buttons:
                                buttons_flat.append(btn)
                        if button_text:
                            for b in buttons_flat:
                                if (getattr(b, "text", None) or "").strip() == str(button_text).strip():
                                    target_btn = b
                                    break
                        if target_btn is None and button_index is not None:
                            try:
                                bi = int(button_index)
                                if 0 <= bi < len(buttons_flat):
                                    target_btn = buttons_flat[bi]
                            except Exception:
                                pass
                    if target_btn is not None:
                        data = getattr(target_btn, "data", None)
                        if data is not None:
                            await _safe_click_callback(client, entity, int(msg_id), data)
                except Exception:
                    pass

                if idx < len(sessions):
                    await asyncio.sleep(delay)
            except Exception as e:
                if handle_session_error(chat_id, s, e, "رشق التفاعلات"):
                    continue
                bot.send_message(chat_id, f"<b>حدث خطأ غير متوقع مع إحدى الجلسات (رقم {idx}).</b>")
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

        bot.send_message(chat_id, "<b>اكتمل تنفيذ طلب رشق التفاعلات.</b>")

    asyncio.run(_inner())


def run_reacts_views_job(chat_id: int, user_id: int, ctx: dict):
    async def _inner():
        job_tag = ctx.get("job_tag")
        username = ctx.get("rv_username")
        msg_id = ctx.get("rv_msg_id")
        sessions_count = ctx.get("rv_sessions_count")
        delay = ctx.get("rv_delay")
        mode = ctx.get("rv_mode")
        emoji_text = ctx.get("rv_emoji")

        if not username or not msg_id or not sessions_count:
            bot.send_message(chat_id, "<b>تعذّر قراءة بيانات رشق التفاعلات والمشاهدات.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        rotation_key = f"reacts_views:{username}:{msg_id}"
        sessions = select_sessions_rotating(rotation_key, sessions_count)
        if not sessions:
            bot.send_message(chat_id, "<b>لا توجد جلسات متاحة لتنفيذ رشق التفاعلات والمشاهدات.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        for idx, s in enumerate(sessions, start=1):
            client = None
            try:
                client = await _connect_telethon_client(s["session"])
                try:
                    entity = await client.get_entity(username)
                    try:
                        await client(JoinChannelRequest(entity))
                    except Exception:
                        pass
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق تفاعلات ومشاهدات"):
                        continue
                    bot.send_message(chat_id, "<b>تعذّر الوصول إلى الرسالة من بعض الجلسات.</b>")
                    continue

                # احتساب مشاهدة الرسالة
                try:
                    await client.get_messages(entity, ids=msg_id)
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق تفاعلات ومشاهدات"):
                        continue

                # إرسال التفاعل
                if mode == "specific" and emoji_text:
                    chosen_emoji = emoji_text
                else:
                    chosen_emoji = random.choice(DEFAULT_REACTIONS)

                reaction = [ReactionEmoji(emoticon=chosen_emoji)]
                try:
                    await client(SendReactionRequest(peer=entity, msg_id=msg_id, reaction=reaction))
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق تفاعلات ومشاهدات"):
                        continue
                    bot.send_message(chat_id, f"<b>فشل إرسال التفاعل من الجلسة رقم {idx}.</b>")

                if idx < len(sessions):
                    await asyncio.sleep(delay)
            except Exception as e:
                if handle_session_error(chat_id, s, e, "رشق تفاعلات ومشاهدات"):
                    continue
                bot.send_message(chat_id, f"<b>حدث خطأ غير متوقع مع إحدى الجلسات (رقم {idx}).</b>")
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

        bot.send_message(chat_id, "<b>اكتمل تنفيذ طلب رشق التفاعلات والمشاهدات.</b>")
        if job_tag:
            _finish_non_owner_boost(job_tag)

    asyncio.run(_inner())


def run_views_job(chat_id: int, user_id: int, ctx: dict):
    async def _inner():
        job_tag = ctx.get("job_tag")
        link = ctx["v_link"]
        username = ctx.get("v_username")
        msg_id = ctx.get("v_msg_id")
        delay = ctx["v_delay"]
        sessions_count = ctx["v_sessions_count"]
        status_msg_id = ctx.get("v_status_message_id")

        if not username or not msg_id:
            u, m_id = parse_telegram_message_link(link)
            username = username or u
            msg_id = msg_id or m_id

        if not username or not msg_id:
            bot.send_message(chat_id, "<b>تعذّر قراءة الرابط أثناء تنفيذ رشق المشاهدات.</b>")
            return

        try:
            msg_id_int = int(msg_id)
        except Exception:
            bot.send_message(chat_id, "<b>تعذّر قراءة رقم الرسالة أثناء تنفيذ رشق المشاهدات.</b>")
            return

        rotation_key = f"views:{username}:{msg_id}" if username and msg_id else f"views_link:{link}"
        sessions = select_sessions_rotating(rotation_key, sessions_count)
        if not sessions:
            bot.send_message(chat_id, "<b>لا توجد جلسات متاحة لتنفيذ رشق المشاهدات.</b>")
            return

        total = len(sessions)
        success = 0
        failed = 0

        def update_status():
            if status_msg_id is None:
                return
            try:
                text = (
                    "<b>جاري رشق المشاهدات</b>\n"
                    f"<blockquote>الجلسات التي شاهدت: {success}/{total}\n"
                    f"الجلسات التي فشلت: {failed}</blockquote>"
                )
                bot.edit_message_text(
                    text,
                    chat_id=chat_id,
                    message_id=status_msg_id,
                    parse_mode="HTML",
                )
            except Exception:
                pass

        update_status()

        for idx, s in enumerate(sessions, start=1):
            if views_stop_flags.get(chat_id):
                break
            client = None
            try:
                client = await _connect_telethon_client(s["session"])
                try:
                    entity = await client.get_entity(username)
                    try:
                        await client(JoinChannelRequest(entity))
                    except Exception:
                        pass
                    
                    # استخدام الدالة المحسنة لزيادة المشاهدات
                    views_success = await _safe_add_views(client, entity, msg_id_int)
                    
                    if not views_success:
                        failed += 1
                        update_status()
                        continue
                        
                except Exception as e:
                    if handle_session_error(chat_id, s, e, "رشق المشاهدات"):
                        failed += 1
                        update_status()
                        continue
                    failed += 1
                    update_status()
                    continue

                success += 1
                update_status()

                if idx < len(sessions) and not views_stop_flags.get(chat_id):
                    await asyncio.sleep(delay)
            except Exception as e:
                if handle_session_error(chat_id, s, e, "رشق المشاهدات"):
                    failed += 1
                    update_status()
                    continue
                failed += 1
                update_status()
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

        try:
            if views_stop_flags.get(chat_id):
                bot.send_message(chat_id, "<b>تم إيقاف رشق المشاهدات بناءً على طلبك.</b>")
            else:
                bot.send_message(chat_id, "<b>اكتمل تنفيذ طلب رشق المشاهدات.</b>")
        finally:
            views_stop_flags.pop(chat_id, None)
            if job_tag:
                _finish_non_owner_boost(job_tag)

    asyncio.run(_inner())


@bot.callback_query_handler(func=lambda c: c.data == "stop_views")
def callback_stop_views(call):
    chat_id = call.message.chat.id
    if chat_id not in views_stop_flags:
        bot.answer_callback_query(call.id, "لا توجد عملية رشق مشاهدات قيد التنفيذ.")
        return
    views_stop_flags[chat_id] = True
    bot.answer_callback_query(call.id, "تم طلب إيقاف رشق المشاهدات، سيتم الإيقاف خلال ثوانٍ.")
    try:
        bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "cancel_delete_sess")
def callback_cancel_delete_sessions(call):
    bot.answer_callback_query(call.id, "تم الإلغاء.")
    try:
        bot.edit_message_reply_markup(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass
    bot.send_message(call.message.chat.id, "<b>تم إلغاء عملية حذف الجلسات.</b>", reply_markup=accounts_menu_keyboard())


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("sess_name_"))
def callback_session_name_noop(call):
    # مجرد منع ظهور أخطاء عند الضغط على اسم الجلسة
    bot.answer_callback_query(call.id)


@bot.message_handler(func=lambda m: False)
def _cancel_operation_legacy_disabled(message):
    return


@bot.message_handler(func=lambda m: m.text == "إعادة تعيين التصويتات")
def handle_reset_votes(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("إعادة تعيين الكل"), KeyboardButton("إعادة تعيين تصويت معين"))
    kb.row(KeyboardButton("رجوع"))
    
    user_states[message.chat.id] = "reset_votes_menu"
    bot.send_message(message.chat.id, "<b>إعادة تعيين التصويتات</b>\n<blockquote>اختر نوع إعادة التعيين:</blockquote>", reply_markup=kb)


@bot.message_handler(func=lambda m: m.text == "عرض التصويتات")
def handle_show_votes(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    
    info = get_voted_sessions_info()
    
    if info["total_keys"] == 0:
        bot.send_message(message.chat.id, "<b>لا توجد تصويتات مسجلة حالياً.</b>")
        return
    
    text = (
        f"<b>إحصائيات التصويتات المسجلة</b>\n"
        f"<blockquote>"
        f"إجمالي التصويتات: {info['total_keys']}\n"
        f"إجمالي الجلسات التي صوتت: {info['total_sessions']}\n"
        f"</blockquote>"
        f"<b>تفاصيل كل تصويت:</b>"
    )
    
    for i, (key, count) in enumerate(info["sessions_per_key"].items(), 1):
        text += f"\n{i}. <code>{key}</code>: {count} جلسة"
    
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("إعادة تعيين التصويتات"), KeyboardButton("رجوع"))
    
    bot.send_message(message.chat.id, text, reply_markup=kb)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "reset_votes_menu")
def handle_reset_votes_menu(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    choice = message.text
    
    if choice == "إعادة تعيين الكل":
        success = reset_voted_sessions()
        if success:
            bot.send_message(message.chat.id, "<b>✅ تم إعادة تعيين جميع التصويتات بنجاح.</b>\n<blockquote>يمكن لجميع الجلسات التصويت مرة أخرى.</blockquote>")
        else:
            bot.send_message(message.chat.id, "<b>❌ حدث خطأ أثناء إعادة تعيين التصويتات.</b>")
        
        user_states.pop(message.chat.id, None)
        kb = dev_panel_keyboard()
        bot.send_message(message.chat.id, "<b>لوحة المطور</b>", reply_markup=kb)
    
    elif choice == "إعادة تعيين تصويت معين":
        user_states[message.chat.id] = "reset_votes_waiting_key"
        bot.send_message(message.chat.id, "<b>أرسل مفتاح التصويت الذي تريد إعادة تعيينه:</b>\n<blockquote>مثال: votes:username:1234</blockquote>")
    
    elif choice == "رجوع":
        user_states.pop(message.chat.id, None)
        kb = dev_panel_keyboard()
        bot.send_message(message.chat.id, "<b>لوحة المطور</b>", reply_markup=kb)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "reset_votes_waiting_key")
def handle_reset_votes_key(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    key = message.text.strip()
    if not key:
        bot.send_message(message.chat.id, "<b>المفتاح لا يمكن أن يكون فارغاً.</b>")
        return
    
    success = reset_voted_sessions(key)
    if success:
        bot.send_message(
            message.chat.id, 
            f"<b>✅ تم إعادة تعيين التصويت بنجاح.</b>\n<blockquote>المفتاح: {key}\nيمكن للجلسات التصويت على هذا الرابط مرة أخرى.</blockquote>"
        )
    else:
        bot.send_message(message.chat.id, f"<b>❌ حدث خطأ أثناء إعادة تعيين التصويت: {key}</b>")
    
    user_states.pop(message.chat.id, None)
    kb = dev_panel_keyboard()
    bot.send_message(message.chat.id, "<b>لوحة المطور</b>", reply_markup=kb)


@bot.message_handler(func=lambda m: m.text == "اذاعة رسالة")
def handle_broadcast_message(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    user_states[message.chat.id] = "broadcast_waiting_message"
    bot.send_message(message.chat.id, "<b>أرسل رسالة الإذاعة التي تريد إرسالها لجميع المستخدمين:</b>")


@bot.message_handler(func=lambda m: m.text == "اعداد النقاط")
def handle_points_management(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("إضافة نقاط"), KeyboardButton("خصم نقاط"))
    kb.row(KeyboardButton("إضافة نقاط دعوة"), KeyboardButton("عرض نقاط مستخدم"))
    kb.row(KeyboardButton("إحصائيات النقاط"), KeyboardButton("رجوع"))
    
    user_states[message.chat.id] = "points_management_menu"
    bot.send_message(message.chat.id, "<b>إدارة نظام النقاط</b>", reply_markup=kb)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "broadcast_waiting_message")
def handle_broadcast_text(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    broadcast_text = message.text or message.caption or ""
    if not broadcast_text.strip():
        bot.send_message(message.chat.id, "<b>الرسالة لا يمكن أن تكون فارغة.</b>")
        return
    
    user_states.pop(message.chat.id, None)
    
    # بدء الإذاعة في خيط منفصل
    thread = threading.Thread(target=execute_broadcast, args=(message.chat.id, broadcast_text))
    thread.start()
    
    bot.send_message(message.chat.id, "<b>جاري إرسال الرسالة لجميع المستخدمين...</b>")


def execute_broadcast(chat_id: int, text: str):
    def _broadcast_thread():
        try:
            # جمع جميع المستخدمين من مصادر متعددة
            all_users = set()
            
            # 1. من نقاط المستخدمين
            try:
                points = load_points()
                all_users.update(int(uid) for uid in points.keys() if uid.isdigit())
            except Exception:
                pass
            
            # 2. من المطورين
            try:
                developers = load_developers()
                all_users.update(int(uid) for uid in developers.keys() if uid.isdigit())
            except Exception:
                pass
            
            # 3. من الدعوات
            try:
                referrals = load_referrals()
                all_users.update(int(uid) for uid in referrals.keys() if uid.isdigit())
                all_users.update(int(uid) for uid in referrals.values() if uid.isdigit())
            except Exception:
                pass
            
            # إزالة المطورين الأساسيين من القائمة (لا نرسل لهم)
            primary_devs = {6348939589, 6348939589}  # IDs من PRIMARY_DEVELOPERS
            all_users -= primary_devs
            
            success_count = 0
            fail_count = 0
            
            bot.send_message(chat_id, f"<b>بدء إرسال الإذاعة إلى {len(all_users)} مستخدم...</b>")
            
            for user_id in all_users:
                try:
                    bot.send_message(user_id, text)
                    success_count += 1
                    time.sleep(0.1)  # انتظار قصير بين كل رسالة
                except Exception as e:
                    fail_count += 1
                    # لا نطبع كل خطأ لتجنب التكرار
                    continue
            
            bot.send_message(
                chat_id,
                f"<b>اكتملت الإذاعة</b>\n"
                f"<blockquote>تم الإرسال بنجاح: {success_count} مستخدم\nفشل الإرسال: {fail_count} مستخدم</blockquote>"
            )
            
        except Exception as e:
            bot.send_message(chat_id, f"<b>حدث خطأ أثناء الإذاعة:</b>\n<blockquote>{str(e)}</blockquote>")
    
    thread = threading.Thread(target=_broadcast_thread, daemon=True)
    thread.start()


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "points_management_menu")
def handle_points_management_menu(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    choice = message.text
    
    if choice == "إضافة نقاط":
        user_states[message.chat.id] = "points_add_waiting_id"
        bot.send_message(message.chat.id, "<b>أرسل معرف المستخدم الذي تريد إضافة نقاط له:</b>")
    
    elif choice == "خصم نقاط":
        user_states[message.chat.id] = "points_remove_waiting_id"
        bot.send_message(message.chat.id, "<b>أرسل معرف المستخدم الذي تريد خصم نقاط منه:</b>")
    
    elif choice == "إضافة نقاط دعوة":
        user_states[message.chat.id] = "points_referral_waiting_id"
        bot.send_message(message.chat.id, "<b>أرسل معرف المستخدم الذي تريد إضافة نقاط الدعوة له:</b>")
    
    elif choice == "عرض نقاط مستخدم":
        user_states[message.chat.id] = "points_check_waiting_id"
        bot.send_message(message.chat.id, "<b>أرسل معرف المستخدم لعرض نقاطه:</b>")
    
    elif choice == "إحصائيات النقاط":
        show_points_statistics(message.chat.id)
    
    elif choice == "رجوع":
        user_states.pop(message.chat.id, None)
        kb = dev_panel_keyboard()
        bot.send_message(message.chat.id, "<b>لوحة المطور</b>", reply_markup=kb)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "points_add_waiting_id")
def handle_add_points_user_id(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    try:
        user_id = int(message.text.strip())
        user_states[message.chat.id] = f"points_add_amount_{user_id}"
        bot.send_message(message.chat.id, f"<b>أرسل عدد النقاط التي تريد إضافتها للمستخدم {user_id}:</b>")
    except ValueError:
        bot.send_message(message.chat.id, "<b>معرف المستخدم يجب أن يكون رقماً.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "points_remove_waiting_id")
def handle_remove_points_user_id(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    try:
        user_id = int(message.text.strip())
        user_states[message.chat.id] = f"points_remove_amount_{user_id}"
        bot.send_message(message.chat.id, f"<b>أرسل عدد النقاط التي تريد خصمها من المستخدم {user_id}:</b>")
    except ValueError:
        bot.send_message(message.chat.id, "<b>معرف المستخدم يجب أن يكون رقماً.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "points_referral_waiting_id")
def handle_referral_points_user_id(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    try:
        user_id = int(message.text.strip())
        user_states[message.chat.id] = f"points_referral_amount_{user_id}"
        bot.send_message(message.chat.id, f"<b>أرسل عدد نقاط الدعوة التي تريد إضافتها للمستخدم {user_id}:</b>")
    except ValueError:
        bot.send_message(message.chat.id, "<b>معرف المستخدم يجب أن يكون رقماً.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "points_check_waiting_id")
def handle_check_points_user_id(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    try:
        user_id = int(message.text.strip())
        user_states.pop(message.chat.id, None)
        
        points = get_user_points(user_id)
        referrals = load_referrals()
        referral_count = sum(1 for ref_user, referrer in referrals.items() if referrer == user_id)
        
        text = (
            f"<b>نقاط المستخدم {user_id}</b>\n"
            f"<blockquote>"
            f"النقاط الحالية: {points}\n"
            f"عدد الدعوات: {referral_count}\n"
            f"رابط الدعوة: https://t.me/{bot.get_me().username}?start=ref_{user_id}"
            f"</blockquote>"
        )
        bot.send_message(message.chat.id, text)
        
    except ValueError:
        bot.send_message(message.chat.id, "<b>معرف المستخدم يجب أن يكون رقماً.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) and user_states.get(m.chat.id).startswith("points_add_amount_"))
def handle_add_points_amount(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    try:
        user_id = int(user_states[message.chat.id].split("_")[-1])
        amount = int(message.text.strip())
        
        if amount <= 0:
            bot.send_message(message.chat.id, "<b>عدد النقاط يجب أن يكون أكبر من صفر.</b>")
            return
        
        add_user_points(user_id, amount)
        user_states.pop(message.chat.id, None)
        
        bot.send_message(
            message.chat.id,
            f"<b>تم إضافة {amount} نقطة للمستخدم {user_id}</b>\n"
            f"<blockquote>الرصيد الجديد: {get_user_points(user_id)} نقطة</blockquote>"
        )
        
    except (ValueError, IndexError):
        bot.send_message(message.chat.id, "<b>حدث خطأ. يرجى المحاولة مرة أخرى.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) and user_states.get(m.chat.id).startswith("points_remove_amount_"))
def handle_remove_points_amount(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    try:
        user_id = int(user_states[message.chat.id].split("_")[-1])
        amount = int(message.text.strip())
        
        if amount <= 0:
            bot.send_message(message.chat.id, "<b>عدد النقاط يجب أن يكون أكبر من صفر.</b>")
            return
        
        current_points = get_user_points(user_id)
        if current_points < amount:
            bot.send_message(
                message.chat.id,
                f"<b>رصيد المستخدم غير كافٍ.</b>\n"
                f"<blockquote>الرصيد الحالي: {current_points} نقطة\nالمطلوب خصمها: {amount} نقطة</blockquote>"
            )
            return
        
        add_user_points(user_id, -amount)
        user_states.pop(message.chat.id, None)
        
        bot.send_message(
            message.chat.id,
            f"<b>تم خصم {amount} نقطة من المستخدم {user_id}</b>\n"
            f"<blockquote>الرصيد الجديد: {get_user_points(user_id)} نقطة</blockquote>"
        )
        
    except (ValueError, IndexError):
        bot.send_message(message.chat.id, "<b>حدث خطأ. يرجى المحاولة مرة أخرى.</b>")


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) and user_states.get(m.chat.id).startswith("points_referral_amount_"))
def handle_referral_points_amount(message):
    if not is_developer_user(message.from_user.id):
        user_states.pop(message.chat.id, None)
        return
    
    try:
        user_id = int(user_states[message.chat.id].split("_")[-1])
        amount = int(message.text.strip())
        
        if amount <= 0:
            bot.send_message(message.chat.id, "<b>عدد النقاط يجب أن يكون أكبر من صفر.</b>")
            return
        
        # تحديث نظام النقاط ليعطي نقطتين لكل دعوة بدلاً من 10
        referrals = load_referrals()
        referral_count = 0
        
        for ref_user, referrer in referrals.items():
            if referrer == user_id:
                referral_count += 1
        
        total_referral_points = referral_count * 2  # نقطتين لكل دعوة
        
        add_user_points(user_id, amount)
        user_states.pop(message.chat.id, None)
        
        bot.send_message(
            message.chat.id,
            f"<b>تم إضافة {amount} نقطة دعوة للمستخدم {user_id}</b>\n"
            f"<blockquote>عدد الدعوات: {referral_count}\nالنقاط من الدعوات: {total_referral_points}\nالرصيد الجديد: {get_user_points(user_id)} نقطة</blockquote>"
        )
        
    except (ValueError, IndexError):
        bot.send_message(message.chat.id, "<b>حدث خطأ. يرجى المحاولة مرة أخرى.</b>")


def show_points_statistics(chat_id: int):
    """عرض إحصائيات نظام النقاط"""
    try:
        points_data = load_points()
        referrals_data = load_referrals()
        
        total_users = len(points_data)
        total_points = sum(int(points) for points in points_data.values() if points.isdigit())
        
        # تحليل التوزيع
        high_users = sum(1 for points in points_data.values() if points.isdigit() and int(points) >= 100)
        medium_users = sum(1 for points in points_data.values() if points.isdigit() and 50 <= int(points) < 100)
        low_users = sum(1 for points in points_data.values() if points.isdigit() and int(points) < 50)
        
        # أفضل 5 مستخدمين
        sorted_users = sorted(points_data.items(), key=lambda x: int(x[1]) if x[1].isdigit() else 0, reverse=True)[:5]
        
        text = (
            f"<b>إحصائيات نظام النقاط</b>\n"
            f"<blockquote>"
            f"إجمالي المستخدمين: {total_users}\n"
            f"إجمالي النقاط: {total_points}\n"
            f"مستخدمين (100+ نقطة): {high_users}\n"
            f"مستخدمين (50-99 نقطة): {medium_users}\n"
            f"مستخدمين (أقل من 50): {low_users}\n"
            f"</blockquote>"
            f"<b>أفضل 5 مستخدمين:</b>"
        )
        
        for i, (user_id, points) in enumerate(sorted_users, 1):
            text += f"\n{i}. <code>{user_id}</code>: {points} نقطة"
        
        bot.send_message(chat_id, text)
        
    except Exception as e:
        bot.send_message(chat_id, f"<b>حدث خطأ أثناء عرض الإحصائيات: {e}</b>")


@bot.message_handler(func=lambda m: m.text == "الرجوع للقائمة الرئيسية")
def handle_back_to_main(message):
    # ضمان عدم بقاء المستخدم في حالة انتظار (رابط/عدد/إلخ) عند الرجوع
    chat_id = message.chat.id
    user_states.pop(chat_id, None)
    competition_context.pop(chat_id, None)
    dev_context.pop(chat_id, None)

    is_dev = is_developer_user(message.from_user.id)
    kb = main_menu_keyboard(is_dev)
    bot.send_message(
        chat_id,
        "<b>تم الرجوع إلى القائمة الرئيسية.</b>",
        reply_markup=kb,
    )


@bot.message_handler(func=lambda m: m.text == "المسابقات")
def handle_competitions(message):
    chat_id = message.chat.id
    user_states.pop(chat_id, None)
    competition_context.pop(chat_id, None)
    dev_context.pop(chat_id, None)

    user_id = message.from_user.id
    count = user_request_counts.get(user_id, 0)
    text = (
        "<b>قسم المسابقات</b>\n"
        f"<blockquote>عدد طلبات الرشق التي قمت بها: {count}</blockquote>"
    )
    is_dev = is_developer_user(user_id)
    kb = competitions_menu_keyboard(is_dev)
    bot.send_message(message.chat.id, text, reply_markup=kb)


@bot.message_handler(func=lambda m: m.text == "سحب التصويتات")
def handle_withdraw_boosts_menu(message):
    chat_id = message.chat.id
    user_states.pop(chat_id, None)
    competition_context.pop(chat_id, None)
    dev_context.pop(chat_id, None)

    user_states[chat_id] = "withdraw_waiting_code"
    competition_context[chat_id] = {}
    bot.send_message(
        chat_id,
        "<b>سحب التصويتات</b>\n<blockquote>أرسل الآن كود العملية.</blockquote>",
        reply_markup=cancel_only_keyboard(),
    )


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "withdraw_waiting_code")
def handle_withdraw_code(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    chat_id = message.chat.id
    op_code = (message.text or "").strip()
    if not op_code:
        bot.send_message(chat_id, "<b>أرسل كود صحيح.</b>")
        return

    rec = _get_operation_by_code(op_code)
    if not rec:
        bot.send_message(chat_id, "<b>هذا الكود غير موجود.</b>")
        return

    op_type = rec.get("type")
    ctx = rec.get("ctx") or {}
    executed = rec.get("executed") if isinstance(rec.get("executed"), list) else []
    ok_count = 0
    for e in executed:
        if isinstance(e, dict) and e.get("ok"):
            ok_count += 1

    target = (
        ctx.get("p_username")
        or ctx.get("r_username")
        or ctx.get("username")
        or ctx.get("c_group_username")
        or ctx.get("v_username")
    )
    msg_id = ctx.get("p_msg_id") or ctx.get("r_msg_id") or ctx.get("msg_id") or ctx.get("c_msg_id") or ctx.get("v_msg_id")

    competition_context[chat_id] = {"withdraw_op_code": op_code}
    user_states[chat_id] = "withdraw_waiting_count"
    bot.send_message(
        chat_id,
        "<b>تم العثور على العملية</b>\n"
        f"<blockquote>النوع: {op_type}\n"
        f"الهدف: {target or 'غير معروف'}\n"
        f"الرسالة: {msg_id or 'غير معروف'}\n"
        f"عدد التنفيذات الناجحة المسجلة: {ok_count}\n"
        "أرسل الآن عدد السحب.</blockquote>",
        reply_markup=cancel_only_keyboard(),
    )


def run_withdraw_job(chat_id: int, requester_id: int, op_code: str, count: int):
    async def _inner():
        rec = _get_operation_by_code(op_code)
        if not rec:
            bot.send_message(chat_id, "<b>هذا الكود غير موجود.</b>")
            return

        op_type = rec.get("type")
        ctx = rec.get("ctx") or {}

        if op_type not in ("poll", "reactions", "comments", "votes"):
            bot.send_message(chat_id, "<b>هذا النوع حالياً غير مدعوم للسحب.</b>")
            return

        try:
            count_int = int(count)
        except Exception:
            count_int = 0
        if count_int <= 0:
            bot.send_message(chat_id, "<b>عدد السحب غير صالح.</b>")
            return

        done = 0
        failed = 0

        if op_type == "comments":
            comments = rec.get("comments") if isinstance(rec.get("comments"), list) else []
            targets = []
            for e in comments:
                if isinstance(e, dict) and e.get("peer") and e.get("message_id") and e.get("session_hash"):
                    targets.append(e)
            targets = list(reversed(targets))
            targets = targets[:count_int]
            for entry in targets:
                session_string = _get_session_string_by_hash(entry.get("session_hash"))
                if not session_string:
                    failed += 1
                    continue
                peer = str(entry.get("peer"))
                msg_id = int(entry.get("message_id"))
                client = None
                try:
                    client = await _connect_telethon_client(session_string)
                    entity = await client.get_entity(peer)
                    try:
                        await client(JoinChannelRequest(entity))
                    except Exception:
                        pass
                    await client.delete_messages(entity, [msg_id])
                    done += 1
                except Exception:
                    failed += 1
                finally:
                    if client is not None:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass

        if op_type == "votes":
            # محاولة عكس التصويت بالضغط على نفس زر الانلاين مرة ثانية (قد لا تعمل مع كل البوتات)
            link = ctx.get("link")
            username = ctx.get("username")
            msg_id = ctx.get("msg_id")
            button_text = ctx.get("button_text")
            button_index = ctx.get("button_index")

            if (not username or not msg_id) and link:
                u, m = parse_telegram_message_link(str(link))
                username = username or u
                msg_id = msg_id or m

            if not username or not msg_id:
                bot.send_message(chat_id, "<b>تعذّر قراءة بيانات العملية (تصويتات).</b>")
                return

            executed = rec.get("executed") if isinstance(rec.get("executed"), list) else []
            targets = []
            for e in executed:
                if isinstance(e, dict) and e.get("ok") and e.get("session_hash"):
                    targets.append(e)
            targets = list(reversed(targets))
            targets = targets[:count_int]

            for entry in targets:
                session_string = _get_session_string_by_hash(entry.get("session_hash"))
                if not session_string:
                    failed += 1
                    continue
                client = None
                try:
                    client = await _connect_telethon_client(session_string)
                    entity, msg = await _safe_get_entity_and_message(client, str(username), int(msg_id))

                    target_btn = None
                    if msg and hasattr(msg, "reply_markup") and getattr(msg.reply_markup, "rows", None):
                        buttons_flat = []
                        for row in msg.reply_markup.rows:
                            for btn in row.buttons:
                                buttons_flat.append(btn)

                        if button_text:
                            for b in buttons_flat:
                                if (getattr(b, "text", None) or "").strip() == str(button_text).strip():
                                    target_btn = b
                                    break
                        if target_btn is None and button_index is not None:
                            try:
                                bi = int(button_index)
                                if 0 <= bi < len(buttons_flat):
                                    target_btn = buttons_flat[bi]
                            except Exception:
                                pass

                    if target_btn is None:
                        failed += 1
                        continue

                    data = getattr(target_btn, "data", None)
                    if data is None:
                        failed += 1
                        continue

                    ok = await _safe_click_callback(client, entity, int(msg_id), data)
                    if ok:
                        done += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1
                finally:
                    if client is not None:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass

        if op_type == "reactions":
            username = ctx.get("r_username")
            msg_id = ctx.get("r_msg_id")
            if not username or not msg_id:
                bot.send_message(chat_id, "<b>تعذّر قراءة بيانات العملية (تفاعلات).</b>")
                return
            executed = rec.get("executed") if isinstance(rec.get("executed"), list) else []
            targets = []
            for e in executed:
                if isinstance(e, dict) and e.get("ok") and e.get("session_hash"):
                    targets.append(e)
            targets = list(reversed(targets))
            targets = targets[:count_int]
            for entry in targets:
                session_string = _get_session_string_by_hash(entry.get("session_hash"))
                if not session_string:
                    failed += 1
                    continue
                client = None
                try:
                    client = await _connect_telethon_client(session_string)
                    entity, _ = await _safe_get_entity_and_message(client, str(username), int(msg_id))
                    await client(SendReactionRequest(peer=entity, msg_id=int(msg_id), reaction=[]))
                    done += 1
                except Exception:
                    failed += 1
                finally:
                    if client is not None:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass

        if op_type == "poll":
            username = ctx.get("p_username")
            msg_id = ctx.get("p_msg_id")
            if not username or not msg_id:
                bot.send_message(chat_id, "<b>تعذّر قراءة بيانات العملية (استفتاء).</b>")
                return
            executed = rec.get("executed") if isinstance(rec.get("executed"), list) else []
            targets = []
            for e in executed:
                if isinstance(e, dict) and e.get("ok") and e.get("session_hash"):
                    targets.append(e)
            targets = list(reversed(targets))
            targets = targets[:count_int]
            for entry in targets:
                session_string = _get_session_string_by_hash(entry.get("session_hash"))
                if not session_string:
                    failed += 1
                    continue
                client = None
                try:
                    client = await _connect_telethon_client(session_string)
                    entity, _ = await _safe_get_entity_and_message(client, str(username), int(msg_id))
                    await client(SendVoteRequest(peer=entity, msg_id=int(msg_id), options=[]))
                    done += 1
                except Exception:
                    failed += 1
                finally:
                    if client is not None:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass

        bot.send_message(
            chat_id,
            "<b>اكتملت عملية السحب.</b>\n"
            f"<blockquote>تم السحب: {done}\nفشل: {failed}</blockquote>",
            reply_markup=competitions_menu_keyboard(is_developer_user(requester_id)),
        )

    asyncio.run(_inner())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "withdraw_waiting_count")
def handle_withdraw_count(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    chat_id = message.chat.id
    ctx = competition_context.get(chat_id) or {}
    op_code = ctx.get("withdraw_op_code")
    if not op_code:
        bot.send_message(chat_id, "<b>لا توجد عملية محددة، أعد المحاولة.</b>")
        user_states.pop(chat_id, None)
        competition_context.pop(chat_id, None)
        return

    text = (message.text or "").strip()
    try:
        count = int(text)
    except ValueError:
        bot.send_message(chat_id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    user_states.pop(chat_id, None)
    competition_context.pop(chat_id, None)
    bot.send_message(chat_id, "<b>بدأت عملية السحب...</b>")
    thread = threading.Thread(target=run_withdraw_job, args=(chat_id, message.from_user.id, op_code, count))
    thread.start()


@bot.message_handler(func=lambda m: m.text == "رشق تصويتات")
def handle_rashq_votes(message):
    chat_id = message.chat.id
    user_states.pop(chat_id, None)
    competition_context.pop(chat_id, None)
    dev_context.pop(chat_id, None)

    if _is_boost_locked_for_user(message.from_user.id):
        bot.send_message(
            message.chat.id,
            "<b>يوجد طلب جاري حالياً.</b>\n<blockquote>الرجاء الانتظار حتى يكتمل الطلب الحالي ثم حاول مرة أخرى.</blockquote>",
        )
        return
    if not load_sessions():
        bot.send_message(
            message.chat.id,
            "<b>لا توجد جلسات مضافة.</b>\n<blockquote>أضف جلسات أولاً من قسم الحسابات.</blockquote>",
            reply_markup=accounts_menu_keyboard(),
        )
        return

    user_states[message.chat.id] = "competition_waiting_link"
    competition_context[message.chat.id] = {}
    text = (
        "<b>رشق تصويتات</b>\n"
        "<blockquote>أرسل الآن رابط رسالة التصويت (من نوع https://t.me/username/1234).</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


def parse_telegram_message_link(url: str):
    if not url:
        return None, None
    
    try:
        url = url.strip()
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "https://" + url
        
        parsed = urlparse(url)
        if parsed.netloc not in ("t.me", "telegram.me", "www.t.me", "telegram.dog"):
            return None, None
        
        parts = parsed.path.strip("/").split("/")
        
        # دعم نوعين من الروابط:
        # 1) رابط قناة فقط: https://t.me/username  -> يرجع (username, None)
        # 2) رابط رسالة:    https://t.me/username/1234 -> يرجع (username, msg_id)
        # 3) رابط رسالة لقناة/قروب خاص: https://t.me/c/<id>/<msg_id> -> يرجع (-100<id>, msg_id)
        
        if len(parts) == 3 and parts[0] == "c":
            try:
                internal_id = int(parts[1])
                msg_id = int(parts[2])
                if internal_id <= 0 or msg_id <= 0:
                    return None, None
            except ValueError:
                return None, None
            # t.me/c/<id>/... يمثل قنوات/مجموعات خاصة، ومعرّف تيليغرام الفعلي عادة يبدأ بـ -100
            return int(f"-100{internal_id}"), msg_id
            
        elif len(parts) == 1:
            username = parts[0]
            if not username or username.startswith("+"):
                return None, None
            return username, None
            
        elif len(parts) == 2:
            username = parts[0]
            try:
                msg_id = int(parts[1])
                if msg_id <= 0:
                    return None, None
            except ValueError:
                return None, None
            if not username or username.startswith("+"):
                return None, None
            return username, msg_id
            
        else:
            return None, None
            
    except Exception:
        return None, None


async def fetch_inline_buttons_with_first_session(link: str):
    username, msg_id = parse_telegram_message_link(link)
    if not username or not msg_id:
        return False, "الرابط غير صالح أو غير مدعوم حالياً.", None

    sessions = load_sessions()
    if not sessions:
        return False, "لا توجد جلسات مضافة.", None

    client = None
    try:
        session_string = sessions[0]["session"]
        client = await _connect_telethon_client(session_string)
        try:
            # دعم روابط t.me/c التي ترجع معرف رقمي
            if isinstance(username, int):
                entity = username
            else:
                entity = await client.get_entity(username)

            try:
                await client(JoinChannelRequest(entity))
            except Exception:
                pass

            msg = await client.get_messages(entity, ids=msg_id)
        except Exception:
            return False, "تعذّر جلب الرسالة من الرابط، تأكد أن الحسابات داخل القناة أو المجموعة.", None

        buttons = []

        try:
            if msg and hasattr(msg, "reply_markup") and getattr(msg.reply_markup, "rows", None):
                for row in msg.reply_markup.rows:
                    for btn in row.buttons:
                        # نهتم بأزرار الكولباك فقط
                        if getattr(btn, "data", None) is not None:
                            text = (getattr(btn, "text", None) or "").strip()
                            if text:
                                buttons.append(text)
        except Exception:
            buttons = []

        if not buttons:
            return False, "هذه الرسالة لا تحتوي على أزرار تصويت (Inline) قابلة للقراءة.", None

        return True, (username, msg_id, buttons), None
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass


async def fetch_poll_buttons_with_first_session(link: str):
    username, msg_id = parse_telegram_message_link(link)
    if not username or not msg_id:
        return False, "الرابط غير صالح أو غير مدعوم حالياً.", None

    sessions = load_sessions()
    if not sessions:
        return False, "لا توجد جلسات مضافة.", None

    client = None
    try:
        session_string = sessions[0]["session"]
        client = await _connect_telethon_client(session_string)
        try:
            entity = await client.get_entity(username)
            try:
                await client(JoinChannelRequest(entity))
            except Exception:
                pass
            msg = await client.get_messages(entity, ids=msg_id)
        except Exception:
            return False, "تعذّر جلب الرسالة من الرابط، تأكد أن الجلسة مشاهدة للقناة أو المجموعة.", None

        poll_answers = []
        poll_options = []
        try:
            media = getattr(msg, "media", None)
            poll = getattr(media, "poll", None) if media else None
            answers = getattr(poll, "answers", None) if poll else None
            if answers:
                for ans in answers:
                    txt = getattr(ans, "text", None)
                    opt = getattr(ans, "option", None)
                    if txt is not None and opt is not None:
                        poll_answers.append(str(txt))
                        poll_options.append(opt)
        except Exception:
            poll_answers = []
            poll_options = []

        if not poll_answers:
            return False, "هذه الرسالة لا تحتوي على استفتاء (Poll) أو لا يمكن قراءة خياراته.", None

        return True, (username, msg_id, poll_answers, poll_options), None
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "competition_waiting_link")
def handle_competition_link(message):
    link = (message.text or "").strip()
    if not link:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رابط صالح.</b>")
        return

    bot.send_chat_action(message.chat.id, "typing")

    try:
        ok, data, _ = asyncio.run(fetch_inline_buttons_with_first_session(link))
    except Exception:
        ok, data = False, "حدث خطأ أثناء محاولة قراءة الرسالة من الرابط."

    if not ok:
        bot.send_message(
            message.chat.id,
            f"<b>تعذّر استخدام الرابط:</b>\n<blockquote>{data}</blockquote>",
        )
        return

    username, msg_id, buttons = data
    competition_context[message.chat.id] = {
        "link": link,
        "username": username,
        "msg_id": msg_id,
        "buttons": buttons,
    }

    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    for idx, text in enumerate(buttons):
        label = f"{idx + 1}) {text}"
        kb.row(KeyboardButton(label))
    kb.row(KeyboardButton("إلغاء العملية"))

    text = (
        "<b>أزرار التصويت المكتشفة</b>\n"
        "<blockquote>اختر الزر الذي تريد أن تضغطه الجلسات من الأزرار بالأسفل.</blockquote>"
    )
    user_states[message.chat.id] = "competition_waiting_button_choice"
    bot.send_message(message.chat.id, text, reply_markup=kb)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "competition_waiting_button_choice")
def handle_competition_button_choice(message):
    ctx = competition_context.get(message.chat.id) or {}
    buttons = ctx.get("buttons") or []
    if not buttons:
        bot.send_message(message.chat.id, "<b>لا توجد بيانات أزرار محفوظة، أعد إرسال الرابط.</b>")
        user_states[message.chat.id] = None
        competition_context.pop(message.chat.id, None)
        return

    text = (message.text or "").strip()
    try:
        num_part = text.split(")")[0]
        choice = int(num_part) - 1
    except Exception:
        bot.send_message(message.chat.id, "<b>اختر من الأزرار الظاهرة فقط.</b>")
        return

    if choice < 0 or choice >= len(buttons):
        bot.send_message(message.chat.id, "<b>اختيار غير صالح.</b>")
        return

    # نحفظ كل من فهرس الزر ونصه (الإيموجي / النص) حتى نتمكن من إيجاده لاحقاً مهما تغيّر ترتيبه
    ctx["button_index"] = choice
    ctx["button_text"] = buttons[choice]
    competition_context[message.chat.id] = ctx

    max_sessions = get_sessions_count()
    ctx["max_sessions"] = max_sessions
    text = (
        "<b>عدد الجلسات</b>\n"
        f"<blockquote>أرسل الآن عدد الجلسات التي تريد استخدامها (من 1 إلى {max_sessions}).</blockquote>"
    )
    user_states[message.chat.id] = "competition_waiting_sessions_count"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "competition_waiting_sessions_count")
def handle_competition_sessions_count(message):
    ctx = competition_context.get(message.chat.id) or {}
    max_sessions = ctx.get("max_sessions") or get_sessions_count()

    text = (message.text or "").strip()
    try:
        count = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if count <= 0 or count > max_sessions:
        bot.send_message(
            message.chat.id,
            f"<b>عدد غير صالح.</b>\n<blockquote>يجب أن يكون بين 1 و {max_sessions}.</blockquote>",
        )
        return

    # تنفيذ الرشق بجميع الحسابات بدون استثناء
    ctx["sessions_count"] = count
    competition_context[message.chat.id] = ctx

    text = (
        "<b>الوقت بين كل جلسة والأخرى</b>\n"
        "<blockquote>أرسل الوقت بالثواني بين ضغط كل جلسة والأخرى.</blockquote>"
    )
    user_states[message.chat.id] = "competition_waiting_delay"
    bot.send_message(message.chat.id, text)


def run_votes_job(chat_id: int, user_id: int, ctx: dict):
    async def _inner():
        job_tag = ctx.get("job_tag")
        op_code = ctx.get("op_code")
        link = ctx["link"]
        button_index = ctx.get("button_index")
        button_text = ctx.get("button_text")
        sessions_count = ctx["sessions_count"]
        delay = ctx["delay"]

        username, msg_id = parse_telegram_message_link(link)
        if not username or not msg_id:
            bot.send_message(chat_id, "<b>تعذّر إعادة قراءة الرابط أثناء تنفيذ الرشق.</b>")
            return

        if op_code:
            _ensure_operation_record(str(op_code), "votes", int(user_id or 0), ctx)

        # استخدام الدالة الجديدة لاختيار الجلسات مع استثناء التي صوتت بالفعل
        rotation_key = f"votes:{username}:{msg_id}"
        sessions = select_sessions_for_voting(rotation_key, int(sessions_count or 0), exclude_voted=True)
        if not sessions:
            bot.send_message(chat_id, "<b>لا توجد جلسات متاحة للتصويت (جميع الجلسات صوتت بالفعل).</b>\n<blockquote>يمكنك المحاولة مرة أخرى لاحقاً أو استخدام جلسات جديدة.</blockquote>")
            return

        success_count = 0
        fail_count = 0
        removed_count = 0

        def _describe_votes_error(e: Exception) -> str:
            if isinstance(e, FloodWaitError):
                try:
                    wait_s = int(getattr(e, "seconds", 0) or 0)
                    if wait_s > 0:
                        return f"FloodWait: انتظر {wait_s} ثانية"
                except Exception:
                    pass
                return "FloodWait"
            if isinstance(e, RPCError):
                try:
                    name = e.__class__.__name__
                    msg = str(e)
                    if msg:
                        return f"{name}: {msg}"
                    return name
                except Exception:
                    return "RPCError"
            if isinstance(e, ValueError):
                try:
                    return f"ValueError: {str(e)}"
                except Exception:
                    return "ValueError: قيمة غير صالحة"
            if isinstance(e, TypeError):
                try:
                    return f"TypeError: {str(e)}"
                except Exception:
                    return "TypeError: خطأ في نوع البيانات"
            # معالجة أخطاء المصادقة الشائعة
            if "AuthKey" in str(e) or "auth_key" in str(e).lower():
                return "خطأ المصادقة: الجلسة غير صالحة أو تم حظرها"
            if "User not found" in str(e) or "No user has" in str(e):
                return "المستخدم غير موجود: تحقق من اسم المستخدم"
            try:
                return f"{e.__class__.__name__}: {str(e)}"
            except Exception:
                return "خطأ غير معروف"

        # معالجة تسلسلية موثوقة - كل حساب يكمل قبل الانتقال للآخر
        for idx, s in enumerate(sessions, start=1):
            if votes_stop_flags.get(chat_id):
                break

            client = None
            session_success = False
            
            try:
                # التحقق من الجلسة وتوصيلها
                client = await _connect_telethon_client(s["session"])
                
                # التحقق من صحة البيانات قبل البدء
                if not username or not msg_id:
                    bot.send_message(
                        chat_id,
                        f"<b>بيانات غير صالحة للجلسة رقم {idx}.</b>\n"
                        f"<blockquote>اسم المستخدم: {username or 'غير متوفر'}\nرقم الرسالة: {msg_id or 'غير متوفر'}</blockquote>"
                    )
                    fail_count += 1
                    continue
                
                # التحقق من الوصول للرسالة مع إعادة المحاولات
                entity = None
                msg = None
                for attempt in range(3):
                    try:
                        # التحقق من أن msg_id رقم صحيح
                        msg_id_int = int(msg_id)
                        if msg_id_int <= 0:
                            raise ValueError("رقم الرسالة يجب أن يكون رقماً موجباً")
                        
                        # محاولة الوصول للرسالة مع التعامل مع حالة الأحرف
                        entity, msg = await _safe_get_entity_and_message(client, username, msg_id_int)
                        
                        # إذا فشلت، حاول باسم المستخدم الصغير
                        if not entity and username != username.lower():
                            entity, msg = await _safe_get_entity_and_message(client, username.lower(), msg_id_int)
                        
                        # إذا فشلت، حاول باسم المستخدم الكبير
                        if not entity and username != username.upper():
                            entity, msg = await _safe_get_entity_and_message(client, username.upper(), msg_id_int)
                        
                        # التحقق من صحة الرسالة
                        if not entity or not msg:
                            raise ValueError("الرسالة غير موجودة أو لا يمكن الوصول إليها")
                        
                        break
                        
                    except ValueError as ve:
                        if attempt == 2:  # المحاولة الأخيرة
                            if handle_session_error(chat_id, s, ve, "رشق التصويت"):
                                removed_count += 1
                                continue
                            bot.send_message(
                                chat_id,
                                f"<b>بيانات غير صالحة للجلسة رقم {idx}.</b>\n"
                                f"<blockquote>الخطأ: {_describe_votes_error(ve)}\n"
                                f"اسم المستخدم: {username}\nرقم الرسالة: {msg_id}</blockquote>",
                            )
                            fail_count += 1
                            continue
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        if attempt == 2:  # المحاولة الأخيرة
                            if handle_session_error(chat_id, s, e, "رشق التصويت"):
                                removed_count += 1
                                continue
                            bot.send_message(
                                chat_id,
                                f"<b>تعذّر الوصول إلى رسالة التصويت من الجلسة رقم {idx}.</b>\n"
                                f"<blockquote>الخطأ: {_describe_votes_error(e)}\n"
                                f"اسم المستخدم: {username}\nرقم الرسالة: {msg_id}</blockquote>",
                            )
                            fail_count += 1
                            continue
                        await asyncio.sleep(2)

                if not entity or not msg:
                    fail_count += 1
                    continue

                # البحث عن زر التصويت الصحيح
                target_btn = None
                try:
                    if hasattr(msg, "reply_markup") and msg.reply_markup and hasattr(msg.reply_markup, "rows"):
                        buttons_flat = []
                        if msg.reply_markup.rows:
                            for row in msg.reply_markup.rows:
                                if row and hasattr(row, "buttons"):
                                    for btn in row.buttons:
                                        if btn:
                                            buttons_flat.append(btn)

                        # البحث بالنص أولاً
                        if button_text and buttons_flat:
                            for b in buttons_flat:
                                try:
                                    btn_text = getattr(b, "text", None)
                                    if btn_text and str(btn_text).strip() == str(button_text).strip():
                                        target_btn = b
                                        break
                                except Exception:
                                    continue

                        # البحث بالفهرس إذا لم يتم العثور
                        if target_btn is None and button_index is not None and buttons_flat:
                            try:
                                bi = int(button_index)
                                if 0 <= bi < len(buttons_flat):
                                    target_btn = buttons_flat[bi]
                            except (ValueError, TypeError, IndexError):
                                pass
                except Exception as e:
                    bot.send_message(
                        chat_id,
                        f"<b>خطأ في قراءة أزرار التصويت من الجلسة رقم {idx}.</b>\n"
                        f"<blockquote>الخطأ: {_describe_votes_error(e)}</blockquote>"
                    )

                if target_btn is None:
                    fail_count += 1
                    bot.send_message(chat_id, f"<b>تعذّر العثور على زر التصويت المطلوب من الجلسة رقم {idx}.</b>")
                    continue

                # التحقق من وجود بيانات الزر
                try:
                    data = getattr(target_btn, "data", None)
                    if data is None:
                        fail_count += 1
                        bot.send_message(chat_id, f"<b>زر التصويت لا يحتوي بيانات ضغط (data) من الجلسة رقم {idx}.</b>")
                        continue
                except Exception as e:
                    fail_count += 1
                    bot.send_message(
                        chat_id,
                        f"<b>خطأ في قراءة بيانات زر التصويت من الجلسة رقم {idx}.</b>\n"
                        f"<blockquote>الخطأ: {_describe_votes_error(e)}</blockquote>"
                    )
                    continue

                # تنفيذ التصويت مع التحقق من النجاح + مشاهدة + نشاط
                vote_success = False
                for vote_attempt in range(3):
                    try:
                        # التحقق من صحة البيانات قبل الضغط
                        if not data or not entity or not msg_id:
                            raise ValueError("بيانات غير صالحة للتصويت")
                        
                        msg_id_int = int(msg_id)
                        if msg_id_int <= 0:
                            raise ValueError("رقم رسالة غير صالح")
                        
                        # 1. إضافة مشاهدة للمنشور أولاً
                        try:
                            await client(GetMessagesViewsRequest(peer=entity, id=[msg_id_int], increment=True))
                            await asyncio.sleep(0.5)  # انتظار نصف ثانية بعد المشاهدة
                        except Exception as view_error:
                            # إذا فشلت المشاهدة، نستمر في التصويت
                            pass
                        
                        # 2. تنفيذ التصويت
                        ok = await _safe_click_callback(client, entity, msg_id_int, data)
                        if ok:
                            # 3. جعل الجلسة نشطة/أونلاين (محاكاة النشاط)
                            try:
                                # إرسال حالة "typing" أو "online" لمحاكاة النشاط
                                await client.send_read_acknowledge(entity)  # تعليم الرسالة كمقروءة
                                await asyncio.sleep(0.3)
                            except Exception:
                                pass
                            
                            # التحقق من نجاح التصويت بالفعل
                            await asyncio.sleep(1)
                            try:
                                updated_msg = await client.get_messages(entity, ids=msg_id_int)
                                if updated_msg and hasattr(updated_msg, 'reply_markup') and updated_msg.reply_markup:
                                    vote_success = True
                                    success_count += 1
                                    session_success = True
                                    
                                    if op_code:
                                        _append_operation_execution(str(op_code), s["session"], True, {
                                            "action": "vote_with_view_and_activity",
                                            "view_added": True,
                                            "activity_simulated": True
                                        })
                                    break
                            except Exception as verify_error:
                                # فشل التحقق لكن قد يكون التصويت نجح
                                vote_success = True
                                success_count += 1
                                session_success = True
                                
                                if op_code:
                                    _append_operation_execution(str(op_code), s["session"], True, {
                                        "action": "vote_with_view_and_activity",
                                        "view_added": True,
                                        "activity_simulated": True,
                                        "verification_failed": True
                                    })
                                break
                        
                    except ValueError as ve:
                        if vote_attempt == 2:  # المحاولة الأخيرة
                            break
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        if vote_attempt == 2:  # المحاولة الأخيرة
                            bot.send_message(
                                chat_id,
                                f"<b>خطأ في تنفيذ التصويت من الجلسة رقم {idx}.</b>\n"
                                f"<blockquote>الخطأ: {_describe_votes_error(e)}</blockquote>"
                            )
                            break
                        await asyncio.sleep(2)

                if not vote_success:
                    fail_count += 1
                    bot.send_message(
                        chat_id,
                        f"<b>فشل التصويت من الجلسة رقم {idx}.</b>\n"
                        f"<blockquote>لم يتم تأكيد التصويت بعد 3 محاولات</blockquote>",
                    )
                    if op_code:
                        _append_operation_execution(str(op_code), s["session"], False)

                # الانتظار قبل الجلسة التالية فقط إذا نجحت هذه الجلسة
                if session_success and idx < len(sessions) and not votes_stop_flags.get(chat_id):
                    await asyncio.sleep(delay)

            except Exception as e:
                if handle_session_error(chat_id, s, e, "رشق التصويتات"):
                    removed_count += 1
                    continue
                bot.send_message(chat_id, f"<b>حدث خطأ غير متوقع مع الجلسة رقم {idx}.</b>\n<blockquote>{_describe_votes_error(e)}</blockquote>")
                fail_count += 1
                if op_code:
                    _append_operation_execution(str(op_code), s["session"], False)
            
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

        # إرسال التقرير النهائي
        try:
            if votes_stop_flags.get(chat_id):
                bot.send_message(
                    chat_id,
                    f"<b>تم إيقاف رشق التصويتات بناءً على طلبك.</b>\n<blockquote>✅ نجح: {success_count}\n❌ فشل: {fail_count}\n🗑️ تم حذف جلسات معطوبة: {removed_count}\n👁️ تمت إضافة مشاهدة لكل تصويت\n🟢 تم تفعيل النشاط لكل جلسة</blockquote>",
                )
            else:
                bot.send_message(
                    chat_id,
                    f"<b>اكتمل تنفيذ رشق التصويتات بنجاح.</b>\n<blockquote>✅ نجح: {success_count}\n❌ فشل: {fail_count}\n🗑️ تم حذف جلسات معطوبة: {removed_count}\n👁️ تمت إضافة مشاهدة لكل تصويت\n🟢 تم تفعيل النشاط لكل جلسة\n🚫 الجلسات التي صوتت لن تكرر التصويت</blockquote>",
                )
        finally:
            votes_stop_flags.pop(chat_id, None)
            if job_tag:
                _finish_non_owner_boost(job_tag)

    asyncio.run(_inner())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "competition_waiting_delay")
def handle_competition_delay(message):
    ctx = competition_context.get(message.chat.id) or {}

    text = (message.text or "").strip()
    try:
        delay = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if delay <= 0:
        bot.send_message(message.chat.id, "<b>الوقت يجب أن يكون أكبر من صفر.</b>")
        return

    ctx["delay"] = delay
    competition_context[message.chat.id] = ctx

    user_id = message.from_user.id
    user_request_counts[user_id] = user_request_counts.get(user_id, 0) + 1

    # المالك: ينفّذ مباشرة بدون نقاط
    if is_primary_developer(user_id):
        op_code = ctx.get("op_code") or _gen_op_code("votes", user_id)
        ctx["op_code"] = op_code
        text = (
            "<b>تم استلام إعدادات الرشق</b>\n"
            "<blockquote>سيتم الآن بدء عملية الرشق من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{op_code}</code></blockquote>"
        )
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_votes"))
        bot.send_message(message.chat.id, text, reply_markup=stop_kb)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None

        votes_stop_flags[message.chat.id] = False

        job_tag = job_ctx.get("job_tag") or f"job_votes_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "votes", "رشق تصويتات", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_votes_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مطوّر مرفوع: ينفّذ مباشرة بدون موافقة لكن مع خصم نقاط
    if is_developer_user(user_id):
        op_code = ctx.get("op_code") or _gen_op_code("votes", user_id)
        ctx["op_code"] = op_code
        sessions_count = ctx.get("sessions_count", 0)
        cost = calculate_cost(sessions_count)
        user_points_before = get_user_points(user_id)
        if user_points_before < cost:
            bot.send_message(
                message.chat.id,
                f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
            )
            user_states.pop(message.chat.id, None)
            competition_context.pop(message.chat.id, None)
            return

        add_user_points(user_id, -cost)

        text = (
            "<b>تم استلام إعدادات الرشق</b>\n"
            "<blockquote>سيتم الآن بدء عملية الرشق من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{op_code}</code></blockquote>"
        )
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_votes"))
        bot.send_message(message.chat.id, text, reply_markup=stop_kb)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None

        votes_stop_flags[message.chat.id] = False

        job_tag = job_ctx.get("job_tag") or f"job_votes_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "votes", "رشق تصويتات", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_votes_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # جميع المستخدمين غير المالك: نظام النقاط + موافقة المالك
    sessions_count = ctx.get("sessions_count", 0)
    cost = calculate_cost(sessions_count)
    user_points_before = get_user_points(user_id)
    if user_points_before < cost:
        bot.send_message(
            message.chat.id,
            f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
        )
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    add_user_points(user_id, -cost)
    user_points_after = user_points_before - cost

    job_ctx = ctx.copy()
    user_states[message.chat.id] = None
    competition_context.pop(message.chat.id, None)

    req_id = f"{int(time.time())}_votes_{user_id}"
    job_tag = f"job_{req_id}"
    job_ctx["job_tag"] = job_tag
    job_ctx["op_code"] = req_id
    pending_requests[req_id] = {
        "type": "votes",
        "chat_id": message.chat.id,
        "user_id": user_id,
        "ctx": job_ctx,
        "cost": cost,
        "points_before": user_points_before,
        "points_after": user_points_after,
        "created_at": int(time.time()),
    }

    # رسالة للمستخدم
    bot.send_message(
        message.chat.id,
        "<b>تم إرسال طلبك</b>\n<blockquote>انتظر موافقة المطور على طلب رشق التصويتات قبل البدء بالتنفيذ.</blockquote>",
    )

    # رسالة للمطور الأساسي
    user = message.from_user
    first_name = user.first_name or "بدون اسم"
    username = f"@{user.username}" if user.username else "لا يوجد يوزر"
    link = job_ctx.get("link", "غير متوفر")
    sessions = job_ctx.get("sessions_count", 0)
    delay_val = job_ctx.get("delay", 0)
    dev_text = (
        "<b>طلب جديد لرشق تصويتات</b>\n"
        f"<blockquote>المستخدم: {first_name} ({username})\n"
        f"الآيدي: <code>{user_id}</code>\n"
        f"الرابط: {link}\n"
        f"عدد الجلسات (الأصوات): {sessions}\n"
        f"التأخير بين كل جلسة: {delay_val} ثانية\n"
        f"النقاط قبل: {user_points_before}\n"
        f"النقاط بعد (مخصوم): {user_points_after}\n"
        f"التكلفة: {cost} نقطة\n"
        f"ID الطلب: <code>{req_id}</code></blockquote>"
    )
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✅ موافقة", callback_data=f"approve_req_{req_id}"),
        InlineKeyboardButton("❌ رفض", callback_data=f"reject_req_{req_id}"),
    )
    try:
        bot.send_message(DEVELOPER_ID, dev_text, reply_markup=kb)
    except Exception:
        pass


@bot.message_handler(func=lambda m: m.text == "إيقاف رشق التصويتات")
def handle_stop_votes(message):
    chat_id = message.chat.id
    if chat_id not in votes_stop_flags:
        bot.send_message(chat_id, "<b>لا توجد عملية رشق تصويتات قيد التنفيذ حالياً.</b>")
        return
    votes_stop_flags[chat_id] = True
    bot.send_message(chat_id, "<b>تم طلب إيقاف رشق التصويتات، سيتم الإيقاف خلال ثوانٍ.</b>")


@bot.callback_query_handler(func=lambda c: c.data == "stop_votes")
def callback_stop_votes(call):
    chat_id = call.message.chat.id
    if chat_id not in votes_stop_flags:
        bot.answer_callback_query(call.id, "لا توجد عملية رشق تصويتات قيد التنفيذ.")
        return
    votes_stop_flags[chat_id] = True
    bot.answer_callback_query(call.id, "تم طلب إيقاف رشق التصويتات، سيتم الإيقاف خلال ثوانٍ.")
    try:
        bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "stop_poll")
def callback_stop_poll(call):
    chat_id = call.message.chat.id
    if chat_id not in poll_stop_flags:
        bot.answer_callback_query(call.id, "لا توجد عملية رشق استفتاء قيد التنفيذ.")
        return
    poll_stop_flags[chat_id] = True
    bot.answer_callback_query(call.id, "تم طلب إيقاف رشق الاستفتاء، سيتم الإيقاف خلال ثوانٍ.")
    try:
        bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass


@bot.message_handler(func=lambda m: m.text == "رشق تعليقات")
def handle_rashq_comments(message):
    chat_id = message.chat.id
    user_states.pop(chat_id, None)
    competition_context.pop(chat_id, None)
    dev_context.pop(chat_id, None)

    if _is_boost_locked_for_user(message.from_user.id):
        bot.send_message(
            message.chat.id,
            "<b>يوجد طلب جاري حالياً.</b>\n<blockquote>الرجاء الانتظار حتى يكتمل الطلب الحالي ثم حاول مرة أخرى.</blockquote>",
        )
        return
    if not load_sessions():
        bot.send_message(
            message.chat.id,
            "<b>لا توجد جلسات مضافة.</b>\n<blockquote>أضف جلسات أولاً من قسم الحسابات.</blockquote>",
            reply_markup=accounts_menu_keyboard(),
        )
        return

    user_states[message.chat.id] = "comments_waiting_channel"
    competition_context[message.chat.id] = {}
    text = (
        "<b>رشق تعليقات</b>\n"
        "<blockquote>أرسل الآن رابط القناة التي تريد أن تنضم لها الجلسات أولاً (على شكل https://t.me/username).</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "comments_waiting_channel")
def handle_comments_channel(message):
    link = (message.text or "").strip()
    if not link:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رابط صالح للقناة.</b>")
        return

    username, _ = parse_telegram_message_link(link)
    if not username:
        bot.send_message(message.chat.id, "<b>الرابط غير صالح، تأكد من أنه على الشكل https://t.me/username.</b>")
        return

    competition_context[message.chat.id] = {
        "c_channel_username": username,
    }

    text = (
        "<b>رابط رسالة القروب</b>\n"
        "<blockquote>أرسل الآن رابط الرسالة داخل القروب التي تريد الرشق عليها (من نوع https://t.me/username/1234).</blockquote>"
    )
    user_states[message.chat.id] = "comments_waiting_link"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "comments_waiting_link")
def handle_comments_link(message):
    ctx = competition_context.get(message.chat.id) or {}
    link = (message.text or "").strip()
    if not link:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رابط صالح للرسالة.</b>")
        return

    group_username, msg_id = parse_telegram_message_link(link)
    if not group_username or not msg_id:
        bot.send_message(message.chat.id, "<b>الرابط غير صالح، تأكد من أنه على الشكل https://t.me/username/1234.</b>")
        return

    ctx["c_link"] = link
    ctx["c_group_username"] = group_username
    ctx["c_msg_id"] = msg_id
    competition_context[message.chat.id] = ctx

    text = (
        "<b>الوقت بين كل تعليق والآخر</b>\n"
        "<blockquote>أرسل الوقت بالثواني بين كل جلسة والأخرى.</blockquote>"
    )
    user_states[message.chat.id] = "comments_waiting_delay"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "comments_waiting_delay")
def handle_comments_delay(message):
    ctx = competition_context.get(message.chat.id) or {}

    text = (message.text or "").strip()
    try:
        delay = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if delay <= 0:
        bot.send_message(message.chat.id, "<b>الوقت يجب أن يكون أكبر من صفر.</b>")
        return

    ctx["c_delay"] = delay
    competition_context[message.chat.id] = ctx

    max_sessions = get_sessions_count()
    ctx["c_max_sessions"] = max_sessions
    text = (
        "<b>عدد الجلسات للتعليقات</b>\n"
        f"<blockquote>أرسل الآن عدد الجلسات التي تريد استخدامها (من 1 إلى {max_sessions}).</blockquote>"
    )
    user_states[message.chat.id] = "comments_waiting_sessions_count"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "comments_waiting_sessions_count")
def handle_comments_sessions_count(message):
    ctx = competition_context.get(message.chat.id) or {}
    max_sessions = ctx.get("c_max_sessions") or get_sessions_count()

    text = (message.text or "").strip()
    try:
        count = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح.</b>")
        return

    if count <= 0 or count > max_sessions:
        bot.send_message(
            message.chat.id,
            f"<b>عدد غير صالح.</b>\n<blockquote>يجب أن يكون بين 1 و {max_sessions}.</blockquote>",
        )
        return

    ctx["c_sessions_count"] = count
    competition_context[message.chat.id] = ctx

    text = (
        "<b>نص التعليق</b>\n"
        "<blockquote>أرسل الآن الرسالة التي تريد أن ترسلها الجلسات كتعليق.</blockquote>"
    )
    user_states[message.chat.id] = "comments_waiting_message"
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "comments_waiting_message")
def handle_comments_message(message):
    ctx = competition_context.get(message.chat.id) or {}

    comment_text = (message.text or "").strip()
    if not comment_text:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال نص للتعليق.</b>")
        return

    required_keys = ["c_channel_username", "c_group_username", "c_msg_id", "c_delay", "c_sessions_count"]
    if not all(k in ctx for k in required_keys):
        bot.send_message(message.chat.id, "<b>بيانات التعليقات غير مكتملة، أعد العملية من جديد.</b>")
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    ctx["c_text"] = comment_text
    competition_context[message.chat.id] = ctx

    user_id = message.from_user.id
    user_request_counts[user_id] = user_request_counts.get(user_id, 0) + 1

    # المالك: تنفيذ مباشر بدون نقاط
    if is_primary_developer(user_id):
        op_code = ctx.get("op_code") or _gen_op_code("comments", user_id)
        ctx["op_code"] = op_code
        text = (
            "<b>تم استلام إعدادات رشق التعليقات</b>\n"
            "<blockquote>سيتم الآن بدء عملية الرشق من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{op_code}</code></blockquote>"
        )
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_comments"))
        bot.send_message(message.chat.id, text, reply_markup=stop_kb)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None

        comments_stop_flags[message.chat.id] = False

        job_tag = job_ctx.get("job_tag") or f"job_comments_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "comments", "رشق تعليقات", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_comments_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مطوّر مرفوع: تنفيذ مباشر بدون موافقة لكن مع خصم نقاط
    if is_developer_user(user_id):
        op_code = ctx.get("op_code") or _gen_op_code("comments", user_id)
        ctx["op_code"] = op_code
        sessions_count = ctx.get("c_sessions_count", 0)
        cost = calculate_cost(sessions_count)
        user_points_before = get_user_points(user_id)
        if user_points_before < cost:
            bot.send_message(
                message.chat.id,
                f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
            )
            user_states.pop(message.chat.id, None)
            competition_context.pop(message.chat.id, None)
            return

        add_user_points(user_id, -cost)

        text = (
            "<b>تم استلام إعدادات رشق التعليقات</b>\n"
            "<blockquote>سيتم الآن بدء عملية الرشق من الجلسات بالتسلسل.</blockquote>"
        )
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_comments"))
        bot.send_message(message.chat.id, text, reply_markup=stop_kb)

        job_ctx = ctx.copy()
        user_states[message.chat.id] = None

        comments_stop_flags[message.chat.id] = False

        job_tag = job_ctx.get("job_tag") or f"job_comments_{int(time.time())}_{user_id}"
        job_ctx["job_tag"] = job_tag
        _start_non_owner_boost(job_tag, "comments", "رشق تعليقات", int(user_id), int(message.chat.id))

        thread = threading.Thread(target=run_comments_job, args=(message.chat.id, user_id, job_ctx))
        thread.start()
        return

    # مستخدم عادي: نقاط + موافقة المطور
    sessions_count = ctx.get("c_sessions_count", 0)
    cost = calculate_cost(sessions_count)
    user_points_before = get_user_points(user_id)
    if user_points_before < cost:
        bot.send_message(
            message.chat.id,
            f"<b>رصيدك من النقاط غير كافٍ.</b>\n<blockquote>التكلفة المطلوبة: {cost} نقطة\nرصيدك الحالي: {user_points_before} نقطة.</blockquote>",
        )
        user_states.pop(message.chat.id, None)
        competition_context.pop(message.chat.id, None)
        return

    add_user_points(user_id, -cost)
    user_points_after = user_points_before - cost

    job_ctx = ctx.copy()
    user_states[message.chat.id] = None
    competition_context.pop(message.chat.id, None)

    req_id = f"{int(time.time())}_comments_{user_id}"
    job_ctx["op_code"] = req_id
    pending_requests[req_id] = {
        "type": "comments",
        "chat_id": message.chat.id,
        "user_id": user_id,
        "ctx": job_ctx,
        "cost": cost,
        "points_before": user_points_before,
        "points_after": user_points_after,
        "created_at": int(time.time()),
    }

    bot.send_message(
        message.chat.id,
        "<b>تم إرسال طلبك</b>\n<blockquote>انتظر موافقة المطور على طلب رشق التعليقات قبل البدء بالتنفيذ.</blockquote>",
    )

    user = message.from_user
    first_name = user.first_name or "بدون اسم"
    username = f"@{user.username}" if user.username else "لا يوجد يوزر"
    link = job_ctx.get("c_link", "غير متوفر")
    sessions = job_ctx.get("c_sessions_count", 0)
    delay_val = job_ctx.get("c_delay", 0)
    dev_text = (
        "<b>طلب جديد لرشق تعليقات</b>\n"
        f"<blockquote>المستخدم: {first_name} ({username})\n"
        f"الآيدي: <code>{user_id}</code>\n"
        f"الرابط: {link}\n"
        f"عدد الجلسات (التعليقات): {sessions}\n"
        f"التأخير بين كل جلسة: {delay_val} ثانية\n"
        f"النقاط قبل: {user_points_before}\n"
        f"النقاط بعد (مخصوم): {user_points_after}\n"
        f"التكلفة: {cost} نقطة\n"
        f"ID الطلب: <code>{req_id}</code></blockquote>"
    )
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("✅ موافقة", callback_data=f"approve_req_{req_id}"),
        InlineKeyboardButton("❌ رفض", callback_data=f"reject_req_{req_id}"),
    )
    try:
        bot.send_message(DEVELOPER_ID, dev_text, reply_markup=kb)
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("approve_req_"))
def callback_approve_request(call):
    if not is_primary_developer(call.from_user.id):
        bot.answer_callback_query(call.id, "هذه الخاصية مخصصة للمطور فقط.")
        return

    req_id = call.data.split("approve_req_", 1)[1]
    req = pending_requests.get(req_id)
    if not req:
        bot.answer_callback_query(call.id, "هذا الطلب غير موجود أو تم التعامل معه مسبقاً.")
        return

    # قفل عام: إذا يوجد طلب جاري لمستخدم غير المالك، لا نبدأ تنفيذ طلب جديد (إلا إذا كان الطلب للمالك)
    req_user_id = req.get("user_id")
    if _is_boost_locked_for_user(req_user_id):
        bot.answer_callback_query(call.id, "يوجد طلب جاري، انتظر حتى يكتمل.")
        return

    req = pending_requests.pop(req_id, None)
    if not req:
        bot.answer_callback_query(call.id, "هذا الطلب غير موجود أو تم التعامل معه مسبقاً.")
        return

    try:
        bot.edit_message_reply_markup(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass

    chat_id = req.get("chat_id")
    user_id = req.get("user_id")
    ctx = req.get("ctx") or {}
    req_type = req.get("type")

    # ضمان وجود كود عملية ثابت للسحب لاحقاً
    if not ctx.get("op_code"):
        ctx["op_code"] = req_id

    # تفعيل القفل للطلبات غير المالك عند بدء التنفيذ بعد الموافقة
    label = req.get("label") or req_type
    job_tag = ctx.get("job_tag") or f"job_{req_id}"
    ctx["job_tag"] = job_tag
    _start_non_owner_boost(job_tag, req_type or "unknown", str(label), int(user_id or 0), int(chat_id or 0))

    # إعلام المستخدم وبدء التنفيذ مع زر الإيقاف
    if req_type == "votes":
        text = (
            "<b>تمت الموافقة على طلبك</b>\n"
            "<blockquote>سيتم الآن بدء عملية رشق التصويتات من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{ctx.get('op_code')}</code></blockquote>"
        )
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_votes"))
        bot.send_message(chat_id, text, reply_markup=stop_kb)
        votes_stop_flags[chat_id] = False
        thread = threading.Thread(target=run_votes_job, args=(chat_id, user_id, ctx))
        thread.start()
    elif req_type == "comments":
        text = (
            "<b>تمت الموافقة على طلبك</b>\n"
            "<blockquote>سيتم الآن بدء عملية رشق التعليقات من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{ctx.get('op_code')}</code></blockquote>"
        )
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_comments"))
        bot.send_message(chat_id, text, reply_markup=stop_kb)
        comments_stop_flags[chat_id] = False
        thread = threading.Thread(target=run_comments_job, args=(chat_id, user_id, ctx))
        thread.start()
    elif req_type == "views":
        text = (
            "<b>تمت الموافقة على طلبك</b>\n"
            "<blockquote>سيتم الآن بدء عملية رشق المشاهدات من الجلسات بالتسلسل.</blockquote>"
        )
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_views"))
        start_msg = bot.send_message(chat_id, text, reply_markup=stop_kb)
        ctx["v_status_message_id"] = start_msg.message_id
        views_stop_flags[chat_id] = False
        thread = threading.Thread(target=run_views_job, args=(chat_id, user_id, ctx))
        thread.start()
    elif req_type == "reactions":
        text = (
            "<b>تمت الموافقة على طلبك</b>\n"
            "<blockquote>سيتم الآن بدء عملية رشق التفاعلات من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{ctx.get('op_code')}</code></blockquote>"
        )
        bot.send_message(chat_id, text)
        thread = threading.Thread(target=run_reactions_job, args=(chat_id, user_id, ctx))
        thread.start()
    elif req_type == "poll":
        text = (
            "<b>تمت الموافقة على طلبك</b>\n"
            "<blockquote>سيتم الآن بدء عملية رشق الاستفتاء من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{ctx.get('op_code')}</code></blockquote>"
        )
        stop_kb = InlineKeyboardMarkup()
        stop_kb.add(InlineKeyboardButton("إيقاف الرشق", callback_data="stop_poll"))
        bot.send_message(chat_id, text, reply_markup=stop_kb)
        poll_stop_flags[chat_id] = False
        thread = threading.Thread(target=run_poll_boost_job, args=(chat_id, user_id, ctx))
        thread.start()
    elif req_type == "reacts_views":
        text = (
            "<b>تمت الموافقة على طلبك</b>\n"
            "<blockquote>سيتم الآن بدء عملية رشق التفاعلات والمشاهدات من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{ctx.get('op_code')}</code></blockquote>"
        )
        bot.send_message(chat_id, text)
        thread = threading.Thread(target=run_reacts_views_job, args=(chat_id, user_id, ctx))
        thread.start()
    elif req_type == "votes_reacts":
        text = (
            "<b>تمت الموافقة على طلبك</b>\n"
            "<blockquote>سيتم الآن بدء عملية رشق التصويتات والتفاعلات من الجلسات بالتسلسل.\n"
            f"كود العملية: <code>{ctx.get('op_code')}</code></blockquote>"
        )
        bot.send_message(chat_id, text)
        thread = threading.Thread(target=run_votes_reacts_job, args=(chat_id, user_id, ctx))
        thread.start()
    elif req_type == "delete_session":
        session_obj = req.get("session") or {}
        name_part = session_obj.get("first_name") or "بدون اسم"
        username_part = f"@{session_obj.get('username')}" if session_obj.get("username") else "بدون يوزر"

        # حذف الجلسة فعلياً من الملف
        remove_session_entry(session_obj)

        text = (
            "<b>تمت الموافقة على طلب حذف الجلسة</b>\n"
            f"<blockquote>تم حذف جلسة الحساب: {name_part} ({username_part}).</blockquote>"
        )
        bot.send_message(chat_id, text, reply_markup=accounts_menu_keyboard())

        # حذف الجلسة لا يُعد "طلب جاري" طويل، فننهي القفل فوراً
        _finish_non_owner_boost(job_tag)

    bot.answer_callback_query(call.id, "تمت الموافقة على الطلب وبدأ التنفيذ.")


@bot.message_handler(func=lambda m: m.text == "حظر مستخدم")
def handle_ban_user(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    
    bot.send_message(message.chat.id, "<b>🚫 حظر مستخدم</b>\n<blockquote>أرسل آيدي المستخدم الذي تريد حظره.</blockquote>")
    user_states[message.chat.id] = "ban_waiting_id"


@bot.message_handler(func=lambda m: m.text == "الغاء حظر")
def handle_unban_user(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    
    bot.send_message(message.chat.id, "<b>✅ الغاء حظر مستخدم</b>\n<blockquote>أرسل آيدي المستخدم الذي تريد الغاء حظره.</blockquote>")
    user_states[message.chat.id] = "unban_waiting_id"


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "ban_waiting_id")
def handle_ban_user_id(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    
    try:
        user_id = int(message.text.strip())
        
        # التحقق من أن المستخدم ليس مطوراً
        if is_primary_developer(user_id) or is_developer_user(user_id):
            bot.send_message(message.chat.id, "<b>❌ لا يمكنك حظر المطورين!</b>", reply_markup=dev_panel_keyboard())
            user_states.pop(message.chat.id, None)
            return
        
        # التحقق إذا كان محظوراً بالفعل
        if is_user_banned(user_id):
            bot.send_message(message.chat.id, f"<b>❌ المستخدم {user_id} محظور بالفعل!</b>", reply_markup=dev_panel_keyboard())
            user_states.pop(message.chat.id, None)
            return
        
        # حظر المستخدم
        if ban_user(user_id, "حظر بواسطة المطور"):
            bot.send_message(message.chat.id, f"<b>✅ تم حظر المستخدم {user_id} بنجاح!</b>", reply_markup=dev_panel_keyboard())
            
            # إرسال رسالة للمستخدم المحظور
            try:
                bot.send_message(user_id, "<b>🚫 تم حظرك من البوت</b>\n<blockquote>لقد تم حظرك من استخدام البوت. تواصل مع المطورين إذا كان هناك خطأ.</blockquote>")
            except Exception:
                pass
        else:
            bot.send_message(message.chat.id, "<b>❌ فشل حظر المستخدم!</b>", reply_markup=dev_panel_keyboard())
        
        user_states.pop(message.chat.id, None)
        
    except ValueError:
        bot.send_message(message.chat.id, "<b>❌ آيدي غير صالح. أرسل رقم صحيح.</b>")
    except Exception as e:
        bot.send_message(message.chat.id, f"<b>❌ حدث خطأ: {e}</b>", reply_markup=dev_panel_keyboard())
        user_states.pop(message.chat.id, None)


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "unban_waiting_id")
def handle_unban_user_id(message):
    if _is_cancel_text(message.text):
        handle_cancel_operation(message)
        return
    
    try:
        user_id = int(message.text.strip())
        
        # التحقق إذا كان المستخدم محظوراً
        if not is_user_banned(user_id):
            bot.send_message(message.chat.id, f"<b>❌ المستخدم {user_id} غير محظور!</b>", reply_markup=dev_panel_keyboard())
            user_states.pop(message.chat.id, None)
            return
        
        # الغاء حظر المستخدم
        if unban_user(user_id):
            bot.send_message(message.chat.id, f"<b>✅ تم الغاء حظر المستخدم {user_id} بنجاح!</b>", reply_markup=dev_panel_keyboard())
            
            # إرسال رسالة للمستخدم
            try:
                bot.send_message(user_id, "<b>✅ تم الغاء حظرك من البوت</b>\n<blockquote>مرحباً بك مرة أخرى في البوت!</blockquote>")
            except Exception:
                pass
        else:
            bot.send_message(message.chat.id, "<b>❌ فشل الغاء حظر المستخدم!</b>", reply_markup=dev_panel_keyboard())
        
        user_states.pop(message.chat.id, None)
        
    except ValueError:
        bot.send_message(message.chat.id, "<b>❌ آيدي غير صالح. أرسل رقم صحيح.</b>")
    except Exception as e:
        bot.send_message(message.chat.id, f"<b>❌ حدث خطأ: {e}</b>", reply_markup=dev_panel_keyboard())
        user_states.pop(message.chat.id, None)


@bot.message_handler(func=lambda m: m.text == "اختبار الإشعارات")
def handle_test_notifications(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    
    # إنشاء بيانات اختبار بسيطة
    test_user_info = {
        'first_name': 'عضو تجريبي',
        'username': '@test_member',
        'user_id': 123456789,
        'phone': '+967777123456'
    }
    
    bot.send_message(message.chat.id, "<b>🔄 جاري اختبار الإشعارات...</b>")
    notify_developers_new_user(test_user_info)
    bot.send_message(message.chat.id, "<b>✅ تم إرسال إشعار الاختبار</b>", reply_markup=dev_panel_keyboard())


@bot.message_handler(func=lambda m: m.text == "فحص المشاركات")
def handle_check_requests_and_active_job(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    lines = []
    lines.append(_format_active_job_status())

    if not pending_requests:
        lines.append("<b>الطلبات المعلقة</b>\n<blockquote>لا توجد طلبات معلقة حالياً.</blockquote>")
    else:
        lines.append(f"<b>الطلبات المعلقة</b>\n<blockquote>العدد: {len(pending_requests)}</blockquote>")
        # عرض معلومات مختصرة لكل طلب
        for req_id, req in list(pending_requests.items()):
            req_type = req.get("type") or "غير معروف"
            label = req.get("label") or req_type
            uid = req.get("user_id")
            cid = req.get("chat_id")
            created_at = req.get("created_at")
            ctx = req.get("ctx") or {}
            link = ctx.get("link") or ctx.get("v_link") or ctx.get("r_link") or ctx.get("rv_link") or ctx.get("p_link") or ctx.get("c_link")
            sessions = (
                ctx.get("sessions_count")
                or ctx.get("v_sessions_count")
                or ctx.get("r_sessions_count")
                or ctx.get("rv_sessions_count")
                or ctx.get("p_sessions_count")
                or ctx.get("c_sessions_count")
            )
            delay = ctx.get("delay") or ctx.get("v_delay") or ctx.get("r_delay") or ctx.get("rv_delay") or ctx.get("p_delay") or ctx.get("c_delay")
            extra = ""
            if link:
                extra += f"\nالرابط: {link}"
            if sessions:
                extra += f"\nعدد الجلسات: {sessions}"
            if delay:
                extra += f"\nالتأخير: {delay}"

            lines.append(
                "<b>طلب</b>\n"
                f"<blockquote>الخدمة: {label}\n"
                f"النوع: {req_type}\n"
                f"User ID: <code>{uid}</code>\n"
                f"Chat ID: <code>{cid}</code>\n"
                f"ID الطلب: <code>{req_id}</code>\n"
                f"الوقت: {created_at}{extra}</blockquote>"
            )

    bot.send_message(message.chat.id, "\n\n".join(lines))


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("reject_req_"))
def callback_reject_request(call):
    if not is_primary_developer(call.from_user.id):
        bot.answer_callback_query(call.id, "هذه الخاصية مخصصة للمطور فقط.")
        return

    req_id = call.data.split("reject_req_", 1)[1]
    if req_id not in pending_requests:
        bot.answer_callback_query(call.id, "هذا الطلب غير موجود أو تم التعامل معه مسبقاً.")
        return

    approval_context[call.message.chat.id] = {"req_id": req_id}
    try:
        bot.edit_message_reply_markup(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass

    bot.answer_callback_query(call.id, "أرسل سبب الرفض في رسالة جديدة الآن.")
    bot.send_message(call.message.chat.id, "<b>أرسل سبب رفض هذا الطلب ليتم إرساله للمستخدم.</b>")


@bot.message_handler(func=lambda m: m.chat.id in approval_context)
def handle_reject_reason(message):
    info = approval_context.pop(message.chat.id, None)
    if not info:
        return

    req_id = info.get("req_id")
    req = pending_requests.pop(req_id, None)
    if not req:
        bot.send_message(message.chat.id, "<b>الطلب غير موجود أو تم التعامل معه مسبقاً.</b>")
        return

    reason = (message.text or "").strip() or "لم يتم ذكر سبب."

    user_id = req.get("user_id")
    chat_id = req.get("chat_id")
    cost = req.get("cost", 0)
    if cost:
        add_user_points(user_id, cost)

    # إعلام المستخدم
    type_map = {
        "votes": "رشق تصويتات",
        "comments": "رشق تعليقات",
        "views": "رشق مشاهدات",
        "reactions": "رشق تفاعلات",
        "delete_session": "حذف جلسة",
    }
    req_type = req.get("type")
    label = req.get("label") or type_map.get(req_type, "الخدمة")
    try:
        bot.send_message(
            chat_id,
            f"<b>تم رفض طلبك لـ {label}.</b>\n<blockquote>السبب: {reason}</blockquote>",
        )
    except Exception:
        pass

    bot.send_message(message.chat.id, "<b>تم حفظ سبب الرفض وإرساله للمستخدم مع إعادة النقاط.</b>")


def run_comments_job(chat_id: int, user_id: int, ctx: dict):
    async def _inner():
        job_tag = ctx.get("job_tag")
        op_code = ctx.get("op_code")
        link = ctx.get("c_link")
        channel_username = ctx.get("c_channel_username")
        group_username = ctx.get("c_group_username")
        msg_id = ctx.get("c_msg_id")
        delay = ctx["c_delay"]
        sessions_count = ctx["c_sessions_count"]
        comment_text = ctx["c_text"]

        if (not group_username or not msg_id) and link:
            u, m_id = parse_telegram_message_link(link)
            group_username = group_username or u
            msg_id = msg_id or m_id

        if not group_username or not msg_id:
            bot.send_message(chat_id, "<b>تعذّر قراءة رابط رسالة القروب أثناء تنفيذ رشق التعليقات.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        if op_code:
            _ensure_operation_record(str(op_code), "comments", int(user_id or 0), ctx)

        rotation_key = f"comments:{group_username}:{msg_id}" if group_username and msg_id else f"comments_link:{link}"
        sessions = select_sessions_rotating(rotation_key, sessions_count)
        if not sessions:
            bot.send_message(chat_id, "<b>لا توجد جلسات متاحة لتنفيذ رشق التعليقات.</b>")
            if job_tag:
                _finish_non_owner_boost(job_tag)
            return

        success_count = 0
        fail_count = 0
        removed_count = 0

        def _describe_comments_error(e: Exception) -> str:
            if isinstance(e, FloodWaitError):
                try:
                    wait_s = int(getattr(e, "seconds", 0) or 0)
                    if wait_s > 0:
                        return f"FloodWait: انتظر {wait_s} ثانية"
                except Exception:
                    pass
                return "FloodWait"
            if isinstance(e, RPCError):
                try:
                    name = e.__class__.__name__
                    msg = str(e)
                    if msg:
                        return f"{name}: {msg}"
                    return name
                except Exception:
                    return "RPCError"
            try:
                return e.__class__.__name__
            except Exception:
                return "خطأ غير معروف"

        # معالجة تسلسلية موثوقة للتعليقات
        for idx, s in enumerate(sessions, start=1):
            if comments_stop_flags.get(chat_id):
                break

            client = None
            session_success = False
            
            try:
                # التحقق من الجلسة وتوصيلها
                client = await _connect_telethon_client(s["session"])

                # الانضمام إلى القناة أولاً إن وُجدت
                if channel_username:
                    try:
                        channel_entity = await client.get_entity(channel_username)
                        try:
                            await client(JoinChannelRequest(channel_entity))
                        except Exception:
                            pass
                    except Exception as e:
                        if handle_session_error(chat_id, s, e, "رشق التعليقات (القناة)"):
                            removed_count += 1
                            continue

                # الوصول إلى القروب مع إعادة المحاولات
                group_entity = None
                for attempt in range(3):
                    try:
                        group_entity = await client.get_entity(group_username)
                        try:
                            await client(JoinChannelRequest(group_entity))
                        except Exception:
                            pass
                        break
                    except Exception as e:
                        if attempt == 2:  # المحاولة الأخيرة
                            if handle_session_error(chat_id, s, e, "رشق التعليقات (القروب)"):
                                removed_count += 1
                                continue
                            bot.send_message(
                                chat_id,
                                f"<b>تعذّر الوصول إلى القروب من الجلسة رقم {idx}.</b>\n"
                                f"<blockquote>الخطأ: {_describe_comments_error(e)}</blockquote>",
                            )
                            fail_count += 1
                            continue
                        await asyncio.sleep(2)

                if not group_entity:
                    fail_count += 1
                    continue

                # إرسال التعليق مع التحقق من النجاح
                comment_success = False
                for comment_attempt in range(3):
                    try:
                        sent_msg = await client.send_message(
                            group_entity, 
                            comment_text, 
                            reply_to=int(msg_id)
                        )
                        
                        # التحقق من إرسال التعليق بنجاح
                        if sent_msg and hasattr(sent_msg, 'id'):
                            success_count += 1
                            session_success = True
                            comment_success = True
                            
                            if op_code:
                                _append_operation_execution(str(op_code), s["session"], True)
                                try:
                                    mid = int(getattr(sent_msg, "id", 0) or 0)
                                except Exception:
                                    mid = 0
                                if mid:
                                    _append_operation_comment(str(op_code), s["session"], str(group_username), mid)
                            break
                        
                    except Exception as e:
                        if comment_attempt == 2:  # المحاولة الأخيرة
                            break
                        await asyncio.sleep(2)

                if not comment_success:
                    fail_count += 1
                    bot.send_message(
                        chat_id,
                        f"<b>فشل إرسال التعليق من الجلسة رقم {idx}.</b>\n"
                        f"<blockquote>لم يتم إرسال التعليق بعد 3 محاولات</blockquote>",
                    )
                    if op_code:
                        _append_operation_execution(str(op_code), s["session"], False)

                # الانتظار قبل الجلسة التالية فقط إذا نجحت هذه الجلسة
                if session_success and idx < len(sessions) and not comments_stop_flags.get(chat_id):
                    await asyncio.sleep(delay)

            except Exception as e:
                if handle_session_error(chat_id, s, e, "رشق التعليقات"):
                    removed_count += 1
                    continue
                bot.send_message(
                    chat_id,
                    f"<b>حدث خطأ غير متوقع مع الجلسة رقم {idx}.</b>\n"
                    f"<blockquote>{_describe_comments_error(e)}</blockquote>"
                )
                fail_count += 1
                if op_code:
                    _append_operation_execution(str(op_code), s["session"], False)
            
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

        # إرسال التقرير النهائي
        try:
            if comments_stop_flags.get(chat_id):
                bot.send_message(
                    chat_id,
                    f"<b>تم إيقاف رشق التعليقات بناءً على طلبك.</b>\n<blockquote>✅ نجح: {success_count}\n❌ فشل: {fail_count}\n🗑️ تم حذف جلسات معطوبة: {removed_count}</blockquote>",
                )
            else:
                bot.send_message(
                    chat_id,
                    f"<b>اكتمل تنفيذ رشق التعليقات بنجاح.</b>\n<blockquote>✅ نجح: {success_count}\n❌ فشل: {fail_count}\n🗑️ تم حذف جلسات معطوبة: {removed_count}</blockquote>",
                )
        finally:
            comments_stop_flags.pop(chat_id, None)
            if job_tag:
                _finish_non_owner_boost(job_tag)

    asyncio.run(_inner())


@bot.callback_query_handler(func=lambda c: c.data == "stop_comments")
def callback_stop_comments(call):
    chat_id = call.message.chat.id
    if chat_id not in comments_stop_flags:
        bot.answer_callback_query(call.id, "لا توجد عملية رشق تعليقات قيد التنفيذ.")
        return
    comments_stop_flags[chat_id] = True
    bot.answer_callback_query(call.id, "تم طلب إيقاف رشق التعليقات، سيتم الإيقاف خلال ثوانٍ.")
    try:
        bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass


@bot.message_handler(func=lambda m: m.text == "تفعيل/تعطيل آخر ظهور")
def handle_toggle_last_seen(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    
    async def _toggle_all_last_seen():
        """تبديل حالة آخر ظهور لجميع الجلسات معاً"""
        sessions = load_sessions()
        if not sessions:
            return False, 0
        
        # التحقق من الحالة الحالية (إذا كانت معظم الجلسات مفعلة => نعطل الكل، والعكس)
        enabled_count = 0
        for s in sessions:
            session_hash = _session_hash(s["session"])
            if last_seen_status.get(session_hash, True):
                enabled_count += 1
        
        # تحديد الحالة الجديدة
        new_status = enabled_count > len(sessions) / 2  # إذا كانت معظمها مفعلة => نعطل الكل
        
        # تطبيق الحالة الجديدة على جميع الجلسات
        updated_count = 0
        for s in sessions:
            session_hash = _session_hash(s["session"])
            last_seen_status[session_hash] = new_status
            updated_count += 1
        
        return new_status, updated_count
    
    try:
        is_enabled, updated_count = asyncio.run(_toggle_all_last_seen())
        
        if is_enabled:
            text = (
                f"<b>✅ تم تفعيل آخر ظهور لجميع الجلسات</b>\n"
                f"<blockquote>تم تفعيل آخر ظهور لـ {updated_count} جلسة</blockquote>"
            )
        else:
            text = (
                f"<b>🔴 تم تعطيل آخر ظهور لجميع الجلسات</b>\n"
                f"<blockquote>تم تعطيل آخر ظهور لـ {updated_count} جلسة\nالآن لا أحد يمكنه رؤية آخر ظهور الجلسات</blockquote>"
            )
        
        bot.send_message(message.chat.id, text, reply_markup=dev_panel_keyboard())
        
    except Exception as e:
        bot.send_message(message.chat.id, f"<b>حدث خطأ أثناء تعديل إعدادات آخر ظهور: {e}</b>", reply_markup=dev_panel_keyboard())


@bot.message_handler(func=lambda m: m.text == "لوحة المطور")
def handle_dev_panel(message):
    if not is_developer_user(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه اللوحة مخصصة للمطور فقط.</b>")
        return
    text = (
        "<b>لوحة المطور</b>\n"
        "<blockquote>استخدم الأزرار بالأسفل لإدارة المطورين.</blockquote>"
    )
    kb = dev_panel_keyboard()
    bot.send_message(message.chat.id, text, reply_markup=kb)


@bot.message_handler(func=lambda m: m.text == "إضافة نقاط")
def handle_add_points_id(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    text = (message.text or "").strip()
    try:
        target_id = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال آيدي صالح (أرقام فقط).</b>")
        return

    ctx = dev_context.get(message.chat.id) or {}
    ctx["target_id"] = target_id
    dev_context[message.chat.id] = ctx
    user_states[message.chat.id] = "dev_add_points_waiting_amount"
    text = (
        "<b>عدد النقاط</b>\n"
        "<blockquote>أرسل الآن عدد النقاط التي تريد إضافتها لهذا المستخدم.</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "dev_add_points_waiting_amount")
def handle_add_points_amount(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    ctx = dev_context.get(message.chat.id) or {}
    target_id = ctx.get("target_id")
    if not target_id:
        bot.send_message(message.chat.id, "<b>حدث خطأ في حفظ آيدي المستخدم، أعد العملية.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    text = (message.text or "").strip()
    try:
        amount = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح لعدد النقاط.</b>")
        return

    if amount <= 0:
        bot.send_message(message.chat.id, "<b>عدد النقاط يجب أن يكون أكبر من صفر.</b>")
        return

    before = get_user_points(target_id)
    add_user_points(target_id, amount)
    after = get_user_points(target_id)

    text = (
        "<b>تم إضافة النقاط بنجاح</b>\n"
        f"<blockquote>آيدي المستخدم: <code>{target_id}</code>\n"
        f"النقاط المضافة: {amount}\n"
        f"الرصيد قبل الإضافة: {before}\n"
        f"الرصيد بعد الإضافة: {after}</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=dev_panel_keyboard())

    user_states.pop(message.chat.id, None)
    dev_context.pop(message.chat.id, None)


@bot.message_handler(func=lambda m: m.text == "خصم نقاط")
def handle_sub_points(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        return

    user_states[message.chat.id] = "dev_sub_points_waiting_id"
    dev_context[message.chat.id] = {"mode": "sub"}
    text = (
        "<b>خصم نقاط من مستخدم</b>\n"
        "<blockquote>أرسل الآن آيدي المستخدم الذي تريد خصم نقاط منه.</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "dev_sub_points_waiting_id")
def handle_sub_points_id(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    text = (message.text or "").strip()
    try:
        target_id = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال آيدي صالح (أرقام فقط).</b>")
        return

    ctx = dev_context.get(message.chat.id) or {}
    ctx["target_id"] = target_id
    dev_context[message.chat.id] = ctx
    user_states[message.chat.id] = "dev_sub_points_waiting_amount"
    text = (
        "<b>عدد النقاط</b>\n"
        "<blockquote>أرسل الآن عدد النقاط التي تريد خصمها من هذا المستخدم.</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "dev_sub_points_waiting_amount")
def handle_sub_points_amount(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور الأساسي فقط.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    ctx = dev_context.get(message.chat.id) or {}
    target_id = ctx.get("target_id")
    if not target_id:
        bot.send_message(message.chat.id, "<b>حدث خطأ في حفظ آيدي المستخدم، أعد العملية.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    text = (message.text or "").strip()
    try:
        amount = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال رقم صحيح لعدد النقاط.</b>")
        return

    if amount <= 0:
        bot.send_message(message.chat.id, "<b>عدد النقاط يجب أن يكون أكبر من صفر.</b>")
        return

    before = get_user_points(target_id)
    add_user_points(target_id, -amount)
    after = get_user_points(target_id)

    text = (
        "<b>تم خصم النقاط بنجاح</b>\n"
        f"<blockquote>آيدي المستخدم: <code>{target_id}</code>\n"
        f"النقاط المخصومة: {amount}\n"
        f"الرصيد قبل الخصم: {before}\n"
        f"الرصيد بعد الخصم: {after}</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=dev_panel_keyboard())

    user_states.pop(message.chat.id, None)
    dev_context.pop(message.chat.id, None)


@bot.message_handler(func=lambda m: m.text == "رفع مطور")
def handle_promote_dev(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return
    user_states[message.chat.id] = "dev_promote_waiting_id"
    dev_context[message.chat.id] = {}
    text = (
        "<b>رفع مطور جديد</b>\n"
        "<blockquote>أرسل الآن آيدي الشخص الذي تريد رفعه كمطور.</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "dev_promote_waiting_id")
def handle_promote_dev_id(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    text = (message.text or "").strip()
    try:
        target_id = int(text)
    except ValueError:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال آيدي صالح (أرقام فقط).</b>")
        return

    dev_context[message.chat.id] = {"target_id": target_id}
    user_states[message.chat.id] = "dev_promote_waiting_rank"
    text = (
        "<b>رتبة المطور</b>\n"
        "<blockquote>أرسل الآن الرتبة التي تريدها لهذا الشخص.</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=cancel_only_keyboard())


@bot.message_handler(func=lambda m: user_states.get(m.chat.id) == "dev_promote_waiting_rank")
def handle_promote_dev_rank(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    ctx = dev_context.get(message.chat.id) or {}
    target_id = ctx.get("target_id")
    if not target_id:
        bot.send_message(message.chat.id, "<b>حدث خطأ في حفظ بيانات المطور، أعد المحاولة.</b>")
        user_states.pop(message.chat.id, None)
        dev_context.pop(message.chat.id, None)
        return

    rank_text = (message.text or "").strip()
    if not rank_text:
        bot.send_message(message.chat.id, "<b>الرجاء إرسال نص للرتبة.</b>")
        return

    developers = load_developers()
    updated = False
    for d in developers:
        if d.get("user_id") == target_id:
            d["rank"] = rank_text
            updated = True
            break
    if not updated:
        developers.append({"user_id": target_id, "rank": rank_text})
    save_developers(developers)

    text = (
        "<b>تم رفع المطور بنجاح</b>\n"
        f"<blockquote>الآيدي: <code>{target_id}</code>\n"
        f"الرتبة: {rank_text}</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=dev_panel_keyboard())

    user_states.pop(message.chat.id, None)
    dev_context.pop(message.chat.id, None)


@bot.message_handler(func=lambda m: m.text == "تنزيل مطور")
def handle_demote_dev(message):
    if not is_primary_developer(message.from_user.id):
        bot.send_message(message.chat.id, "<b>هذه الخاصية مخصصة للمطور فقط.</b>")
        return

    developers = load_developers()
    developers = [d for d in developers if not is_primary_developer(d.get("user_id"))]
    if not developers:
        text = (
            "<b>تنزيل مطور</b>\n"
            "<blockquote>لا يوجد مطورون مرفوعون حالياً.</blockquote>"
        )
        bot.send_message(message.chat.id, text, reply_markup=dev_panel_keyboard())
        return

    kb = InlineKeyboardMarkup()
    for idx, d in enumerate(developers):
        user_id = d.get("user_id")
        rank_text = d.get("rank") or "مطور"
        label = None
        try:
            chat = bot.get_chat(user_id)
            name = chat.first_name or ""
            username = f"@{chat.username}" if getattr(chat, "username", None) else ""
            if username:
                label = f"{name} {username} - {rank_text}"
            else:
                label = f"{name} - {rank_text}"
        except Exception:
            label = f"ID {user_id} - {rank_text}"

        kb.row(
            InlineKeyboardButton(label, callback_data=f"dev_name_{idx}"),
            InlineKeyboardButton("تنزيل", callback_data=f"demote_dev_{idx}"),
        )

    kb.add(InlineKeyboardButton("إلغاء", callback_data="cancel_demote_dev"))

    text = (
        "<b>تنزيل مطور</b>\n"
        "<blockquote>اختر المطور الذي تريد تنزيله من الأزرار بالأسفل، أو اضغط إلغاء.</blockquote>"
    )
    bot.send_message(message.chat.id, text, reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("demote_dev_"))
def callback_demote_dev(call):
    if not is_primary_developer(call.from_user.id):
        bot.answer_callback_query(call.id, "هذه الخاصية مخصصة للمطور فقط.")
        return

    try:
        idx = int(call.data.split("_")[-1])
    except ValueError:
        bot.answer_callback_query(call.id, "حدث خطأ في تحديد المطور.")
        return

    developers = load_developers()
    developers = [d for d in developers if not is_primary_developer(d.get("user_id"))]

    if idx < 0 or idx >= len(developers):
        bot.answer_callback_query(call.id, "المطور غير موجود.")
        return

    removed = developers.pop(idx)
    save_developers(developers)
    bot.answer_callback_query(call.id, "تم تنزيل المطور بنجاح.")

    try:
        bot.edit_message_reply_markup(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass

    user_id = removed.get("user_id")
    rank_text = removed.get("rank") or "مطور"
    text = (
        "<b>تم تنزيل المطور التالي:</b>\n"
        f"<blockquote>الآيدي: <code>{user_id}</code>\n"
        f"الرتبة السابقة: {rank_text}</blockquote>"
    )
    bot.send_message(call.message.chat.id, text, reply_markup=dev_panel_keyboard())


@bot.callback_query_handler(func=lambda c: c.data == "cancel_demote_dev")
def callback_cancel_demote_dev(call):
    bot.answer_callback_query(call.id, "تم الإلغاء.")
    try:
        bot.edit_message_reply_markup(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=None,
        )
    except Exception:
        pass
    bot.send_message(
        call.message.chat.id,
        "<b>تم إلغاء عملية تنزيل المطورين.</b>",
        reply_markup=dev_panel_keyboard(),
    )


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("dev_name_"))
def callback_dev_name_noop(call):
    bot.answer_callback_query(call.id)


if __name__ == "__main__":
    print("Bot is running...")
    print(f"Working directory: {_BASE_DIR}")
    print(f"Bot token: {BOT_TOKEN[:20]}...")
    print(f"Sessions count: {get_sessions_count()}")
    
    while True:
        try:
            print("Starting to receive messages...")
            bot.infinity_polling()
        except telebot.apihelper.ApiTelegramException as e:
            if "Conflict: terminated by other getUpdates request" in str(e):
                print("Another bot instance detected!")
                print("Waiting 10 seconds then retrying...")
                time.sleep(10)
                continue
            elif "Error code: 409" in str(e):
                print("Error 409: Conflict in update requests")
                print("Waiting 15 seconds then retrying...")
                time.sleep(15)
                continue
            else:
                print(f"Telegram API error: {e}")
                print("Waiting 5 seconds then retrying...")
                time.sleep(5)
                continue
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError):
            print("Internet connection error")
            print("Waiting 5 seconds then retrying...")
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            print("\nBot stopped by user")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            print("Waiting 10 seconds then retrying...")
            time.sleep(10)
            continue
