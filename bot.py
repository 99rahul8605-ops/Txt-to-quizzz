import os
import logging
import threading
import time
import traceback
import asyncio
import html
import secrets
import string
import random
import aiohttp
import re
from flask import Flask, request as flask_request, jsonify, send_from_directory
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ApplicationBuilder,
    CallbackQueryHandler,
    PollAnswerHandler,
    InlineQueryHandler
)
from telegram.error import RetryAfter, BadRequest
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
import concurrent.futures

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
bot_start_time = time.time()
BOT_VERSION = "8.2"  # Premium plans version
temp_params = {}
DB = None  # Global async database instance
MONGO_CLIENT = None  # Global MongoDB client
SESSION = None  # Global aiohttp session

# API Configuration
AD_API = os.getenv('AD_API', '446b3a3f0039a2826f1483f22e9080963974ad3b')
WEBSITE_URL = os.getenv('WEBSITE_URL', 'upshrink.com')
YOUTUBE_TUTORIAL = "https://youtu.be/WeqpaV6VnO4?si=Y0pDondqe-nmIuht"
GITHUB_REPO = "Admin ko contact karo"
PREMIUM_CONTACT = "@rahul_g8"  # Premium contact

# Quiz limit configuration
DAILY_QUIZ_LIMIT = int(os.getenv('DAILY_QUIZ_LIMIT', 20))  # Default is 20 quizzes/day

# Invite & Points System
INVITE_POINTS = 2          # Points earned per successful invite
REDEEM_POINTS_REQUIRED = 10  # Points needed to redeem premium
REDEEM_PREMIUM_DAYS = 2    # Days of premium given on redemption

# Caches for performance
SUDO_CACHE = {}
TOKEN_CACHE = {}
PREMIUM_CACHE = {}
CACHE_EXPIRY = 60  # seconds

# Broadcast state
BROADCAST_STATE = {}

# Active quiz sessions for group quiz feature
# { session_id: { chat_id, questions, current_index, poll_message_id, owner_id, title } }
ACTIVE_QUIZ_SESSIONS = {}

# Pending save confirmations { user_id: { questions: [...], chat_id: int } }
PENDING_QUIZ_SAVE = {}

# Waiting for quiz title input { user_id: { questions: [...], chat_id: int } }
WAITING_QUIZ_TITLE = {}

# Pending group quiz approvals
# { approval_id: { chat_id, quiz_doc, owner_id, joined: set(), message_id, expires_at } }
PENDING_GROUP_QUIZ = {}

# Pending token rewards from webapp (Flask -> async bot bridge)
pending_tokens = {}

# Store message_ids of /token messages to delete after reward
TOKEN_MESSAGES = {}  # user_id -> (chat_id, message_id)

# Reference to bot application for use in background tasks
application_ref = [None]

# Reference to the async event loop (for Flask -> async bridge)
ASYNC_LOOP = [None]

# Flask app for health checks and Mini Web App
app = Flask(__name__)

@app.route('/')
@app.route('/health')
@app.route('/status')
def health_check():
    return "Bot is running", 200

@app.route('/webapp')
def serve_webapp():
    """Serve the Mini Web App HTML page"""
    return send_from_directory('.', 'webapp.html')

async def _db_get_temp_param(user_id):
    """Async helper: get temp param from DB"""
    try:
        if DB is not None:
            doc = await DB.temp_params.find_one({"user_id": user_id})
            if doc:
                return doc.get("param")
    except Exception as e:
        logger.error(f"DB temp_param get error: {e}")
    return None

async def _db_delete_temp_param(user_id):
    """Async helper: delete temp param from DB"""
    try:
        if DB is not None:
            await DB.temp_params.delete_one({"user_id": user_id})
    except Exception as e:
        logger.error(f"DB temp_param delete error: {e}")

@app.route('/claim', methods=['POST'])
def claim_reward():
    """API endpoint called by webapp after user watches ad"""
    try:
        data = flask_request.get_json()
        user_id = data.get('user_id')
        param = data.get('param')

        if not user_id or not param:
            return jsonify({"ok": False, "error": "Missing params"}), 400

        user_id = int(user_id)

        # Step 1: Check in-memory first (fast path)
        stored_param = temp_params.get(user_id)

        # Step 2: If not in memory (restart/redeploy), check DB
        if not stored_param and ASYNC_LOOP[0] is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(
                    _db_get_temp_param(user_id), ASYNC_LOOP[0]
                )
                stored_param = future.result(timeout=5)
            except Exception as e:
                logger.error(f"DB param fetch error: {e}")

        if not stored_param or stored_param != param:
            logger.warning(f"Claim failed for user {user_id}: param mismatch. Got={param}, Stored={stored_param}")
            return jsonify({"ok": False, "error": "Invalid or expired session"}), 403

        # Store token in pending_tokens dict — bot's async loop will pick it up
        pending_tokens[user_id] = {
            "token": stored_param,
            "created_at": datetime.utcnow(),
            "quiz_limit": int(os.getenv('DAILY_QUIZ_LIMIT', 20)),
            "quiz_used": 0
        }

        # Remove temp param from memory and DB
        temp_params.pop(user_id, None)
        TOKEN_CACHE.pop(user_id, None)
        if ASYNC_LOOP[0] is not None:
            try:
                asyncio.run_coroutine_threadsafe(
                    _db_delete_temp_param(user_id), ASYNC_LOOP[0]
                )
            except Exception:
                pass

        return jsonify({"ok": True, "message": "Access granted for 24 hours!"})

    except Exception as e:
        logger.error(f"Claim error: {e}")
        return jsonify({"ok": False, "error": "Server error"}), 500

def run_flask():
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, threaded=True)

# Convert UTC to IST (UTC+5:30)
def to_ist(utc_time):
    return utc_time + timedelta(hours=5, minutes=30)

# Format time in IST (12-hour format with AM/PM)
def format_ist(utc_time):
    ist_time = to_ist(utc_time)
    return ist_time.strftime("%Y-%m-%d %I:%M:%S %p")

# Format time left
def format_time_left(expiry):
    now = datetime.utcnow()
    if expiry < now:
        return "Expired"
    
    delta = expiry - now
    days = delta.days
    seconds = delta.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    
    parts = []
    if days > 0:
        parts.append(f"{days} days")
    if hours > 0:
        parts.append(f"{hours} hours")
    if minutes > 0:
        parts.append(f"{minutes} minutes")
    
    return ", ".join(parts) if parts else "Less than 1 minute"

# Async MongoDB connection
async def init_db():
    global DB, MONGO_CLIENT
    try:
        mongo_uri = os.getenv('MONGO_URI')
        if not mongo_uri:
            logger.error("MONGO_URI environment variable not set")
            return None
            
        MONGO_CLIENT = AsyncIOMotorClient(mongo_uri, maxPoolSize=100, minPoolSize=10)
        DB = MONGO_CLIENT.get_database("telegram_bot")
        await DB.command('ping')  # Test connection
        logger.info("MongoDB connection successful")
        return DB
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        return None

# Create index for tokens collection
async def create_ttl_index():
    try:
        if DB is not None:
            await DB.tokens.create_index("user_id", unique=True)
            logger.info("Created index for tokens")
    except Exception as e:
        logger.error(f"Error creating token index: {e}")

# Create index for sudo users
async def create_sudo_index():
    try:
        if DB is not None:
            await DB.sudo_users.create_index("user_id", unique=True)
            logger.info("Created index for sudo_users")
    except Exception as e:
        logger.error(f"Error creating sudo index: {e}")

# Create index for premium users
async def create_premium_index():
    try:
        if DB is not None:
            await DB.premium_users.create_index("user_id", unique=True)
            await DB.premium_users.create_index("expiry_date")
            logger.info("Created index for premium_users")
    except Exception as e:
        logger.error(f"Error creating premium index: {e}")

# Create index for invite points collection
async def create_invite_index():
    try:
        if DB is not None:
            await DB.invite_points.create_index("user_id", unique=True)
            await DB.invite_points.create_index("invited_users")
            await DB.redeem_requests.create_index("user_id")
            await DB.redeem_requests.create_index("status")
            logger.info("Created index for invite_points and redeem_requests")
    except Exception as e:
        logger.error(f"Error creating invite index: {e}")

# Create index for saved_quizzes collection
async def create_quiz_index():
    try:
        if DB is not None:
            await DB.saved_quizzes.create_index("quiz_id", unique=True)
            await DB.saved_quizzes.create_index("user_id")
            logger.info("Created index for saved_quizzes")
    except Exception as e:
        logger.error(f"Error creating quiz index: {e}")

# Optimized user interaction recording
async def record_user_interaction(update: Update):
    try:
        # Check if DB is initialized (not None)
        if DB is None:
            return
            
        user = update.effective_user
        if not user:
            return
            
        # Use update with upsert
        await DB.users.update_one(
            {"user_id": user.id},
            {"$set": {
                "first_name": user.first_name,
                "last_name": user.last_name,
                "username": user.username,
                "last_interaction": datetime.utcnow()
            }},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error saving user data: {e}")

# Generate a random parameter
def generate_random_param(length=8):
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# Optimized URL shortening with connection pooling
async def get_shortened_url(deep_link):
    global SESSION
    try:
        if SESSION is None:
            SESSION = aiohttp.ClientSession()
            
        api_url = f"https://{WEBSITE_URL}/api?api={AD_API}&url={deep_link}"
        async with SESSION.get(api_url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("status") == "success":
                    return data.get("shortenedUrl")
        return None
    except asyncio.TimeoutError:
        logger.warning("URL shortening timed out")
        return None
    except Exception as e:
        logger.error(f"URL shortening failed: {e}")
        return None

# Optimized sudo check with caching
async def is_sudo(user_id):
    # Check cache first
    cached = SUDO_CACHE.get(user_id)
    if cached and time.time() < cached['expiry']:
        return cached['result']
        
    owner_id = os.getenv('OWNER_ID')
    if owner_id and str(user_id) == owner_id:
        result = True
    else:
        result = False
        # Check if DB is initialized (not None)
        if DB is not None:
            try:
                result = await DB.sudo_users.find_one({"user_id": user_id}) is not None
            except Exception as e:
                logger.error(f"Sudo check error: {e}")
    
    # Update cache
    SUDO_CACHE[user_id] = {
        'result': result,
        'expiry': time.time() + CACHE_EXPIRY
    }
    return result

# ─── INVITE & POINTS HELPERS ──────────────────────────────────────────────────

async def get_user_points(user_id: int) -> int:
    """Get current invite points for a user"""
    if DB is None:
        return 0
    try:
        doc = await DB.invite_points.find_one({"user_id": user_id})
        return doc.get("points", 0) if doc else 0
    except Exception as e:
        logger.error(f"get_user_points error: {e}")
        return 0

async def add_invite_points(referrer_id: int, new_user_id: int) -> bool:
    """
    Award INVITE_POINTS to referrer when a new user joins via their link.
    Returns True if points were awarded (first-time invite only).
    """
    if DB is None:
        return False
    try:
        # Ensure the new user hasn't already been counted
        existing = await DB.invite_points.find_one({"invited_users": new_user_id})
        if existing:
            return False  # already credited for this user

        result = await DB.invite_points.update_one(
            {"user_id": referrer_id},
            {
                "$inc": {"points": INVITE_POINTS},
                "$push": {"invited_users": new_user_id},
                "$setOnInsert": {"user_id": referrer_id}
            },
            upsert=True
        )
        return True
    except Exception as e:
        logger.error(f"add_invite_points error: {e}")
        return False

async def redeem_points_for_premium(user_id: int) -> tuple[bool, str]:
    """
    Auto-grant REDEEM_PREMIUM_DAYS of premium if user has enough points.
    """
    if DB is None:
        return False, "Database unavailable."
    try:
        doc = await DB.invite_points.find_one({"user_id": user_id})
        points = doc.get("points", 0) if doc else 0

        if points < REDEEM_POINTS_REQUIRED:
            return False, f"You only have *{points} points*. Need *{REDEEM_POINTS_REQUIRED}* to redeem."

        # Deduct points first
        await DB.invite_points.update_one(
            {"user_id": user_id},
            {"$inc": {"points": -REDEEM_POINTS_REQUIRED}}
        )

        # Grant premium
        now = datetime.utcnow()
        expiry = now + timedelta(days=REDEEM_PREMIUM_DAYS)

        # If already has premium, extend it
        existing = await DB.premium_users.find_one({"user_id": user_id})
        if existing and existing["expiry_date"] > now:
            expiry = existing["expiry_date"] + timedelta(days=REDEEM_PREMIUM_DAYS)

        await DB.premium_users.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id": user_id,
                "expiry_date": expiry,
                "plan": f"{REDEEM_PREMIUM_DAYS}-Day (Points Redeem)",
                "granted_at": now
            }},
            upsert=True
        )
        PREMIUM_CACHE.pop(user_id, None)

        expiry_ist = format_ist(expiry)
        return True, (
            f"🎉 *Premium Activated!*\n\n"
            f"✅ *{REDEEM_PREMIUM_DAYS}-day Premium* unlocked\n"
            f"📅 Expires: `{expiry_ist}` IST\n\n"
            f"Enjoy unlimited quiz creation! 🚀"
        )
    except Exception as e:
        logger.error(f"redeem_points_for_premium error: {e}")
        return False, "Something went wrong. Please try again."

# ─── INVITE COMMAND ────────────────────────────────────────────────────────────

async def invite_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user = update.effective_user
    user_id = user.id

    points = await get_user_points(user_id)
    bot_username = (await context.bot.get_me()).username
    invite_link = f"https://t.me/{bot_username}?start=ref_{user_id}"

    needed = max(0, REDEEM_POINTS_REQUIRED - points)
    progress_filled = min(points, REDEEM_POINTS_REQUIRED)
    progress_bar = "🟢" * progress_filled + "⚪" * (REDEEM_POINTS_REQUIRED - progress_filled)

    msg = (
        f"👥 *Your Invite Dashboard*\n\n"
        f"🔗 *Your invite link:*\n`{invite_link}`\n\n"
        f"📊 *Points Progress:*\n"
        f"{progress_bar}\n"
        f"💎 `{points}/{REDEEM_POINTS_REQUIRED}` points\n\n"
        f"📋 *How it works:*\n"
        f"• Share your link with friends\n"
        f"• Each new user = *+{INVITE_POINTS} points* for you\n"
        f"• Collect *{REDEEM_POINTS_REQUIRED} points* → get *{REDEEM_PREMIUM_DAYS}-day Premium FREE*\n\n"
    )

    if points >= REDEEM_POINTS_REQUIRED:
        msg += f"🎁 *You have enough points to redeem premium!*"
        keyboard = [
            [InlineKeyboardButton("🎁 Redeem Premium Now!", callback_data="redeem_points")],
            [InlineKeyboardButton("📤 Share Invite Link", url=f"https://t.me/share/url?url={invite_link}&text=Join+this+quiz+bot!")]
        ]
    else:
        msg += f"📌 Invite *{needed} more friend(s)* to unlock free premium!"
        keyboard = [
            [InlineKeyboardButton("📤 Share Invite Link", url=f"https://t.me/share/url?url={invite_link}&text=Join+this+quiz+bot!")],
            [InlineKeyboardButton("💎 Buy Premium Instead", callback_data="premium_plans")]
        ]

    await update.message.reply_text(
        msg,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ─── POINTS COMMAND ───────────────────────────────────────────────────────────

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user_id = update.effective_user.id
    points = await get_user_points(user_id)
    needed = max(0, REDEEM_POINTS_REQUIRED - points)

    progress_filled = min(points, REDEEM_POINTS_REQUIRED)
    progress_bar = "🟢" * progress_filled + "⚪" * (REDEEM_POINTS_REQUIRED - progress_filled)

    msg = (
        f"💎 *Your Points Balance*\n\n"
        f"{progress_bar}\n"
        f"`{points}/{REDEEM_POINTS_REQUIRED}` points collected\n\n"
    )

    if points >= REDEEM_POINTS_REQUIRED:
        msg += "✅ You can redeem *{}-day Premium* right now!".format(REDEEM_PREMIUM_DAYS)
        keyboard = [[InlineKeyboardButton("🎁 Redeem Premium", callback_data="redeem_points")]]
    else:
        msg += f"📌 Need *{needed} more point(s)* to redeem free premium.\nUse /invite to earn points!"
        keyboard = [
            [InlineKeyboardButton("👥 Invite Friends", callback_data="show_invite")],
            [InlineKeyboardButton("💎 Buy Premium", callback_data="premium_plans")]
        ]

    await update.message.reply_text(
        msg, parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ─── REDEEM COMMAND ───────────────────────────────────────────────────────────

async def redeem_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user_id = update.effective_user.id

    if await is_premium(user_id):
        await update.message.reply_text(
            "🌟 You already have an active premium plan!\nUse /myplan to check details.",
            parse_mode='Markdown'
        )
        return

    success, msg = await redeem_points_for_premium(user_id)
    keyboard = [[InlineKeyboardButton("📋 View My Plan", callback_data="my_plan")]] if success else \
               [[InlineKeyboardButton("👥 Invite Friends", callback_data="show_invite"),
                 InlineKeyboardButton("💎 Buy Premium", callback_data="premium_plans")]]
    await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

# ──────────────────────────────────────────────────────────────────────────────

# Ad-based access command (replaces old token system)
async def token_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user = update.effective_user
    user_id = user.id

    # Block during active quiz in this chat
    if await is_quiz_running(update.effective_chat.id):
        await update.message.reply_text(
            "⏳ Quiz chal rahi hai! Pehle /stopquiz se rok do.",
            parse_mode='Markdown'
        )
        return

    # Premium and sudo users don't need tokens
    if await is_sudo(user_id) or await is_premium(user_id):
        await update.message.reply_text(
            "🌟 You are a premium user! You don't need to watch ads.",
            parse_mode='Markdown'
        )
        return

    # Check if user already has active token — cooldown 1 hour
    if DB is not None:
        token_data = await DB.tokens.find_one({"user_id": user_id})
        if token_data:
            quiz_used = token_data.get("quiz_used", 0)
            quiz_limit = token_data.get("quiz_limit", DAILY_QUIZ_LIMIT)
            created_at = token_data.get("created_at")
            quizzes_left = quiz_limit - quiz_used

            if quizzes_left > 0 and created_at:
                cooldown_end = created_at + timedelta(hours=1)
                if datetime.utcnow() < cooldown_end:
                    time_left = cooldown_end - datetime.utcnow()
                    minutes = int(time_left.total_seconds() // 60)
                    seconds = int(time_left.total_seconds() % 60)
                    await update.message.reply_text(
                        f"⏳ <b>Cooldown Active!</b>\n\n"
                        f"You still have <b>{quizzes_left} quiz(es)</b> remaining.\n"
                        f"Please wait <b>{minutes}m {seconds}s</b> before watching another ad.",
                        parse_mode='HTML'
                    )
                    return

    # Generate a session param tied to this user
    param = generate_random_param()
    temp_params[user_id] = param

    # Auto-detect server URL — koi env set karne ki zaroorat nahi
    # Render pe RENDER_EXTERNAL_URL automatically available hota hai
    webapp_base = (
        os.getenv('WEBAPP_URL') or
        os.getenv('RENDER_EXTERNAL_URL') or
        f"http://localhost:{os.environ.get('PORT', 8000)}"
    )
    webapp_url = f"{webapp_base}/webapp?user_id={user_id}&param={param}"

    response_text = (
        "🎬 <b>Watch a short ad to unlock 24-hour access!</b>\n\n"
        "✨ <b>What you'll get:</b>\n"
        "1. Full access for 24 hours\n"
        "2. Unlimited commands\n"
        "3. All features unlocked\n\n"
        "👇 Tap the button below, watch the ad, then claim your reward."
    )

    keyboard = [[
        InlineKeyboardButton(
            "▶️ Watch Ad & Get Access",
            web_app=WebAppInfo(url=webapp_url)
        )
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    sent = await update.message.reply_text(
        response_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )
    # Store message info for deletion after reward
    TOKEN_MESSAGES[user_id] = (update.effective_chat.id, sent.message_id)

# Refresh command — wipes all user data from DB and cache
async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id

    # Only admin/sudo can use this
    if not await is_sudo(user_id):
        await update.message.reply_text("❌ This command is restricted to admins only.")
        return

    # Clear all in-memory caches completely
    TOKEN_CACHE.clear()
    PREMIUM_CACHE.clear()
    SUDO_CACHE.clear()
    temp_params.clear()
    TOKEN_MESSAGES.clear()
    pending_tokens.clear()

    # Delete ALL users data from DB
    deleted = {}
    if DB is not None:
        try:
            r1 = await DB.tokens.delete_many({})
            r2 = await DB.users.delete_many({})
            r3 = await DB.invite_points.delete_many({})
            deleted = {
                "tokens": r1.deleted_count,
                "users": r2.deleted_count,
                "invite_points": r3.deleted_count,
            }
        except Exception as e:
            logger.error(f"Refresh DB error: {e}")

    await update.message.reply_text(
        "🔄 <b>Full Reset Complete!</b>\n\n"
        "<b>All users data cleared:</b>\n"
        f"• ✅ Tokens: <code>{deleted.get('tokens', 0)}</code> records\n"
        f"• ✅ Users: <code>{deleted.get('users', 0)}</code> records\n"
        f"• ✅ Invite points: <code>{deleted.get('invite_points', 0)}</code> records\n"
        "• ✅ All in-memory caches\n\n"
        "Everyone will need to /token again.",
        parse_mode='HTML'
    )

# Token verification helper
# ─── QUIZ ACTIVE GUARD ────────────────────────────────────────────────────────

def get_active_session_for_chat(chat_id: int):
    """Return (session_id, session) if a quiz is running in this chat, else None"""
    for sid, sess in ACTIVE_QUIZ_SESSIONS.items():
        if sess.get("chat_id") == chat_id:
            return sid, sess
    return None

async def is_quiz_running(chat_id: int) -> bool:
    return get_active_session_for_chat(chat_id) is not None

async def stopquiz_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stop an active quiz — DM: only starter | Group: starter or admin"""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    chat_type = update.effective_chat.type  # 'private', 'group', 'supergroup'

    result = get_active_session_for_chat(chat_id)
    if not result:
        await update.message.reply_text("⚠️ Is chat mein koi quiz nahi chal rahi abhi.")
        return

    session_id, session = result
    owner_id = session.get("owner_id")

    # Permission check
    allowed = False

    if chat_type == "private":
        # DM mein sirf starter allowed hai (ya sudo)
        allowed = (user_id == owner_id) or await is_sudo(user_id)
    else:
        # Group mein: starter, sudo, ya group admin
        if user_id == owner_id or await is_sudo(user_id):
            allowed = True
        else:
            try:
                member = await context.bot.get_chat_member(chat_id, user_id)
                if member.status in ("administrator", "creator"):
                    allowed = True
            except Exception:
                pass

    if not allowed:
        await update.message.reply_text(
            "🚫 Sirf quiz start karne wala ya group admin hi quiz rok sakta hai."
        )
        return

    # Stop the quiz
    ACTIVE_QUIZ_SESSIONS.pop(session_id, None)
    title = session.get('title', 'Unknown')
    done = session.get('current_index', 0)
    total = len(session.get('questions', []))
    await update.message.reply_text(
        f"🛑 *Quiz rok di gayi!*\n\n"
        f"📋 Quiz: *{title}*\n"
        f"❓ Completed: {done}/{total} questions",
        parse_mode='Markdown'
    )

# ──────────────────────────────────────────────────────────────────────────────

async def check_access(update: Update, context: ContextTypes.DEFAULT_TYPE, handler):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # Block all commands while a quiz is active in this chat
    if await is_quiz_running(chat_id):
        await update.message.reply_text(
            "⏳ *Quiz chal rahi hai!*\n\n"
            "Quiz khatam hone tak doosre commands nahi chalenge.\n"
            "Quiz rok ne ke liye /stopquiz use karein.",
            parse_mode='Markdown'
        )
        return

    if await is_sudo(user_id) or await is_premium(user_id) or await has_valid_token(user_id):
        return await handler(update, context)
    
    await update.message.reply_text(
        "🔒 Access restricted! You need premium or to watch an ad to use this feature.\n\n"
        "Use /token to watch a short ad and get 24-hour access, or contact us for premium.",
        parse_mode='Markdown'
    )

# Wrapper functions for access verification
async def start_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Skip token check for the start command itself
    await start(update, context)

async def help_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, help_command)

async def create_quiz_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, create_quiz)

async def stats_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, stats_command)

async def handle_document_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await check_access(update, context, handle_document)

# Original command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user = update.effective_user
    user_id = user.id

    # ── Handle referral deep-link (/start ref_XXXXXXX) ──────────────────────
    if context.args:
        arg = context.args[0]

        # Handle quiz deep-link (/start quiz_QUIZID) — used when bot added to group
        if arg.startswith("quiz_"):
            quiz_id = arg[5:]
            chat_id = update.effective_chat.id
            try:
                quiz_doc = await DB.saved_quizzes.find_one({"quiz_id": quiz_id})
                if quiz_doc:
                    is_group = update.effective_chat.type in ("group", "supergroup")
                    if is_group:
                        await start_group_quiz_with_approval(context.bot, chat_id, quiz_doc, user_id)
                    else:
                        session_id = str(chat_id) + "_" + quiz_id
                        ACTIVE_QUIZ_SESSIONS[session_id] = {
                            "chat_id": chat_id,
                            "questions": quiz_doc["questions"],
                            "current_index": 0,
                            "title": quiz_doc["title"],
                            "quiz_id": quiz_id,
                            "owner_id": user_id,
                            "poll_message_id": None,
                            "active_poll_id": None,
                            "scores": {},
                            "open_period": quiz_doc.get("open_period", 10)
                        }
                        msg = await update.message.reply_text(
                            "📋 *" + quiz_doc["title"] + "*\n❓ " + str(quiz_doc["total"]) + " questions\n\nShuru ho rahi hai... 🎯",
                            parse_mode='Markdown'
                        )
                        await countdown_and_start(context.bot, chat_id, session_id, msg.message_id)
                    return
                else:
                    await update.message.reply_text("Quiz nahi mili. Shayad delete ho gayi.")
                    return
            except Exception as e:
                logger.error(f"Quiz deep link error: {e}")
                await update.message.reply_text("Quiz start karne mein error aaya.")
                return

        if arg.startswith("ref_"):
            try:
                referrer_id = int(arg.split("_", 1)[1])
                if referrer_id != user_id:          # can't invite yourself
                    awarded = await add_invite_points(referrer_id, user_id)
                    if awarded:
                        # Notify referrer
                        try:
                            referrer_points = await get_user_points(referrer_id)
                            notif = (
                                f"🎉 *Someone joined using your invite link!*\n\n"
                                f"💎 You earned *+{INVITE_POINTS} points*\n"
                                f"📊 Total points: *{referrer_points}/{REDEEM_POINTS_REQUIRED}*\n"
                            )
                            if referrer_points >= REDEEM_POINTS_REQUIRED:
                                notif += f"\n🎁 *You can now redeem {REDEEM_PREMIUM_DAYS}-day Premium!* Use /redeem"
                            else:
                                left = REDEEM_POINTS_REQUIRED - referrer_points
                                notif += f"\n📌 Invite *{left} more* to unlock free premium!"

                            keyboard = [[InlineKeyboardButton("🎁 Redeem Premium", callback_data="redeem_points")]] \
                                if referrer_points >= REDEEM_POINTS_REQUIRED else \
                                [[InlineKeyboardButton("👥 My Invites", callback_data="show_invite")]]

                            await context.bot.send_message(
                                chat_id=referrer_id,
                                text=notif,
                                parse_mode='Markdown',
                                reply_markup=InlineKeyboardMarkup(keyboard)
                            )
                        except Exception:
                            pass
            except (ValueError, IndexError):
                pass
    # ────────────────────────────────────────────────────────────────────────

    welcome_msg = (
        "🌟 *Welcome to Quiz Bot!* 🌟\n\n"
        "I can turn your text files into interactive 10-second quizzes!\n\n"
        "🔹 /createquiz — Start quiz creation\n"
        "🔹 /help — Show formatting guide\n"
        "🔹 /token — Watch a short ad for 24h access\n"
        "🔹 /invite — Invite friends & earn free Premium\n"
        "🔹 /points — Check your invite points\n"
        "🔹 Premium users get unlimited access!\n\n"
    )

    if not (await is_sudo(user_id) or await is_premium(user_id)):
        welcome_msg += "🔒 Use /token to unlock all features for 24 hours\n\n"

    welcome_msg += "Let's make learning fun!"

    keyboard = [
        [
            InlineKeyboardButton("🎥 Watch Tutorial", url=YOUTUBE_TUTORIAL),
            InlineKeyboardButton("💎 Premium Plans", callback_data="premium_plans")
        ],
        [
            InlineKeyboardButton("👥 Invite & Earn Points", callback_data="show_invite")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        welcome_msg,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    keyboard = [
        [
            InlineKeyboardButton("🎥 Watch Tutorial", url=YOUTUBE_TUTORIAL),
            InlineKeyboardButton("💎 Premium Plans", callback_data="premium_plans")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📝 *Quiz File Format Guide:*\n\n"
        "```\n"
        "What is 2+2?\n"
        "A) 3\n"
        "B) 4\n"
        "C) 5\n"
        "D) 6\n"
        "Answer: 2\n"
        "The correct answer is 4\n\n"
        "Python is a...\n"
        "A. Snake\n"
        "B. Programming language\n"
        "C. Coffee brand\n"
        "D. Movie\n"
        "Answer: 2\n"
        "```\n\n"
        "📌 *Rules:*\n"
        "• One question per block (separated by blank lines)\n"
        "• Exactly 4 options (any prefix format accepted)\n"
        "• Answer format: 'Answer: <1-4>' (1=first option, 2=second, etc.)\n"
        "• Optional 7th line for explanation (any text)\n\n"
        "💡 *Premium Benefits:*\n"
        "- Unlimited quiz creation\n"
        "- No token required\n"
        "- Priority support",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def plan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Create premium plans message with HTML formatting
    plans_message = (
        "<b>💠 UPGRADE TO PREMIUM 💠</b>\n\n"
        "<b>🚀 Premium Features:</b>\n"
        "🧠 UNLIMITED QUIZ CREATION\n\n"
        
        "<b>🔓 FREE PLAN</b> (with restrictions)\n"
        "🕰️ <b>Expiry:</b> Never\n"
        "💰 <b>Price:</b> ₹0\n\n"
        
        "<b>🕐 1-DAY PLAN</b>\n"
        "💰 <b>Price:</b> ₹10 🇮🇳\n"
        "📅 <b>Duration:</b> 1 Day\n\n"
        
        "<b>📆 1-WEEK PLAN</b>\n"
        "💰 <b>Price:</b> ₹25 🇮🇳\n"
        "📅 <b>Duration:</b> 10 Days\n\n"
        
        "<b>🗓️ MONTHLY PLAN</b>\n"
        "💰 <b>Price:</b> ₹50 🇮🇳\n"
        "📅 <b>Duration:</b> 1 Month\n\n"
        
        "<b>🪙 2-MONTH PLAN</b>\n"
        "💰 <b>Price:</b> ₹100 🇮🇳\n"
        "📅 <b>Duration:</b> 2 Months\n\n"
        
        f"📞 <b>Contact Now to Upgrade</b>\n👉 {PREMIUM_CONTACT}"
    )
    
    keyboard = [
        [InlineKeyboardButton("💎 Get Premium", url=f"https://t.me/{PREMIUM_CONTACT.lstrip('@')}")],
        [InlineKeyboardButton("📋 My Plan", callback_data="my_plan")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Check if we're in a callback context (button press)
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text(
            text=plans_message,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            plans_message,
            parse_mode='HTML',
            reply_markup=reply_markup
        )

async def create_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    await update.message.reply_text(
        "📤 *Ready to create your quiz!*\n\n"
        "Please send me a .txt file containing your questions.\n\n"
        "Need format help? Use /help",
        parse_mode='Markdown'
    )

def preprocess_content(content: str) -> str:
    """Preprocess content to handle various text formats"""
    # Normalize line endings
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    
    # Handle numbered questions (1., 2., etc.)
    content = re.sub(r'^\d+\.\s*', '', content, flags=re.MULTILINE)
    
    # Handle bullet points
    content = re.sub(r'^[•\-*]\s*', '', content, flags=re.MULTILINE)
    
    # Remove extra blank lines but keep question separators
    content = re.sub(r'\n\s*\n', '\n\n', content)
    
    # Trim whitespace from each line
    lines = [line.strip() for line in content.split('\n')]
    
    # Remove empty lines at start and end
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    
    return '\n'.join(lines)

def parse_quiz_file(content: str) -> tuple:
    """Robust quiz parser that handles different text formats"""
    # Normalize line endings and clean up content
    content = content.replace('\r\n', '\n').replace('\r', '\n')  # Convert all line endings to \n
    content = re.sub(r'\n\s*\n', '\n\n', content)  # Normalize multiple blank lines
    content = content.strip()  # Remove leading/trailing whitespace
    
    blocks = content.split('\n\n')
    valid_questions = []
    errors = []
    
    for i, block in enumerate(blocks, 1):
        if not block.strip():
            continue
            
        lines = [line.strip() for line in block.split('\n') if line.strip()]
        
        # More flexible validation - allow 5-7 lines per question block
        if len(lines) < 5:
            errors.append(f"❌ Question {i}: Too few lines ({len(lines)}), need at least 5")
            continue
            
        if len(lines) > 7:
            errors.append(f"❌ Question {i}: Too many lines ({len(lines)}), maximum 7 allowed")
            continue
        
        # Extract components with flexible parsing
        question = lines[0]
        
        # Find options (next 4 non-empty lines or until answer line)
        options = []
        option_lines = []
        
        for line in lines[1:]:
            # Stop if we find an answer line
            if line.lower().startswith('answer:'):
                break
            option_lines.append(line)
        
        # Take first 4 lines as options
        if len(option_lines) >= 4:
            options = option_lines[:4]
        else:
            errors.append(f"❌ Q{i}: Need exactly 4 options, found {len(option_lines)}")
            continue
        
        # Find answer line
        answer_line = None
        explanation = None
        
        for j, line in enumerate(lines):
            if line.lower().startswith('answer:'):
                answer_line = line
                # Check if there's an explanation after the answer
                if j + 1 < len(lines):
                    explanation = lines[j + 1]
                break
        
        if not answer_line:
            errors.append(f"❌ Q{i}: Missing 'Answer:' line")
            continue
        
        # Parse answer number
        try:
            answer_text = answer_line.split(':', 1)[1].strip()
            # Handle various answer formats: "1", "A", "a", "B)", etc.
            if answer_text.isdigit():
                answer_num = int(answer_text)
            else:
                # Handle letter answers: A=1, B=2, C=3, D=4
                answer_char = answer_text.upper()[0]
                if answer_char in 'ABCD':
                    answer_num = ord(answer_char) - ord('A') + 1
                else:
                    raise ValueError(f"Invalid answer format: {answer_text}")
            
            if not 1 <= answer_num <= 4:
                errors.append(f"❌ Q{i}: Invalid answer number {answer_num}")
                continue
                
        except (ValueError, IndexError, TypeError) as e:
            errors.append(f"❌ Q{i}: Malformed answer line - {str(e)}")
            continue
        
        # Validate that explanation doesn't look like another question
        if explanation and len(explanation.split()) > 10:
            # If explanation is too long, it might be the next question
            explanation = None
        
        valid_questions.append((question, options, answer_num - 1, explanation))
    
    return valid_questions, errors

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_id = user.id
    await record_user_interaction(update)
    
    # Check if user is premium
    is_prem = await is_premium(user_id)
    
    # For token users, check token quiz limit
    token_data = None
    quiz_used = 0
    quiz_limit = DAILY_QUIZ_LIMIT
    if not is_prem and DB is not None:
        token_data = await DB.tokens.find_one({"user_id": user_id})
        if token_data:
            quiz_used = token_data.get("quiz_used", 0)
            quiz_limit = token_data.get("quiz_limit", DAILY_QUIZ_LIMIT)

        if quiz_used >= quiz_limit:
            # Check user's current invite points
            user_points = await get_user_points(user_id)
            can_redeem = user_points >= REDEEM_POINTS_REQUIRED

            limit_msg = (
                f"⚠️ <b>Quiz Limit Reached!</b>\n\n"
                f"You've used all <b>{quiz_limit} quizzes</b> from your current access.\n\n"
                f"<b>How to continue?</b>\n"
                f"▶️ Watch another ad — get {DAILY_QUIZ_LIMIT} more quizzes free\n"
                f"💎 Upgrade to Premium — unlimited quizzes\n"
                f"🎁 Redeem invite points — get {REDEEM_PREMIUM_DAYS}-day premium free\n\n"
                f"💡 Your points: <b>{user_points}/{REDEEM_POINTS_REQUIRED}</b>"
            )

            keyboard = [
                [InlineKeyboardButton("▶️ Watch Ad for More", callback_data="get_token")],
                [InlineKeyboardButton("💎 Get Premium", callback_data="premium_plans")],
            ]
            if can_redeem:
                keyboard.insert(1, [InlineKeyboardButton(f"🎁 Redeem {REDEEM_PREMIUM_DAYS}-Day Premium ({REDEEM_POINTS_REQUIRED} pts)", callback_data="redeem_points")])
            else:
                keyboard.append([InlineKeyboardButton(f"👥 Invite Friends & Earn Points ({user_points}/{REDEEM_POINTS_REQUIRED})", callback_data="show_invite")])

            await update.message.reply_text(
                limit_msg,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
    
    if not update.message.document.file_name.endswith('.txt'):
        await update.message.reply_text("❌ Please send a .txt file")
        return
    
    try:
        # Download directly to memory
        file = await context.bot.get_file(update.message.document.file_id)
        content = await file.download_as_bytearray()
        content = content.decode('utf-8')
        
        # Preprocess and parse
        processed_content = preprocess_content(content)
        valid_questions, errors = parse_quiz_file(processed_content)
        
        # For non-premium users, enforce token quiz limit
        if not is_prem and valid_questions and token_data:
            remaining_quota = quiz_limit - quiz_used
            if remaining_quota <= 0:
                await update.message.reply_text(
                    f"⚠️ <b>Quiz limit reached!</b>\n\nWatch another ad with /token to get more quizzes.",
                    parse_mode='HTML'
                )
                return
            if len(valid_questions) > remaining_quota:
                valid_questions = valid_questions[:remaining_quota]
                if not errors:
                    errors = []
                errors.append(f"⚠️ Only first {remaining_quota} questions sent due to token limit")
        
        # Report errors
        if errors:
            error_msg = "\n".join(errors[:5])
            if len(errors) > 5:
                error_msg += f"\n\n...and {len(errors)-5} more errors"
            await update.message.reply_text(
                f"⚠️ Found {len(errors)} error(s):\n\n{error_msg}"
            )
        
        # Send quizzes with rate limiting
        if valid_questions:
            msg = await update.message.reply_text(
                f"✅ Sending {len(valid_questions)} quiz question(s)..."
            )
            
            opt_prefix_re = re.compile(r'^[A-Da-d][\.\):\s]+')
            sent_count = 0
            for question, options, correct_id, explanation in valid_questions:
                try:
                    # Telegram poll question limit is 300 characters
                    POLL_QUESTION_LIMIT = 300

                    if len(question) > POLL_QUESTION_LIMIT:
                        # Step 1: Send full question + options in code block
                        option_labels = ['A', 'B', 'C', 'D']
                        options_text = ""
                        for idx, opt in enumerate(options):
                            opt_clean = opt_prefix_re.sub('', opt).strip()
                            options_text += option_labels[idx] + ") " + opt_clean + "\n"

                        msg_text = "*📋 Question:*\n```\n" + question + "\n\n" + options_text.rstrip() + "\n```"

                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text=msg_text,
                            parse_mode='Markdown'
                        )

                        # Step 2: Send poll with placeholder question and A/B/C/D options
                        poll_options = []
                        for idx, label in enumerate(['A', 'B', 'C', 'D']):
                            opt_clean = opt_prefix_re.sub('', options[idx]).strip()
                            poll_options.append((label + ") " + opt_clean)[:100])

                        poll_params = {
                            "chat_id": update.effective_chat.id,
                            "question": "⬆️ Read above question and answers correctly",
                            "options": poll_options,
                            "type": 'quiz',
                            "correct_option_id": correct_id,
                            "is_anonymous": False,
                            "open_period": 10
                        }
                        if explanation:
                            poll_params["explanation"] = explanation[:200]

                        await context.bot.send_poll(**poll_params)

                    else:
                        # Normal flow
                        poll_params = {
                            "chat_id": update.effective_chat.id,
                            "question": question,
                            "options": [opt[:100] for opt in options],
                            "type": 'quiz',
                            "correct_option_id": correct_id,
                            "is_anonymous": False,
                            "open_period": 10
                        }
                        if explanation:
                            poll_params["explanation"] = explanation
                        await context.bot.send_poll(**poll_params)

                    sent_count += 1
                    
                    # Update progress every 5 questions
                    if sent_count % 5 == 0:
                        await msg.edit_text(
                            f"✅ Sent {sent_count}/{len(valid_questions)} questions..."
                        )
                    
                    # Rate limit: 20 messages per second (Telegram limit)
                    await asyncio.sleep(0.05)
                    
                except RetryAfter as e:
                    # Handle flood control
                    wait_time = e.retry_after + 1
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                    continue
                except Exception as e:
                    logger.error(f"Poll creation error: {str(e)}")
            
            # Update quiz_used count in token
            if not is_prem and DB is not None and token_data:
                await DB.tokens.update_one(
                    {"user_id": user_id},
                    {"$inc": {"quiz_used": sent_count}}
                )
                TOKEN_CACHE.pop(user_id, None)  # clear cache
            
            await msg.edit_text(
                f"✅ Successfully sent {sent_count} quiz questions!"
            )

            # Ask user if they want to save this quiz
            PENDING_QUIZ_SAVE[user_id] = {
                "questions": valid_questions,
                "chat_id": update.effective_chat.id
            }
            keyboard = [
                [
                    InlineKeyboardButton("✅ Yes, Save Quiz", callback_data="save_quiz_yes"),
                    InlineKeyboardButton("❌ No", callback_data="save_quiz_no")
                ]
            ]
            await update.message.reply_text(
                "💾 *Kya aap ye quiz save karna chahte hain?*\n\nSave karne ke baad aap ise group mein bhi run kar sakte hain!",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text("❌ No valid questions found in file")
            
    except Exception as e:
        logger.error(f"File processing error: {str(e)}")
        await update.message.reply_text("⚠️ Error processing file. Please try again.")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Check if user is owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return

    # Check if DB is initialized (not None)
    if DB is None:
        await update.message.reply_text("⚠️ Database connection error. Stats unavailable.")
        return
        
    try:
        # Calculate stats concurrently
        tasks = [
            DB.users.count_documents({}),
            DB.tokens.count_documents({}),
            DB.sudo_users.count_documents({}),
            DB.premium_users.count_documents({})
        ]
        total_users, active_tokens, sudo_count, premium_count = await asyncio.gather(*tasks)
        
        # Ping calculation
        start_time = time.time()
        ping_msg = await update.message.reply_text("🏓 Pong!")
        ping_time = (time.time() - start_time) * 1000
        
        # Uptime calculation
        uptime_seconds = int(time.time() - bot_start_time)
        uptime = str(timedelta(seconds=uptime_seconds))
        
        # Format stats message
        stats_message = (
            f"📊 *Bot Statistics*\n\n"
            f"• Total Users: `{total_users}`\n"
            f"• Active Tokens: `{active_tokens}`\n"
            f"• Sudo Users: `{sudo_count}`\n"
            f"• Premium Users: `{premium_count}`\n"
            f"• Current Ping: `{ping_time:.2f} ms`\n"
            f"• Uptime: `{uptime}`\n"
            f"• Version: `{BOT_VERSION}`\n"
            f"• Quiz Limit: `{DAILY_QUIZ_LIMIT}`/day\n\n"
            f"_Updated at {format_ist(datetime.utcnow())} IST_"
        )
        
        # Edit the ping message with full stats
        await ping_msg.edit_text(stats_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await update.message.reply_text("⚠️ Error retrieving statistics. Please try again later.")

# Broadcast commands
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Check if user is owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    BROADCAST_STATE[update.effective_user.id] = {
        'state': 'waiting_message',
        'message': None
    }
    
    await update.message.reply_text(
        "📢 <b>Broadcast Mode Activated</b>\n\n"
        "Please send the message you want to broadcast to all users.\n"
        "You can send text, photos, videos, stickers, documents, or any other media.\n\n"
        "When ready, use /confirm_broadcast to send or /cancel_broadcast to abort.",
        parse_mode='HTML'
    )

async def confirm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Check if user is owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    user_id = update.effective_user.id
    if user_id not in BROADCAST_STATE or BROADCAST_STATE[user_id]['state'] != 'ready':
        await update.message.reply_text("⚠️ No broadcast message prepared. Use /broadcast first.")
        return
        
    broadcast_data = BROADCAST_STATE[user_id]['message']
    if not broadcast_data:
        await update.message.reply_text("⚠️ No broadcast message found. Please try again.")
        return
        
    # Get all users from DB
    if DB is None:
        await update.message.reply_text("⚠️ Database connection error. Broadcast failed.")
        return
        
    try:
        total_users = await DB.users.count_documents({})
        if total_users == 0:
            await update.message.reply_text("ℹ️ No users found in database.")
            return
            
        progress_msg = await update.message.reply_text(
            f"📤 Starting broadcast to {total_users} users...\n"
            "Sent: 0 | Failed: 0"
        )
        
        users = DB.users.find({})
        sent_count = 0
        failed_count = 0
        
        async for user in users:
            try:
                # Forward the original message to each user
                await context.bot.forward_message(
                    chat_id=user['user_id'],
                    from_chat_id=broadcast_data['chat_id'],
                    message_id=broadcast_data['message_id']
                )
                sent_count += 1
                
                # Update progress every 20 messages
                if sent_count % 20 == 0:
                    await progress_msg.edit_text(
                        f"📤 Broadcasting to {total_users} users...\n"
                        f"Sent: {sent_count} | Failed: {failed_count}"
                    )
                
                # Respect Telegram rate limits (30 messages/second)
                await asyncio.sleep(0.1)
                    
            except BadRequest as e:
                if "chat not found" in str(e).lower() or "user is deactivated" in str(e).lower():
                    # User blocked the bot or deleted account
                    failed_count += 1
                    continue
                else:
                    # Other errors, try to send a copy instead
                    try:
                        if broadcast_data.get('text'):
                            await context.bot.send_message(
                                chat_id=user['user_id'],
                                text=broadcast_data['text'],
                                parse_mode=broadcast_data.get('parse_mode'),
                                entities=broadcast_data.get('entities')
                            )
                            sent_count += 1
                        else:
                            # For media messages, we'll need to handle them differently
                            failed_count += 1
                            logger.error(f"Could not forward media message to {user['user_id']}: {e}")
                    except Exception as inner_e:
                        logger.error(f"Broadcast failed to {user['user_id']}: {str(inner_e)}")
                        failed_count += 1
            except Exception as e:
                logger.error(f"Broadcast failed to {user['user_id']}: {str(e)}")
                failed_count += 1
                
                # If we get rate limited, wait longer
                if "RetryAfter" in str(e):
                    wait_time = 5
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds")
                    await asyncio.sleep(wait_time)
        
        # Final update
        await progress_msg.edit_text(
            f"✅ Broadcast completed!\n"
            f"• Total users: {total_users}\n"
            f"• Sent successfully: {sent_count}\n"
            f"• Failed: {failed_count}"
        )
        
        # Clean up broadcast state
        if user_id in BROADCAST_STATE:
            del BROADCAST_STATE[user_id]
            
    except Exception as e:
        logger.error(f"Broadcast error: {str(e)}")
        await update.message.reply_text("⚠️ Error during broadcast. Please try again.")

async def cancel_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Check if user is owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    user_id = update.effective_user.id
    if user_id in BROADCAST_STATE:
        del BROADCAST_STATE[user_id]
        
    await update.message.reply_text("❌ Broadcast cancelled.")

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Check if user is in broadcast state
    user_id = update.effective_user.id

    # Check if user is inputting quiz title
    if user_id in WAITING_QUIZ_TITLE and update.message and update.message.text:
        title = update.message.text.strip()
        if not title:
            await update.message.reply_text("Khaali title nahi chalta. Dobara likhein:")
            return
        data = WAITING_QUIZ_TITLE.pop(user_id)
        questions = data["questions"]
        open_period = data.get("open_period", 10)
        success = await save_quiz_to_db(user_id, title, questions, open_period)
        if success:
            keyboard = [
                [InlineKeyboardButton("📚 My Quizzes", callback_data="back_myquiz")]
            ]
            await update.message.reply_text(
                f"✅ Quiz Save Ho Gayi!\n\n"
                f"📝 Title: {title}\n"
                f"❓ Questions: {len(questions)}\n"
                f"⏱ Time per question: {open_period} sec\n\n"
                "Aap /myquiz se apni quizzes dekh sakte hain.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text("Save karne mein error aaya. Dobara try karein.")
        return

    if user_id not in BROADCAST_STATE or BROADCAST_STATE[user_id]['state'] != 'waiting_message':
        return
        
    # Store the original message with all its properties
    message = update.message
    broadcast_data = {
        'type': 'message',
        'message_id': message.message_id,
        'chat_id': message.chat_id,
        'has_media': any([message.photo, message.video, message.document, message.sticker]),
        'text': message.text or message.caption,
        'parse_mode': 'HTML' if (message.text_html or message.caption_html) else None,
        'entities': message.entities or message.caption_entities
    }
    
    # Save broadcast message and update state
    BROADCAST_STATE[user_id] = {
        'state': 'ready',
        'message': broadcast_data
    }
    
    # Create a better preview
    preview_text = (
        "📢 <b>Broadcast Preview</b>\n\n"
        "This message will be sent to all users exactly as shown below:\n\n"
    )
    
    if message.text:
        preview_text += message.text_html if message.text_html else html.escape(message.text)
    elif message.caption:
        preview_text += message.caption_html if message.caption_html else html.escape(message.caption)
    
    preview_text += "\n\nUse /confirm_broadcast to send or /cancel_broadcast to abort."
    
    # Try to forward the message as a preview
    try:
        await context.bot.forward_message(
            chat_id=user_id,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )
        await update.message.reply_text(
            preview_text,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Could not forward message: {e}")
        await update.message.reply_text(
            "⚠️ Could not create a proper preview, but the message has been saved.\n\n"
            "Use /confirm_broadcast to send or /cancel_broadcast to abort.",
            parse_mode='HTML'
        )

# Premium management commands
async def add_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Verify owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    # Check arguments
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "ℹ️ Usage:\n"
            "/add <username/userid/reply> <duration>\n"
            "Durations: 1hr, 2day, 3month, 1year, etc.\n\n"
            "Example: /add @username 1month\n"
            "          /add 123456789 1year\n"
            "          Reply to a user and use /add 1day"
        )
        return
        
    # Get target user
    target_user = None
    target_user_id = None
    target_fullname = "Unknown"
    
    # Check if reply
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        target_user_id = target_user.id
        target_fullname = target_user.full_name
    else:
        # Check if first argument is username or user ID
        user_ref = context.args[0]
        
        # Try to parse as user ID
        try:
            target_user_id = int(user_ref)
            # Try to get user from database
            if DB is not None:
                user_data = await DB.users.find_one({"user_id": target_user_id})
                if user_data:
                    target_fullname = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        except ValueError:
            # Not an integer, treat as username
            username = user_ref.lstrip('@')
            if DB is not None:
                user_data = await DB.users.find_one({"username": username})
                if user_data:
                    target_user_id = user_data["user_id"]
                    target_fullname = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
    
    # Get duration - flexible format (1hr, 2day, 3month, etc.)
    duration_str = context.args[-1].lower()
    duration_map = {
        "hr": timedelta(hours=1),
        "hour": timedelta(hours=1),
        "day": timedelta(days=1),
        "month": timedelta(days=30),
        "year": timedelta(days=365)
    }
    
    # Parse duration string (e.g., "2hr", "3day", "1month")
    match = re.match(r'^(\d+)(hr|hour|day|month|year)s?$', duration_str)
    if not match:
        await update.message.reply_text("❌ Invalid duration format. Use: 2hr, 3day, 1month, 1year")
        return
    
    amount = int(match.group(1))
    unit = match.group(2)
    duration = duration_map[unit] * amount
    
    if target_user_id is None:
        await update.message.reply_text("❌ User not found. Please make sure the user has interacted with the bot.")
        return
    
    # Calculate dates
    now = datetime.utcnow()
    expiry_date = now + duration
    
    # Format dates for IST display (12-hour format with AM/PM)
    join_date_ist = format_ist(now)
    expiry_date_ist = format_ist(expiry_date)
    
    # Add to premium collection
    if DB is not None:
        await DB.premium_users.update_one(
            {"user_id": target_user_id},
            {"$set": {
                "full_name": target_fullname,
                "start_date": now,
                "expiry_date": expiry_date,
                "added_by": update.effective_user.id,
                "plan": f"{amount}{unit}"
            }},
            upsert=True
        )
        
        # Clear premium cache
        if target_user_id in PREMIUM_CACHE:
            del PREMIUM_CACHE[target_user_id]
        
        # Send message to premium user
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=(
                    f"👋 ʜᴇʏ {target_fullname},\n"
                    "ᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴘᴜʀᴄʜᴀꜱɪɴɢ ᴘʀᴇᴍɪᴜᴍ.\n"
                    "ᴇɴᴊᴏʏ !! ✨🎉\n\n"
                    f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : {amount}{unit}\n"
                    f"⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {join_date_ist} IST\n"
                    f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_date_ist} IST"
                )
            )
        except Exception as e:
            logger.error(f"Could not send premium message to user: {e}")
        
        # Send confirmation to admin
        await update.message.reply_text(
            "ᴘʀᴇᴍɪᴜᴍ ᴀᴅᴅᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ✅\n\n"
            f"👤 ᴜꜱᴇʀ : {target_fullname}\n"
            f"⚡ ᴜꜱᴇʀ ɪᴅ : `{target_user_id}`\n"
            f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇꜱꜱ : {amount}{unit}\n\n"
            f"⏳ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {join_date_ist} IST\n"
            f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_date_ist} IST",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("⚠️ Database error. Premium not added.")

async def remove_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Verify owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    # Get target user
    target_user_id = None
    
    # Check if reply
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        target_user_id = target_user.id
    elif context.args:
        # Try to parse as user ID
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            # Treat as username
            username = context.args[0].lstrip('@')
            if DB is not None:
                user_data = await DB.users.find_one({"username": username})
                if user_data:
                    target_user_id = user_data["user_id"]
    
    if target_user_id is None:
        await update.message.reply_text("❌ Please specify a user by replying or providing user ID/username")
        return
    
    # Remove from premium collection
    if DB is not None:
        result = await DB.premium_users.delete_one({"user_id": target_user_id})
        
        if result.deleted_count > 0:
            # Clear premium cache
            if target_user_id in PREMIUM_CACHE:
                del PREMIUM_CACHE[target_user_id]
            
            await update.message.reply_text(
                f"✅ Premium access removed for user ID: `{target_user_id}`",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("ℹ️ User not found in premium list")
    else:
        await update.message.reply_text("⚠️ Database error. Premium not removed.")

async def list_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Verify owner
    owner_id = os.getenv('OWNER_ID')
    if not owner_id or str(update.effective_user.id) != owner_id:
        await update.message.reply_text("🚫 This command is only available to the bot owner.")
        return
        
    if DB is None:
        await update.message.reply_text("⚠️ Database connection error.")
        return
    
    try:
        # Get all premium users
        premium_users = []
        async for user in DB.premium_users.find({}):
            premium_users.append(user)
        
        if not premium_users:
            await update.message.reply_text("ℹ️ No premium users found.")
            return
            
        response = "🌟 *Premium Users List* 🌟\n\n"
        
        for user in premium_users:
            user_id = user["user_id"]
            full_name = user.get("full_name", "Unknown")
            plan = user.get("plan", "Unknown")
            start_date = format_ist(user["start_date"])
            expiry_date = format_ist(user["expiry_date"])
            
            response += (
                f"👤 *User*: {full_name}\n"
                f"🆔 *ID*: `{user_id}`\n"
                f"📦 *Plan*: {plan}\n"
                f"⏱️ *Start*: {start_date} IST\n"
                f"⏳ *Expiry*: {expiry_date} IST\n"
                f"────────────────────\n"
            )
        
        await update.message.reply_text(
            response,
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Premium list error: {e}")
        await update.message.reply_text("⚠️ Error retrieving premium users.")

async def my_plan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    
    # Check if we're in a callback context
    if update.callback_query:
        query = update.callback_query
        user_id = query.from_user.id
        message = query.message
    else:
        user_id = update.effective_user.id
        message = update.message
    
    # Check if user is premium
    if not await is_premium(user_id):
        # Suggest premium plans
        keyboard = [
            [InlineKeyboardButton("💎 Premium Plans", callback_data="premium_plans")],
            [InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{PREMIUM_CONTACT.lstrip('@')}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        response_text = "🔒 You don't have an active premium plan.\n\nUpgrade to premium for unlimited quiz creation and other benefits!"
        
        if update.callback_query:
            await query.edit_message_text(response_text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await message.reply_text(response_text, reply_markup=reply_markup, parse_mode='Markdown')
        return
    
    # Get premium details
    if DB is not None:
        premium_data = await DB.premium_users.find_one({"user_id": user_id})
        if premium_data:
            # Format dates in IST (12-hour format with AM/PM)
            start_date = format_ist(premium_data["start_date"])
            expiry_date = format_ist(premium_data["expiry_date"])
            time_left = format_time_left(premium_data["expiry_date"])
            plan_name = premium_data.get("plan", "Premium")
            
            response = (
                "⚜️ ᴘʀᴇᴍɪᴜᴍ ᴜꜱᴇʀ ᴅᴀᴛᴀ :\n\n"
                f"👤 ᴜꜱᴇʀ : {premium_data.get('full_name', update.effective_user.full_name)}\n"
                f"⚡ ᴜꜱᴇʀ ɪᴅ : `{user_id}`\n"
                f"⏰ ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴ : {plan_name}\n\n"
                f"⏱️ ᴊᴏɪɴɪɴɢ ᴅᴀᴛᴇ : {start_date} IST\n"
                f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {expiry_date} IST\n"
                f"⏳ ᴛɪᴍᴇ ʟᴇꜰᴛ : {time_left}"
            )
            
            if update.callback_query:
                await query.edit_message_text(response, parse_mode='Markdown')
            else:
                await message.reply_text(response, parse_mode='Markdown')
            return
    
    # Fallback if data not found
    response_text = "⚠️ Could not retrieve your premium information. Please contact support."
    if update.callback_query:
        await query.edit_message_text(response_text, parse_mode='Markdown')
    else:
        await message.reply_text(response_text, parse_mode='Markdown')

# Button handler
async def handle_inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline queries for quiz sharing — @botname [title or quiz_ID]"""
    from telegram import InlineQueryResultArticle, InputTextMessageContent
    import uuid
    query = update.inline_query
    if not query:
        return

    user_id = query.from_user.id
    search = query.query.strip()

    # Get user quizzes
    quizzes = await get_user_quizzes(user_id)
    if not quizzes:
        await query.answer([], switch_pm_text="Pehle /createquiz se quiz banao!", switch_pm_parameter="start")
        return

    # Filter: match by quiz_id (exact) OR title (partial, case-insensitive)
    if search:
        # Strip "quiz_" prefix if it came from switch_inline_query="quiz_<id>"
        clean_search = search[5:] if search.lower().startswith("quiz_") else search
        filtered = []
        for q in quizzes:
            qid = str(q.get("quiz_id", str(q["_id"])))
            if qid == clean_search or clean_search.lower() in q["title"].lower():
                filtered.append(q)
        quizzes = filtered

    results = []
    bot_username = (await context.bot.get_me()).username
    for q in quizzes[:10]:
        quiz_id = str(q.get("quiz_id", str(q["_id"])))
        startgroup_link = "https://t.me/" + bot_username + "?startgroup=quiz_" + quiz_id
        start_dm_link = "https://t.me/" + bot_username + "?start=quiz_" + quiz_id
        results.append(
            InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="📋 " + q["title"],
                description=str(q["total"]) + " questions | ID: " + quiz_id,
                input_message_content=InputTextMessageContent(
                    "📋 *" + q["title"] + "*\n" +
                    "❓ " + str(q["total"]) + " questions\n\n" +
                    "Neeche buttons se quiz start karein! 👇",
                    parse_mode='Markdown'
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Start Quiz in Bot", url=start_dm_link)],
                    [InlineKeyboardButton("👥 Start Quiz in Group", url=startgroup_link)],
                    [InlineKeyboardButton("📤 Share Quiz", switch_inline_query="quiz_" + quiz_id)],
                ])
            )
        )

    await query.answer(results, cache_time=10)

async def startquiz_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /startquiz_<quiz_id> command in groups"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = update.message.text or ""
    # Extract quiz_id from command like /startquiz_<id>
    parts = text.strip().split("_", 1)
    if len(parts) < 2:
        await update.message.reply_text("Invalid command format.")
        return
    quiz_id = parts[1].split("@")[0].strip()  # remove bot username if present
    try:
        quiz_doc = await DB.saved_quizzes.find_one({"quiz_id": quiz_id})
    except Exception:
        await update.message.reply_text("Quiz nahi mili. Sahi ID use karein.")
        return
    if not quiz_doc:
        await update.message.reply_text("Quiz nahi mili.")
        return

    is_group = update.effective_chat.type in ("group", "supergroup")
    if is_group:
        await start_group_quiz_with_approval(context.bot, chat_id, quiz_doc, user_id)
    else:
        session_id = str(chat_id) + "_" + quiz_id
        ACTIVE_QUIZ_SESSIONS[session_id] = {
            "chat_id": chat_id,
            "questions": quiz_doc["questions"],
            "current_index": 0,
            "title": quiz_doc["title"],
            "quiz_id": quiz_id,
            "owner_id": user_id,
            "poll_message_id": None,
            "active_poll_id": None,
            "scores": {},
            "open_period": quiz_doc.get("open_period", 10)
        }
        msg = await update.message.reply_text(
            f"📋 *{quiz_doc['title']}*\n❓ {quiz_doc['total']} questions\n\nShuru ho rahi hai... 🎯",
            parse_mode='Markdown'
        )
        await countdown_and_start(context.bot, chat_id, session_id, msg.message_id)

# ─── SAVED QUIZ HELPERS ───────────────────────────────────────────────────────

def generate_quiz_id(length=10):
    """Generate a short alphanumeric quiz ID"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

async def save_quiz_to_db(user_id: int, title: str, questions: list, open_period: int = 10) -> bool:
    """Save quiz questions to MongoDB"""
    if DB is None:
        return False
    try:
        questions_data = []
        for q, opts, correct_id, explanation in questions:
            questions_data.append({
                "question": q,
                "options": opts,
                "correct_option_id": correct_id,
                "explanation": explanation
            })
        # Check if quiz with same user_id+title already exists — reuse its quiz_id
        existing = await DB.saved_quizzes.find_one({"user_id": user_id, "title": title})
        quiz_id = existing["quiz_id"] if existing and "quiz_id" in existing else generate_quiz_id()

        await DB.saved_quizzes.update_one(
            {"user_id": user_id, "title": title},
            {"$set": {
                "user_id": user_id,
                "title": title,
                "quiz_id": quiz_id,
                "questions": questions_data,
                "created_at": datetime.utcnow(),
                "total": len(questions_data),
                "open_period": open_period
            }},
            upsert=True
        )
        return True
    except Exception as e:
        logger.error(f"save_quiz_to_db error: {e}")
        return False

async def get_user_quizzes(user_id: int) -> list:
    """Get all saved quizzes for a user"""
    if DB is None:
        return []
    try:
        cursor = DB.saved_quizzes.find({"user_id": user_id})
        return await cursor.to_list(length=50)
    except Exception as e:
        logger.error(f"get_user_quizzes error: {e}")
        return []


async def countdown_and_start(bot, chat_id: int, session_id: str, countdown_msg_id: int = None):
    """Edit a message with 5→1 countdown then start the quiz"""
    title = ACTIVE_QUIZ_SESSIONS.get(session_id, {}).get('title', 'Quiz')
    for i in range(5, 0, -1):
        text = (
            f"🎯 *{title}*\n\n"
            f"⏳ Quiz shuru ho rahi hai...\n\n"
            f"{'🔴' * i}{'⚪' * (5 - i)}  *{i}*"
        )
        try:
            if countdown_msg_id:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=countdown_msg_id,
                    text=text, parse_mode='Markdown'
                )
            else:
                msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
                countdown_msg_id = msg.message_id
        except Exception:
            pass
        await asyncio.sleep(1)
    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=countdown_msg_id,
            text=f"🚀 *{title}* — Shuru! 🎯", parse_mode='Markdown'
        )
    except Exception:
        pass
    await asyncio.sleep(0.5)
    await send_quiz_question(bot, session_id)


async def start_group_quiz_with_approval(bot, chat_id: int, quiz_doc: dict, owner_id: int):
    """
    In a group: send a join message, wait for ≥2 players to press Ready,
    then countdown and start. Times out after 60 seconds.
    """
    import uuid as _uuid
    approval_id = _uuid.uuid4().hex[:12]
    expires_at = datetime.utcnow() + timedelta(seconds=60)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🙋 Ready to Play!", callback_data="join_quiz_" + approval_id)],
        [InlineKeyboardButton("▶️ Start Now (owner only)", callback_data="forcestart_" + approval_id)],
    ])

    msg = await bot.send_message(
        chat_id=chat_id,
        text=(
            f"📋 *{quiz_doc['title']}*\n"
            f"❓ {quiz_doc['total']} questions\n\n"
            f"Quiz shuru karne ke liye *kam se kam 2 players* chahiye!\n"
            f"Neeche button dabao taiyaar hone ke liye 👇\n\n"
            f"✅ Ready: 0 players\n"
            f"⏰ 60 seconds mein auto-cancel ho jaayegi agar 2 log ready nahi hue."
        ),
        parse_mode='Markdown',
        reply_markup=keyboard
    )

    PENDING_GROUP_QUIZ[approval_id] = {
        "chat_id": chat_id,
        "quiz_doc": quiz_doc,
        "owner_id": owner_id,
        "joined": set(),
        "joined_names": {},
        "message_id": msg.message_id,
        "expires_at": expires_at,
    }

    # Background task: auto-cancel after timeout
    async def auto_cancel():
        await asyncio.sleep(62)
        pending = PENDING_GROUP_QUIZ.pop(approval_id, None)
        if pending:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=pending["message_id"],
                    text="⏰ *Quiz cancelled!*\n\nKafi players ready nahi hue. Dobara try karein.",
                    parse_mode='Markdown'
                )
            except Exception:
                pass
    asyncio.create_task(auto_cancel())

async def send_quiz_question(bot, session_id: str):
    """Send next question in an active quiz session"""
    session = ACTIVE_QUIZ_SESSIONS.get(session_id)
    if not session:
        return

    idx = session["current_index"]
    questions = session["questions"]
    chat_id = session["chat_id"]

    if idx >= len(questions):
        # Quiz finished — show leaderboard
        scores = session.get("scores", {})
        total_q = len(questions)
        quiz_id = session.get("quiz_id", "")
        if scores:
            sorted_scores = sorted(scores.items(), key=lambda x: x[1]["score"], reverse=True)
            medals = ["\U0001f947", "\U0001f948", "\U0001f949"]
            leaderboard = ""
            for rank, (uid, data) in enumerate(sorted_scores, 1):
                medal = medals[rank - 1] if rank <= 3 else str(rank) + "."
                name = data.get("name", "User")
                sc = data["score"]
                pct = int((sc / total_q) * 100)
                leaderboard += medal + " " + name + " - " + str(sc) + "/" + str(total_q) + " (" + str(pct) + "%)\n"
            result_text = "\U0001f3c1 *Quiz Khatam!*\n\n" + "\U0001f4cb *" + session["title"] + "*\n" + "\U0001f4ca Total Questions: " + str(total_q) + "\n\n" + "\U0001f3c6 *Leaderboard:*\n\n" + leaderboard
        else:
            result_text = "\U0001f3c1 *Quiz Khatam!*\n\n" + "\U0001f4cb *" + session["title"] + "*\n" + "\U0001f4ca Total Questions: " + str(total_q) + "\n\nKisi ne bhi answer nahi kiya."

        # Share keyboard — only if we have a quiz_id
        share_markup = None
        if quiz_id:
            try:
                bot_username = (await bot.get_me()).username
                startgroup_link = "https://t.me/" + bot_username + "?startgroup=quiz_" + quiz_id
                start_dm_link = "https://t.me/" + bot_username + "?start=quiz_" + quiz_id
                share_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Start Quiz in Bot", url=start_dm_link)],
                    [InlineKeyboardButton("👥 Start Quiz in Group", url=startgroup_link)],
                    [InlineKeyboardButton("📤 Share Quiz", switch_inline_query="quiz_" + quiz_id)],
                ])
            except Exception:
                pass

        await bot.send_message(chat_id=chat_id, text=result_text, parse_mode='Markdown', reply_markup=share_markup)
        ACTIVE_QUIZ_SESSIONS.pop(session_id, None)
        return

    q = questions[idx]
    question_text = q["question"]
    options = q["options"]
    correct_id = q["correct_option_id"]
    explanation = q.get("explanation")

    # Session mein stored open_period use karo, default 10 sec
    open_period = session.get("open_period", 10)

    opt_prefix_re2 = __import__('re').compile(r'^[A-Da-d][\.\):\s]+')

    try:
        POLL_QUESTION_LIMIT = 300
        if len(question_text) > POLL_QUESTION_LIMIT:
            option_labels = ['A', 'B', 'C', 'D']
            options_text = ""
            for i, opt in enumerate(options):
                opt_clean = opt_prefix_re2.sub('', opt).strip()
                options_text += option_labels[i] + ") " + opt_clean + "\n"
            msg_text = "*📋 Question:*\n```\n" + question_text + "\n\n" + options_text.rstrip() + "\n```"
            await bot.send_message(chat_id=chat_id, text=msg_text, parse_mode='Markdown')

            poll_options = []
            for i, label in enumerate(['A', 'B', 'C', 'D']):
                opt_clean = opt_prefix_re2.sub('', options[i]).strip()
                poll_options.append((label + ") " + opt_clean)[:100])

            sent = await bot.send_poll(
                chat_id=chat_id,
                question="⬆️ Read above question and answers correctly",
                options=poll_options,
                type='quiz',
                correct_option_id=correct_id,
                is_anonymous=False,
                open_period=open_period,
                explanation=explanation[:200] if explanation else None
            )
        else:
            # Truncate each option to Telegram's 100-char limit
            safe_options = [opt[:100] for opt in options]
            poll_kwargs = {
                "chat_id": chat_id,
                "question": question_text,
                "options": safe_options,
                "type": 'quiz',
                "correct_option_id": correct_id,
                "is_anonymous": False,
                "open_period": open_period
            }
            if explanation:
                poll_kwargs["explanation"] = explanation
            sent = await bot.send_poll(**poll_kwargs)

        # Store poll info
        session["poll_message_id"] = sent.message_id
        session["active_poll_id"] = sent.poll.id if sent.poll else None
        session["current_index"] = idx + 1
        session["answered"] = False  # reset per question
        ACTIVE_QUIZ_SESSIONS[session_id] = session

        # DM: chat_id > 0; Group: chat_id < 0
        is_dm = chat_id > 0

        # Schedule next question after poll timer ends + 3 sec buffer
        bot_ref = bot

        async def next_after_timer(sid, b, period):
            await asyncio.sleep(period + 3)
            sess = ACTIVE_QUIZ_SESSIONS.get(sid)
            # DM mein agar user ne answer de diya tha toh timer fire nahi karna
            if sess and not sess.get("answered", False):
                await send_quiz_question(b, sid)
            elif sess and sess.get("answered", False):
                pass  # already handled by poll answer handler

        task = asyncio.create_task(next_after_timer(session_id, bot_ref, open_period))

        # DM sessions mein task store karo taaki answer pe cancel kar sakein
        if is_dm:
            session["timer_task"] = task
            ACTIVE_QUIZ_SESSIONS[session_id] = session

    except Exception as e:
        logger.error(f"send_quiz_question error: {e}")

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Triggered when a quiz poll closes — send next question"""
    poll = update.poll
    if not poll or not poll.is_closed:
        return

    # Find session with matching poll
    for session_id, session in list(ACTIVE_QUIZ_SESSIONS.items()):
        if session.get("poll_message_id") and poll.id == session.get("poll_id"):
            await asyncio.sleep(2)  # Small delay before next question
            await send_quiz_question(context.bot, session_id)
            break

async def handle_poll_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Called when a poll message is updated (closed). Triggers next question."""
    if not update.poll:
        return
    poll = update.poll
    if not poll.is_closed:
        return
    for session_id, session in list(ACTIVE_QUIZ_SESSIONS.items()):
        if session.get("active_poll_id") == poll.id:
            await asyncio.sleep(2)
            await send_quiz_question(context.bot, session_id)
            return

async def handle_poll_answer_track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Track user poll answers to build leaderboard"""
    answer = update.poll_answer
    if not answer:
        return

    poll_id = answer.poll_id
    user = answer.user
    chosen = answer.option_ids  # list of chosen option indices

    # Find matching session
    for session_id, session in list(ACTIVE_QUIZ_SESSIONS.items()):
        if session.get("active_poll_id") != poll_id:
            continue

        # Get correct answer for current question (already sent, index was incremented)
        q_index = session["current_index"] - 1
        questions = session["questions"]
        if q_index < 0 or q_index >= len(questions):
            break

        correct_id = questions[q_index]["correct_option_id"]
        is_correct = len(chosen) > 0 and chosen[0] == correct_id

        uid = str(user.id)
        scores = session.setdefault("scores", {})
        if uid not in scores:
            name = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
            scores[uid] = {"name": name.strip() or "User", "score": 0}

        if is_correct:
            scores[uid]["score"] += 1

        session["scores"] = scores

        # DM check: chat_id > 0 means private chat
        is_dm = session.get("chat_id", 0) > 0

        if is_dm and not session.get("answered", False):
            # Mark as answered to prevent timer from firing too
            session["answered"] = True
            ACTIVE_QUIZ_SESSIONS[session_id] = session

            # Cancel the scheduled timer task
            timer_task = session.get("timer_task")
            if timer_task and not timer_task.done():
                timer_task.cancel()

            # Short delay so Telegram shows the answer result, then next question
            async def send_next_dm(sid, b):
                await asyncio.sleep(1.5)
                if sid in ACTIVE_QUIZ_SESSIONS:
                    await send_quiz_question(b, sid)

            asyncio.create_task(send_next_dm(session_id, context.bot))
        else:
            ACTIVE_QUIZ_SESSIONS[session_id] = session

        break

async def myquiz_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show user's saved quizzes"""
    await record_user_interaction(update)
    user_id = update.effective_user.id

    if await is_quiz_running(update.effective_chat.id):
        await update.message.reply_text(
            "⏳ Quiz chal rahi hai! Pehle /stopquiz se rok do.",
            parse_mode='Markdown'
        )
        return
    quizzes = await get_user_quizzes(user_id)
    if not quizzes:
        await update.message.reply_text("📭 Aapke paas koi saved quiz nahi hai.\n\nPehle /createquiz se ek quiz banayein!", parse_mode='Markdown')
        return

    text = "📚 *Aapke Saved Quizzes:*\n\n"
    keyboard = []
    for i, q in enumerate(quizzes[:10], 1):
        text += f"{i}. *{q['title']}* — {q['total']} questions\n"
        keyboard.append([
            InlineKeyboardButton(f"▶️ {q['title']}", callback_data="startq_" + str(q.get('quiz_id', str(q['_id'])))),
        ])
    keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_menu")])

    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "premium_plans":
        await plan_command(update, context)

    elif query.data == "my_plan":
        if hasattr(update, 'message'):
            await my_plan_command(update, context)
        else:
            fake_update = Update(update.update_id, message=query.message)
            await my_plan_command(fake_update, context)

    elif query.data == "get_token":
        # Trigger the ad/token flow inline
        fake_update = Update(update.update_id, message=query.message)
        # We need the real effective_user from the callback
        # Build a minimal context-aware approach: just guide user to /token
        await query.edit_message_text(
            "▶️ <b>Get More Quiz Access</b>\n\n"
            "Tap the button below or type /token to watch a short ad and unlock more quizzes!",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Watch Ad Now", callback_data="open_token")],
                [InlineKeyboardButton("💎 Get Premium", callback_data="premium_plans")]
            ])
        )

    elif query.data == "open_token":
        # Actually run token_command for this user
        user_id = query.from_user.id
        param = generate_random_param()
        temp_params[user_id] = param
        webapp_base = (
            os.getenv('WEBAPP_URL') or
            os.getenv('RENDER_EXTERNAL_URL') or
            f"http://localhost:{os.environ.get('PORT', 8000)}"
        )
        webapp_url = f"{webapp_base}/webapp?user_id={user_id}&param={param}"
        keyboard = [[InlineKeyboardButton("▶️ Watch Ad & Get Access", web_app=WebAppInfo(url=webapp_url))]]
        sent = await query.message.reply_text(
            "🎬 <b>Watch a short ad to unlock more quizzes!</b>\n\n"
            "👇 Tap below to watch the ad, then claim your reward.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        TOKEN_MESSAGES[user_id] = (query.message.chat_id, sent.message_id)

    elif query.data == "redeem_points":
        user_id = query.from_user.id
        if await is_premium(user_id):
            await query.edit_message_text(
                "🌟 You already have an active premium plan!\nUse /myplan to check details.",
                parse_mode='Markdown'
            )
            return
        success, msg = await redeem_points_for_premium(user_id)
        keyboard = [[InlineKeyboardButton("📋 View My Plan", callback_data="my_plan")]] if success else \
                   [[InlineKeyboardButton("👥 Invite Friends", callback_data="show_invite"),
                     InlineKeyboardButton("💎 Buy Premium", callback_data="premium_plans")]]
        await query.edit_message_text(msg, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == "show_invite":
        user_id = query.from_user.id
        points = await get_user_points(user_id)
        bot_username = (await context.bot.get_me()).username
        invite_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        needed = max(0, REDEEM_POINTS_REQUIRED - points)
        progress_filled = min(points, REDEEM_POINTS_REQUIRED)
        progress_bar = "🟢" * progress_filled + "⚪" * (REDEEM_POINTS_REQUIRED - progress_filled)

        msg = (
            f"👥 *Your Invite Dashboard*\n\n"
            f"🔗 *Your invite link:*\n`{invite_link}`\n\n"
            f"📊 *Progress:*\n{progress_bar}\n"
            f"💎 `{points}/{REDEEM_POINTS_REQUIRED}` points\n\n"
            f"• Each new user = *+{INVITE_POINTS} points*\n"
            f"• {REDEEM_POINTS_REQUIRED} points = *{REDEEM_PREMIUM_DAYS}-Day Premium FREE* 🎁\n"
        )

        if points >= REDEEM_POINTS_REQUIRED:
            msg += "\n🎉 *Ready to redeem!*"
            keyboard = [
                [InlineKeyboardButton("🎁 Redeem Premium Now!", callback_data="redeem_points")],
                [InlineKeyboardButton("📤 Share Link", url=f"https://t.me/share/url?url={invite_link}&text=Join+this+quiz+bot!")]
            ]
        else:
            msg += f"\n📌 Invite *{needed} more friend(s)* to unlock free premium!"
            keyboard = [
                [InlineKeyboardButton("📤 Share Invite Link", url=f"https://t.me/share/url?url={invite_link}&text=Join+this+quiz+bot!")],
                [InlineKeyboardButton("💎 Buy Premium Instead", callback_data="premium_plans")]
            ]

        await query.edit_message_text(msg, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data == "save_quiz_yes":
        user_id = query.from_user.id
        if user_id not in PENDING_QUIZ_SAVE:
            await query.edit_message_text("⚠️ Session expire ho gayi. Dobara file bhejein.")
            return
        keyboard = [
            [
                InlineKeyboardButton("⚡ 10 sec", callback_data="qtime_10"),
                InlineKeyboardButton("⏱ 15 sec", callback_data="qtime_15"),
                InlineKeyboardButton("🕐 20 sec", callback_data="qtime_20"),
            ],
            [
                InlineKeyboardButton("🕑 30 sec", callback_data="qtime_30"),
                InlineKeyboardButton("🕓 45 sec", callback_data="qtime_45"),
                InlineKeyboardButton("🕕 60 sec", callback_data="qtime_60"),
            ],
            [
                InlineKeyboardButton("⏳ 2 min (120 sec)", callback_data="qtime_120"),
            ]
        ]
        await query.edit_message_text(
            "⏱ *Har question ke liye kitna time dena chahte hain?*\n\n"
            "• 10 sec — Fast (competitive)\n"
            "• 15-20 sec — Normal\n"
            "• 30-45 sec — Easy\n"
            "• 60 sec / 2 min — Long questions",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        # PENDING_QUIZ_SAVE ko abhi pop mat karo — time select hone par karenge

    elif query.data.startswith("qtime_"):
        user_id = query.from_user.id
        if user_id not in PENDING_QUIZ_SAVE:
            await query.edit_message_text("⚠️ Session expire ho gayi. Dobara file bhejein.")
            return
        time_sec = int(query.data.split("_")[1])
        data = PENDING_QUIZ_SAVE.pop(user_id)
        data["open_period"] = time_sec
        WAITING_QUIZ_TITLE[user_id] = data
        time_label = f"{time_sec} seconds" if time_sec < 60 else ("1 minute" if time_sec == 60 else f"{time_sec // 60} minutes")
        await query.edit_message_text(
            f"✅ Time set: *{time_label}* per question\n\n"
            "✏️ *Ab quiz ka naam/title likhein:*",
            parse_mode='Markdown'
        )

    elif query.data == "save_quiz_no":
        PENDING_QUIZ_SAVE.pop(query.from_user.id, None)
        await query.edit_message_text("👍 Theek hai! Quiz save nahi kiya gaya.")

    elif query.data == "close_menu":
        await query.message.delete()

    elif query.data.startswith("startq_"):
        quiz_id = query.data[7:]
        user_id = query.from_user.id
        try:
            quiz_doc = await DB.saved_quizzes.find_one({"quiz_id": quiz_id})
        except Exception:
            await query.answer("Quiz nahi mila!", show_alert=True)
            return
        if not quiz_doc:
            await query.answer("Quiz nahi mila!", show_alert=True)
            return

        keyboard = [
            [InlineKeyboardButton("▶️ Start Quiz Here", callback_data="runq_here_" + quiz_id)],
            [InlineKeyboardButton("👥 Start in Group", callback_data="runq_group_" + quiz_id)],
            [InlineKeyboardButton("📤 Share Quiz", switch_inline_query="quiz_" + quiz_id)],
            [InlineKeyboardButton("🗑️ Delete Quiz", callback_data="delq_" + quiz_id)],
            [InlineKeyboardButton("🔙 Back", callback_data="back_myquiz")]
        ]
        await query.edit_message_text(
            f"📋 *{quiz_doc['title']}*\n\n"
            f"📊 Questions: {quiz_doc['total']}\n"
            f"📅 Created: {format_ist(quiz_doc['created_at'])} IST\n\n"
            f"Kya karna chahte hain?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif query.data.startswith("runq_here_"):
        quiz_id = query.data[10:]
        chat_id = query.message.chat_id
        user_id = query.from_user.id
        try:
            quiz_doc = await DB.saved_quizzes.find_one({"quiz_id": quiz_id})
        except Exception:
            await query.answer("Quiz nahi mila!", show_alert=True)
            return
        session_id = str(user_id) + "_" + quiz_id
        ACTIVE_QUIZ_SESSIONS[session_id] = {
            "chat_id": chat_id,
            "questions": quiz_doc["questions"],
            "current_index": 0,
            "title": quiz_doc["title"],
            "quiz_id": quiz_id,
            "owner_id": user_id,
            "poll_message_id": None,
            "active_poll_id": None,
            "scores": {},
            "open_period": quiz_doc.get("open_period", 10)
        }
        await query.edit_message_text(
            f"🚀 *{quiz_doc['title']}* shuru ho rahi hai!\n\n"
            f"Total {quiz_doc['total']} questions. Shuru karte hain... 🎯\n\n"
            f"⚠️ Quiz khatam hone tak doosre commands kaam nahi karenge.\n"
            f"Beech mein rokna ho to /stopquiz likhein.",
            parse_mode='Markdown'
        )
        await asyncio.sleep(1)
        await send_quiz_question(context.bot, session_id)

    elif query.data.startswith("runq_group_"):
        quiz_id = query.data[11:]
        bot_username = (await context.bot.get_me()).username
        startgroup_link = "https://t.me/" + bot_username + "?startgroup=quiz_" + quiz_id
        keyboard = [
            [InlineKeyboardButton("👥 Group mein Add Karein & Start Karein", url=startgroup_link)]
        ]
        await query.answer()
        await query.message.reply_text(
            "👥 *Group mein Quiz Start Karein*\n\n"
            "Neeche button dabao — bot apne group mein add hoga aur quiz automatically shuru ho jaayegi!\n\n"
            "Ya yeh link copy karke group admin ko bhejo:\n"
            "`" + startgroup_link + "`",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif query.data.startswith("join_quiz_"):
        approval_id = query.data[10:]
        user_id = query.from_user.id
        pending = PENDING_GROUP_QUIZ.get(approval_id)
        if not pending:
            await query.answer("Quiz session expired ya start ho gayi!", show_alert=True)
            return
        if datetime.utcnow() > pending["expires_at"]:
            await query.answer("Time out ho gaya!", show_alert=True)
            return
        name = query.from_user.first_name or "Player"
        pending["joined"].add(user_id)
        pending["joined_names"][user_id] = name
        count = len(pending["joined"])
        await query.answer(f"✅ Tum ready ho, {name}!")
        # Update the join message
        names_list = ", ".join(pending["joined_names"].values())
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🙋 Ready to Play!", callback_data="join_quiz_" + approval_id)],
            [InlineKeyboardButton("▶️ Start Now (owner only)", callback_data="forcestart_" + approval_id)],
        ])
        try:
            await query.edit_message_text(
                f"📋 *{pending['quiz_doc']['title']}*\n"
                f"❓ {pending['quiz_doc']['total']} questions\n\n"
                "Quiz shuru karne ke liye *kam se kam 2 players* chahiye!\n"
                "Neeche button dabao taiyaar hone ke liye 👇\n\n"
                f"✅ Ready: {count} players — {names_list}\n"
                "⏰ Auto-cancel hogi agar 2 log ready nahi hue.",
                parse_mode='Markdown',
                reply_markup=keyboard
            )
        except Exception:
            pass
        # Auto-start when 2+ players ready
        if count >= 2:
            PENDING_GROUP_QUIZ.pop(approval_id, None)
            quiz_doc = pending["quiz_doc"]
            chat_id = pending["chat_id"]
            owner_id = pending["owner_id"]
            quiz_id = quiz_doc["quiz_id"]
            session_id = str(chat_id) + "_" + quiz_id
            ACTIVE_QUIZ_SESSIONS[session_id] = {
                "chat_id": chat_id,
                "questions": quiz_doc["questions"],
                "current_index": 0,
                "title": quiz_doc["title"],
                "quiz_id": quiz_id,
                "owner_id": owner_id,
                "poll_message_id": None,
                "active_poll_id": None,
                "scores": {},
                "open_period": quiz_doc.get("open_period", 10)
            }
            # Edit join message to show countdown then start
            try:
                await query.edit_message_text(
                    f"✅ *{count} players ready!*\n\n🚀 Starting in 5...",
                    parse_mode='Markdown'
                )
            except Exception:
                pass
            await countdown_and_start(context.bot, chat_id, session_id, pending["message_id"])

    elif query.data.startswith("forcestart_"):
        approval_id = query.data[11:]
        user_id = query.from_user.id
        pending = PENDING_GROUP_QUIZ.get(approval_id)
        if not pending:
            await query.answer("Session expired ya quiz shuru ho gayi!", show_alert=True)
            return
        if user_id != pending["owner_id"]:
            await query.answer("Sirf quiz start karne wala force start kar sakta hai!", show_alert=True)
            return
        count = len(pending["joined"])
        if count < 1:
            await query.answer("Koi bhi ready nahi hai abhi!", show_alert=True)
            return
        PENDING_GROUP_QUIZ.pop(approval_id, None)
        quiz_doc = pending["quiz_doc"]
        chat_id = pending["chat_id"]
        owner_id = pending["owner_id"]
        quiz_id = quiz_doc["quiz_id"]
        session_id = str(chat_id) + "_" + quiz_id
        ACTIVE_QUIZ_SESSIONS[session_id] = {
            "chat_id": chat_id,
            "questions": quiz_doc["questions"],
            "current_index": 0,
            "title": quiz_doc["title"],
            "quiz_id": quiz_id,
            "owner_id": owner_id,
            "poll_message_id": None,
            "active_poll_id": None,
            "scores": {},
            "open_period": quiz_doc.get("open_period", 10)
        }
        try:
            await query.edit_message_text(
                "✅ *Owner ne force start kiya!*\n\n🚀 Starting in 5...",
                parse_mode='Markdown'
            )
        except Exception:
            pass
        await countdown_and_start(context.bot, chat_id, session_id, pending["message_id"])

    elif query.data.startswith("delq_"):
        quiz_id = query.data[5:]
        user_id = query.from_user.id
        try:
            await DB.saved_quizzes.delete_one({"quiz_id": quiz_id, "user_id": user_id})
            await query.edit_message_text("🗑️ Quiz delete ho gaya!")
        except Exception as e:
            await query.edit_message_text("⚠️ Delete karne mein error aaya.")

    elif query.data == "back_myquiz":
        user_id = query.from_user.id
        quizzes = await get_user_quizzes(user_id)
        if not quizzes:
            await query.edit_message_text("📭 Koi saved quiz nahi hai.")
            return
        text = "📚 *Aapke Saved Quizzes:*\n\n"
        keyboard = []
        for i, q in enumerate(quizzes[:10], 1):
            text += f"{i}. *{q['title']}* — {q['total']} questions\n"
            keyboard.append([InlineKeyboardButton("▶️ " + q['title'], callback_data="startq_" + str(q.get('quiz_id', str(q['_id']))))])
        keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_menu")])
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

# Optimized token validation with caching
async def has_valid_token(user_id):
    if await is_sudo(user_id) or await is_premium(user_id):
        return True
        
    # Check cache first
    cached = TOKEN_CACHE.get(user_id)
    if cached and time.time() < cached['expiry']:
        return cached['result']
        
    result = False
    # Check if DB is initialized (not None)
    if DB is not None:
        try:
            token_data = await DB.tokens.find_one({"user_id": user_id})
            if token_data:
                quiz_used = token_data.get("quiz_used", 0)
                quiz_limit = token_data.get("quiz_limit", int(os.getenv('DAILY_QUIZ_LIMIT', 20)))
                result = quiz_used < quiz_limit
        except Exception as e:
            logger.error(f"Token check error: {e}")
    
    # Update cache
    TOKEN_CACHE[user_id] = {
        'result': result,
        'expiry': time.time() + CACHE_EXPIRY
    }
    return result

# Premium check with caching
async def is_premium(user_id):
    # Check cache first
    cached = PREMIUM_CACHE.get(user_id)
    if cached and time.time() < cached['expiry']:
        return cached['result']
        
    result = False
    # Check if DB is initialized (not None)
    if DB is not None:
        try:
            premium_data = await DB.premium_users.find_one({"user_id": user_id})
            if premium_data:
                # Check if premium has expired
                if premium_data["expiry_date"] > datetime.utcnow():
                    result = True
                else:
                    # Remove expired premium
                    await DB.premium_users.delete_one({"_id": premium_data["_id"]})
        except Exception as e:
            logger.error(f"Premium check error: {e}")
    
    # Update cache
    PREMIUM_CACHE[user_id] = {
        'result': result,
        'expiry': time.time() + CACHE_EXPIRY
    }
    return result

async def process_pending_tokens():
    """Background task: flush pending_tokens from Flask into MongoDB"""
    while True:
        await asyncio.sleep(2)
        if not pending_tokens:
            continue
        items = list(pending_tokens.items())
        for user_id, token_data in items:
            try:
                if DB is not None:
                    await DB.tokens.update_one(
                        {"user_id": user_id},
                        {"$set": token_data},
                        upsert=True
                    )
                pending_tokens.pop(user_id, None)
                logger.info(f"Token saved for user {user_id}")

                # Delete old /token message and send success notification
                bot = application_ref[0]
                if bot and user_id in TOKEN_MESSAGES:
                    chat_id, msg_id = TOKEN_MESSAGES.pop(user_id)
                    try:
                        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
                    except Exception:
                        pass
                    try:
                        quiz_limit = int(os.getenv('DAILY_QUIZ_LIMIT', 20))
                        await bot.send_message(
                            chat_id=chat_id,
                            text=f"🎉 <b>Access Granted!</b>\n\nYou can now generate <b>{quiz_limit} quizzes</b>! 🚀\n\nSend a .txt file to /createquiz to get started.",
                            parse_mode='HTML'
                        )
                    except Exception as e:
                        logger.error(f"Failed to send reward msg to {user_id}: {e}")

            except Exception as e:
                logger.error(f"Error saving pending token for {user_id}: {e}")

async def main_async() -> None:
    """Async main function"""
    global DB, SESSION
    
    # Initialize database
    DB = await init_db()
    
    # Only proceed if DB initialization was successful (DB is not None)
    if DB is not None:
        await asyncio.gather(
            create_ttl_index(),
            create_sudo_index(),
            create_premium_index(),
            create_invite_index(),
            create_quiz_index()
        )
    
    # Get token from environment
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN:
        logger.error("No TELEGRAM_TOKEN found in environment!")
        return
    
    # Create Telegram application
    application = ApplicationBuilder().token(TOKEN).pool_timeout(30).build()
    application_ref[0] = application.bot
    
    # Add handlers
    # stopquiz registered FIRST with group=0 so it works even during active quiz
    application.add_handler(CommandHandler("stopquiz", stopquiz_command), group=0)
    application.add_handler(CommandHandler("start", start_wrapper))
    application.add_handler(CommandHandler("help", help_command_wrapper))
    application.add_handler(CommandHandler("createquiz", create_quiz_wrapper))
    application.add_handler(CommandHandler("stats", stats_command_wrapper))
    application.add_handler(CommandHandler("token", token_command))
    application.add_handler(CommandHandler("refresh", refresh_command))
    application.add_handler(CommandHandler("plan", plan_command))
    application.add_handler(CommandHandler("myplan", my_plan_command))
    application.add_handler(CommandHandler("invite", invite_command))
    application.add_handler(CommandHandler("points", points_command))
    application.add_handler(CommandHandler("redeem", redeem_command))
    application.add_handler(MessageHandler(filters.Document.TEXT, handle_document_wrapper))
    application.add_handler(CommandHandler("myquiz", myquiz_command))
    from telegram.ext import InlineQueryHandler
    application.add_handler(InlineQueryHandler(handle_inline_query))
    application.add_handler(MessageHandler(filters.COMMAND & filters.Regex(r"^/startquiz_"), startquiz_group_command))
    # Poll close is handled via PollAnswerHandler — no separate handler needed
    application.add_handler(PollAnswerHandler(handle_poll_answer_track))
    
    # Add broadcast commands
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    application.add_handler(CommandHandler("confirm_broadcast", confirm_broadcast))
    application.add_handler(CommandHandler("cancel_broadcast", cancel_broadcast))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_broadcast_message))
    
    # Add premium management commands
    application.add_handler(CommandHandler("add", add_premium))
    application.add_handler(CommandHandler("rem", remove_premium))
    application.add_handler(CommandHandler("premium", list_premium))
    
    # Add button handler
    application.add_handler(CallbackQueryHandler(button_handler))
    
    logger.info("Starting Telegram bot in polling mode...")
    try:
        await application.initialize()
        await application.start()

        # Delete any existing webhook and drop pending updates from old instance
        await application.bot.delete_webhook(drop_pending_updates=True)
        # Small delay to let old instance fully release
        await asyncio.sleep(3)

        await application.updater.start_polling(
            poll_interval=0.5,
            timeout=10,
            read_timeout=10,
            drop_pending_updates=True
        )
        logger.info("Bot is now running")

        # Start background task to flush pending tokens to DB
        asyncio.create_task(process_pending_tokens())

        # Keep running until interrupted
        while True:
            await asyncio.sleep(3600)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.critical(f"Telegram bot failed: {e}")
    finally:
        # Cleanup
        if SESSION:
            await SESSION.close()
        if MONGO_CLIENT:
            MONGO_CLIENT.close()
        await application.stop()
        logger.info("Bot stopped gracefully")

def main() -> None:
    """Run the bot and HTTP server"""
    # Start Flask server in a daemon thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"Flask server started in separate thread")
    
    # Run async main
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        # Attempt to restart after delay
        time.sleep(10)
        main()

if __name__ == '__main__':
    main()
