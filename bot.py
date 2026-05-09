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
    CallbackQueryHandler
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

# Caches for performance
SUDO_CACHE = {}
TOKEN_CACHE = {}
PREMIUM_CACHE = {}
CACHE_EXPIRY = 60  # seconds

# Broadcast state
BROADCAST_STATE = {}

# Pending token rewards from webapp (Flask -> async bot bridge)
pending_tokens = {}

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

        # Validate param
        stored_param = temp_params.get(user_id)
        if not stored_param or stored_param != param:
            return jsonify({"ok": False, "error": "Invalid or expired session"}), 403

        # Store token in pending_tokens dict — bot's async loop will pick it up
        # This avoids asyncio conflict between Flask and bot's event loop
        pending_tokens[user_id] = {
            "token": stored_param,
            "created_at": datetime.utcnow(),
            "expires_at": datetime.utcnow() + timedelta(hours=24)
        }

        # Remove temp param after successful claim
        temp_params.pop(user_id, None)
        # Clear token cache
        TOKEN_CACHE.pop(user_id, None)

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

# Create TTL index for token expiration
async def create_ttl_index():
    try:
        if DB is not None:
            await DB.tokens.create_index("expires_at", expireAfterSeconds=0)
            logger.info("Created TTL index for token expiration")
    except Exception as e:
        logger.error(f"Error creating TTL index: {e}")

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

# Ad-based access command (replaces old token system)
async def token_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await record_user_interaction(update)
    user = update.effective_user
    user_id = user.id

    # Premium and sudo users don't need tokens
    if await is_sudo(user_id) or await is_premium(user_id):
        await update.message.reply_text(
            "🌟 You are a premium user! You don't need to watch ads.",
            parse_mode='Markdown'
        )
        return

    # Check if user already has valid token
    if await has_valid_token(user_id):
        await update.message.reply_text(
            "✅ You already have active access! Enjoy your 24-hour access.",
            parse_mode='Markdown'
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

    await update.message.reply_text(
        response_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )

# Token verification helper
async def check_access(update: Update, context: ContextTypes.DEFAULT_TYPE, handler):
    user_id = update.effective_user.id
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
    welcome_msg = (
        "🌟 *Welcome to Quiz Bot!* 🌟\n\n"
        "I can turn your text files into interactive 10-second quizzes!\n\n"
        "🔹 Use /createquiz - Start quiz creation\n"
        "🔹 Use /help - Show formatting guide\n"
        "🔹 Use /token - Watch a short ad for 24h access\n"
        "🔹 Premium users get unlimited access!\n\n"
    )
    
    # Add access status for non-premium users
    if not (await is_sudo(update.effective_user.id) or await is_premium(update.effective_user.id)):
        welcome_msg += (
            "🔒 Watch a short ad with /token to unlock all features for 24 hours\n\n"
        )
    
    welcome_msg += "Let's make learning fun!"
    
    # Create keyboard with tutorial and premium buttons
    keyboard = [
        [
            InlineKeyboardButton("🎥 Watch Tutorial", url=YOUTUBE_TUTORIAL),
            InlineKeyboardButton("💎 Premium Plans", callback_data="premium_plans")
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
    
    # For token users, check daily quiz limit
    if not is_prem:
        # Get today's date in UTC
        today_utc = datetime.utcnow().date()
        
        # Check daily quiz count
        if DB is not None:
            user_data = await DB.users.find_one({"user_id": user_id})
            quiz_count = 0
            
            if user_data:
                # Check if last quiz date is today
                last_quiz_date = user_data.get("last_quiz_date")
                if last_quiz_date and last_quiz_date.date() == today_utc:
                    quiz_count = user_data.get("quiz_count", 0)
            
            # Check if user has exceeded daily limit
            if quiz_count >= DAILY_QUIZ_LIMIT:
                # Create message with button
                message_text = (
                    f"⚠️ You've reached your daily quiz limit ({DAILY_QUIZ_LIMIT} quizzes).\n\n"
                    f"Token users are limited to {DAILY_QUIZ_LIMIT} quizzes per day.\n"
                    "Upgrade to premium for unlimited access!\n\n"
                    "Send /plan to know our premium plans"
                )
                
                # Create inline buttons
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "💎 Contact for Premium",
                            url=f"https://t.me/{PREMIUM_CONTACT.lstrip('@')}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "📋 View Premium Plans",
                            callback_data="premium_plans"
                        )
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await update.message.reply_text(
                    message_text,
                    parse_mode='Markdown',
                    reply_markup=reply_markup
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
        
        # For non-premium users, enforce daily limit
        if not is_prem and valid_questions:
            # Get current count again to be safe
            if DB is not None:
                user_data = await DB.users.find_one({"user_id": user_id})
                quiz_count = 0
                if user_data:
                    last_quiz_date = user_data.get("last_quiz_date")
                    if last_quiz_date and last_quiz_date.date() == today_utc:
                        quiz_count = user_data.get("quiz_count", 0)
            
            remaining_quota = DAILY_QUIZ_LIMIT - quiz_count
            if remaining_quota <= 0:
                # Create message with button
                message_text = (
                    f"⚠️ You've reached your daily quiz limit ({DAILY_QUIZ_LIMIT} quizzes).\n\n"
                    f"Token users are limited to {DAILY_QUIZ_LIMIT} quizzes per day.\n"
                    "Upgrade to premium for unlimited access!\n\n"
                    "Send /plan to know our premium plans"
                )
                
                # Create inline buttons
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "💎 Contact for Premium",
                            url=f"https://t.me/{PREMIUM_CONTACT.lstrip('@')}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "📋 View Premium Plans",
                            callback_data="premium_plans"
                        )
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await update.message.reply_text(
                    message_text,
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )
                return
                
            if len(valid_questions) > remaining_quota:
                valid_questions = valid_questions[:remaining_quota]
                if not errors:
                    errors = []
                errors.append(f"⚠️ Only first {remaining_quota} questions sent due to daily limit")
        
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
            
            sent_count = 0
            for question, options, correct_id, explanation in valid_questions:
                try:
                    poll_params = {
                        "chat_id": update.effective_chat.id,
                        "question": question,
                        "options": options,
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
            
            # Update quiz count for token users
            if not is_prem and DB is not None:
                today_utc = datetime.utcnow().date()
                await DB.users.update_one(
                    {"user_id": user_id},
                    {
                        "$set": {"last_quiz_date": datetime.utcnow()},
                        "$inc": {"quiz_count": sent_count}
                    },
                    upsert=True
                )
            
            await msg.edit_text(
                f"✅ Successfully sent {sent_count} quiz questions!"
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
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    if query.data == "premium_plans":
        await plan_command(update, context)
    elif query.data == "my_plan":
        # Use the message from the callback query instead of update.message
        if hasattr(update, 'message'):
            await my_plan_command(update, context)
        else:
            # Create a fake update with message for callback context
            fake_update = Update(update.update_id, message=query.message)
            await my_plan_command(fake_update, context)

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
            result = token_data is not None
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
            create_premium_index()
        )
    
    # Get token from environment
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    if not TOKEN:
        logger.error("No TELEGRAM_TOKEN found in environment!")
        return
    
    # Create Telegram application
    application = ApplicationBuilder().token(TOKEN).pool_timeout(30).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start_wrapper))
    application.add_handler(CommandHandler("help", help_command_wrapper))
    application.add_handler(CommandHandler("createquiz", create_quiz_wrapper))
    application.add_handler(CommandHandler("stats", stats_command_wrapper))
    application.add_handler(CommandHandler("token", token_command))
    application.add_handler(CommandHandler("plan", plan_command))
    application.add_handler(CommandHandler("myplan", my_plan_command))
    application.add_handler(MessageHandler(filters.Document.TEXT, handle_document_wrapper))
    
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
    
    # Start polling
    logger.info("Starting Telegram bot in polling mode...")
    try:
        await application.initialize()
        await application.start()
        await application.updater.start_polling(
            poll_interval=0.1,
            timeout=10,
            read_timeout=10
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
