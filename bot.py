import os
import json
import logging
import asyncio
import threading
import time
from datetime import datetime

from flask import Flask, request
import firebase_admin
from firebase_admin import credentials, firestore
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, 
    InputMediaPhoto, BotCommand
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, 
    MessageHandler, filters, ContextTypes
)
from telegram.constants import ParseMode
import re
import html
from cachetools import TTLCache
from google.api_core.exceptions import ResourceExhausted
from urllib.parse import quote_plus, unquote_plus

# -----------------------------------------------------------------------------
# 1. CONFIGURATION & SETUP
# -----------------------------------------------------------------------------

BOT_TOKEN = os.environ.get("BOT_TOKEN")
PUBLIC_CHANNEL_ID = os.environ.get("PUBLIC_CHANNEL_ID")
PUBLIC_CHANNEL_LINK = os.environ.get("PUBLIC_CHANNEL_LINK")
ADMIN_TELEGRAM_ID = int(os.environ.get("ADMIN_TELEGRAM_ID", 0))
FIREBASE_KEY_JSON = os.environ.get("FIREBASE_KEY")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

if not firebase_admin._apps:
    cred_dict = json.loads(FIREBASE_KEY_JSON)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)

db = firestore.client()

app = Flask(__name__)

# ----------------------------- CACHING & GLOBALS -----------------------------
USER_CACHE = TTLCache(maxsize=2000, ttl=60*10)
AD_CACHE = {'ads': [], 'last_refresh': 0}
MAINTENANCE_TEXT = "⚠️ Service temporarily under maintenance. Please try again later."
QUESTIONS_CACHE = TTLCache(maxsize=3000, ttl=60*60)
DEPTS_CACHE = {'deps': [], 'last_refresh': 0}


def get_user_data(user_id, force_refresh=False):
    """Fetch user data with optional force refresh (critical for referral checks)."""
    key = str(user_id)
    if not force_refresh and key in USER_CACHE:
        return USER_CACHE[key]

    try:
        doc = db.collection('users').document(key).get()
    except ResourceExhausted:
        raise
    except Exception as e:
        logger.error(f"Error fetching user {user_id}: {e}")
        return USER_CACHE.get(key) if key in USER_CACHE else None

    if doc and doc.exists:
        data = doc.to_dict() or {}
        USER_CACHE[key] = data
        return data
    return None


def get_question(dept_id, q_num):
    key = f"{dept_id}:{q_num}"
    if key in QUESTIONS_CACHE:
        return QUESTIONS_CACHE[key]

    try:
        doc = db.collection('departments').document(dept_id).collection('questions').document(str(q_num)).get()
    except ResourceExhausted:
        raise
    except Exception as e:
        logger.error(f"Error fetching question {dept_id}/{q_num}: {e}")
        return None

    if doc and doc.exists:
        data = doc.to_dict()
        QUESTIONS_CACHE[key] = data
        return data
    return None


def get_active_departments():
    now_ts = int(time.time())
    if (now_ts - DEPTS_CACHE['last_refresh']) < 600 and DEPTS_CACHE['deps']:
        return DEPTS_CACHE['deps']

    try:
        deps_ref = db.collection('departments').where('isActive', '==', True).stream()
        deps = []
        for d in deps_ref:
            d_data = d.to_dict() or {}
            d_data['_id'] = d.id
            deps.append(d_data)
        DEPTS_CACHE['deps'] = deps
        DEPTS_CACHE['last_refresh'] = now_ts
        return deps
    except ResourceExhausted:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch departments: {e}")
        return []


def get_dept_display(dept_id):
    deps = DEPTS_CACHE.get('deps') or []
    for d in deps:
        if d.get('_id') == dept_id:
            return d.get('displayName') or d.get('_id')

    try:
        doc = db.collection('departments').document(str(dept_id)).get()
        if doc and doc.exists:
            return doc.to_dict().get('displayName') or str(dept_id)
    except Exception:
        pass
    return str(dept_id)


def safe_async_handler(fn):
    async def wrapper(update, context, *args, **kwargs):
        try:
            return await fn(update, context, *args, **kwargs)
        except ResourceExhausted:
            logger.error("Firestore quota exceeded")
            try:
                await context.bot.send_message(chat_id=update.effective_user.id, text=MAINTENANCE_TEXT)
            except Exception:
                pass
            return
    return wrapper


@app.route('/')
def home():
    return "Bot is running..."


# -----------------------------------------------------------------------------
# 2. HELPER FUNCTIONS
# -----------------------------------------------------------------------------

async def check_membership(user_id, bot):
    try:
        member = await bot.get_chat_member(chat_id=PUBLIC_CHANNEL_ID, user_id=user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Membership check failed: {e}")
        return False


def create_user(user_id, referrer_id=None, ref_dept=None):
    """Create user + department-specific referral logic (FIXED)"""
    now = datetime.utcnow()
    user_key = str(user_id)
    user_data = {
        'totalAttempts': 0,
        'totalCorrect': 0,
        'referral_counts': {},
        'unlocked_departments': {},
        'referralCount': 0,
        'createdAt': now,
        'currentSession': None
    }
    try:
        db.collection('users').document(user_key).set(user_data)
        USER_CACHE[user_key] = user_data   # ← FIXED typo
    except ResourceExhausted:
        raise
    except Exception as e:
        logger.error(f"Failed to create user {user_id}: {e}")

    if referrer_id and str(referrer_id) != user_key and ref_dept:
        try:
            # Record referral
            db.collection('referrals').document().set({
                'inviter_id': str(referrer_id),
                'invited_id': user_key,
                'dept_id': str(ref_dept),
                'timestamp': now
            })

            inviter_ref = db.collection('users').document(str(referrer_id))
            inv_key = str(referrer_id)

            # Increment referral count
            inviter_ref.set({f'referral_counts.{ref_dept}': firestore.Increment(1)}, merge=True)
            USER_CACHE.pop(inv_key, None)

            # Get fresh count
            inviter_doc = inviter_ref.get()
            inviter_data = inviter_doc.to_dict() or {} if inviter_doc.exists else {}
            dept_count = int(inviter_data.get('referral_counts', {}).get(ref_dept, 0))

            dept_unlocked = False
            if dept_count >= 2:
                dept_unlocked = True
                # Prefer update() for atomic write; fallback to set(..., merge=True) with retries
                try:
                    inviter_ref.update({f'unlocked_departments.{ref_dept}': True})
                    USER_CACHE.pop(inv_key, None)
                except Exception:
                    max_attempts = 3
                    for attempt in range(1, max_attempts + 1):
                        try:
                            inviter_ref.set({f'unlocked_departments.{ref_dept}': True}, merge=True)
                            USER_CACHE.pop(inv_key, None)
                            break
                        except Exception as e:
                            logger.warning(f"Unlock retry {attempt} failed for {inv_key}/{ref_dept}: {e}")
                            if attempt < max_attempts:
                                time.sleep(0.5 * attempt)
                            else:
                                logger.error(f"Failed to persist unlocked_departments after {max_attempts} attempts for {inv_key}/{ref_dept}")

            return True, True, dept_unlocked
        except ResourceExhausted:
            raise
        except Exception as e:
            logger.error(f"Referral recording failed: {e}")
            return True, False, False

    return True, False, False


def get_ad_to_show(current_index):
    now_ts = int(time.time())
    if (now_ts - AD_CACHE['last_refresh']) > 3600 or not AD_CACHE['ads']:
        try:
            ads_ref = db.collection('ads').where('isActive', '==', True).order_by('order_index').stream()
            AD_CACHE['ads'] = [ad.to_dict() for ad in ads_ref]
            AD_CACHE['last_refresh'] = now_ts
        except ResourceExhausted:
            raise
        except Exception as e:
            logger.error(f"Failed to refresh ads: {e}")

    ads = AD_CACHE.get('ads', [])
    if not ads:
        return None
    return ads[current_index % len(ads)]


def escape_html(text):
    if not isinstance(text, str):
        return text
    return html.escape(text)


# -----------------------------------------------------------------------------
# 3. BOT HANDLERS
# -----------------------------------------------------------------------------

@safe_async_handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args
    referrer = None
    ref_dept = None

    if args and args[0].startswith('ref_'):
        rest = unquote_plus(args[0][4:])
        if '_dept_' in rest:
            inviter, dept_code = rest.split('_dept_', 1)
            referrer = inviter
            ref_dept = dept_code
    elif args and args[0].startswith('dept_'):
        # direct dept start handled later
        pass

    user_data = get_user_data(user.id)
    referral_recorded = False
    dept_unlocked = False

    if not user_data:
        _, referral_recorded, dept_unlocked = create_user(user.id, referrer, ref_dept)
        user_data = get_user_data(user.id, force_refresh=True)  # ← fresh after referral

    # Update profile
    try:
        db.collection('users').document(str(user.id)).update({
            'first_name': user.first_name or '',
            'last_name': user.last_name or '',
            'username': user.username or ''
        })
    except Exception:
        db.collection('users').document(str(user.id)).set({
            'first_name': user.first_name or '',
            'last_name': user.last_name or '',
            'username': user.username or ''
        }, merge=True)

    # Notify inviter
    if referral_recorded and referrer and ref_dept and dept_unlocked:
        try:
            dept_display = get_dept_display(ref_dept)
            await context.bot.send_message(
                chat_id=int(referrer),
                text=f"🎉 **Congratulations!**\n\nYou have invited 2 users for *{dept_display}*. That department is now unlocked for you!",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

    # Membership check, session resume, etc. (unchanged)
    is_member = await check_membership(user.id, context.bot)
    if not is_member:
        keyboard = [
            [InlineKeyboardButton("Join Channel", url=PUBLIC_CHANNEL_LINK)],
            [InlineKeyboardButton("Try Again", callback_data="check_membership")]
        ]
        await update.message.reply_text("⚠️ **Access Denied**\n\nPlease join our channel to access the quizzes.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        return

    if user_data and user_data.get('currentSession') and user_data['currentSession'].get('sessionActive'):
        keyboard = [
            [InlineKeyboardButton("Resume Session", callback_data="resume_session")],
            [InlineKeyboardButton("Start New", callback_data="main_menu")]
        ]
        await update.message.reply_text("🔄 **Resume Quiz?**\n\nYou have an unfinished session.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        return

    await show_main_menu(update, context)


@safe_async_handler
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    deps = get_active_departments()
    keyboard = []
    for d in deps:
        if d.get('totalQuestions', 0) > 0:
            label = d.get('displayName') or d.get('_id')
            keyboard.append([InlineKeyboardButton(label, callback_data=f"dept_{d.get('_id')}")])
    keyboard.append([InlineKeyboardButton("📊 My Score", callback_data="show_score")])

    text = "📚 **Select a Department**\nChoose a subject to start the quiz."
    if update.callback_query:
        await update.callback_query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)


# -----------------------------------------------------------------------------
# 4. QUIZ ENGINE (only changed the critical lock parts)
# -----------------------------------------------------------------------------

@safe_async_handler
async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE, dept_id):
    user_id = update.effective_user.id
    session = {
        'department_id': dept_id,
        'current_question_index': 0,
        'correct_in_session': 0,
        'attempted_in_session': 0,
        'sessionActive': True,
        'ad_break_counter': 0
    }
    db.collection('users').document(str(user_id)).update({'currentSession': session})
    USER_CACHE.pop(str(user_id), None)
    await send_question(update, context, user_id, session)


@safe_async_handler
async def send_question(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, session):
    dept_id = session['department_id']
    q_index = session['current_question_index']

    # --- CHECK REFERRAL LOCK (After Q25) --- FIXED
    if q_index == 25:
        user_doc = get_user_data(user_id, force_refresh=True)  # ← always fresh
        unlocked = bool(user_doc.get('unlocked_departments', {}).get(dept_id, False)) if user_doc else False

        if not unlocked:
            await send_session_summary(update, context, session, "🔒 Progress Locked")
            ref_param = f"ref_{user_id}_dept_{dept_id}"
            ref_link = f"https://t.me/{context.bot.username}?start={quote_plus(ref_param)}"
            dept_display = get_dept_display(dept_id)
            text = (
                "🔒 **Content Locked**\n\n"
                f"You have completed the free 25 questions for *{dept_display}*.\n"
                "**Invite 2 friends using your department referral link** to unlock the remaining 75 questions!\n\n"
                f"Your Referral Link:\n`{ref_link}`"
            )
            cb_dept = quote_plus(dept_id)
            keyboard = [[InlineKeyboardButton("Check Status & Continue", callback_data=f"check_lock_{cb_dept}")]]
            if update.callback_query:
                await update.callback_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
            return

    # ... rest of send_question unchanged (ad logic, question fetch, etc.) ...
    # --- CHECK COMPLETION (After Q100) ---
    if q_index >= 100:
        await send_session_summary(update, context, session, "🏆 Session Complete")
        db.collection('users').document(str(user_id)).update({'currentSession.sessionActive': False})
        try:
            if str(user_id) in USER_CACHE and USER_CACHE[str(user_id)].get('currentSession'):
                USER_CACHE[str(user_id)]['currentSession']['sessionActive'] = False
        except Exception:
            USER_CACHE.pop(str(user_id), None)
        await show_main_menu(update, context)
        return

    # --- AD LOGIC (Every 5 Qs, but not at 0) ---
    if q_index > 0 and q_index % 5 == 0:
        ad = get_ad_to_show(session.get('ad_break_counter', 0))
        if ad:
            # Increment ad break counter
            session['ad_break_counter'] = session.get('ad_break_counter', 0) + 1
            db.collection('users').document(str(user_id)).update({'currentSession': session})
            # Update cache for session
            try:
                if str(user_id) in USER_CACHE:
                    USER_CACHE[str(user_id)]['currentSession'] = session
                else:
                    USER_CACHE[str(user_id)] = {'currentSession': session}
            except Exception:
                USER_CACHE.pop(str(user_id), None)
            
            # Send Ad
            try:
                # Ad storage supports: {type: 'photo'|'video'|'text', file_id, caption, text}
                if ad.get('type') == 'photo' and ad.get('file_id'):
                    await context.bot.send_photo(chat_id=user_id, photo=ad.get('file_id'), caption=ad.get('caption',''))
                elif ad.get('type') == 'video' and ad.get('file_id'):
                    try:
                        await context.bot.send_video(chat_id=user_id, video=ad.get('file_id'), caption=ad.get('caption',''))
                    except Exception:
                        # Fallback: if original was uploaded as a document, try sending as document
                        try:
                            await context.bot.send_document(chat_id=user_id, document=ad.get('file_id'), caption=ad.get('caption',''))
                        except Exception as e:
                            logger.error(f"Failed to send video ad as video or document: {e}")
                elif ad.get('type') == 'text' and ad.get('text'):
                    await context.bot.send_message(chat_id=user_id, text=ad.get('text'))
                elif ad.get('message_link'):
                    await context.bot.send_message(chat_id=user_id, text=f"📢 **Sponsor**\n{ad['message_link']}")

                time.sleep(2)
            except Exception as e:
                logger.error(f"Ad error: {e}")

    # --- FETCH QUESTION ---
    # Questions are stored in 'departments/{dept_id}/questions/{q_id}'
    # We assume questions are ordered by 'question_number' or ID.
    # To avoid high reads, we should query by number.
    q_num = q_index + 1
    
    # Fetch specific question (cached)
    question_data = get_question(dept_id, q_num)
    
    if not question_data:
        # Fallback if question missing
        await context.bot.send_message(chat_id=user_id, text="Error: Question not found. Ending session.")
        await show_main_menu(update, context)
        return

    # Construct UI: Put full options in the message text, keep buttons short (A/B/C/D)
    opts = question_data['options']
    # Use HTML-escaped DB content to avoid visible backslashes from Markdown escaping.
    q_text = escape_html(question_data['question_text'])
    a_text = escape_html(opts.get('a', ''))
    b_text = escape_html(opts.get('b', ''))
    c_text = escape_html(opts.get('c', ''))
    d_text = escape_html(opts.get('d', ''))

    text = (
        f"<b>Question {q_num}/100</b>\n\n{q_text}\n\n"
        f"A. {a_text}\n"
        f"B. {b_text}\n"
        f"C. {c_text}\n"
        f"D. {d_text}"
    )

    # Buttons: short labels only so long option text isn't truncated on small screens
    row1 = [
        InlineKeyboardButton("A", callback_data=f"ans_a_{q_num}"),
        InlineKeyboardButton("B", callback_data=f"ans_b_{q_num}")
    ]
    row2 = [
        InlineKeyboardButton("C", callback_data=f"ans_c_{q_num}"),
        InlineKeyboardButton("D", callback_data=f"ans_d_{q_num}")
    ]
    row3 = [InlineKeyboardButton("🏠 Home", callback_data="home_confirm")]

    markup = InlineKeyboardMarkup([row1, row2, row3])
    
    # Store Correct Answer in Callback Data is risky for cheaters, 
    # but strictly following prompt we check answer in backend.
    
    if update.callback_query:
        # If previous message was an answer explanation, send new message
        # If simply flow, edit (but editing text with different height can be jumpy)
        # Spec says: "Edit message" for results. For new question, usually send new.
        await update.callback_query.message.reply_text(text=text, reply_markup=markup, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text=text, reply_markup=markup, parse_mode=ParseMode.HTML)

@safe_async_handler
async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    data = query.data # e.g. ans_a_12
    
    parts = data.split('_')
    selected_opt = parts[1] # 'a'
    q_num = int(parts[2])
    
    # Get Session (from cache to avoid a read)
    user_ref = db.collection('users').document(str(user_id))
    user_data = get_user_data(user_id)
    session = user_data.get('currentSession') if user_data else None
    
    if not session or not session.get('sessionActive'):
        await query.answer("Session expired.")
        return

    # Prevent spamming (Check if question index matches)
    if (session['current_question_index'] + 1) != q_num:
        await query.answer("Please wait...", show_alert=True)
        return

    # Fetch Question Data again for verification (cached)
    dept_id = session['department_id']
    q_data = get_question(dept_id, q_num)
        
    is_correct = (selected_opt == q_data['answer'])
    
    # Update Stats
    updates = {
        'totalAttempts': firestore.Increment(1),
        'currentSession.attempted_in_session': firestore.Increment(1),
        'currentSession.current_question_index': firestore.Increment(1)
    }
    
    if is_correct:
        updates['totalCorrect'] = firestore.Increment(1)
        updates['currentSession.correct_in_session'] = firestore.Increment(1)
        status_text = "✓ Correct"
    else:
        status_text = "✗ Incorrect"
        
    # Apply updates without read-before-write
    try:
        user_ref.update(updates)
    except ResourceExhausted:
        raise
    except Exception as e:
        logger.error(f"Failed to update user stats: {e}")

    # Invalidate cache for this user so subsequent reads are fresh
    USER_CACHE.pop(str(user_id), None)
    
    # Update Session Object Locally for Next Step
    session['current_question_index'] += 1
    session['attempted_in_session'] += 1
    if is_correct:
        session['correct_in_session'] += 1

    # --- Update leaderboard per-department ---
    try:
        lb_ref = db.collection('leaderboard').document(str(user_id))
        dept = dept_id
        # Use set with merge to avoid read-before-write and multiple updates
        payload = {
            f'departments.{dept}.attempts': firestore.Increment(1),
            'updatedAt': datetime.utcnow()
        }
        if is_correct:
            payload[f'departments.{dept}.correct'] = firestore.Increment(1)
        lb_ref.set(payload, merge=True)
    except ResourceExhausted:
        raise
    except Exception as e:
        logger.error(f"Leaderboard update failed: {e}")

    # Edit Message
    explanation = q_data.get('explanation', 'No explanation provided.')
    correct_ans_key = q_data['answer']
    correct_text = q_data['options'][correct_ans_key]

    # HTML-escape DB-provided texts for safe HTML formatting
    esc_question_text = escape_html(q_data['question_text'])
    esc_opts = {k: escape_html(v) for k, v in q_data['options'].items()}
    esc_explanation = escape_html(explanation)
    esc_correct_text = escape_html(correct_text)

    # Build the result block to append below the question and options (do not remove/modify the question)
    result_block = (
        f"{status_text}\n\n"
        f"<b>Correct Answer:</b> {correct_ans_key.upper()}. {esc_correct_text}\n"
        f"<b>Explanation:</b> {esc_explanation}"
    )

    # Reconstruct original question+options text (same format as when sent)
    original_text = (
        f"<b>Question {q_num}/100</b>\n\n{esc_question_text}\n\n"
        f"A. {esc_opts.get('a','')}\n"
        f"B. {esc_opts.get('b','')}\n"
        f"C. {esc_opts.get('c','')}\n"
        f"D. {esc_opts.get('d','')}"
    )

    # Combine and replace the option buttons with navigation (so the four choice buttons are removed)
    combined_text = f"{original_text}\n\n{result_block}"

    nav_buttons = [[InlineKeyboardButton("Next ➡️", callback_data="next_question")]]

    await query.edit_message_text(text=combined_text, reply_markup=InlineKeyboardMarkup(nav_buttons), parse_mode=ParseMode.HTML)

@safe_async_handler
async def next_question_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_data = get_user_data(user_id)
    session = user_data.get('currentSession')
    await send_question(update, context, user_id, session)

async def send_session_summary(update: Update, context: ContextTypes.DEFAULT_TYPE, session, title):
    total = session.get('attempted_in_session', 0)
    correct = session.get('correct_in_session', 0)
    acc = (correct / total * 100) if total > 0 else 0
    
    text = (
        f"<b>{title}</b>\n\n"
        f"Department: {get_dept_display(session['department_id'])}\n"
        f"Questions Attempted: {total}\n"
        f"Correct Answers: {correct}\n"
        f"Accuracy: {acc:.1f}%"
    )
    if update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)




# -----------------------------------------------------------------------------
# 5. CALLBACK ROUTER - FIXED check_lock
# -----------------------------------------------------------------------------

@safe_async_handler
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = update.effective_user.id

    if data == "check_membership":
        if await check_membership(user_id, context.bot):
            await query.answer("Access Granted!")
            await show_main_menu(update, context)
        else:
            await query.answer("Still not a member.", show_alert=True)

    elif data == "main_menu":
        await show_main_menu(update, context)

    elif data.startswith("dept_"):
        dept_id = data.replace("dept_", "")
        await start_quiz(update, context, dept_id)

    elif data.startswith("ans_"):
        await handle_answer(update, context)

    elif data == "next_question":
        await next_question_handler(update, context)

    elif data == "home_confirm":
        # Ask for confirmation
        kb = [
            [InlineKeyboardButton("Yes, Exit", callback_data="home_exit"), InlineKeyboardButton("Cancel", callback_data="home_cancel")]
        ]
        await query.message.reply_text(
            "⚠️ **Exit Session?**\n\nYour progress will be saved.",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=ParseMode.MARKDOWN
        )

    elif data == "home_exit":
        user_data = get_user_data(user_id)
        if user_data and user_data.get('currentSession'):
            await send_session_summary(update, context, user_data['currentSession'], "Paused Session")
        await show_main_menu(update, context)

    elif data == "home_cancel":
        await query.message.delete() # Remove the warning message
        
    elif data == "show_score":
        user_data = get_user_data(user_id)
        attempts = user_data.get('totalAttempts', 0)
        correct = user_data.get('totalCorrect', 0)
        acc = (correct / attempts * 100) if attempts > 0 else 0
        text = (
            "📊 **Your Overall Performance**\n\n"
            f"Total Attempts: {attempts}\n"
            f"Total Correct: {correct}\n"
            f"Overall Accuracy: {acc:.1f}%"
        )
        kb = [[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
    
    elif data == "check_lock":
        # Re-check referral count (legacy non-departmental)
        user_data = get_user_data(user_id)
        if user_data.get('referralCount', 0) >= 2:
            await query.answer("Unlocked!", show_alert=True)
            await next_question_handler(update, context)
        else:
            await query.answer(f"Referrals: {user_data.get('referralCount',0)}/2. Invite more!", show_alert=True)

            
    elif data.startswith('check_lock_'):
        # FIXED - always fresh read
        dept_encoded = data[len('check_lock_'):]
        try:
            dept_id = unquote_plus(dept_encoded)
        except Exception:
            dept_id = dept_encoded

        # ALWAYS fresh read for referral status
        fresh_doc = db.collection('users').document(str(user_id)).get()
        user_data = fresh_doc.to_dict() or {} if fresh_doc.exists else {}
        USER_CACHE[str(user_id)] = user_data  # sync cache

        unlocked = bool(user_data.get('unlocked_departments', {}).get(dept_id, False))
        dept_count = int(user_data.get('referral_counts', {}).get(dept_id, 0))

        if unlocked or dept_count >= 2:
            # Ensure flag is set
            try:
                db.collection('users').document(str(user_id)).set(
                    {f'unlocked_departments.{dept_id}': True}, merge=True)
                USER_CACHE.pop(str(user_id), None)
            except Exception:
                pass
            await query.answer("✅ Department Unlocked! Continuing...", show_alert=True)
            await next_question_handler(update, context)
        else:
            await query.answer(f"Referrals for this department: {dept_count}/2\nInvite 2 friends!", show_alert=True)

    elif data == "resume_session":
        await next_question_handler(update, context)

    await query.answer()


# -----------------------------------------------------------------------------
# 6. ADMIN HANDLERS (unchanged)
# -----------------------------------------------------------------------------

@safe_async_handler
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_TELEGRAM_ID:
        return
    
    kb = [
        [InlineKeyboardButton("Upload JSON", callback_data="admin_upload")],
        [InlineKeyboardButton("Total Users", callback_data="admin_users")],
        [InlineKeyboardButton("Add Ad", callback_data="admin_ad")]
    ]
    await update.message.reply_text("🛠 **Admin Panel**", reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)

@safe_async_handler
async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    
    if data == "admin_users":
        # Count Users
        users = db.collection('users').count().get()
        count = users[0][0].value
        await query.message.reply_text(f"👥 Total Registered Users: {count}")
    
    elif data == "admin_upload":
        await query.message.reply_text("📂 **Upload Mode**\n\n1. Reply with the Department Name.\n2. Then upload the JSON file.")
        context.user_data['admin_state'] = 'awaiting_dept_name'

    # --- ADDED THIS BLOCK ---
    elif data == "admin_ad":
        await query.message.reply_text(
            "📣 Send the ad now as either:\n- Photo (with optional caption)\n- Video (with optional caption)\n- Plain text\n\nThe bot will store only the Telegram `file_id` or the text."
        )
        context.user_data['admin_state'] = 'awaiting_ad'
    
    await query.answer() # Always answer the query to stop the loading animation

@safe_async_handler
async def admin_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_TELEGRAM_ID:
        return

    state = context.user_data.get('admin_state')
    
    # 1. Capture Department Name
    if state == 'awaiting_dept_name':
        dept_name = update.message.text.strip()
        context.user_data['upload_dept'] = dept_name
        context.user_data['admin_state'] = 'awaiting_json'
        await update.message.reply_text(f"Selected Department: **{dept_name}**\nNow upload the JSON file.", parse_mode=ParseMode.MARKDOWN)
        return

    # 2. Process JSON File
    if state == 'awaiting_json' and update.message.document:
        doc = update.message.document
        f = await doc.get_file()
        byte_array = await f.download_as_bytearray()
        
        try:
            questions = json.loads(byte_array.decode('utf-8'))
            dept_name = context.user_data['upload_dept']
            
            # Save to Firestore
            dept_ref = db.collection('departments').document(dept_name)
            batch = db.batch()
            
            # Update Department Info
            dept_ref.set({
                'isActive': True,
                'totalQuestions': len(questions)
            })
            # Invalidate departments cache so menu shows updated department list
            DEPTS_CACHE['deps'] = []
            DEPTS_CACHE['last_refresh'] = 0
            
            # Upload Questions
            for q in questions:
                # Ensure structure matches spec
                q_num = q.get('question_number')
                q_doc = dept_ref.collection('questions').document(str(q_num))
                # Not using batch for subcollection in loop to avoid limits on huge files, 
                # but for <500 items batch is fine. Using direct set for safety.
                q_doc.set(q)
            
            await update.message.reply_text(f"✅ Successfully uploaded {len(questions)} questions to {dept_name}.")
            context.user_data['admin_state'] = None
            
            # Ask to post to channel
            await update.message.reply_text("Send a photo with caption to post this update to the public channel (or /cancel).")
            context.user_data['admin_state'] = 'awaiting_post'
            context.user_data['post_dept'] = dept_name
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error processing JSON: {e}")

    # 3. Post to Public Channel
    if state == 'awaiting_post' and update.message.photo:
        dept_name = context.user_data.get('post_dept')
        caption = update.message.caption or f"New Quiz Available: {dept_name}"
        
        # Add Deep Link
        deep_link = f"https://t.me/{context.bot.username}?start=dept_{dept_name}"
        final_caption = f"{caption}\n\n👉 Start Quiz: {deep_link}"
        
        await context.bot.send_photo(
            chat_id=PUBLIC_CHANNEL_ID,
            photo=update.message.photo[-1].file_id,
            caption=final_caption
        )
        await update.message.reply_text("✅ Posted to channel.")
        context.user_data['admin_state'] = None
        
    # 4. Add Ad
    if state == 'awaiting_ad':
        # Photo
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            caption = update.message.caption or ''
            db.collection('ads').add({
                'type': 'photo',
                'file_id': file_id,
                'caption': caption,
                'order_index': int(time.time()),
                'isActive': True
            })
            # Invalidate ad cache so admin-added ads appear on next refresh
            AD_CACHE['ads'] = []
            AD_CACHE['last_refresh'] = 0
            await update.message.reply_text("✅ Photo ad saved (file_id stored).")
            context.user_data['admin_state'] = None
            return

        # Video sent as a document (some clients send videos as documents)
        if update.message.document and getattr(update.message.document, 'mime_type', '').startswith('video'):
            file_id = update.message.document.file_id
            caption = update.message.caption or ''
            db.collection('ads').add({
                'type': 'video',
                'file_id': file_id,
                'caption': caption,
                'order_index': int(time.time()),
                'isActive': True
            })
            AD_CACHE['ads'] = []
            AD_CACHE['last_refresh'] = 0
            await update.message.reply_text("✅ Video ad saved (file_id stored).")
            context.user_data['admin_state'] = None
            return

        # Video
        if update.message.video:
            file_id = update.message.video.file_id
            caption = update.message.caption or ''
            db.collection('ads').add({
                'type': 'video',
                'file_id': file_id,
                'caption': caption,
                'order_index': int(time.time()),
                'isActive': True
            })
            AD_CACHE['ads'] = []
            AD_CACHE['last_refresh'] = 0
            await update.message.reply_text("✅ Video ad saved (file_id stored).")
            context.user_data['admin_state'] = None
            return

        # Plain Text
        if update.message.text:
            text = update.message.text
            db.collection('ads').add({
                'type': 'text',
                'text': text,
                'order_index': int(time.time()),
                'isActive': True
            })
            AD_CACHE['ads'] = []
            AD_CACHE['last_refresh'] = 0
            await update.message.reply_text("✅ Text ad saved.")
            context.user_data['admin_state'] = None
            return

async def admin_ad_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "admin_ad":
        await query.message.reply_text("Send the text/link content for the Ad.")
        context.user_data['admin_state'] = 'awaiting_ad_link'


@safe_async_handler
async def admin_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: compute and display leaderboards.
    Shows Top 10 overall by average department accuracy and Top 3 per department.
    """
    user_id = update.effective_user.id
    if user_id != ADMIN_TELEGRAM_ID:
        return

    # Fetch all leaderboard entries
    docs = db.collection('leaderboard').stream()
    overall_list = []  # (user_id, overall_avg, total_attempts)
    dept_lists = {}    # dept_id -> [(user_id, acc, attempts)]

    for d in docs:
        uid = d.id
        data = d.to_dict() or {}
        depts = data.get('departments', {})
        per_accs = []
        total_attempts = 0
        for dept, stats in depts.items():
            att = int(stats.get('attempts', 0))
            cor = int(stats.get('correct', 0))
            if att > 0:
                acc = cor / att
                per_accs.append(acc)
                total_attempts += att
                dept_lists.setdefault(dept, []).append((uid, acc, att))

        if per_accs:
            overall_avg = sum(per_accs) / len(per_accs)
            overall_list.append((uid, overall_avg, total_attempts))

    # Top 10 overall
    overall_list.sort(key=lambda x: x[1], reverse=True)
    top10 = overall_list[:10]

    parts = []
    parts.append("*Top 10 Users — Overall Average Accuracy*\n")
    if not top10:
        parts.append("No leaderboard data available.")
    else:
            for idx, (uid, avg, attempts) in enumerate(top10, start=1):
                udata = get_user_data(uid) or {}
                name = udata.get('first_name') or udata.get('username') or uid
                parts.append(f"{idx}. {name} — {avg*100:.1f}% ({attempts} attempts)")

    # Top 3 per department
    parts.append("\n*Top 3 Per Department*\n")
    if not dept_lists:
        parts.append("No per-department scores yet.")
    else:
        for dept, arr in dept_lists.items():
            parts.append(f"{get_dept_display(dept)}:")
            arr.sort(key=lambda x: x[1], reverse=True)
            for j, (uid, acc, att) in enumerate(arr[:3], start=1):
                udata = get_user_data(uid) or {}
                name = udata.get('first_name') or udata.get('username') or uid
                parts.append(f" {j}. {name} — {acc*100:.1f}% ({att} attempts)")

    text = "\n".join(parts)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# -----------------------------------------------------------------------------
# 7. MAIN EXECUTION
# -----------------------------------------------------------------------------

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)


def main():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(callback_router))
    application.add_handler(CommandHandler("ethioegzam", admin_panel))
    application.add_handler(CommandHandler("leaderboard", admin_leaderboard))
    application.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO | filters.TEXT | filters.VIDEO, admin_message_handler))

    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    print("Bot is polling...")
    application.run_polling()


if __name__ == '__main__':
    main()
