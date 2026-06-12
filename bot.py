import os
import json
import time
import threading
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from dotenv import load_dotenv
from flask import Flask
import telebot
import html
from telebot.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove
)
import firebase_admin
from firebase_admin import credentials, firestore

# ==========================================
# 1. CONFIGURATION & ENVIRONMENT SETUP
# ==========================================
load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
BOT_USERNAME = os.getenv('BOT_USERNAME')
ADMIN_TELEGRAM_ID = int(os.getenv('ADMIN_TELEGRAM_ID', 0))
PUBLIC_CHANNEL_ID = os.getenv('PUBLIC_CHANNEL_ID')
PUBLIC_CHANNEL_LINK = os.getenv('PUBLIC_CHANNEL_LINK')
FIREBASE_KEY_STR = os.getenv('FIREBASE_KEY')

bot = telebot.TeleBot(BOT_TOKEN, parse_mode='HTML')

# ==========================================
# 2. FIREBASE INITIALIZATION
# ==========================================
try:
    firebase_cred_dict = json.loads(FIREBASE_KEY_STR)
    cred = credentials.Certificate(firebase_cred_dict)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("Firebase initialized successfully.")
except Exception as e:
    print(f"Error initializing Firebase: {e}")

# ==========================================
# 3. GLOBAL CACHE & STATE MANAGEMENT
# ==========================================
CACHE = {
    'entrance_subjects': {}, # {name: code}
    'exit_departments': {},  # {name: code}
    'exams': {},             # {exam_id: questions_list}
    'exam_lists': {},        # {category_subject: [exam_types]}
    'ad_data': None,         # {"chat_id": admin_id, "message_id": msg_id}
    'total_users': 0
}

# User states: active sessions, navigation path, etc.
user_states = {}       # {user_id: {"menu": "main", "category": "entrance", "subject": "Biology", "premium_type": "entrance", "target_code": "all"}}
active_sessions = {}   # {user_id: {"exam_id": ..., "questions": [...], "current_index": 0, "correct": 0, "last_activity": timestamp, "locked": False, "referrals": 0}}
queued_broadcasts = {} # {user_id: [messages]}
MAINTENANCE_MODE = False

# ==========================================
# 4. FLASK SERVER (KEEP-ALIVE)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is alive and running!"

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))

# ==========================================
# 5. HELPER FUNCTIONS
# ==========================================
def load_cache():
    """Initial cache load from Firestore"""
    global CACHE
    try:
        # Load Ad
        ad_doc = db.collection('settings').document('advertisement').get()
        if ad_doc.exists:
            CACHE['ad_data'] = ad_doc.to_dict()
        
        # Load Entrance
        ent_ref = db.collection('exam').document('entrance').collection('subjects').stream()
        for doc in ent_ref:
            CACHE['entrance_subjects'][doc.to_dict().get('name', doc.id)] = doc.id
            
        # Load Exit
        ext_ref = db.collection('exam').document('exit').collection('departments').stream()
        for doc in ext_ref:
            CACHE['exit_departments'][doc.to_dict().get('name', doc.id)] = doc.id
            
        # Load User Count
        users = db.collection('users').count().get()
        CACHE['total_users'] = users[0][0].value
    except Exception as e:
        print(f"Cache load error: {e}")

def check_membership(user_id):
    try:
        member = bot.get_chat_member(PUBLIC_CHANNEL_ID, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception:
        return False

def build_inline_keyboard(buttons_data, cols=1):
    markup = InlineKeyboardMarkup()
    row = []
    for text, callback_data in buttons_data:
        row.append(InlineKeyboardButton(text, callback_data=callback_data))
        if len(row) == cols:
            markup.add(*row)
            row = []
    if row:
        markup.add(*row)
    return markup

def build_reply_keyboard(buttons_text, cols=2, add_nav=False):
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    row = []
    for text in buttons_text:
        row.append(KeyboardButton(text))
        if len(row) == cols:
            markup.add(*row)
            row = []
    if row:
        markup.add(*row)
    if add_nav:
        markup.add(KeyboardButton("Back"), KeyboardButton("Home"))
    return markup

def register_user(user_id, username):
    doc_ref = db.collection('users').document(str(user_id))
    if not doc_ref.get().exists:
        doc_ref.set({"username": username, "joined_at": datetime.now(), "referrals": 0})
        CACHE['total_users'] += 1

def update_activity(user_id):
    if user_id in active_sessions:
        active_sessions[user_id]['last_activity'] = datetime.now()


def safe_html(text):
    """Escape text for HTML parse_mode while preserving bot-owned tags."""
    try:
        return html.escape(str(text))
    except Exception:
        return str(text)


def format_exam_display(exam_id):
    """Format exam_id into a user-friendly string"""
    try:
        parts = str(exam_id).split("_", 2)
        if len(parts) >= 2:
            category = parts[0]
            item_code = parts[1]
            type_name = parts[2] if len(parts) == 3 else ""

            item_name = None
            if category.lower() == 'entrance':
                for name, code in CACHE.get('entrance_subjects', {}).items():
                    if code == item_code:
                        item_name = name
                        break
            elif category.lower() == 'exit':
                for name, code in CACHE.get('exit_departments', {}).items():
                    if code == item_code:
                        item_name = name
                        break

            if not item_name:
                item_name = item_code

            if type_name:
                return f"{category} : {item_name} - {type_name}"
            else:
                return f"{category} : {item_name}"
    except Exception:
        pass
    return str(exam_id)

# ==========================================
# 6. MIDDLEWARE / PRE-CHECKS
# ==========================================
@bot.message_handler(func=lambda msg: MAINTENANCE_MODE and msg.from_user.id != ADMIN_TELEGRAM_ID)
def maintenance_check(message):
    bot.reply_to(message, "The bot is currently under maintenance.\nPlease try again later.")

# ==========================================
# 7. USER FLOW: START & NAVIGATION
# ==========================================
@bot.message_handler(commands=['start'])
def send_welcome(message):
    user_id = message.from_user.id
    user_doc_ref = db.collection('users').document(str(user_id))
    try:
        user_doc = user_doc_ref.get()
        user_already_registered = user_doc.exists
    except Exception:
        user_already_registered = False

    args = message.text.split()
    if not user_already_registered and len(args) > 1 and args[1].startswith("ref_"):
        parts = args[1].split("_", 2)
        if len(parts) == 3:
            _, ref_user_id_str, exam_id = parts
            try:
                ref_user_id = int(ref_user_id_str)
            except Exception:
                ref_user_id = None

            if ref_user_id and ref_user_id != user_id:
                try:
                    db.collection('referrals').add({
                        'inviter_id': ref_user_id,
                        'invited_id': user_id,
                        'exam_id': exam_id,
                        'timestamp': datetime.utcnow()
                    })
                except Exception:
                    pass

                try:
                    inviter_ref = db.collection('users').document(str(ref_user_id))
                    inviter_ref.update({
                        f"referrals_map.{exam_id}": firestore.Increment(1),
                        "referrals": firestore.Increment(1)
                    })
                except Exception:
                    try:
                        db.collection('users').document(str(ref_user_id)).update({"referrals": firestore.Increment(1)})
                    except Exception:
                        pass

                if ref_user_id in active_sessions and active_sessions[ref_user_id].get('exam_id') == exam_id:
                    active_sessions[ref_user_id].setdefault('referrals', 0)
                    active_sessions[ref_user_id]['referrals'] += 1

                try:
                    inviter_doc = db.collection('users').document(str(ref_user_id)).get()
                    inviter_data = inviter_doc.to_dict() if inviter_doc.exists else {}
                    referrals_map = inviter_data.get('referrals_map', {}) if inviter_data else {}
                    count_for_exam = referrals_map.get(exam_id, 0)
                    unlocked = inviter_data.get('unlocked_exams', []) if inviter_data else []

                    if count_for_exam >= 2 and exam_id not in unlocked:
                        try:
                            db.collection('users').document(str(ref_user_id)).update({
                                'unlocked_exams': firestore.ArrayUnion([exam_id])
                            })
                        except Exception:
                            pass

                        if ref_user_id in active_sessions and active_sessions[ref_user_id].get('exam_id') == exam_id:
                            active_sessions[ref_user_id]['locked'] = False
                            try:
                                display = format_exam_display(exam_id)
                                bot.send_message(ref_user_id, f"🔓 Your {safe_html(display)} has been unlocked (2 referrals).")
                            except Exception:
                                bot.send_message(ref_user_id, f"🔓 Your {safe_html(exam_id)} has been unlocked (2 referrals).")
                        else:
                            try:
                                display = format_exam_display(exam_id)
                                bot.send_message(ref_user_id, f"🔓 Your {safe_html(display)} has been unlocked because you invited 2 users.")
                            except Exception:
                                pass
                except Exception:
                    pass

    register_user(user_id, message.from_user.username)

    if not check_membership(user_id):
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Join Channel", url=PUBLIC_CHANNEL_LINK))
        markup.add(InlineKeyboardButton("Check Membership", callback_data="check_membership"))
        bot.send_message(user_id, "You must join our channel to use this bot.", reply_markup=markup)
        return

    show_main_menu(user_id)

@bot.message_handler(commands=['cancel'])
def cancel_operation(message):
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    
    # Only cancel if they are in the premium flow
    if state.get("menu") in ["awaiting_payment", "premium_exit_select", "awaiting_screenshot"]:
        user_states[user_id]["menu"] = "main"
        bot.send_message(user_id, "❌ Premium subscription process cancelled.")
        show_main_menu(user_id)
    else:
        bot.send_message(user_id, "There is no active process to cancel right now.")


@bot.callback_query_handler(func=lambda call: call.data == "check_membership")
def verify_membership_callback(call):
    if check_membership(call.from_user.id):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_main_menu(call.from_user.id)
    else:
        bot.answer_callback_query(call.id, "You haven't joined the channel yet!", show_alert=True)

def show_main_menu(user_id):
    user_states[user_id] = {"menu": "main"}
    markup = build_reply_keyboard(["Entrance Exam", "Exit Exam", "Score"], cols=2)
    bot.send_message(user_id, "Welcome! Please select a category:", reply_markup=markup)

@bot.message_handler(func=lambda msg: msg.text in ["Entrance Exam", "Exit Exam", "Home", "Back"])
def navigation_handler(message):
    user_id = message.from_user.id
    text = message.text
    
    if user_id in active_sessions and text in ["Home", "Back"]:
        markup = build_inline_keyboard([("Yes, Exit", f"confirm_{text.lower()}"), ("No, Cancel", "cancel_nav")], cols=2)
        bot.send_message(user_id, f"Are you sure you want to go {text}? Your current exam session will be closed.", reply_markup=markup)
        return

    mapped_action = text
    if text == "Entrance Exam":
        mapped_action = "Entrance"
    elif text == "Exit Exam":
        mapped_action = "Exit"

    handle_navigation_action(user_id, mapped_action)

@bot.message_handler(func=lambda msg: msg.text == "Score")
def show_cumulative_score(message):
    user_id = message.from_user.id
    try:
        doc = db.collection('users').document(str(user_id)).get()
        data = doc.to_dict() if doc.exists else {}
        total_attempts = data.get('total_attempts', 0)
        total_correct = data.get('total_correct', 0)
    except Exception:
        total_attempts = 0
        total_correct = 0

    bot.send_message(user_id, f"📊 <b>Your Cumulative Score</b>\n\nTotal Attempts: {total_attempts}\nTotal Correct: {total_correct}")

def handle_navigation_action(user_id, action):
    state = user_states.get(user_id, {"menu": "main"})
    
    if action == "Home":
        show_main_menu(user_id)
        return
        
    if action == "Entrance":
        user_states[user_id] = {"menu": "entrance_subjects"}
        subjects = list(CACHE['entrance_subjects'].keys())
        markup = build_reply_keyboard(subjects, cols=2, add_nav=True)
        bot.send_message(user_id, "Select a Subject:", reply_markup=markup)
        
    elif action == "Exit":
        user_states[user_id] = {"menu": "exit_departments"}
        departments = list(CACHE['exit_departments'].keys())
        markup = build_reply_keyboard(departments, cols=2, add_nav=True)
        bot.send_message(user_id, "Select a Department:", reply_markup=markup)
        
    elif action == "Back":
        if state.get("menu") in ["entrance_subjects", "exit_departments"]:
            show_main_menu(user_id)
        elif state.get("menu") == "exam_selection":
            category = state.get("category", "Entrance")
            handle_navigation_action(user_id, category)

@bot.callback_query_handler(func=lambda call: call.data in ["confirm_home", "confirm_back", "cancel_nav"])
def nav_confirmation(call):
    user_id = call.from_user.id
    bot.delete_message(call.message.chat.id, call.message.message_id)
    
    if call.data == "cancel_nav":
        return
        
    if user_id in active_sessions:
        try:
            sess = active_sessions[user_id]
            correct = sess.get('correct', 0)
            attempts = sess.get('current_index', 0)
            bot.send_message(user_id, f"🔔 <b>Temporary Score</b>\n\nCorrect: {correct}\nAttempts: {attempts}")
        except Exception:
            pass

        save_session_progress(user_id)
        del active_sessions[user_id]
        
    if call.data == "confirm_home":
        handle_navigation_action(user_id, "Home")
    elif call.data == "confirm_back":
        handle_navigation_action(user_id, "Back")

@bot.message_handler(func=lambda msg: msg.text in CACHE['entrance_subjects'] or msg.text in CACHE['exit_departments'])
def item_selection_handler(message):
    user_id = message.from_user.id
    text = message.text
    state = user_states.get(user_id, {})
    menu = state.get('menu')

    if menu == 'entrance_subjects':
        category = 'Entrance'
    elif menu == 'exit_departments':
        category = 'Exit'
    else:
        category = "Entrance" if text in CACHE['entrance_subjects'] else "Exit"

    if category == 'Entrance':
        item_code = CACHE['entrance_subjects'].get(text)
    else:
        item_code = CACHE['exit_departments'].get(text)
        
    if not item_code:
        item_code = CACHE['entrance_subjects'].get(text) or CACHE['exit_departments'].get(text)
    
    user_states[user_id] = {"menu": "exam_selection", "category": category, "item_code": item_code, "item_name": text}
    
    cache_key = f"{category}_{item_code}"
    if cache_key not in CACHE['exam_lists']:
        try:
            if category == "Entrance":
                exams_ref = db.collection('exam').document('entrance').collection('subjects').document(item_code).collection('exams').stream()
            else:
                exams_ref = db.collection('exam').document('exit').collection('departments').document(item_code).collection('exams').stream()
            
            exam_types = []
            for doc in exams_ref:
                data = doc.to_dict()
                exam_types.append(data.get('typeName', doc.id))
            CACHE['exam_lists'][cache_key] = exam_types
        except Exception:
            CACHE['exam_lists'][cache_key] = []
            
    exams = CACHE['exam_lists'][cache_key]
    if not exams:
        bot.send_message(user_id, "No exams available for this selection yet.")
        return
        
    markup = build_reply_keyboard(exams, cols=2, add_nav=True)
    bot.send_message(user_id, f"Select an exam type for {text}:", reply_markup=markup)

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get("menu") == "exam_selection" and not msg.text.startswith('/'))
def start_exam(message):
    user_id = message.from_user.id
    exam_type = message.text
    state = user_states[user_id]
    
    category = state.get("category")
    item_code = state.get("item_code")
    cache_key = f"{category}_{item_code}"
    
    if exam_type in ["Home", "Back"]:
        navigation_handler(message)
        return
        
    if exam_type not in CACHE['exam_lists'].get(cache_key, []):
        bot.send_message(user_id, "Invalid exam type. Please select from the keyboard.")
        return

    exam_doc_id = f"{cache_key}_{exam_type}"
    if exam_doc_id not in CACHE['exams']:
        bot.send_message(user_id, "Loading exam...")
        try:
            if category == "Entrance":
                query = db.collection('exam').document('entrance').collection('subjects').document(item_code).collection('exams').where('typeName', '==', exam_type).limit(1).stream()
            else:
                query = db.collection('exam').document('exit').collection('departments').document(item_code).collection('exams').where('typeName', '==', exam_type).limit(1).stream()
            
            questions = []
            for doc in query:
                questions = doc.to_dict().get('questions', [])
                break
            
            if not questions:
                bot.send_message(user_id, "No questions found for this exam.")
                return
            CACHE['exams'][exam_doc_id] = questions
        except Exception as e:
            bot.send_message(user_id, "Error loading exam.")
            return

    active_sessions[user_id] = {
        "exam_id": exam_doc_id,
        "title": f"{state.get('item_name')} - {exam_type}",
        "questions": CACHE['exams'][exam_doc_id],
        "current_index": 0,
        "correct": 0,
        "last_activity": datetime.now(),
        "locked": False,
        "referrals": 0
    }
    
    try:
        nav_only = build_reply_keyboard([], cols=2, add_nav=True)
        bot.send_message(user_id, "Starting exam... Good luck!", reply_markup=nav_only)
    except Exception:
        pass


    user_states[user_id]["menu"] = "active_exam"
    send_question(user_id)

# ==========================================
# 8. NEW FEATURE: PREMIUM WORKFLOW
# ==========================================

@bot.message_handler(commands=['premium'])
def premium_start(message):
    """Entry point for users to purchase premium"""
    user_id = message.from_user.id
    markup = build_inline_keyboard([
        ("Entrance - 150 ETB ", "premcat_entrance"),
        ("Exit - 150 ETB ", "premcat_exit")
    ], cols=1)
    bot.send_message(user_id, "🌟 <b>Upgrade to Premium</b>\n\nChoose a category to upgrade:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "trigger_premium")
def trigger_premium_callback(call):
    """Triggered from the exam lock screen"""
    bot.delete_message(call.message.chat.id, call.message.message_id)
    premium_start(call.message)

@bot.callback_query_handler(func=lambda call: call.data.startswith("premcat_"))
def premium_category_select(call):
    user_id = call.from_user.id
    cat = call.data.split("_")[1]

    if cat == "entrance":
        user_states[user_id] = {"menu": "awaiting_payment", "premium_type": "entrance", "target_code": "all"}
        send_payment_info(user_id, "Entrance (All Subjects)")
    else:
        user_states[user_id] = {"menu": "premium_exit_select"}
        departments = [(name, f"premdept_{code}") for name, code in CACHE['exit_departments'].items()]
        
        # --- NEW: Add a Back button pointing to the previous menu ---
        departments.append(("🔙 Back", "trigger_premium")) 
        
        markup = build_inline_keyboard(departments, cols=2)
        bot.edit_message_text("Select the Exit Exam department you want to unlock:", user_id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("premdept_"))
def premium_dept_select(call):
    user_id = call.from_user.id
    code = call.data.split("_")[1]
    dept_name = next((name for name, c in CACHE['exit_departments'].items() if c == code), code)

    user_states[user_id] = {"menu": "awaiting_payment", "premium_type": "exit", "target_code": code}
    send_payment_info(user_id, f"Exit Exam ({dept_name})")

def send_payment_info(user_id, target_name):
    text = (f"💳 <b>Payment Details for {target_name}</b>\n\n"
            "Amount: <b>150 ETB</b>\n"
            "CBE Account: 1000649561382 (Jemal Hussen Hassen)\n"
            "Telebirr: 0906365418\n\n"
            "Once you have made the transfer, click the button below to upload your screenshot.\n"
            "<i>(Or send /cancel to abort)</i>") # Friendly reminder of the command
            
    # --- NEW: Add a Cancel button ---
    markup = build_inline_keyboard([
        ("📤 Upload Screenshot", "upload_screenshot"),
        ("❌ Cancel", "cancel_premium")
    ], cols=1)
    
    bot.send_message(user_id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "cancel_premium")
def cancel_premium_callback(call):
    user_id = call.from_user.id
    # Safely reset state
    user_states[user_id] = {"menu": "main"}
    bot.edit_message_text("❌ Premium subscription cancelled.", user_id, call.message.message_id)
    show_main_menu(user_id)

@bot.callback_query_handler(func=lambda call: call.data == "upload_screenshot")
def upload_screenshot_prompt(call):
    user_id = call.from_user.id
    state = user_states.get(user_id, {})
    if state.get("menu") != "awaiting_payment":
        bot.answer_callback_query(call.id, "Please select a premium package first using /premium", show_alert=True)
        return

    state["menu"] = "awaiting_screenshot"
    bot.edit_message_text("📸 Please send the screenshot image of your payment now.", user_id, call.message.message_id)

@bot.message_handler(content_types=['photo'], func=lambda msg: user_states.get(msg.from_user.id, {}).get("menu") == "awaiting_screenshot")
def receive_screenshot(message):
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    ptype = state.get("premium_type")
    tcode = state.get("target_code")

    bot.send_message(user_id, "✅ Screenshot received! We will verify your payment and notify you shortly. Please be patient.")

    admin_text = f"💰 <b>New Premium Payment Request</b>\n\nUser ID: <code>{user_id}</code>\nUsername: @{message.from_user.username}\nType: {ptype.upper()}\nTarget Code: {tcode}"

    # Strict 64-byte limit compliance for callbacks
    approve_data = f"approve_{user_id}_{ptype}_{tcode}"
    reject_data = f"reject_{user_id}"

    markup = build_inline_keyboard([("✅ Approve", approve_data), ("❌ Reject", reject_data)], cols=2)
    bot.send_photo(ADMIN_TELEGRAM_ID, message.photo[-1].file_id, caption=admin_text, reply_markup=markup)
    
    # Reset State safely
    user_states[user_id]["menu"] = "main"

@bot.callback_query_handler(func=lambda call: call.data.startswith("approve_") or call.data.startswith("reject_"))
def admin_payment_action(call):
    if call.from_user.id != ADMIN_TELEGRAM_ID:
        return

    parts = call.data.split("_")
    action = parts[0]
    target_user_id = int(parts[1])

    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)

    if action == "reject":
        bot.send_message(call.message.chat.id, f"Rejected payment for user {target_user_id}.")
        try:
            bot.send_message(target_user_id, "❌ Your premium payment was rejected. Please contact the admin if you think this is a mistake.")
        except Exception:
            pass
        return

    # Process Approval
    ptype = parts[2]
    tcode = parts[3]

    try:
        user_ref = db.collection('users').document(str(target_user_id))
        if ptype == "entrance":
            user_ref.set({"premium_entrance": True}, merge=True)
            target_name = "Entrance Exams (All Subjects)"
        else:
            user_ref.set({"premium_exit": firestore.ArrayUnion([tcode])}, merge=True)
            target_name = next((name for name, c in CACHE.get('exit_departments', {}).items() if c == tcode), tcode)
            target_name = f"Exit Exam ({target_name})"

        bot.send_message(call.message.chat.id, f"Approved {ptype} premium for user {target_user_id}.")

        try:
            bot.send_message(target_user_id, f"🎉 <b>Payment Approved!</b>\n\nYour premium access for <b>{target_name}</b> has been activated. Enjoy your unlimited access!")

            # Auto-unlock their active session if it matches their purchase
            if target_user_id in active_sessions:
                sess = active_sessions[target_user_id]
                if sess.get('locked'):
                    exam_id = sess.get('exam_id', '')
                    sess_cat = exam_id.split("_")[0].lower() if "_" in exam_id else ""
                    sess_code = exam_id.split("_")[1] if "_" in exam_id else ""

                    if (ptype == "entrance" and sess_cat == "entrance") or (ptype == "exit" and sess_cat == "exit" and sess_code == tcode):
                        sess['locked'] = False
                        bot.send_message(target_user_id, "🔓 Your active exam session has been unlocked! You can continue answering questions.")

        except Exception as e:
            print(f"Could not notify user: {e}")

    except Exception as e:
        bot.send_message(call.message.chat.id, f"Error updating database: {e}")


# ==========================================
# 9. QUIZ INTERFACE & LOGIC
# ==========================================
def send_question(user_id, edit_msg_id=None):
    session = active_sessions.get(user_id)
    if not session:
        return

    if session['current_index'] >= 25 and not session['locked']:
        exam_id = session.get('exam_id')
        try:
            user_doc = db.collection('users').document(str(user_id)).get()
            user_data = user_doc.to_dict() if user_doc.exists else {}
            unlocked = user_data.get('unlocked_exams', []) if user_data else []

            # --- NEW: Check Database for Premium Access ---
            is_premium = False
            if "_" in exam_id:
                category = exam_id.split("_")[0].lower() 
                item_code = exam_id.split("_")[1]

                if category == "entrance" and user_data.get("premium_entrance") == True:
                    is_premium = True
                elif category == "exit" and item_code in user_data.get("premium_exit", []):
                    is_premium = True
            # --------------------------------------------

        except Exception:
            unlocked = []
            is_premium = False

        if exam_id in unlocked or is_premium:
            session['locked'] = False
        else:
            session['locked'] = True

    if session['locked']:
        if session['referrals'] >= 2:
            session['locked'] = False
        else:
            bot_username = BOT_USERNAME
            ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}_{session['exam_id']}"
            share_text = (
                "ለ Entrance እና Exit Exam ዝግጅት የሚሆን ምርጥ Bot አግኝቻለሁ!\n\n"
                "ይህ Bot የ2015፣ 2016፣ 2017 እና 2018 ያለፉ ፈተናዎችን እንዲሁም ከ50,000 በላይ ተጨማሪ ሞዴል ጥያቄዎችን ከነሙሉ ማብራሪያቸው አጠቃልሎ የያዘ ነው።\n\n"
                f"{ref_link}"
            )
            share_url = f"https://t.me/share/url?text={quote_plus(share_text)}"

            text = (
            f"🔒 <b>Exam Locked!</b>\n\n"
            f"You've completed the free 25 questions.\n\n"
            f"👉 Invite 2 users to unlock more questions for free.\n"
            f"👉 Or send /premium for instant access to all exam categories.\n\n"
            f"Share the bot with your friends or upgrade to Premium to continue."
            )
          
            markup = InlineKeyboardMarkup()
            try:
                markup.add(InlineKeyboardButton("Share", url=share_url))
                markup.add(InlineKeyboardButton("Check Status", callback_data="check_referral"))
            except Exception:
                markup = build_inline_keyboard([("Check Status", "check_referral")], cols=1)

            if edit_msg_id:
                bot.edit_message_text(text, user_id, edit_msg_id, reply_markup=markup)
            else:
                bot.send_message(user_id, text, reply_markup=markup)

            try:
                temp_correct = session.get('correct', 0)
                temp_attempts = session.get('current_index', 0)
                bot.send_message(user_id, f"🔔 <b>Current Score</b>\n\nCorrect: {temp_correct}\nAttempts: {temp_attempts}")
            except Exception:
                pass

            return
        
    if session['current_index'] >= len(session['questions']):
        end_exam(user_id, edit_msg_id)
        return

    if session['current_index'] > 0 and session['current_index'] % 5 == 0 and not session.get(f"ad_shown_{session['current_index']}"):
        session[f"ad_shown_{session['current_index']}"] = True
        last_q_msg_id = edit_msg_id or session.get('last_msg_id')
        show_advertisement(user_id, last_question_msg_id=last_q_msg_id)
        return

    q_data = session['questions'][session['current_index']]
    total_q = len(session['questions'])
    text = (f"<b>{safe_html(session['title'])}</b>\n\n"
            f"Question {session['current_index'] + 1} / {total_q}\n\n"
            f"{safe_html(q_data.get('question_text',''))}\n\n"
            f"A. {safe_html(q_data.get('options',{}).get('a',''))}\n"
            f"B. {safe_html(q_data.get('options',{}).get('b',''))}\n"
            f"C. {safe_html(q_data.get('options',{}).get('c',''))}\n"
            f"D. {safe_html(q_data.get('options',{}).get('d',''))}")
            
    markup = build_inline_keyboard([
        ("A", "ans_a"), ("B", "ans_b"),
        ("C", "ans_c"), ("D", "ans_d")
    ], cols=2)
    
    try:
        if edit_msg_id:
            bot.edit_message_text(text, user_id, edit_msg_id, reply_markup=markup)
            session['last_msg_id'] = edit_msg_id
        else:
            msg = bot.send_message(user_id, text, reply_markup=markup)
            session['last_msg_id'] = msg.message_id
    except Exception:
        msg = bot.send_message(user_id, text, reply_markup=markup)
        session['last_msg_id'] = getattr(msg, 'message_id', None)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ans_"))
def handle_answer(call):
    user_id = call.from_user.id
    update_activity(user_id)
    session = active_sessions.get(user_id)
    if not session:
        bot.answer_callback_query(call.id, "Session expired.")
        return
        
    user_ans = call.data.split('_')[1] 
    q_data = session['questions'][session['current_index']]
    correct_ans = q_data['answer'].lower()
    
    is_correct = user_ans == correct_ans
    if is_correct:
        session['correct'] += 1
        result_icon = "✓ Correct"
    else:
        result_icon = "✗ Incorrect"
        
    total_q = len(session['questions'])
    text = (f"<b>{safe_html(session['title'])}</b>\n\n"
            f"Question {session['current_index'] + 1} / {total_q}\n\n"
            f"{safe_html(q_data.get('question_text',''))}\n\n"
            f"A. {safe_html(q_data.get('options',{}).get('a',''))}\n"
            f"B. {safe_html(q_data.get('options',{}).get('b',''))}\n"
            f"C. {safe_html(q_data.get('options',{}).get('c',''))}\n"
            f"D. {safe_html(q_data.get('options',{}).get('d',''))}\n\n"
            f"<b>{result_icon}</b>\n"
            f"Correct Answer: {safe_html(correct_ans.upper())}\n\n"
            f"Explanation:\n{safe_html(q_data.get('explanation', 'No explanation provided.'))}")
            
    markup = build_inline_keyboard([("Next", "next_question")], cols=1)
    
    bot.edit_message_text(text, user_id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "next_question")
def next_question_callback(call):
    user_id = call.from_user.id
    update_activity(user_id)
    if user_id in active_sessions:
        active_sessions[user_id]['current_index'] += 1
        send_question(user_id, call.message.message_id)
    else:
        bot.answer_callback_query(call.id, "Session expired.")

@bot.callback_query_handler(func=lambda call: call.data == "check_referral")
def check_referral_callback(call):
    user_id = call.from_user.id
    session = active_sessions.get(user_id)
    
    # ==========================================
    # NEW: Smart Fallback for Expired Sessions
    # ==========================================
    if not session:
        try:
            # Clean up the old, dead locked message so the user doesn't get stuck clicking it
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
            
        # Check their global premium status
        try:
            user_doc = db.collection('users').document(str(user_id)).get()
            user_data = user_doc.to_dict() if user_doc.exists else {}
            is_premium = user_data.get("premium_entrance") == True or len(user_data.get("premium_exit", [])) > 0
            
            if is_premium:
                bot.answer_callback_query(call.id, "🌟 Your Premium is ACTIVE! Please start your exam.", show_alert=True)
            else:
                bot.answer_callback_query(call.id, "⏳ This exam session expired due to inactivity. Please start a new one.", show_alert=True)
        except Exception:
            bot.answer_callback_query(call.id, "Session expired. Please start a new exam.", show_alert=True)
        
        # Bring them back to the main menu instantly
        show_main_menu(user_id)
        return
    # ==========================================

    exam_id = session.get('exam_id')
    is_premium = False 
    
    try:
        inviter_doc = db.collection('users').document(str(user_id)).get()
        inviter_data = inviter_doc.to_dict() if inviter_doc.exists else {}
        referrals_map = inviter_data.get('referrals_map', {}) if inviter_data else {}
        count_for_exam = referrals_map.get(exam_id, 0)
        unlocked = inviter_data.get('unlocked_exams', []) if inviter_data else []
        
        # Check Database for Premium Access 
        if "_" in exam_id:
            category = exam_id.split("_")[0].lower() 
            item_code = exam_id.split("_")[1]

            if category == "entrance" and inviter_data.get("premium_entrance") == True:
                is_premium = True
            elif category == "exit" and item_code in inviter_data.get("premium_exit", []):
                is_premium = True
        
    except Exception:
        count_for_exam = session.get('referrals', 0)
        unlocked = []
        
    # Check if they have enough referrals, previously unlocked it, OR have premium
    if count_for_exam >= 2 or exam_id in unlocked or is_premium:
        
        if count_for_exam >= 2 and exam_id not in unlocked:
            try:
                db.collection('users').document(str(user_id)).update({
                    'unlocked_exams': firestore.ArrayUnion([exam_id])
                })
            except Exception:
                pass

        session['locked'] = False
        bot.answer_callback_query(call.id, "Unlocked! Resuming exam...", show_alert=True)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        send_question(user_id)
    else:
        remaining = max(0, 2 - count_for_exam)
        bot.answer_callback_query(call.id, f"You need {remaining} more users to join, or upgrade to Premium.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == "skip_ad")
def skip_ad_callback(call):
    user_id = call.from_user.id
    session = active_sessions.get(user_id)
    if not session:
        bot.answer_callback_query(call.id, "Session expired.", show_alert=True)
        return

    ad_ctx = session.pop('ad_context', {}) if session else {}
    try:
        if ad_ctx.get('countdown_msg_id'):
            try:
                bot.delete_message(user_id, ad_ctx['countdown_msg_id'])
            except Exception:
                pass
        if ad_ctx.get('ad_copy_msg_id'):
            try:
                bot.delete_message(user_id, ad_ctx['ad_copy_msg_id'])
            except Exception:
                pass
    except Exception:
        pass

    send_question(user_id)
    bot.answer_callback_query(call.id)

def show_advertisement(user_id, last_question_msg_id=None):
    if last_question_msg_id:
        try:
            bot.delete_message(user_id, last_question_msg_id)
        except Exception:
            pass

    ad = CACHE.get('ad_data')
    ad_copy_msg_id = None
    if ad and ad.get('message_id') and ad.get('chat_id'):
        try:
            copied = bot.copy_message(user_id, ad['chat_id'], ad['message_id'])
            ad_copy_msg_id = getattr(copied, 'message_id', None)
        except Exception as e:
            print(f"Failed to copy ad message: {e}")

    try:
        countdown_text = "⏳ Advertisement — resuming in 5s"
        countdown_msg = bot.send_message(user_id, countdown_text)
        countdown_msg_id = getattr(countdown_msg, 'message_id', None)
    except Exception as e:
        print(f"Failed to send countdown message: {e}")
        return

    session = active_sessions.get(user_id)
    if session is not None:
        session['ad_context'] = {
            'ad_copy_msg_id': ad_copy_msg_id,
            'countdown_msg_id': countdown_msg_id
        }
    
    def run_countdown(chat_id, message_id):
        time.sleep(5)
        try:
            final_text = "⏳ Advertisement — you can skip it now"
            markup = build_inline_keyboard(
                [("Skip", "skip_ad")],
                cols=1
            )
            bot.edit_message_text(
                final_text,
                chat_id,
                message_id,
                reply_markup=markup
            )
        except Exception as e:
            print(e)

    threading.Thread(target=run_countdown, args=(user_id, countdown_msg_id), daemon=True).start()

def save_session_progress(user_id):
    session = active_sessions.get(user_id)
    if not session:
        return
    try:
        score = session['correct']
        attempts = session['current_index']
        if attempts > 0:
            doc_ref = db.collection('users').document(str(user_id))
            doc_ref.set({
                "total_correct": firestore.Increment(score),
                "total_attempts": firestore.Increment(attempts),
                "completed_exams": firestore.ArrayUnion([session['exam_id']]),
            }, merge=True)
    except Exception as e:
        print(f"Error saving progress: {e}")

def end_exam(user_id, msg_id):
    session = active_sessions.get(user_id)
    score = session['correct']
    total = len(session['questions'])
    bot.edit_message_text(f"🏁 <b>Exam Completed!</b>\n\nYour Score: {score} / {total}", user_id, msg_id)
    save_session_progress(user_id)
    del active_sessions[user_id]
    handle_navigation_action(user_id, "Home")

# ==========================================
# 10. ADMIN PANEL & COMMANDS
# ==========================================
@bot.message_handler(commands=['ethioegzam'])
def admin_panel(message):
    if message.from_user.id != ADMIN_TELEGRAM_ID:
        return
        
    markup = build_inline_keyboard([
        ("Add Field", "admin_add_field"),
        ("Add Quiz", "admin_add_quiz"),
        ("Add Ad", "admin_add_ad"),
        ("Total User", "admin_total_user"),
        ("Broadcast", "admin_broadcast"),
        ("Clear Cache", "admin_clear_cache"),
        ("Maintenance", "admin_maintenance")
    ], cols=2)
    
    bot.send_message(message.from_user.id, "🛠 <b>Welcome! Admin Panel</b>", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_"))
def admin_callbacks(call):
    if call.from_user.id != ADMIN_TELEGRAM_ID:
        return
        
    action = call.data
    bot.answer_callback_query(call.id)
    
    if action == "admin_add_field":
        msg = bot.send_message(call.from_user.id, "Upload the list of fields in JSON format.\nOr send /cancel to cancel.")
        bot.register_next_step_handler(msg, process_add_field)
        
    elif action == "admin_add_quiz":
        markup = build_inline_keyboard([("Entrance", "quizcat_entrance"), ("Exit", "quizcat_exit")], cols=2)
        bot.send_message(call.from_user.id, "Select exam category:", reply_markup=markup)
        
    elif action == "admin_add_ad":
        msg = bot.send_message(call.from_user.id, "Send a photo OR video with a caption for the advertisement.")
        bot.register_next_step_handler(msg, process_add_ad)
        
    elif action == "admin_total_user":
        bot.send_message(call.from_user.id, f"Total registered users: {CACHE['total_users']}")
        
    elif action == "admin_broadcast":
        msg = bot.send_message(call.from_user.id, "Send text message OR photo/video with caption for broadcast.")
        bot.register_next_step_handler(msg, process_broadcast)
        
    elif action == "admin_clear_cache":
        markup = build_inline_keyboard([("Yes", "cache_yes"), ("Cancel", "cache_cancel")], cols=2)
        bot.send_message(call.from_user.id, "Are you sure you want to clear the global cache?", reply_markup=markup)
        
    elif action == "admin_maintenance":
        global MAINTENANCE_MODE
        status = "Deactivate" if MAINTENANCE_MODE else "Activate"
        markup = build_inline_keyboard([(status, "toggle_maintenance")], cols=1)
        bot.send_message(call.from_user.id, f"Maintenance Mode is currently {'ON' if MAINTENANCE_MODE else 'OFF'}", reply_markup=markup)

def process_add_field(message):
    if message.text == "/cancel":
        bot.send_message(message.from_user.id, "Operation cancelled.")
        return
        
    try:
        if message.document:
            file_info = bot.get_file(message.document.file_id)
            downloaded_file = bot.download_file(file_info.file_path)
            data = json.loads(downloaded_file)
        else:
            data = json.loads(message.text)
            
        if 'entrance' in data and 'subjects' in data['entrance']:
            for sub in data['entrance']['subjects']:
                code = sub.get('code', sub['name'].lower().replace(' ', '_'))
                db.collection('exam').document('entrance').collection('subjects').document(code).set(sub)
                
        if 'exit' in data and 'departments' in data['exit']:
            for dept in data['exit']['departments']:
                code = dept.get('code', dept['name'].lower().replace(' ', '_'))
                db.collection('exam').document('exit').collection('departments').document(code).set(dept)
                
        bot.send_message(message.from_user.id, "Fields successfully added.")
        load_cache() 
    except Exception as e:
        bot.send_message(message.from_user.id, f"Error processing JSON: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("quizcat_"))
def admin_quiz_category(call):
    cat = call.data.split("_")[1]
    items = []
    if cat == "entrance":
        items = [(name, f"quizsub_entrance_{code}") for name, code in CACHE['entrance_subjects'].items()]
    else:
        items = [(name, f"quizsub_exit_{code}") for name, code in CACHE['exit_departments'].items()]
        
    markup = build_inline_keyboard(items, cols=3)
    bot.edit_message_text("Select Subject/Department:", call.from_user.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("quizsub_"))
def admin_quiz_subject(call):
    parts = call.data.split("_")
    cat = parts[1]
    code = parts[2]
    
    msg = bot.send_message(call.from_user.id, "Enter the exam type name (e.g., 2015, model01):")
    bot.register_next_step_handler(msg, process_quiz_type, cat, code)

def process_quiz_type(message, cat, code):
    type_name = message.text
    msg = bot.send_message(message.from_user.id, "Upload the quiz questions JSON file.")
    bot.register_next_step_handler(msg, process_quiz_upload, cat, code, type_name)

def process_quiz_upload(message, cat, code, type_name):
    try:
        if not message.document:
            bot.send_message(message.from_user.id, "Please upload a valid JSON file document.")
            return
            
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        questions = json.loads(downloaded_file)
        
        doc_data = {"typeName": type_name, "questions": questions}
        
        if cat == "entrance":
            db.collection('exam').document('entrance').collection('subjects').document(code).collection('exams').add(doc_data)
        else:
            db.collection('exam').document('exit').collection('departments').document(code).collection('exams').add(doc_data)
            
        bot.send_message(message.from_user.id, "Quiz successfully uploaded and saved.")
        cache_key = f"{cat.capitalize()}_{code}"
        if cache_key in CACHE['exam_lists']:
            del CACHE['exam_lists'][cache_key]
    except Exception as e:
        bot.send_message(message.from_user.id, f"Error saving quiz: {e}")

def process_add_ad(message):
    try:
        chat_id = message.chat.id
        msg_id = message.message_id
        ad_data = {"chat_id": chat_id, "message_id": msg_id}
        
        db.collection('settings').document('advertisement').set(ad_data)
        CACHE['ad_data'] = ad_data
        
        bot.send_message(message.from_user.id, "Advertisement saved successfully.")
    except Exception as e:
        bot.send_message(message.from_user.id, f"Error saving Ad: {e}")

@bot.callback_query_handler(func=lambda call: call.data in ["cache_yes", "cache_cancel", "toggle_maintenance"])
def admin_misc_callbacks(call):
    if call.data == "cache_cancel":
        bot.delete_message(call.message.chat.id, call.message.message_id)
        return
        
    if call.data == "cache_yes":
        CACHE['entrance_subjects'].clear()
        CACHE['exit_departments'].clear()
        CACHE['exams'].clear()
        CACHE['exam_lists'].clear()
        load_cache()
        bot.edit_message_text("Cache cleared successfully.", call.from_user.id, call.message.message_id)
        
    elif call.data == "toggle_maintenance":
        global MAINTENANCE_MODE
        MAINTENANCE_MODE = not MAINTENANCE_MODE
        status = "ON" if MAINTENANCE_MODE else "OFF"
        bot.edit_message_text(f"Maintenance mode is now {status}.", call.from_user.id, call.message.message_id)

def process_broadcast(message):
    bot.send_message(message.from_user.id, "Starting broadcast...")
    success = 0
    try:
        users = list(db.collection('users').stream())
        for user_doc in users:
            uid = int(user_doc.id)
            if uid == ADMIN_TELEGRAM_ID:
                continue
            
            if uid in active_sessions:
                if uid not in queued_broadcasts:
                    queued_broadcasts[uid] = []
                queued_broadcasts[uid].append({"chat_id": message.chat.id, "message_id": message.message_id})
            else:
                try:
                    bot.copy_message(uid, message.chat.id, message.message_id)
                    success += 1
                except Exception:
                    pass
                    
        bot.send_message(message.from_user.id, f"Broadcast sent to {success} users immediately. Others queued.")
    except Exception as e:
        bot.send_message(message.from_user.id, f"Broadcast error: {e}")

# ==========================================
# 11. BACKGROUND TASKS (SESSION EXPIRY & QUEUE)
# ==========================================
def background_worker():
    while True:
        now = datetime.now()
        expired_users = []
        
        for uid, session in list(active_sessions.items()):
            if now - session['last_activity'] > timedelta(hours=4):
                expired_users.append(uid)
                
        for uid in expired_users:
            try:
                save_session_progress(uid)
                del active_sessions[uid]
                bot.send_message(uid, "Your exam session has expired due to 4 hour of inactivity.")
                
                if uid in queued_broadcasts:
                    for msg in queued_broadcasts[uid]:
                        bot.copy_message(uid, msg['chat_id'], msg['message_id'])
                    del queued_broadcasts[uid]
            except Exception:
                pass
                
        time.sleep(60) 

# ==========================================
# 12. ENTRY POINT
# ==========================================
if __name__ == '__main__':
    load_cache()
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=background_worker, daemon=True).start()
    
    print("Bot is polling...")
    bot.infinity_polling()