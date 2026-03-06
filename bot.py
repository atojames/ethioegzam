import os
import json
import time
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask
import telebot
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
user_states = {}       # {user_id: {"menu": "main", "category": "entrance", "subject": "Biology"}}
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
    register_user(user_id, message.from_user.username)
    
    # Handle referral links
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        _, ref_user_id, exam_id = args[1].split("_")
        ref_user_id = int(ref_user_id)
        if ref_user_id != user_id:
            # Increment referral count for referrer
            db.collection('users').document(str(ref_user_id)).update({"referrals": firestore.Increment(1)})
            # If referrer is in active session and locked, update their local state
            if ref_user_id in active_sessions and active_sessions[ref_user_id].get('locked'):
                active_sessions[ref_user_id]['referrals'] += 1
                bot.send_message(ref_user_id, "🎉 Someone joined using your link!")

    if not check_membership(user_id):
        markup = build_inline_keyboard([
            ("Join Channel", PUBLIC_CHANNEL_LINK),
            ("Check Membership", "check_membership")
        ], cols=1)
        # Inline buttons that contain URLs don't have callbacks. Correct format:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Join Channel", url=PUBLIC_CHANNEL_LINK))
        markup.add(InlineKeyboardButton("Check Membership", callback_data="check_membership"))
        bot.send_message(user_id, "You must join our channel to use this bot.", reply_markup=markup)
        return

    show_main_menu(user_id)

@bot.callback_query_handler(func=lambda call: call.data == "check_membership")
def verify_membership_callback(call):
    if check_membership(call.from_user.id):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_main_menu(call.from_user.id)
    else:
        bot.answer_callback_query(call.id, "You haven't joined the channel yet!", show_alert=True)

def show_main_menu(user_id):
    user_states[user_id] = {"menu": "main"}
    markup = build_reply_keyboard(["Entrance", "Exit"], cols=2)
    bot.send_message(user_id, "Welcome! Please select a category:", reply_markup=markup)

@bot.message_handler(func=lambda msg: msg.text in ["Entrance", "Exit", "Home", "Back"])
def navigation_handler(message):
    user_id = message.from_user.id
    text = message.text
    
    # Check if user is in an active session to prompt confirmation
    if user_id in active_sessions and text in ["Home", "Back"]:
        markup = build_inline_keyboard([("Yes", f"confirm_{text.lower()}"), ("Cancel", "cancel_nav")], cols=2)
        bot.send_message(user_id, f"Are you sure you want to go {text}? Your current exam session will be closed.", reply_markup=markup)
        return

    handle_navigation_action(user_id, text)

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
        if state["menu"] in ["entrance_subjects", "exit_departments"]:
            show_main_menu(user_id)
        elif state["menu"] == "exam_selection":
            category = state.get("category", "Entrance")
            handle_navigation_action(user_id, category)

@bot.callback_query_handler(func=lambda call: call.data in ["confirm_home", "confirm_back", "cancel_nav"])
def nav_confirmation(call):
    user_id = call.from_user.id
    bot.delete_message(call.message.chat.id, call.message.message_id)
    
    if call.data == "cancel_nav":
        return
        
    # User confirmed, delete active session
    if user_id in active_sessions:
        save_session_progress(user_id)
        del active_sessions[user_id]
        
    if call.data == "confirm_home":
        handle_navigation_action(user_id, "Home")
    elif call.data == "confirm_back":
        handle_navigation_action(user_id, "Back")

# Catch subject/department selections
@bot.message_handler(func=lambda msg: msg.text in CACHE['entrance_subjects'] or msg.text in CACHE['exit_departments'])
def item_selection_handler(message):
    user_id = message.from_user.id
    text = message.text
    
    category = "Entrance" if text in CACHE['entrance_subjects'] else "Exit"
    item_code = CACHE['entrance_subjects'].get(text) or CACHE['exit_departments'].get(text)
    
    user_states[user_id] = {"menu": "exam_selection", "category": category, "item_code": item_code, "item_name": text}
    
    # Check Cache for exam lists
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

# Catch Exam Type Selection
@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get("menu") == "exam_selection")
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

    # Load Exam
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
    
    send_question(user_id)

# ==========================================
# 8. QUIZ INTERFACE & LOGIC
# ==========================================
def send_question(user_id, edit_msg_id=None):
    session = active_sessions.get(user_id)
    if not session:
        return
        
    # Check Referral Lock
    if session['current_index'] >= 25 and not session['locked']:
        # Lock activated
        session['locked'] = True
    
    if session['locked']:
        if session['referrals'] >= 2:
            session['locked'] = False
        else:
            bot_username = BOT_USERNAME
            ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}_{session['exam_id']}"
            text = (f"🔒 <b>Exam Locked!</b>\n\nYou have completed 25 questions.\n"
                    f"To continue, invite 2 new users using your referral link:\n\n"
                    f"{ref_link}\n\nUsers invited so far: {session['referrals']}/2")
            markup = build_inline_keyboard([("Check Status and Continue", "check_referral")], cols=1)
            
            if edit_msg_id:
                bot.edit_message_text(text, user_id, edit_msg_id, reply_markup=markup)
            else:
                bot.send_message(user_id, text, reply_markup=markup)
            return

    # Check End of Exam
    if session['current_index'] >= len(session['questions']):
        end_exam(user_id, edit_msg_id)
        return

    # Trigger Ad
    if session['current_index'] > 0 and session['current_index'] % 5 == 0 and not session.get(f"ad_shown_{session['current_index']}"):
        session[f"ad_shown_{session['current_index']}"] = True
        show_advertisement(user_id)

    q_data = session['questions'][session['current_index']]
    total_q = len(session['questions'])
    
    text = (f"<b>{session['title']}</b>\n\n"
            f"Question {session['current_index'] + 1} / {total_q}\n\n"
            f"{q_data['question_text']}\n\n"
            f"A. {q_data['options']['a']}\n"
            f"B. {q_data['options']['b']}\n"
            f"C. {q_data['options']['c']}\n"
            f"D. {q_data['options']['d']}")
            
    markup = build_inline_keyboard([
        ("A", "ans_a"), ("B", "ans_b"),
        ("C", "ans_c"), ("D", "ans_d")
    ], cols=2)
    
    if edit_msg_id:
        bot.edit_message_text(text, user_id, edit_msg_id, reply_markup=markup)
    else:
        bot.send_message(user_id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ans_"))
def handle_answer(call):
    user_id = call.from_user.id
    update_activity(user_id)
    session = active_sessions.get(user_id)
    if not session:
        bot.answer_callback_query(call.id, "Session expired.")
        return
        
    user_ans = call.data.split('_')[1] # 'a', 'b', 'c', 'd'
    q_data = session['questions'][session['current_index']]
    correct_ans = q_data['answer'].lower()
    
    is_correct = user_ans == correct_ans
    if is_correct:
        session['correct'] += 1
        result_icon = "✓ Correct"
    else:
        result_icon = "✗ Incorrect"
        
    total_q = len(session['questions'])
    
    text = (f"<b>{session['title']}</b>\n\n"
            f"Question {session['current_index'] + 1} / {total_q}\n\n"
            f"{q_data['question_text']}\n\n"
            f"A. {q_data['options']['a']}\n"
            f"B. {q_data['options']['b']}\n"
            f"C. {q_data['options']['c']}\n"
            f"D. {q_data['options']['d']}\n\n"
            f"<b>{result_icon}</b>\n"
            f"Correct Answer: {correct_ans.upper()}\n\n"
            f"Explanation:\n{q_data.get('explanation', 'No explanation provided.')}")
            
    markup = build_inline_keyboard([("Next", "next_question")], cols=1)
    
    bot.edit_message_text(text, user_id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "next_question")
def next_question_callback(call):
    user_id = call.from_user.id
    update_activity(user_id)
    if user_id in active_sessions:
        active_sessions[user_id]['current_index'] += 1
        # Send the next question as a new message so the previous one remains visible
        send_question(user_id)
    else:
        bot.answer_callback_query(call.id, "Session expired.")

@bot.callback_query_handler(func=lambda call: call.data == "check_referral")
def check_referral_callback(call):
    user_id = call.from_user.id
    session = active_sessions.get(user_id)
    if session and session.get('locked'):
        if session['referrals'] >= 2:
            session['locked'] = False
            bot.answer_callback_query(call.id, "Unlocked! Resuming exam...", show_alert=True)
            # Resume by sending the next question as a new message
            send_question(user_id)
        else:
            bot.answer_callback_query(call.id, f"You need {2 - session['referrals']} more users to join.", show_alert=True)

def show_advertisement(user_id):
    ad = CACHE.get('ad_data')
    if ad and ad.get('message_id') and ad.get('chat_id'):
        try:
            bot.copy_message(user_id, ad['chat_id'], ad['message_id'])
        except Exception as e:
            print(f"Failed to show ad: {e}")

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
# 9. ADMIN PANEL & COMMANDS
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
    
    bot.send_message(message.from_user.id, "🛠 <b>Admin Panel</b>", reply_markup=markup)

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

# Admin: Add Field Logic
def process_add_field(message):
    if message.text == "/cancel":
        bot.send_message(message.from_user.id, "Operation cancelled.")
        return
        
    try:
        # Assuming admin uploads JSON as a file or sends raw text
        if message.document:
            file_info = bot.get_file(message.document.file_id)
            downloaded_file = bot.download_file(file_info.file_path)
            data = json.loads(downloaded_file)
        else:
            data = json.loads(message.text)
            
        # Parse Entrance
        if 'entrance' in data and 'subjects' in data['entrance']:
            for sub in data['entrance']['subjects']:
                code = sub.get('code', sub['name'].lower().replace(' ', '_'))
                db.collection('exam').document('entrance').collection('subjects').document(code).set(sub)
                
        # Parse Exit
        if 'exit' in data and 'departments' in data['exit']:
            for dept in data['exit']['departments']:
                code = dept.get('code', dept['name'].lower().replace(' ', '_'))
                db.collection('exam').document('exit').collection('departments').document(code).set(dept)
                
        bot.send_message(message.from_user.id, "Fields successfully added.")
        load_cache() # Reload
    except Exception as e:
        bot.send_message(message.from_user.id, f"Error processing JSON: {e}")

# Admin: Add Quiz Logic
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
        # Clear specific exam list cache to force reload
        cache_key = f"{cat.capitalize()}_{code}"
        if cache_key in CACHE['exam_lists']:
            del CACHE['exam_lists'][cache_key]
    except Exception as e:
        bot.send_message(message.from_user.id, f"Error saving quiz: {e}")

# Admin: Add Ad
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

# Admin: Clear Cache & Maintenance
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

# Admin: Broadcast
def process_broadcast(message):
    bot.send_message(message.from_user.id, "Starting broadcast...")
    success = 0
    try:
        users = db.collection('users').stream()
        for user_doc in users:
            uid = int(user_doc.id)
            if uid == ADMIN_TELEGRAM_ID:
                continue
            
            # If user has active session, queue it
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
# 10. BACKGROUND TASKS (SESSION EXPIRY & QUEUE)
# ==========================================
def background_worker():
    while True:
        now = datetime.now()
        expired_users = []
        
        # Check sessions
        for uid, session in list(active_sessions.items()):
            if now - session['last_activity'] > timedelta(hours=1):
                expired_users.append(uid)
                
        for uid in expired_users:
            try:
                save_session_progress(uid)
                del active_sessions[uid]
                bot.send_message(uid, "Your exam session has expired due to 1 hour of inactivity.")
                
                # Send queued broadcasts
                if uid in queued_broadcasts:
                    for msg in queued_broadcasts[uid]:
                        bot.copy_message(uid, msg['chat_id'], msg['message_id'])
                    del queued_broadcasts[uid]
            except Exception:
                pass
                
        time.sleep(60) # Check every minute

# ==========================================
# 11. ENTRY POINT
# ==========================================
if __name__ == '__main__':
    # Initial Cache Load
    load_cache()
    
    # Start Flask keep-alive
    threading.Thread(target=run_flask, daemon=True).start()
    
    # Start Background worker
    threading.Thread(target=background_worker, daemon=True).start()
    
    print("Bot is polling...")
    bot.infinity_polling()
