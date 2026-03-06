import os
import json
import time
import logging
from functools import wraps
from flask import Flask, request

import firebase_admin
from firebase_admin import credentials, firestore

from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, ParseMode
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters, CallbackQueryHandler, ConversationHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment
ADMIN_TELEGRAM_ID = int(os.environ.get("ADMIN_TELEGRAM_ID", "0"))
BOT_TOKEN = os.environ.get("BOT_TOKEN")
BOT_USERNAME = os.environ.get("BOT_USERNAME")
FIREBASE_KEY = os.environ.get("FIREBASE_KEY")
PUBLIC_CHANNEL_ID = os.environ.get("PUBLIC_CHANNEL_ID")
PUBLIC_CHANNEL_LINK = os.environ.get("PUBLIC_CHANNEL_LINK")

if BOT_TOKEN is None:
    logger.error("BOT_TOKEN not set in environment")
    raise RuntimeError("BOT_TOKEN required")

# Initialize Firebase
if FIREBASE_KEY:
    try:
        key_json = json.loads(FIREBASE_KEY)
        cred = credentials.Certificate(key_json)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        logger.info("Initialized Firebase Firestore")
    except Exception as e:
        logger.exception("Failed to initialize Firebase: %s", e)
        db = None
else:
    logger.warning("FIREBASE_KEY not set. Firestore disabled.")
    db = None

# Simple in-memory cache and maintenance flag
cache = {
    "subjects": {},
    "departments": {},
    "ads": None,
}
maintenance_mode = False

# Conversation states
ADMIN_WAIT_JSON = 10
ADD_QUIZ_CATEGORY = 11
ADD_QUIZ_SUBJECT = 12
ADD_QUIZ_TYPENAME = 13
ADD_QUIZ_UPLOAD = 14
ADD_AD_UPLOAD = 20
BROADCAST_WAIT = 21

app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)
# Dispatcher/updater will be created later depending on run mode (webhook vs polling)
dispatcher = None
updater = None

# Choose run mode: polling if POLLING env var is true, otherwise webhook (Flask)
USE_POLLING = os.environ.get('POLLING', 'false').lower() in ('1', 'true', 'yes')

def admin_only(func):
    @wraps(func)
    def wrapper(update, context):
        user_id = None
        if update.message:
            user_id = update.message.from_user.id
        elif update.callback_query:
            user_id = update.callback_query.from_user.id
        if user_id != ADMIN_TELEGRAM_ID:
            return
        return func(update, context)
    return wrapper

def save_fields_to_firestore(data):
    if db is None:
        return False, "Firestore not initialized"
    exam_col = db.collection('exam')
    # entrance
    entrance = data.get('entrance', {})
    subjects = entrance.get('subjects', [])
    if subjects:
        entrance_doc = exam_col.document('entrance')
        subjects_col = entrance_doc.collection('subjects')
        for s in subjects:
            name = s.get('name')
            code = s.get('code')
            desc = s.get('description', '')
            slug = name.strip().lower().replace(' ', '_')
            subjects_col.document(slug).set({'name': name, 'code': code, 'description': desc})
            # create an empty exams subcollection marker doc so subcollection exists
            # create a small marker document with an auto-generated id so the subcollection exists
            subjects_col.document(slug).collection('exams').document().set({'_marker': True})
    # exit
    exit_ = data.get('exit', {})
    departments = exit_.get('departments', [])
    if departments:
        exit_doc = exam_col.document('exit')
        deps_col = exit_doc.collection('departments')
        for d in departments:
            name = d.get('name')
            code = d.get('code')
            desc = d.get('description', '')
            slug = name.strip().lower().replace(' ', '_')
            deps_col.document(slug).set({'name': name, 'code': code, 'description': desc})
            # create a small marker document with an auto-generated id so the subcollection exists
            deps_col.document(slug).collection('exams').document().set({'_marker': True})
    return True, "Saved"

def get_subjects_from_firestore():
    if cache['subjects']:
        return cache['subjects']
    if db is None:
        return {}
    subjects = {}
    try:
        col = db.collection('exam').document('entrance').collection('subjects').stream()
        for doc in col:
            data = doc.to_dict()
            subjects[doc.id] = data
        cache['subjects'] = subjects
    except Exception:
        logger.exception('Failed to fetch subjects')
    return subjects

def get_departments_from_firestore():
    if cache['departments']:
        return cache['departments']
    if db is None:
        return {}
    deps = {}
    try:
        col = db.collection('exam').document('exit').collection('departments').stream()
        for doc in col:
            data = doc.to_dict()
            deps[doc.id] = data
        cache['departments'] = deps
    except Exception:
        logger.exception('Failed to fetch departments')
    return deps

def send_admin_panel(chat_id):
    keyboard = [
        [InlineKeyboardButton('Add Field', callback_data='admin_add_field'), InlineKeyboardButton('Add Quiz', callback_data='admin_add_quiz')],
        [InlineKeyboardButton('Add Ad', callback_data='admin_add_ad'), InlineKeyboardButton('Total User', callback_data='admin_total_user')],
        [InlineKeyboardButton('Broadcast', callback_data='admin_broadcast'), InlineKeyboardButton('Clear Cache', callback_data='admin_clear_cache')],
        [InlineKeyboardButton('Maintenance', callback_data='admin_maintenance')]
    ]
    bot.send_message(chat_id=chat_id, text='Welcome admin. Choose an action:', reply_markup=InlineKeyboardMarkup(keyboard))

def start_command(update, context):
    global maintenance_mode
    if update.message is None:
        return
    user = update.message.from_user
    chat_id = update.message.chat_id
    if maintenance_mode and user.id != ADMIN_TELEGRAM_ID:
        update.message.reply_text('Bot is updating. Please come back later.')
        return

    # Register user in Firestore
    try:
        if db:
            udoc = db.collection('users').document(str(user.id))
            if not udoc.get().exists:
                udoc.set({'first_name': user.first_name, 'username': user.username, 'created_at': firestore.SERVER_TIMESTAMP})
    except Exception:
        logger.exception('Failed to register user')

    # Check membership in PUBLIC_CHANNEL_ID if set
    if PUBLIC_CHANNEL_ID:
        try:
            member = bot.get_chat_member(chat_id=PUBLIC_CHANNEL_ID, user_id=user.id)
            if member.status in ('member', 'creator', 'administrator'):
                show_exam_type_menu(update)
                return
        except Exception:
            logger.debug('Membership check failed or not a member')
    # ask to join
    keyboard = [[InlineKeyboardButton('Join Channel', url=PUBLIC_CHANNEL_LINK or 'https://t.me')], [InlineKeyboardButton('Check Membership', callback_data='check_membership')]]
    update.message.reply_text('Please join our channel to continue:', reply_markup=InlineKeyboardMarkup(keyboard))

def show_exam_type_menu(update_or_query):
    if hasattr(update_or_query, 'message'):
        chat_id = update_or_query.message.chat_id
    else:
        chat_id = update_or_query.callback_query.message.chat_id
    keyboard = ReplyKeyboardMarkup([['Entrance', 'Exit'], ['Quiz Navigation']], resize_keyboard=True)
    bot.send_message(chat_id=chat_id, text='Choose Exam Type', reply_markup=keyboard)

@admin_only
def ethioegzam_command(update, context):
    if update.message is None:
        return
    send_admin_panel(update.message.chat_id)

@admin_only
def admin_callback_handler(update, context):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id
    if data == 'admin_add_field':
        bot.send_message(chat_id=chat_id, text='Upload the list of fields in JSON format or type /cancel to cancel.')
        # set a marker in cache so next message is treated as field upload
        cache['awaiting_add_field'] = chat_id
    elif data == 'admin_add_quiz':
        keyboard = [[InlineKeyboardButton('Entrance', callback_data='add_quiz_entrance'), InlineKeyboardButton('Exit', callback_data='add_quiz_exit')]]
        bot.send_message(chat_id=chat_id, text='Select category', reply_markup=InlineKeyboardMarkup(keyboard))
    elif data == 'admin_add_ad':
        bot.send_message(chat_id=chat_id, text='Please upload a photo or video with caption for the ad.')
        cache['awaiting_add_ad'] = chat_id
    elif data == 'admin_total_user':
        total = 0
        try:
            if db:
                users = db.collection('users').stream()
                total = sum(1 for _ in users)
        except Exception:
            logger.exception('Failed to count users')
        bot.send_message(chat_id=chat_id, text=f'Total registered users: {total}')
    elif data == 'admin_broadcast':
        bot.send_message(chat_id=chat_id, text='Send the broadcast message (text or photo/video with caption).')
        cache['awaiting_broadcast'] = chat_id
    elif data == 'admin_clear_cache':
        keyboard = [[InlineKeyboardButton('Yes', callback_data='confirm_clear_cache'), InlineKeyboardButton('Cancel', callback_data='cancel_clear_cache')]]
        bot.send_message(chat_id=chat_id, text='Clear cache? Confirm:', reply_markup=InlineKeyboardMarkup(keyboard))
    elif data == 'confirm_clear_cache':
        cache.clear()
        cache.update({'subjects': {}, 'departments': {}, 'ads': None})
        bot.send_message(chat_id=chat_id, text='Cache cleared.')
    elif data == 'cancel_clear_cache':
        bot.send_message(chat_id=chat_id, text='Cancelled.')
    elif data == 'admin_maintenance':
        global maintenance_mode
        if not maintenance_mode:
            keyboard = [[InlineKeyboardButton('Deactivate', callback_data='do_deactivate')]]
            bot.send_message(chat_id=chat_id, text='Bot is active. Choose:', reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton('Activate', callback_data='do_activate')]]
            bot.send_message(chat_id=chat_id, text='Bot is in maintenance. Choose:', reply_markup=InlineKeyboardMarkup(keyboard))
    elif data == 'do_deactivate':
        maintenance_mode = True
        bot.send_message(chat_id=chat_id, text='Bot is now in maintenance mode.')
    elif data == 'do_activate':
        maintenance_mode = False
        bot.send_message(chat_id=chat_id, text='Bot activated.')
    # quiz category
    elif data == 'add_quiz_entrance':
        subjects = get_subjects_from_firestore()
        keyboard = []
        row = []
        for i, (slug, s) in enumerate(subjects.items(), start=1):
            row.append(InlineKeyboardButton(s.get('name', slug), callback_data=f'add_quiz_subject:entrance:{slug}'))
            if i % 3 == 0:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        bot.send_message(chat_id=chat_id, text='Select subject', reply_markup=InlineKeyboardMarkup(keyboard))
    elif data == 'add_quiz_exit':
        deps = get_departments_from_firestore()
        keyboard = []
        row = []
        for i, (slug, d) in enumerate(deps.items(), start=1):
            row.append(InlineKeyboardButton(d.get('name', slug), callback_data=f'add_quiz_subject:exit:{slug}'))
            if i % 3 == 0:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        bot.send_message(chat_id=chat_id, text='Select department', reply_markup=InlineKeyboardMarkup(keyboard))
    elif data.startswith('add_quiz_subject:'):
        # store selection and ask for typeName
        _, category, slug = data.split(':', 2)
        cache['add_quiz'] = {'chat_id': chat_id, 'category': category, 'slug': slug}
        bot.send_message(chat_id=chat_id, text='Enter typeName (e.g., 2015, 2016, model01)')

def message_handler(update, context):
    msg = update.message
    if msg is None:
        return
    user = msg.from_user
    # handle admin adding fields
    if cache.get('awaiting_add_field') == msg.chat_id and user.id == ADMIN_TELEGRAM_ID:
        # accept file or text
        text = None
        if msg.document:
            file = bot.get_file(msg.document.file_id)
            content = file.download_as_bytearray()
            text = content.decode('utf-8')
        else:
            text = msg.text
        try:
            data = json.loads(text)
            ok, reason = save_fields_to_firestore(data)
            if ok:
                bot.send_message(chat_id=msg.chat_id, text='Fields saved successfully.')
                # clear cache for subjects/departments
                cache['subjects'] = {}
                cache['departments'] = {}
            else:
                bot.send_message(chat_id=msg.chat_id, text=f'Failed: {reason}')
        except Exception as e:
            logger.exception('Failed to parse/upload fields')
            bot.send_message(chat_id=msg.chat_id, text=f'Invalid JSON: {e}')
        cache.pop('awaiting_add_field', None)
        return

    # admin add quiz typeName and JSON upload flow
    if cache.get('add_quiz') and user.id == ADMIN_TELEGRAM_ID:
        state = cache['add_quiz']
        if 'typename' not in state:
            # expecting typename
            if msg.text:
                state['typename'] = msg.text.strip()
                cache['add_quiz'] = state
                bot.send_message(chat_id=msg.chat_id, text='Now upload the quiz JSON file containing questions.')
                return
            else:
                bot.send_message(chat_id=msg.chat_id, text='Please send a text typeName first.')
                return
        else:
            # expecting JSON file or text
            text = None
            if msg.document:
                file = bot.get_file(msg.document.file_id)
                content = file.download_as_bytearray()
                text = content.decode('utf-8')
            else:
                text = msg.text
            try:
                questions = json.loads(text)
                # save to Firestore
                if db:
                    exam_doc = db.collection('exam').document(state['category']).collection('subjects' if state['category']=='entrance' else 'departments').document(state['slug']).collection('exams').document()
                    exam_doc.set({'typeName': state['typename'], 'questions': questions, 'created_at': firestore.SERVER_TIMESTAMP})
                    bot.send_message(chat_id=msg.chat_id, text='Quiz uploaded successfully.')
                    # clear subjects cache so new exam types found later
                    cache['subjects'] = {}
                    cache['departments'] = {}
                else:
                    bot.send_message(chat_id=msg.chat_id, text='Firestore not configured; cannot save quiz.')
            except Exception as e:
                logger.exception('Failed to save quiz')
                bot.send_message(chat_id=msg.chat_id, text=f'Invalid JSON or save error: {e}')
            cache.pop('add_quiz', None)
            return

    # admin add ad
    if cache.get('awaiting_add_ad') == msg.chat_id and user.id == ADMIN_TELEGRAM_ID:
        # only accept photo or video
        try:
            store = None
            if msg.photo:
                # get largest photo
                photo = msg.photo[-1]
                store = {'chat_id': msg.chat_id, 'message_id': msg.message_id, 'type': 'photo'}
            elif msg.video:
                store = {'chat_id': msg.chat_id, 'message_id': msg.message_id, 'type': 'video'}
            if store:
                if db:
                    db.collection('ads').document('current').set(store)
                cache['ads'] = store
                bot.send_message(chat_id=msg.chat_id, text='Ad stored (message id saved).')
            else:
                bot.send_message(chat_id=msg.chat_id, text='Please upload a photo or video with caption.')
        except Exception:
            logger.exception('Failed to store ad')
            bot.send_message(chat_id=msg.chat_id, text='Failed to store ad.')
        cache.pop('awaiting_add_ad', None)
        return

    # admin broadcast
    if cache.get('awaiting_broadcast') == msg.chat_id and user.id == ADMIN_TELEGRAM_ID:
        # Broadcast logic: send to users with expired sessions now, schedule others for later
        try:
            if db:
                users = db.collection('users').stream()
                now = time.time()
                sent = 0
                for u in users:
                    uid = int(u.id)
                    try:
                        bot.send_message(chat_id=uid, text=msg.text or '')
                        sent += 1
                    except Exception:
                        logger.exception('Failed to send broadcast to %s', uid)
                bot.send_message(chat_id=msg.chat_id, text=f'Broadcast sent to {sent} users (attempted).')
            else:
                bot.send_message(chat_id=msg.chat_id, text='Firestore not configured; cannot perform smart broadcast. Sent nothing.')
        except Exception:
            logger.exception('Broadcast failed')
            bot.send_message(chat_id=msg.chat_id, text='Broadcast failed.')
        cache.pop('awaiting_broadcast', None)
        return

def callback_query_handler(update, context):
    query = update.callback_query
    data = query.data
    user = query.from_user
    if data == 'check_membership':
        try:
            member = bot.get_chat_member(chat_id=PUBLIC_CHANNEL_ID, user_id=user.id)
            if member.status in ('member', 'creator', 'administrator'):
                bot.send_message(chat_id=user.id, text='Membership confirmed. Enjoy!')
                show_exam_type_menu(query)
                query.answer()
                return
        except Exception:
            logger.debug('Membership check failed')
        bot.send_message(chat_id=user.id, text='You are not a member yet. Join and press Check Membership again.')
        query.answer()
        return

    if data.startswith('answer:'):
        # format: answer:<session_id>:<index>:<choice>
        parts = data.split(':')
        if len(parts) < 4:
            query.answer()
            return
        _, session_id, index_s, choice = parts
        # load session from Firestore or simple cache - here we store sessions in cache for simplicity
        session = cache.get('sessions', {}).get(session_id)
        if not session:
            query.answer('Session expired')
            return
        index = int(index_s)
        question = session['questions'][index]
        correct = question.get('answer')
        explanation = question.get('explanation', '')
        correct_label = correct.upper()
        chosen_label = choice.upper()
        is_correct = (chosen_label.lower() == correct.lower())
        # build new text
        total = len(session['questions'])
        qnum = index + 1
        text = f"Question {qnum}/{total}\n\n{question.get('question_text')}\n\n"
        opts = question.get('options', {})
        text += f"A. {opts.get('a','')}\nB. {opts.get('b','')}\nC. {opts.get('c','')}\nD. {opts.get('d','')}\n\n"
        text += '✓ Correct\n' if is_correct else '✗ Incorrect\n'
        text += f"\nCorrect Answer: {correct_label}. {opts.get(correct, '')}\n\nExplanation:\n{explanation}"
        # replace buttons with Next
        kb = [[InlineKeyboardButton('Next', callback_data=f'next:{session_id}:{index+1}')]]
        try:
            bot.edit_message_text(chat_id=query.message.chat_id, message_id=query.message.message_id, text=text, reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            logger.exception('Failed to edit message after answer')
        query.answer()
        # Update session stats
        if is_correct:
            session['correct'] += 1
        session['attempts'] += 1
        session['last_activity'] = time.time()
        cache.setdefault('sessions', {})[session_id] = session
        return

    if data.startswith('next:'):
        _, session_id, idx_s = data.split(':')
        idx = int(idx_s)
        session = cache.get('sessions', {}).get(session_id)
        if not session:
            query.answer('Session expired')
            return
        if idx >= len(session['questions']):
            # session complete
            summary = f"Exam finished! Correct: {session['correct']} / {len(session['questions'])}"
            bot.send_message(chat_id=query.message.chat_id, text=summary)
            # persist results in Firestore
            try:
                if db:
                    db.collection('results').document().set({'user_id': session['user_id'], 'correct': session['correct'], 'attempts': session['attempts'], 'type': session['type'], 'finished_at': firestore.SERVER_TIMESTAMP})
            except Exception:
                logger.exception('Failed to save results')
            # show ad every 5 questions if exists
            if cache.get('ads'):
                ad = cache['ads']
                try:
                    bot.copy_message(chat_id=query.message.chat_id, from_chat_id=ad['chat_id'], message_id=ad['message_id'])
                except Exception:
                    logger.exception('Failed to show ad')
            # cleanup session
            cache['sessions'].pop(session_id, None)
            query.answer()
            return
        # send next question by editing message
        q = session['questions'][idx]
        text = f"Question {idx+1}/{len(session['questions'])}\n\n{q.get('question_text')}\n\nA. {q.get('options',{}).get('a','')}\nB. {q.get('options',{}).get('b','')}\nC. {q.get('options',{}).get('c','')}\nD. {q.get('options',{}).get('d','')}"
        kb = [[InlineKeyboardButton('A', callback_data=f'answer:{session_id}:{idx}:a'), InlineKeyboardButton('B', callback_data=f'answer:{session_id}:{idx}:b')], [InlineKeyboardButton('C', callback_data=f'answer:{session_id}:{idx}:c'), InlineKeyboardButton('D', callback_data=f'answer:{session_id}:{idx}:d')]]
        try:
            bot.edit_message_text(chat_id=query.message.chat_id, message_id=query.message.message_id, text=text, reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            logger.exception('Failed to edit for next question')
        session['last_activity'] = time.time()
        cache.setdefault('sessions', {})[session_id] = session
        query.answer()

# For simple demo: when user selects subject and an exam, start a session
def text_message_router(update, context):
    msg = update.message
    if msg is None:
        return
    text = msg.text
    if text in ('Entrance', 'Exit'):
        if text == 'Entrance':
            subjects = get_subjects_from_firestore()
            kb = []
            for k, v in subjects.items():
                kb.append([InlineKeyboardButton(v.get('name',''), callback_data=f'start_subject:entrance:{k}')])
            bot.send_message(chat_id=msg.chat_id, text='Select subject:', reply_markup=InlineKeyboardMarkup(kb))
        else:
            deps = get_departments_from_firestore()
            kb = []
            for k, v in deps.items():
                kb.append([InlineKeyboardButton(v.get('name',''), callback_data=f'start_subject:exit:{k}')])
            bot.send_message(chat_id=msg.chat_id, text='Select department:', reply_markup=InlineKeyboardMarkup(kb))

def start_subject_handler(update, context):
    query = update.callback_query
    data = query.data
    _, category, slug = data.split(':', 2)
    # fetch exam types for this subject/department
    try:
        if db:
            exams_col = db.collection('exam').document(category).collection('subjects' if category=='entrance' else 'departments').document(slug).collection('exams')
            docs = list(exams_col.stream())
            if not docs:
                bot.send_message(chat_id=query.message.chat_id, text='No exams uploaded yet for this subject.')
                query.answer()
                return
            kb = []
            for d in docs:
                data = d.to_dict()
                typename = data.get('typeName', 'exam')
                kb.append([InlineKeyboardButton(typename, callback_data=f'start_exam:{category}:{slug}:{d.id}')])
            bot.send_message(chat_id=query.message.chat_id, text='Select exam type:', reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        logger.exception('Failed to list exams')
        bot.send_message(chat_id=query.message.chat_id, text='Failed to load exams.')
    query.answer()

def start_exam_handler(update, context):
    query = update.callback_query
    data = query.data
    _, category, slug, exam_doc_id = data.split(':', 3)
    try:
        doc = db.collection('exam').document(category).collection('subjects' if category=='entrance' else 'departments').document(slug).collection('exams').document(exam_doc_id).get()
        if not doc.exists:
            bot.send_message(chat_id=query.message.chat_id, text='Exam not found.')
            query.answer()
            return
        d = doc.to_dict()
        questions = d.get('questions', [])
        if not questions:
            bot.send_message(chat_id=query.message.chat_id, text='No questions found in this exam.')
            query.answer()
            return
        # create session id
        session_id = f"s{int(time.time())}{query.from_user.id}"
        session = {'user_id': query.from_user.id, 'questions': questions, 'index': 0, 'correct': 0, 'attempts': 0, 'last_activity': time.time(), 'type': d.get('typeName','exam')}
        cache.setdefault('sessions', {})[session_id] = session
        # send first question
        q = questions[0]
        text = f"Question 1/{len(questions)}\n\n{q.get('question_text')}\n\nA. {q.get('options',{}).get('a','')}\nB. {q.get('options',{}).get('b','')}\nC. {q.get('options',{}).get('c','')}\nD. {q.get('options',{}).get('d','')}"
        kb = [[InlineKeyboardButton('A', callback_data=f'answer:{session_id}:0:a'), InlineKeyboardButton('B', callback_data=f'answer:{session_id}:0:b')], [InlineKeyboardButton('C', callback_data=f'answer:{session_id}:0:c'), InlineKeyboardButton('D', callback_data=f'answer:{session_id}:0:d')]]
        bot.send_message(chat_id=query.message.chat_id, text=text, reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        logger.exception('Failed to start exam')
        bot.send_message(chat_id=query.message.chat_id, text='Failed to start exam.')
    query.answer()

# Flask webhook endpoint
@app.route('/webhook', methods=['POST'])
def webhook():
    update = Update.de_json(request.get_json(force=True), bot)
    dispatcher.process_update(update)
    return 'OK'


@app.route('/', methods=['GET'])
def index():
    return "OK", 200

@app.route('/health', methods=['GET'])
def health():
    return "OK", 200

# Create dispatcher/updater and register handlers depending on run mode
if USE_POLLING:
    # polling mode: use Updater
    from telegram.ext import Updater
    updater = Updater(token=BOT_TOKEN, use_context=True)
    dispatcher = updater.dispatcher
else:
    # webhook mode: create a low-level Dispatcher for Flask updates
    from telegram.ext import Dispatcher as TBDispatcher
    dispatcher = TBDispatcher(bot, None, workers=0, use_context=False)

# Register handlers
dispatcher.add_handler(CommandHandler('start', start_command))
dispatcher.add_handler(CommandHandler('ethioegzam', ethioegzam_command))
dispatcher.add_handler(CallbackQueryHandler(admin_callback_handler, pattern='^admin_'))
dispatcher.add_handler(CallbackQueryHandler(start_subject_handler, pattern='^start_subject:'))
dispatcher.add_handler(CallbackQueryHandler(start_exam_handler, pattern='^start_exam:'))
dispatcher.add_handler(CallbackQueryHandler(callback_query_handler, pattern='^(check_membership|answer:|next:|add_quiz_|do_|confirm_|cancel_).*'))
dispatcher.add_handler(MessageHandler(Filters.text | Filters.document | Filters.photo | Filters.video, message_handler))
dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), text_message_router))

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    if USE_POLLING:
        # Start Flask in background so Render health checks succeed, then polling
        import threading
        threading.Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': port}, daemon=True).start()
        updater.start_polling()
        updater.idle()
    else:
        app.run(host='0.0.0.0', port=port)
