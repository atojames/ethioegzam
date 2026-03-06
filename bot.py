# -*- coding: utf-8 -*-
"""Quiz Telegram Bot using Firebase and Flask.

This module contains a complete implementation of the Quiz Telegram Bot as
specified by the user.  It handles both the admin panel (using
InlineKeyboardMarkup exclusively) and the user interface (primarily
ReplyKeyboardMarkup).  Firestore is used as a backend; an in‑memory cache
reduces the number of reads.

The bot is designed to run on Render in polling mode.  A tiny Flask server is
embedded to keep the dyno alive.
"""

import os
import json
import logging
from datetime import datetime, timedelta

from flask import Flask
from telegram import (
	InlineKeyboardButton,
	InlineKeyboardMarkup,
	ReplyKeyboardMarkup,
)
from telegram.ext import (
	Updater,
	CommandHandler,
	MessageHandler,
	Filters,
	CallbackContext,
	CallbackQueryHandler,
	ConversationHandler,
)

import firebase_admin
from firebase_admin import credentials, firestore

# ---------------------------------------------------------------------------
# configuration & initialization
# ---------------------------------------------------------------------------

logging.basicConfig(
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

ADMIN_TELEGRAM_ID = int(os.getenv('ADMIN_TELEGRAM_ID', '0'))
BOT_TOKEN = os.getenv('BOT_TOKEN')
BOT_USERNAME = os.getenv('BOT_USERNAME')
PUBLIC_CHANNEL_ID = os.getenv('PUBLIC_CHANNEL_ID')
PUBLIC_CHANNEL_LINK = os.getenv('PUBLIC_CHANNEL_LINK')

firebase_key = os.getenv('FIREBASE_KEY')
if not firebase_key:
	raise RuntimeError('FIREBASE_KEY environment variable is required')

cred = credentials.Certificate(json.loads(firebase_key))
firebase_admin.initialize_app(cred)
db = firestore.client()

# ---------------------------------------------------------------------------
# caches & in‑memory state
# ---------------------------------------------------------------------------

subjects_cache = {}       # category -> list of names
exams_cache = {}          # (category, subject) -> list of typeNames
quiz_cache = {}           # exam_doc_id -> questions list
ad_message_id = None      # stored advertisement message id
users_cache = set()       # user ids of registered users
maintenance_mode = False  # global toggle

user_sessions = {}  # user_id -> {"last_active": datetime, ...}

# conversation states
(
    ADMIN_MENU,
    ADD_FIELD,
    ADD_QUIZ_CHOOSE_CATEGORY,
    ADD_QUIZ_CHOOSE_SUBJECT,
    ADD_QUIZ_TYPE_NAME,
    ADD_QUIZ_UPLOAD,
    ADD_AD_UPLOAD,
    BROADCAST_MESSAGE,
    CONFIRM_CLEAR_CACHE,
    TOGGLE_MAINTENANCE,
) = range(10)

# ---------------------------------------------------------------------------
# helper utilities
# ---------------------------------------------------------------------------

def _clear_caches() -> None:
	global subjects_cache, exams_cache, quiz_cache, ad_message_id
	subjects_cache.clear()
	exams_cache.clear()
	quiz_cache.clear()
	ad_message_id = None


def _load_subjects(category: str) -> list:
	if category in subjects_cache:
		return subjects_cache[category]
	lst = []
	doc = db.collection('exam').document(category)
	snap = doc.get()
	if snap.exists:
		field = 'subjects' if category == 'entrance' else 'departments'
		for item in snap.to_dict().get(field, []):
			lst.append(item.get('name', '').lower())
	subjects_cache[category] = lst
	return lst


def _make_inline_buttons(items, prefix='', cols=3):
	keyboard = []
	row = []
	for i, it in enumerate(items):
		row.append(InlineKeyboardButton(it.title(), callback_data=f'{prefix}{it}'))
		if (i + 1) % cols == 0:
			keyboard.append(row)
			row = []
	if row:
		keyboard.append(row)
	return keyboard


def _load_exam_list(category: str, subject: str) -> list:
	key = (category, subject)
	if key in exams_cache:
		return exams_cache[key]
	exams = []
	col = db.collection('exam').document(category).collection(
		'subjects' if category == 'entrance' else 'departments'
	).document(subject).collection('exams')
	for d in col.stream():
		exams.append(d.to_dict().get('typeName', ''))
	exams_cache[key] = exams
	return exams


def _get_exam_doc(category: str, subject: str, typename: str) -> dict | None:
	col = db.collection('exam').document(category).collection(
		'subjects' if category == 'entrance' else 'departments'
	).document(subject).collection('exams')
	for d in col.where('typeName', '==', typename).limit(1).stream():
		return d.to_dict()
	return None


def _show_main_menu(update):
	kb = ReplyKeyboardMarkup([['Entrance', 'Exit'], ['Home', 'Back']], resize_keyboard=True)
	update.message.reply_text('Main Menu', reply_markup=kb)

# ---------------------------------------------------------------------------
# telegram handlers
# ---------------------------------------------------------------------------

def start(update: Update, context: CallbackContext) -> None:
	user_id = update.effective_user.id
	if maintenance_mode and user_id != ADMIN_TELEGRAM_ID:
		update.message.reply_text(
			'The bot is currently under maintenance.\nPlease try again later.'
		)
		return

	users_cache.add(user_id)

	try:
		member = context.bot.get_chat_member(PUBLIC_CHANNEL_ID, user_id)
		if member.status not in ('member', 'creator', 'administrator'):
			raise ValueError
		_show_main_menu(update)
	except Exception:
		keyboard = [
			[InlineKeyboardButton('Join Channel', url=PUBLIC_CHANNEL_LINK)],
			[InlineKeyboardButton('Check Membership', callback_data='check_member')],
		]
		update.message.reply_text(
			'Please join the channel to continue.',
			reply_markup=InlineKeyboardMarkup(keyboard),
		)


def check_membership_callback(update: Update, context: CallbackContext) -> None:
	query = update.callback_query
	user_id = query.from_user.id
	try:
		member = context.bot.get_chat_member(PUBLIC_CHANNEL_ID, user_id)
		if member.status in ('member', 'creator', 'administrator'):
			query.answer('Membership confirmed')
			_show_main_menu(query)
		else:
			query.answer('You are not a member yet.')
	except Exception:
		query.answer('You are not a member yet.')


def admin_entry(update: Update, context: CallbackContext):
	user_id = update.effective_user.id
	if user_id != ADMIN_TELEGRAM_ID:
		return
	keyboard = [
		[InlineKeyboardButton('Add Field', callback_data='add_field'),
		 InlineKeyboardButton('Add Quiz', callback_data='add_quiz')],
		[InlineKeyboardButton('Add Ad', callback_data='add_ad'),
		 InlineKeyboardButton('Total User', callback_data='total_user')],
		[InlineKeyboardButton('Broadcast', callback_data='broadcast'),
		 InlineKeyboardButton('Clear Cache', callback_data='clear_cache')],
		[InlineKeyboardButton('Maintenance', callback_data='maintenance')],
	]
	update.message.reply_text('Welcome to admin panel',
							  reply_markup=InlineKeyboardMarkup(keyboard))
	return ADMIN_MENU


def admin_button_handler(update: Update, context: CallbackContext):
	# declare globals used for modification
	global maintenance_mode
	query = update.callback_query
	data = query.data
	if data == 'add_field':
		query.edit_message_text(
			'Upload the list of fields in JSON format.\nOr send /cancel to cancel the operation.'
		)
		return ADD_FIELD
	elif data == 'add_quiz':
		keyboard = [
			[InlineKeyboardButton('Entrance', callback_data='adm_quiz_entrance')],
			[InlineKeyboardButton('Exit', callback_data='adm_quiz_exit')],
		]
		query.edit_message_text('Select exam category',
								reply_markup=InlineKeyboardMarkup(keyboard))
		return ADD_QUIZ_CHOOSE_CATEGORY
	elif data == 'add_ad':
		query.edit_message_text('Send photo or video with caption for advertisement')
		return ADD_AD_UPLOAD
	elif data == 'total_user':
		total = len(users_cache)
		query.answer()
		query.edit_message_text(f'Total registered users: {total}')
		return ADMIN_MENU
	elif data == 'broadcast':
		query.edit_message_text('Send text or media to broadcast:')
		return BROADCAST_MESSAGE
	elif data == 'clear_cache':
		keyboard = [
			[InlineKeyboardButton('Yes', callback_data='clear_cache_yes'),
			 InlineKeyboardButton('Cancel', callback_data='clear_cache_cancel')],
		]
		query.edit_message_text('Are you sure you want to clear cache?',
								reply_markup=InlineKeyboardMarkup(keyboard))
		return CONFIRM_CLEAR_CACHE
	elif data == 'maintenance':
		btn_text = 'Deactivate' if not maintenance_mode else 'Activate'
		query.edit_message_text('Maintenance mode control',
								reply_markup=InlineKeyboardMarkup([
									[InlineKeyboardButton(btn_text, callback_data='toggle_maintenance')]
								]))
		return TOGGLE_MAINTENANCE
	elif data == 'toggle_maintenance':
		global maintenance_mode
		maintenance_mode = not maintenance_mode
		state = 'activated' if not maintenance_mode else 'deactivated'
		query.edit_message_text(f'Maintenance mode {state}.')
		return ADMIN_MENU
	elif data == 'clear_cache_yes':
		_clear_caches()
		query.edit_message_text('Cache cleared successfully.')
		return ADMIN_MENU
	elif data == 'clear_cache_cancel':
		query.edit_message_text('Cache clear cancelled.')
		return ADMIN_MENU
	return ADMIN_MENU


def handle_add_field(update: Update, context: CallbackContext):
	text = update.message.text
	try:
		data = json.loads(text)
	except Exception:
		update.message.reply_text('Invalid JSON. Operation cancelled.')
		return ADMIN_MENU
	for category in ('entrance', 'exit'):
		if category in data:
			db.collection('exam').document(category).set(data[category])
	update.message.reply_text('Fields successfully added.')
	return ADMIN_MENU


def handle_add_quiz_category(update: Update, context: CallbackContext):
	query = update.callback_query
	data = query.data
	if data == 'adm_quiz_entrance':
		subjects = _load_subjects('entrance')
		keyboard = _make_inline_buttons(subjects, prefix='adm_subj_', cols=3)
		query.edit_message_text('Select subject', reply_markup=InlineKeyboardMarkup(keyboard))
		return ADD_QUIZ_CHOOSE_SUBJECT
	elif data == 'adm_quiz_exit':
		depts = _load_subjects('exit')
		keyboard = _make_inline_buttons(depts, prefix='adm_dept_', cols=3)
		query.edit_message_text('Select department', reply_markup=InlineKeyboardMarkup(keyboard))
		return ADD_QUIZ_CHOOSE_SUBJECT
	return ADMIN_MENU


def handle_add_quiz_subject(update: Update, context: CallbackContext):
	query = update.callback_query
	data = query.data
	if data.startswith('adm_subj_') or data.startswith('adm_dept_'):
		context.user_data['quiz_category'] = 'entrance' if 'subj' in data else 'exit'
		context.user_data['quiz_subject'] = data.split('_')[-1]
		query.edit_message_text('Enter the exam type name (e.g. 2015)')
		return ADD_QUIZ_TYPE_NAME
	return ADMIN_MENU


def handle_add_quiz_typename(update: Update, context: CallbackContext):
	text = update.message.text.strip()
	context.user_data['quiz_typename'] = text
	update.message.reply_text('Upload the quiz questions JSON file.')
	return ADD_QUIZ_UPLOAD


def handle_add_quiz_upload(update: Update, context: CallbackContext):
	txt = update.message.text
	try:
		questions = json.loads(txt)
	except Exception:
		update.message.reply_text('Invalid JSON. Operation cancelled.')
		return ADMIN_MENU
	cat = context.user_data.get('quiz_category')
	subj = context.user_data.get('quiz_subject')
	typename = context.user_data.get('quiz_typename')
	col = db.collection('exam').document(cat).collection(
		'subjects' if cat == 'entrance' else 'departments'
	)
	doc = col.document(subj).collection('exams').document()
	doc.set({'typeName': typename, 'questions': questions})
	update.message.reply_text('Quiz added successfully.')
	exams_cache.pop((cat, subj), None)
	return ADMIN_MENU


def handle_add_ad_upload(update: Update, context: CallbackContext):
	msg = update.message
	if msg.photo or msg.video:
		update.message.reply_text('Advertisement saved.')
		global ad_message_id
		ad_message_id = msg.message_id
	else:
		update.message.reply_text('Please send a photo or video.')
	return ADMIN_MENU


def handle_broadcast_message(update: Update, context: CallbackContext):
	text = update.message.text or ''
	media = None
	if update.message.photo or update.message.video:
		media = update.message
	for user in users_cache:
		try:
			if media:
				context.bot.copy_message(
					chat_id=user,
					from_chat_id=update.effective_chat.id,
					message_id=media.message_id,
				)
			else:
				context.bot.send_message(chat_id=user, text=text)
		except Exception:
			pass
	update.message.reply_text('Broadcast sent.')
	return ADMIN_MENU


def handle_text(update: Update, context: CallbackContext):
	text = update.message.text
	if text.lower() in ['entrance', 'exit']:
		cat = text.lower()
		items = _load_subjects(cat)
		rows = []
		for i in range(0, len(items), 2):
			row = [items[i].title()]
			if i + 1 < len(items):
				row.append(items[i + 1].title())
			rows.append(row)
		kb = ReplyKeyboardMarkup(rows + [['Home', 'Back']], resize_keyboard=True)
		update.message.reply_text('Select:', reply_markup=kb)
		context.user_data['category'] = cat
		return
	if (
		'category' in context.user_data
		and text.lower()
		in [x.title() for x in _load_subjects(context.user_data['category'])]
	):
		subj = text.lower()
		context.user_data['subject'] = subj
		exams = _load_exam_list(context.user_data['category'], subj)
		rows = []
		for i in range(0, len(exams), 2):
			row = [exams[i]]
			if i + 1 < len(exams):
				row.append(exams[i + 1])
			rows.append(row)
		kb = ReplyKeyboardMarkup(rows + [['Home', 'Back']], resize_keyboard=True)
		update.message.reply_text('Select exam type:', reply_markup=kb)
		return
	if (
		'subject' in context.user_data
		and text in _load_exam_list(context.user_data['category'], context.user_data['subject'])
	):
		exam_doc = _get_exam_doc(context.user_data['category'], context.user_data['subject'], text)
		if exam_doc:
			questions = exam_doc.get('questions', [])
			context.user_data['quiz'] = questions
			context.user_data['quiz_index'] = 0
			context.user_data['correct'] = 0
			_send_question(update, context)
		return


def _send_question(update: Update, context: CallbackContext):
	qs = context.user_data['quiz']
	idx = context.user_data['quiz_index']
	if idx >= len(qs):
		_finish_quiz(update, context)
		return
	q = qs[idx]
	text = (
		f"{q.get('question_number')} / {len(qs)}\n"
		f"{q.get('question_text')}\n"
		f"A. {q['options']['a']}\n"
		f"B. {q['options']['b']}\n"
		f"C. {q['options']['c']}\n"
		f"D. {q['options']['d']}"
	)
	buttons = [
		[InlineKeyboardButton('A', callback_data='ans_a'), InlineKeyboardButton('B', callback_data='ans_b')],
		[InlineKeyboardButton('C', callback_data='ans_c'), InlineKeyboardButton('D', callback_data='ans_d')],
	]
	update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))


def answer_callback(update: Update, context: CallbackContext):
	query = update.callback_query
	choice = query.data.split('_')[1]
	idx = context.user_data['quiz_index']
	q = context.user_data['quiz'][idx]
	correct = q.get('answer')
	is_correct = choice == correct
	if is_correct:
		context.user_data['correct'] += 1
	text = (
		f"{q.get('question_number')} / {len(context.user_data['quiz'])}\n"
		f"{q.get('question_text')}\n"
		f"A. {q['options']['a']}\n"
		f"B. {q['options']['b']}\n"
		f"C. {q['options']['c']}\n"
		f"D. {q['options']['d']}\n\n"
	)
	text += '✓ Correct' if is_correct else '✗ Incorrect'
	text += f"\nCorrect Answer: {correct.upper()}\n\nExplanation:\n{q.get('explanation','')}"
	keyboard = [[InlineKeyboardButton('Next', callback_data='next_q')]]
	query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


def next_question_callback(update: Update, context: CallbackContext):
	context.user_data['quiz_index'] += 1
	_send_question(update, context)


def _finish_quiz(update: Update, context: CallbackContext):
	correct = context.user_data.get('correct', 0)
	total = len(context.user_data.get('quiz', []))
	update.message.reply_text(
		f'You scored {correct} out of {total}',
		reply_markup=ReplyKeyboardMarkup([['Home']], resize_keyboard=True),
	)
	context.user_data.clear()


def cancel(update: Update, context: CallbackContext):
	update.message.reply_text('Operation cancelled.',
							  reply_markup=ReplyKeyboardMarkup([['Home']], resize_keyboard=True))
	return ConversationHandler.END


def main():
	updater = Updater(BOT_TOKEN, use_context=True)
	dp = updater.dispatcher

	admin_conv = ConversationHandler(
		entry_points=[CommandHandler('ethioegzam', admin_entry)],
		states={
			ADMIN_MENU: [CallbackQueryHandler(admin_button_handler)],
			ADD_FIELD: [MessageHandler(Filters.text & ~Filters.command, handle_add_field)],
			ADD_QUIZ_CHOOSE_CATEGORY: [CallbackQueryHandler(handle_add_quiz_category)],
			ADD_QUIZ_CHOOSE_SUBJECT: [CallbackQueryHandler(handle_add_quiz_subject)],
			ADD_QUIZ_TYPE_NAME: [MessageHandler(Filters.text & ~Filters.command, handle_add_quiz_typename)],
			ADD_QUIZ_UPLOAD: [MessageHandler(Filters.text & ~Filters.command, handle_add_quiz_upload)],
			ADD_AD_UPLOAD: [MessageHandler(Filters.photo | Filters.video, handle_add_ad_upload)],
			BROADCAST_MESSAGE: [MessageHandler(Filters.all & ~Filters.command, handle_broadcast_message)],
			CONFIRM_CLEAR_CACHE: [CallbackQueryHandler(admin_button_handler)],
			TOGGLE_MAINTENANCE: [CallbackQueryHandler(admin_button_handler)],
		},
		fallbacks=[CommandHandler('cancel', cancel)],
		allow_reentry=True,
	)
	dp.add_handler(admin_conv)

	dp.add_handler(CommandHandler('start', start))
	dp.add_handler(CallbackQueryHandler(check_membership_callback, pattern='check_member'))
	dp.add_handler(CallbackQueryHandler(answer_callback, pattern='ans_'))
	dp.add_handler(CallbackQueryHandler(next_question_callback, pattern='next_q'))
	dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

	app = Flask(__name__)

	@app.route('/')
	def index():
		return 'OK'

	from threading import Thread
	Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))).start()

	updater.start_polling()
	updater.idle()


if __name__ == '__main__':
	main()

