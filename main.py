import os
import re
import uuid
import sqlite3
import logging
import asyncio
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    ChatMember,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    PreCheckoutQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set!")

DB_PATH = os.path.join(os.path.dirname(__file__), "users.db")
FREE_LIMIT = 25
PREMIUM_STARS = 10
CHANNEL = "@karosaver"
CHANNEL_URL = "https://t.me/karosaver"
BAN_HOURS = 24
WARN_SECS = 30
DEV_PASSWORD = "imdev123"

S_REPORT_DESC = "report_desc"
S_REPORT_PHOTO = "report_photo"
S_DEV_PASS = "dev_pass"
S_APPROVE_PASS = "approve_pass"

URL_PATTERN = re.compile(r'https?://[^\s]+')

url_storage: dict[str, str] = {}
pending_bans: dict[int, asyncio.Task] = {}
warn_started: dict[int, datetime] = {}


# ── База данных ───────────────────────────────────────────────────────────────

def init_db() -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id         INTEGER PRIMARY KEY,
                is_premium      INTEGER NOT NULL DEFAULT 0,
                req_date        TEXT    NOT NULL DEFAULT '',
                req_count       INTEGER NOT NULL DEFAULT 0,
                total_downloads INTEGER NOT NULL DEFAULT 0,
                joined_date     TEXT    NOT NULL DEFAULT '',
                banned_until    TEXT    NOT NULL DEFAULT ''
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL,
                url          TEXT    NOT NULL,
                quality      TEXT    NOT NULL,
                filename     TEXT    NOT NULL DEFAULT '',
                file_size_mb REAL    NOT NULL DEFAULT 0,
                downloaded_at TEXT   NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL,
                username     TEXT    NOT NULL DEFAULT '',
                description  TEXT    NOT NULL DEFAULT '',
                photo_file_id TEXT   NOT NULL DEFAULT '',
                created_at   TEXT    NOT NULL,
                status       TEXT    NOT NULL DEFAULT 'pending'
            )
        """)
        existing = {row[1] for row in con.execute("PRAGMA table_info(users)")}
        for col, defn in [
            ("total_downloads", "INTEGER NOT NULL DEFAULT 0"),
            ("joined_date", "TEXT NOT NULL DEFAULT ''"),
            ("banned_until", "TEXT NOT NULL DEFAULT ''"),
        ]:
            if col not in existing:
                con.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
        con.commit()


def _ensure_user(user_id: int) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT OR IGNORE INTO users (user_id, joined_date) VALUES (?, ?)",
            (user_id, str(date.today()))
        )
        con.commit()


def get_user_row(user_id: int) -> tuple:
    _ensure_user(user_id)
    with sqlite3.connect(DB_PATH) as con:
        return con.execute(
            "SELECT is_premium, req_date, req_count, total_downloads, joined_date, banned_until "
            "FROM users WHERE user_id=?", (user_id,)
        ).fetchone()


def check_limit(user_id: int) -> tuple[bool, int, bool]:
    row = get_user_row(user_id)
    is_premium = bool(row[0])
    today = str(date.today())
    used = row[2] if row[1] == today else 0
    return is_premium or used < FREE_LIMIT, used, is_premium


def increment_requests(user_id: int, url: str, quality: str,
                       filename: str, size_mb: float) -> int:
    today = str(date.today())
    row = get_user_row(user_id)
    req_count = row[2] if row[1] == today else 0
    req_count += 1
    total = row[3] + 1
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "UPDATE users SET req_date=?, req_count=?, total_downloads=? WHERE user_id=?",
            (today, req_count, total, user_id)
        )
        con.execute(
            "INSERT INTO downloads (user_id, url, quality, filename, file_size_mb, downloaded_at)"
            " VALUES (?,?,?,?,?,?)",
            (user_id, url, quality, filename, round(size_mb, 2), now)
        )
        con.commit()
    return req_count


def get_last_downloads(user_id: int, limit: int = 5) -> list:
    with sqlite3.connect(DB_PATH) as con:
        return con.execute(
            "SELECT filename, quality, file_size_mb, downloaded_at FROM downloads "
            "WHERE user_id=? ORDER BY id DESC LIMIT ?", (user_id, limit)
        ).fetchall()


def set_premium(user_id: int) -> None:
    _ensure_user(user_id)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET is_premium=1 WHERE user_id=?", (user_id,))
        con.commit()


def ban_user_db(user_id: int) -> datetime:
    until = datetime.utcnow() + timedelta(hours=BAN_HOURS)
    _ensure_user(user_id)
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "UPDATE users SET banned_until=? WHERE user_id=?",
            (until.strftime("%Y-%m-%d %H:%M:%S"), user_id)
        )
        con.commit()
    return until


def unban_user_db(user_id: int) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET banned_until='' WHERE user_id=?", (user_id,))
        con.commit()


def get_ban_until(user_id: int) -> datetime | None:
    row = get_user_row(user_id)
    if not row[5]:
        return None
    try:
        dt = datetime.strptime(row[5], "%Y-%m-%d %H:%M:%S")
        return dt if dt > datetime.utcnow() else None
    except Exception:
        return None


def save_report(user_id: int, username: str, description: str,
                photo_file_id: str = "") -> int:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute(
            "INSERT INTO reports (user_id, username, description, photo_file_id, created_at)"
            " VALUES (?,?,?,?,?)",
            (user_id, username, description, photo_file_id, now)
        )
        con.commit()
        return cur.lastrowid


def get_pending_reports() -> list:
    with sqlite3.connect(DB_PATH) as con:
        return con.execute(
            "SELECT id, user_id, username, description, photo_file_id, created_at "
            "FROM reports WHERE status='pending' ORDER BY id DESC LIMIT 20"
        ).fetchall()


def get_report(report_id: int) -> tuple | None:
    with sqlite3.connect(DB_PATH) as con:
        return con.execute(
            "SELECT id, user_id, username, description, photo_file_id, created_at "
            "FROM reports WHERE id=?", (report_id,)
        ).fetchone()


def update_report_status(report_id: int, status: str) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE reports SET status=? WHERE id=?", (status, report_id))
        con.commit()


# ── URL хранилище ─────────────────────────────────────────────────────────────

def store_url(url: str) -> str:
    key = uuid.uuid4().hex[:12]
    url_storage[key] = url
    if len(url_storage) > 500:
        del url_storage[list(url_storage.keys())[0]]
    return key


# ── Подписка ──────────────────────────────────────────────────────────────────

async def is_subscribed(bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        return member.status in (ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER)
    except Exception as e:
        logger.warning(f"Sub check failed for {user_id}: {e}")
        return True


# ── Бан-таймер ────────────────────────────────────────────────────────────────

async def _ban_task(user_id: int, chat_id: int, bot) -> None:
    try:
        await asyncio.sleep(WARN_SECS)
    except asyncio.CancelledError:
        return

    try:
        if not await is_subscribed(bot, user_id):
            until = ban_user_db(user_id)
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🚫 *Вы заблокированы на {BAN_HOURS} часов.*\n\n"
                        f"Причина: не подписались на {CHANNEL} в течение {WARN_SECS} секунд.\n\n"
                        f"🕐 Разблокировка: *{until.strftime('%d.%m.%Y %H:%M')} UTC*"
                    ),
                    parse_mode="Markdown",
                    reply_markup=ban_keyboard(),
                )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Ban task error {user_id}: {e}")
    finally:
        pending_bans.pop(user_id, None)
        warn_started.pop(user_id, None)


def start_ban_timer(user_id: int, chat_id: int, bot) -> None:
    if user_id not in pending_bans:
        warn_started[user_id] = datetime.utcnow()
        task = asyncio.create_task(_ban_task(user_id, chat_id, bot))
        pending_bans[user_id] = task


def cancel_ban_timer(user_id: int) -> None:
    if user_id in pending_bans:
        pending_bans[user_id].cancel()
        pending_bans.pop(user_id, None)
        warn_started.pop(user_id, None)


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def warning_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Подписаться на канал", url=CHANNEL_URL)],
        [InlineKeyboardButton("✅ Я подписался", callback_data="check_sub")],
    ])


def ban_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ Купить Premium — без блокировок", callback_data="buy_premium")],
        [InlineKeyboardButton("⚠️ Произошла ошибка?", callback_data="start_report")],
    ])


def quality_keyboard(url_key: str, is_premium: bool = False) -> InlineKeyboardMarkup:
    rows = []
    if is_premium:
        rows.append([InlineKeyboardButton("👑 Full HD (1080p)", callback_data=f"1080|{url_key}")])
    rows.append([InlineKeyboardButton("🔝 Лучшее качество", callback_data=f"best|{url_key}")])
    rows.append([
        InlineKeyboardButton("📹 HD (720p)", callback_data=f"720|{url_key}"),
        InlineKeyboardButton("📱 SD (480p)", callback_data=f"480|{url_key}"),
    ])
    rows.append([InlineKeyboardButton("🎵 Аудио (MP3)", callback_data=f"audio|{url_key}")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def premium_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"⭐ Купить Premium за {PREMIUM_STARS} звёзд",
            callback_data="buy_premium"
        )],
    ])


def report_action_keyboard(report_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Одобрено", callback_data=f"approve|{report_id}"),
            InlineKeyboardButton("❌ Отказ", callback_data=f"decline|{report_id}"),
        ]
    ])


# ── Проверка доступа ──────────────────────────────────────────────────────────

async def check_access(user_id: int, chat_id: int,
                        context: ContextTypes.DEFAULT_TYPE) -> bool:
    row = get_user_row(user_id)
    is_premium = bool(row[0])

    if is_premium:
        return True

    ban_until = get_ban_until(user_id)
    if ban_until:
        remaining = ban_until - datetime.utcnow()
        secs = int(remaining.total_seconds())
        h, m = secs // 3600, (secs % 3600) // 60
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🚫 *Вы заблокированы.*\n\n"
                f"Причина: покинули канал {CHANNEL}\n"
                f"⏳ Осталось: *{h}ч {m}мин*\n"
                f"Разблокировка: *{ban_until.strftime('%d.%m.%Y %H:%M')} UTC*"
            ),
            parse_mode="Markdown",
            reply_markup=ban_keyboard(),
        )
        return False

    subscribed = await is_subscribed(context.bot, user_id)
    if subscribed:
        cancel_ban_timer(user_id)
        return True

    if user_id in pending_bans:
        elapsed = (datetime.utcnow() - warn_started.get(user_id, datetime.utcnow())).total_seconds()
        left = max(1, int(WARN_SECS - elapsed))
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"⏳ *Осталось {left} сек.*\n\n"
                f"Подпишитесь на {CHANNEL}, иначе бот заблокирует вас на {BAN_HOURS} часов."
            ),
            parse_mode="Markdown",
            reply_markup=warning_keyboard(),
        )
    else:
        start_ban_timer(user_id, chat_id, context.bot)
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "📢 *Требуется подписка*\n\n"
                f"Для использования бота необходимо подписаться на {CHANNEL}.\n\n"
                f"⚠️ Если вы выйдете из канала — вы немедленно получите блокировку на {BAN_HOURS} часов.\n\n"
                f"⏳ У вас *{WARN_SECS} секунд* для подписки.\n\n"
                "Подпишитесь и нажмите *«Я подписался»*."
            ),
            parse_mode="Markdown",
            reply_markup=warning_keyboard(),
        )
    return False


# ── Команды ───────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    _ensure_user(user_id)

    if not await check_access(user_id, chat_id, context):
        return

    _, used, is_premium = check_limit(user_id)
    status = (
        "👑 *Premium* — безлимитные запросы"
        if is_premium
        else f"🆓 *Бесплатный* — {used}/{FREE_LIMIT} сегодня"
    )
    await update.message.reply_text(
        "📥 *Video Downloader Bot*\n\n"
        "Отправьте ссылку на видео из любой соцсети:\n"
        "▸ YouTube  ▸ TikTok  ▸ Instagram\n"
        "▸ Twitter/X  ▸ VK  ▸ и 1000+ других\n\n"
        f"📊 Статус: {status}\n\n"
        "Команды: /help /requests /premium",
        parse_mode="Markdown",
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await check_access(user_id, update.effective_chat.id, context):
        return
    await update.message.reply_text(
        "📖 *Как пользоваться:*\n\n"
        "1. Отправьте ссылку на видео\n"
        "2. Выберите качество\n"
        "3. Получите файл\n\n"
        "👑 *Premium включает:*\n"
        "▸ Безлимитные запросы\n"
        "▸ Full HD 1080p\n"
        "▸ Нет блокировки за выход из канала\n\n"
        "📋 *Команды:*\n"
        "/start — главное меню\n"
        "/requests — статистика\n"
        "/premium — купить Premium\n"
        "/help — справка",
        parse_mode="Markdown",
    )


async def requests_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await check_access(user_id, update.effective_chat.id, context):
        return

    row = get_user_row(user_id)
    is_premium = bool(row[0])
    today = str(date.today())
    used_today = row[2] if row[1] == today else 0
    total_dl = row[3]
    joined = row[4]
    remaining = "∞" if is_premium else str(max(0, FREE_LIMIT - used_today))

    if is_premium:
        bar = "█" * 10
        limit_line = "Лимит: *безлимитно*"
    else:
        filled = int(used_today / FREE_LIMIT * 10) if FREE_LIMIT > 0 else 0
        bar = "█" * filled + "░" * (10 - filled)
        limit_line = f"Лимит: *{used_today}/{FREE_LIMIT}* сегодня"

    lines = [
        "📊 *Ваша статистика*\n",
        f"{'👑' if is_premium else '🆓'} Тариф: *{'Premium' if is_premium else 'Бесплатный'}*",
        f"📅 В боте с: *{joined or '—'}*",
        "",
        limit_line,
        f"`{bar}`",
        f"Осталось сегодня: *{remaining}*",
        f"📥 Всего загружено: *{total_dl}* видео",
    ]

    last = get_last_downloads(user_id, 5)
    if last:
        lines.append("\n🎬 *Последние загрузки:*")
        q_labels = {
            "best": "Лучшее", "1080": "1080p",
            "720": "720p", "480": "480p", "audio": "MP3",
        }
        for i, (fname, quality, size_mb, dl_at) in enumerate(last, 1):
            short = fname[:35] + "…" if len(fname) > 35 else fname
            ql = q_labels.get(quality, quality)
            lines.append(f"*{i}.* {short}\n   _{ql} • {size_mb:.1f} МБ • {dl_at[:16]}_")

    if not is_premium:
        lines.append("\n👑 Безлимит и 1080p — /premium")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await check_access(user_id, update.effective_chat.id, context):
        return

    _, _, is_premium = check_limit(user_id)
    if is_premium:
        await update.message.reply_text(
            "👑 *Premium активен.*\n\nБезлимитные загрузки и 1080p доступны.",
            parse_mode="Markdown",
        )
        return

    await update.message.reply_text(
        "👑 *Video Downloader Premium*\n\n"
        "🆓 *Бесплатный тариф:*\n"
        f"▸ {FREE_LIMIT} запросов в день\n"
        "▸ До 720p\n"
        "▸ Блокировка за выход из канала\n\n"
        "⭐ *Premium:*\n"
        "▸ Безлимитные запросы\n"
        "▸ Full HD 1080p\n"
        "▸ Без блокировок за выход из канала\n"
        "▸ Поддержка разработчика\n\n"
        f"Стоимость: *{PREMIUM_STARS} звёзд* Telegram — навсегда.",
        parse_mode="Markdown",
        reply_markup=premium_keyboard(),
    )


async def reports_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = S_DEV_PASS
    await update.message.reply_text("🔐 Введите пароль:")


# ── Обработчики сообщений ─────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = update.message.text.strip() if update.message.text else ""
    state = context.user_data.get('state')

    if state == S_DEV_PASS:
        context.user_data.pop('state', None)
        if text != DEV_PASSWORD:
            await update.message.reply_text("❌ Неверный пароль.")
            return
        reports = get_pending_reports()
        if not reports:
            await update.message.reply_text("✅ Нет ожидающих репортов.")
            return
        await update.message.reply_text(f"📋 *Репорты ({len(reports)}):*", parse_mode="Markdown")
        for r in reports:
            rid, ruid, rusername, rdesc, rphoto, rtime = r
            text_msg = (
                f"📋 *Репорт #{rid}*\n"
                f"👤 @{rusername or 'unknown'} (ID: `{ruid}`)\n"
                f"🕐 {rtime[:16]}\n\n"
                f"📝 {rdesc}"
            )
            kb = report_action_keyboard(rid)
            if rphoto:
                await context.bot.send_photo(
                    chat_id=chat_id, photo=rphoto,
                    caption=text_msg, parse_mode="Markdown", reply_markup=kb
                )
            else:
                await update.message.reply_text(text_msg, parse_mode="Markdown", reply_markup=kb)
        return

    if state == S_APPROVE_PASS:
        report_id = context.user_data.pop('approve_report_id', None)
        context.user_data.pop('state', None)
        if text != DEV_PASSWORD:
            await update.message.reply_text("❌ Неверный пароль. Операция отменена.")
            return
        if not report_id:
            await update.message.reply_text("❌ ID репорта потерян.")
            return
        report = get_report(report_id)
        if not report:
            await update.message.reply_text("❌ Репорт не найден.")
            return
        rid, ruid = report[0], report[1]
        update_report_status(rid, 'approved')
        unban_user_db(ruid)
        try:
            await context.bot.send_message(
                chat_id=ruid,
                text=(
                    "✅ *Ваш репорт одобрен.*\n\n"
                    "Блокировка снята. Вы снова можете использовать бота."
                ),
                parse_mode="Markdown",
            )
        except Exception:
            pass
        await update.message.reply_text(
            f"✅ Репорт #{rid} одобрен. Пользователь {ruid} (@{report[2] or '?'}) разблокирован."
        )
        return

    if state == S_REPORT_DESC:
        context.user_data['report_description'] = text
        context.user_data['state'] = S_REPORT_PHOTO
        await update.message.reply_text(
            "📸 Отправьте скриншот ошибки.\n"
            "Или напишите *«нет»*, если скриншота нет.",
            parse_mode="Markdown",
        )
        return

    if state == S_REPORT_PHOTO:
        username = update.effective_user.username or ""
        desc = context.user_data.pop('report_description', text)
        context.user_data.pop('state', None)
        report_id = save_report(user_id, username, desc, "")
        await update.message.reply_text(
            f"✅ *Репорт #{report_id} отправлен.*\n\nМы рассмотрим его в ближайшее время.",
            parse_mode="Markdown",
        )
        return

    url_match = URL_PATTERN.search(text)
    if not url_match:
        if not await check_access(user_id, chat_id, context):
            return
        await update.message.reply_text("Отправьте ссылку на видео.")
        return

    if not await check_access(user_id, chat_id, context):
        return

    can, used, is_premium = check_limit(user_id)
    if not can:
        await update.message.reply_text(
            f"⛔ *Лимит исчерпан.*\n\n"
            f"Загружено сегодня: *{used}/{FREE_LIMIT}*.\n\n"
            "Оформите Premium для безлимитных загрузок и 1080p.",
            parse_mode="Markdown",
            reply_markup=premium_keyboard(),
        )
        return

    url_key = store_url(url_match.group())
    await update.message.reply_text(
        "📥 Выберите качество:",
        reply_markup=quality_keyboard(url_key, is_premium),
    )


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    state = context.user_data.get('state')

    if state == S_REPORT_PHOTO:
        photo_file_id = update.message.photo[-1].file_id
        username = update.effective_user.username or ""
        desc = context.user_data.pop('report_description', 'Нет описания')
        context.user_data.pop('state', None)
        report_id = save_report(user_id, username, desc, photo_file_id)
        await update.message.reply_text(
            f"✅ *Репорт #{report_id} отправлен со скриншотом.*\n\nМы рассмотрим его в ближайшее время.",
            parse_mode="Markdown",
        )
        return

    if not await check_access(user_id, update.effective_chat.id, context):
        return
    await update.message.reply_text("Отправьте ссылку на видео, а не фото.")


# ── Скачивание ────────────────────────────────────────────────────────────────

async def download_video(url: str, quality: str, tmpdir: str) -> tuple[str | None, str | None]:
    output_template = os.path.join(tmpdir, "%(title).50s.%(ext)s")
    fmt_map = {
        "1080": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best[height<=1080]",
        "best": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "720":  "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]",
        "480":  "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best[height<=480]",
        "audio": "bestaudio/best",
    }
    cmd = [
        "yt-dlp", "--no-playlist", "--max-filesize", "49m",
        "-f", fmt_map.get(quality, "best"),
        "-o", output_template, "--merge-output-format", "mp4",
        "--no-warnings", "--quiet",
    ]
    if quality == "audio":
        cmd += ["-x", "--audio-format", "mp3", "--audio-quality", "192K"]
    cmd.append(url)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, cwd=tmpdir,
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="ignore")
            logger.error(f"yt-dlp: {err}")
            return None, err
        files = [f for f in Path(tmpdir).iterdir() if f.is_file()]
        if not files:
            return None, "Файл не найден"
        return str(max(files, key=lambda f: f.stat().st_size)), None
    except asyncio.TimeoutError:
        return None, "timeout"
    except Exception as e:
        logger.exception("Download error")
        return None, str(e)


def classify_error(error: str) -> str:
    e = error.lower()
    if "unsupported url" in e: return "Сайт не поддерживается."
    if "private" in e or "login" in e or "sign in" in e: return "Видео приватное или требует авторизации."
    if "filesize" in e or "file is larger" in e: return "Файл слишком большой (лимит 49 МБ)."
    if "copyright" in e or "blocked" in e: return "Видео заблокировано правообладателем."
    if "timeout" in e: return "Превышено время ожидания. Попробуйте ещё раз."
    if "404" in error or "not found" in e: return "Видео не найдено."
    return "Не удалось скачать видео."


# ── Callback-кнопки ───────────────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if query.data == "cancel":
        await query.edit_message_text("Отменено.")
        return

    if query.data == "check_sub":
        if await is_subscribed(context.bot, user_id):
            cancel_ban_timer(user_id)
            await query.edit_message_text(
                "✅ *Подписка подтверждена.*\n\nОтправьте ссылку на видео.",
                parse_mode="Markdown",
            )
        else:
            await query.answer(
                "Вы ещё не подписались. Подпишитесь и попробуйте снова.",
                show_alert=True,
            )
        return

    if query.data == "start_report":
        context.user_data['state'] = S_REPORT_DESC
        await query.edit_message_text(
            "📝 *Опишите проблему:*\n\nНапишите подробно, что произошло.",
            parse_mode="Markdown",
        )
        return

    if query.data == "buy_premium":
        row = get_user_row(user_id)
        if bool(row[0]):
            await query.edit_message_text("👑 Premium уже активен.")
            return
        await context.bot.send_invoice(
            chat_id=chat_id,
            title="Video Downloader Premium",
            description=(
                f"Безлимитные загрузки + Full HD 1080p навсегда. "
                f"Вместо {FREE_LIMIT} в день — без ограничений. Без блокировок."
            ),
            payload="premium_purchase",
            currency="XTR",
            prices=[LabeledPrice("Premium", PREMIUM_STARS)],
        )
        return

    if query.data.startswith("approve|"):
        report_id = int(query.data.split("|")[1])
        context.user_data['state'] = S_APPROVE_PASS
        context.user_data['approve_report_id'] = report_id
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🔐 Введите пароль для одобрения репорта #{report_id}:",
        )
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if query.data.startswith("decline|"):
        report_id = int(query.data.split("|")[1])
        update_report_status(report_id, 'declined')
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Репорт #{report_id} отклонён. Пользователь ожидает разблокировки.",
        )
        return

    parts = query.data.split("|", 1)
    if len(parts) != 2:
        await query.edit_message_text("Ошибка. Попробуйте снова.")
        return

    quality, url_key = parts
    url = url_storage.get(url_key)
    if not url:
        await query.edit_message_text("Ссылка устарела. Отправьте снова.")
        return

    if not await check_access(user_id, chat_id, context):
        return

    row = get_user_row(user_id)
    is_premium = bool(row[0])

    if quality == "1080" and not is_premium:
        await query.edit_message_text(
            "👑 *1080p доступно только в Premium.*\n/premium",
            parse_mode="Markdown",
        )
        return

    can, used, _ = check_limit(user_id)
    if not can:
        await query.edit_message_text(
            f"⛔ *Лимит исчерпан.*\n\nЗагружено: *{used}/{FREE_LIMIT}*\n\n/premium",
            parse_mode="Markdown",
        )
        return

    quality_names = {
        "1080": "Full HD 1080p", "best": "лучшем качестве",
        "720": "HD 720p", "480": "SD 480p", "audio": "аудио MP3",
    }
    await query.edit_message_text(
        f"⏳ Загрузка в {quality_names.get(quality, quality)}...\n_(до 2 минут)_",
        parse_mode="Markdown",
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path, error = await download_video(url, quality, tmpdir)
        if error or not file_path:
            await query.edit_message_text(
                f"❌ *Ошибка загрузки*\n\n{classify_error(error or '')}\n\nПопробуйте другую ссылку или качество.",
                parse_mode="Markdown",
            )
            return

        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if size_mb > 50:
            await query.edit_message_text(
                "❌ Файл слишком большой (>50 МБ). Попробуйте качество пониже.",
                parse_mode="Markdown",
            )
            return

        try:
            fname = Path(file_path).name
            new_count = increment_requests(user_id, url, quality, fname, size_mb)
            remaining = "∞" if is_premium else str(max(0, FREE_LIMIT - new_count))
            caption = (
                f"✅ Готово\n\n"
                f"📁 {fname[:50]}\n"
                f"💾 {size_mb:.1f} МБ\n"
                f"📊 Осталось запросов: *{remaining}*"
            )
            with open(file_path, "rb") as f:
                if quality == "audio" or file_path.endswith(".mp3"):
                    await context.bot.send_audio(
                        chat_id=chat_id, audio=f,
                        caption=caption, parse_mode="Markdown",
                    )
                else:
                    await context.bot.send_video(
                        chat_id=chat_id, video=f,
                        caption=caption, parse_mode="Markdown",
                        supports_streaming=True,
                    )
            await query.edit_message_text("✅ Готово.", parse_mode="Markdown")
        except Exception:
            logger.exception("Send error")
            await query.edit_message_text(
                "❌ Файл скачан, но не удалось отправить. Попробуйте снова.",
                parse_mode="Markdown",
            )


# ── Оплата ────────────────────────────────────────────────────────────────────

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.pre_checkout_query
    if q.invoice_payload == "premium_purchase":
        await q.answer(ok=True)
    else:
        await q.answer(ok=False, error_message="Неизвестный платёж.")


async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if update.message.successful_payment.invoice_payload == "premium_purchase":
        set_premium(user_id)
        cancel_ban_timer(user_id)
        unban_user_db(user_id)
        await update.message.reply_text(
            "✅ *Premium активирован.*\n\n"
            "▸ Безлимитные загрузки\n"
            "▸ Full HD 1080p\n"
            "▸ Без блокировок за выход из канала",
            parse_mode="Markdown",
        )


# ── Обнаружение выхода из канала ──────────────────────────────────────────────

async def handle_channel_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.chat_member:
        return
    chat = update.chat_member.chat
    if not chat.username or chat.username.lower() != CHANNEL.lstrip("@").lower():
        return

    new = update.chat_member.new_chat_member
    user = new.user

    if new.status in (ChatMember.LEFT, ChatMember.BANNED):
        row = get_user_row(user.id)
        if bool(row[0]):
            return
        user_id = user.id
        if user_id in pending_bans:
            return
        start_ban_timer(user_id, user_id, context.bot)
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"⚠️ *Вы покинули канал {CHANNEL}.*\n\n"
                    f"У вас *{WARN_SECS} секунд* чтобы вернуться,\n"
                    f"иначе доступ к боту будет закрыт на {BAN_HOURS} часов.\n\n"
                    "Подпишитесь снова и нажмите *«Я подписался»*."
                ),
                parse_mode="Markdown",
                reply_markup=warning_keyboard(),
            )
        except Exception as e:
            logger.warning(f"Can't warn {user_id}: {e}")


# ── Запуск ────────────────────────────────────────────────────────────────────

def main() -> None:
    init_db()
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("requests", requests_command))
    app.add_handler(CommandHandler("premium", premium_command))
    app.add_handler(CommandHandler("reports", reports_command))
    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(ChatMemberHandler(handle_channel_member, ChatMemberHandler.CHAT_MEMBER))

    logger.info("Bot is starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
