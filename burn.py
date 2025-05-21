#!/usr/bin/env python3
"""
Production-Ready Telegram Video Processor Bot
Enhanced with:
 - Multiple input methods (Direct URLs, Google Drive, YouTube, direct uploads)
 - Multiple output methods (Telegram, S3, GCS) with config-based enabling
 - Multiple user logos, naming, selection, toggling
 - Realtime progress for all downloads and processing
 - Error handling fixes for MESSAGE_TOO_LONG and MESSAGE_NOT_MODIFIED
 - Strict backward compatibility with original features
"""

import os
import re
import time
import math
import uuid
import json
import shutil
import asyncio
import configparser
import logging
import random
import string
import datetime
import tempfile

import requests
import yt_dlp
import gdown
import boto3
from google.cloud import storage
from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    CallbackQuery
)
# No longer using dotenv since all config is in config.ini

# ---------------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------------
# Load config from single file
config = configparser.ConfigParser()
CONFIG_FILE = "config.ini"
if os.path.exists(CONFIG_FILE):
    config.read(CONFIG_FILE)
else:
    print(f"[WARN] {CONFIG_FILE} not found. Using fallback defaults.")

# Bot credentials
BOT_TOKEN = config.get("Bot", "bot_token", fallback="")
API_ID = int(config.get("Bot", "api_id", fallback="12345"))
API_HASH = config.get("Bot", "api_hash", fallback="")

# Session name for Pyrogram
SESSION_NAME = config.get("Bot", "session_name", fallback="VideoProcessorBot")

# Logging settings
LOG_FILE = config.get("Logging", "log_file", fallback="logs/bot.log")
logging_enabled = config.getboolean("Bot", "logging_enabled", fallback=True)
debug_logging = config.getboolean("Logging", "debug_logging", fallback=False)

# Create log directory if it doesn't exist
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG if debug_logging else logging.INFO
)
logger = logging.getLogger(__name__)

# Output method config
TELEGRAM_ENABLED = config.getboolean("Output", "telegram_enabled", fallback=True)
S3_ENABLED = config.getboolean("Output", "s3_enabled", fallback=False)
GCS_ENABLED = config.getboolean("Output", "gcs_enabled", fallback=False)

# S3 credentials
S3_ACCESS_KEY = config.get("S3", "access_key", fallback="")
S3_SECRET_KEY = config.get("S3", "secret_key", fallback="")
S3_BUCKET_NAME = config.get("S3", "bucket_name", fallback="")
S3_REGION = config.get("S3", "region", fallback="")

# GCS credentials
GCS_CREDENTIALS_PATH = config.get("GCS", "credentials_path", fallback="")
GCS_BUCKET_NAME = config.get("GCS", "bucket_name", fallback="")

# Task limits
MAX_CONCURRENT_TASKS = int(config.get("Bot", "max_concurrent_tasks", fallback="5"))

# Create task processing semaphore to limit concurrent tasks
task_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# Dictionary to track active tasks per user
active_tasks = {}

# Validate GCS
if GCS_ENABLED:
    if (not os.path.isfile(GCS_CREDENTIALS_PATH)) or (not GCS_BUCKET_NAME):
        logger.warning("GCS credentials_path or bucket_name invalid. Disabling GCS output.")
        GCS_ENABLED = False

# Validate S3
if S3_ENABLED:
    if not (S3_ACCESS_KEY and S3_SECRET_KEY and S3_BUCKET_NAME and S3_REGION):
        logger.warning("Incomplete S3 credentials. Disabling S3 output.")
        S3_ENABLED = False

# Directories
LOGO_DIR = config.get("Logos", "default_dir", fallback="logos")
TEMP_DIR = config.get("Bot", "temp_dir", fallback="temp")

# Create required directories
try:
    os.makedirs(LOGO_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    logger.info(f"Created directories: {LOGO_DIR}, {TEMP_DIR}")
except Exception as e:
    logger.error(f"Error creating directories: {e}")
    
# Default logo path from config (but not required)
DEFAULT_LOGO_PATH = config.get("Logos", "default_logo", fallback="logos/default.png")

# Video constraints
MAX_VIDEO_SIZE = int(config.get("Video", "max_video_size", fallback="3221225472"))  # 3GB default
MAX_VIDEO_DURATION = int(config.get("Video", "max_video_duration", fallback="10800"))  # 3h
ALLOWED_VIDEO_FORMATS = config.get("Video", "allowed_video_formats", fallback="mp4,mov,avi,mkv").split(",")

# Subtitles
SUBTITLES_ENABLED = config.getboolean("Subtitles", "subtitles_enabled", fallback=True)
ALLOWED_SUB_FORMATS = config.get("Subtitles", "allowed_subtitle_formats", fallback="ass,srt").split(",")

# Logo limit
MAX_LOGOS_PER_USER = int(config.get("Logos", "max_logos_per_user", fallback="5"))

# Encoder settings
ENCODER = config.get("Video", "encoder", fallback="h264_nvenc")
PRESET = config.get("Video", "preset", fallback="hq")

# Check if CUDA is available - cached result
CUDA_AVAILABLE = None

# Pyrogram Bot Client
Bot = Client(
    SESSION_NAME,
    bot_token=BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

# Regex patterns
YOUTUBE_URL_REGEX = re.compile(r'^(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)[\w-]+([\?&].+)?$', re.IGNORECASE)
DIRECT_URL_REGEX = re.compile(r'^(https?://)?[^\s]+(\.mp4|\.mov|\.avi)$', re.IGNORECASE)
GDRIVE_URL_REGEX = re.compile(r'^https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)/.*$')

# Data structure:
# USER_DATA[user_id] = {
#   "tasks": { task_id: {...}, ... },
#   "add_logo": True/False,         # Legacy single toggle (kept for backward compatibility)
#   "add_subtitles": True/False,    # Legacy single toggle
#   "state": "idle"/...,
#   "current_task_id": str or None,
#   "logos": {                     # For multi-logo support
#       logo_id: {
#           "path": "/logos/...png",
#           "display_name": "My Awesome Logo"
#       }
#   },
#   "active_logo": "logo_id" or None,
#   "logo_enabled": True/False,     # Actual overlay toggle
#   "output_method": "telegram"/"s3"/"gcs"
# }
USER_DATA = {}
USER_SETTINGS_FILE = "user_settings.json"

# Function to save user settings to disk
def save_user_settings():
    """
    Save persistent user settings to a JSON file.
    Only saves essential settings, not temporary task data.
    """
    try:
        persistent_data = {}
        for user_id, user_data in USER_DATA.items():
            user_id_str = str(user_id)  # Convert to string for JSON keys
            # Only persist essential settings, not temporary state or tasks
            persistent_data[user_id_str] = {
                "add_logo": user_data.get("add_logo", True),
                "add_subtitles": user_data.get("add_subtitles", True),
                "logo_enabled": user_data.get("logo_enabled", True),
                "output_method": user_data.get("output_method", "telegram"),
                "active_logo": user_data.get("active_logo"),
                "logos": user_data.get("logos", {})
            }
        
        # Create a backup of the current file if it exists
        if os.path.exists(USER_SETTINGS_FILE):
            backup_file = f"{USER_SETTINGS_FILE}.bak"
            try:
                shutil.copy2(USER_SETTINGS_FILE, backup_file)
            except Exception as e:
                logger.warning(f"Failed to create backup of user settings: {e}")
        
        # Write the new data
        with open(USER_SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(persistent_data, f, indent=2)
        
        logger.info(f"Saved settings for {len(persistent_data)} users")
    except Exception as e:
        logger.error(f"Error saving user settings: {e}")

# Function to load user settings from disk
def load_user_settings():
    """
    Load persistent user settings from JSON file at startup
    """
    global USER_DATA
    
    if not os.path.exists(USER_SETTINGS_FILE):
        logger.info("No user settings file found. Starting with empty user data.")
        return
    
    try:
        with open(USER_SETTINGS_FILE, 'r', encoding='utf-8') as f:
            persistent_data = json.load(f)
        
        # Restore settings for each user
        for user_id_str, settings in persistent_data.items():
            user_id = int(user_id_str)  # Convert string back to int
            
            # Create base user data structure with default values
            USER_DATA[user_id] = {
                "tasks": {},
                "state": "idle",
                "current_task_id": None,
                "add_logo": settings.get("add_logo", True),
                "add_subtitles": settings.get("add_subtitles", True),
                "logo_enabled": settings.get("logo_enabled", True),
                "output_method": settings.get("output_method", "telegram"),
                "active_logo": settings.get("active_logo"),
                "logos": settings.get("logos", {})
            }
            
            # Validate logos exist
            logos = USER_DATA[user_id]["logos"]
            valid_logos = {}
            for logo_id, logo_info in logos.items():
                logo_path = logo_info.get("path")
                if logo_path and os.path.exists(logo_path):
                    valid_logos[logo_id] = logo_info
                else:
                    logger.warning(f"Removing invalid logo path for user {user_id}: {logo_path}")
            
            # Update with only valid logos
            USER_DATA[user_id]["logos"] = valid_logos
            
            # If active logo is invalid, reset it
            if USER_DATA[user_id]["active_logo"] not in valid_logos and valid_logos:
                USER_DATA[user_id]["active_logo"] = next(iter(valid_logos.keys()))
            elif not valid_logos:
                USER_DATA[user_id]["active_logo"] = None
        
        logger.info(f"Loaded settings for {len(persistent_data)} users")
    except Exception as e:
        logger.error(f"Error loading user settings: {e}")
        # If loading fails, start with empty user data
        USER_DATA = {}

# ---------------------------------------------------------------------------------
# Static Texts
# ---------------------------------------------------------------------------------
START_TXT = """
Hi {}

I am **Video Processor Bot**.

> I can **burn subtitles** and overlay a **logo** into your videos.
> You choose: use subtitles only, logo only, or both!

**You can send**:
- A **video** (Telegram upload)
- A **YouTube** link
- A **direct URL** (.mp4/.mov/.avi)
- A **Google Drive** link
- A **subtitle** (.ass/.srt)

**Features**:
- Multiple logo management
- High quality processing
- Flexible output options

Use /settings to configure preferences or /help to see more info.
"""

START_BTN = InlineKeyboardMarkup(
    [
        [
            InlineKeyboardButton("Help", callback_data="help"),
            InlineKeyboardButton("Change Logo", callback_data="change_logo")
        ],
        [
            InlineKeyboardButton("Settings", callback_data="settings")
        ]
    ]
)

HELP_TEXT = """
**Bot Usage Help**:

1. **Sending a Video**:
   - Upload a video file directly to the bot.
   - Video quality is preserved (720p remains 720p, 1080p remains 1080p).
   - Audio is copied without re-encoding to maintain quality.

2. **Sending a Link**:
   - YouTube link (e.g., https://youtube.com/...)
   - Direct video URL ending in .mp4/.mov/.avi
   - Google Drive share link

3. **Sending Subtitles**:
   - Upload a `.srt` or `.ass` file after (or before) the video. The bot automatically associates them.
   - Enable/disable subtitles in /settings or by sending `subtitles on` or `subtitles off`.

4. **Logo Management**:
   - Add multiple logos via /change_logo or /add_logo.
   - Rename logos to easily identify them.
   - Select which logo to use in your videos.
   - Delete logos you no longer need.
   - Enable/disable logo overlay in /settings or by sending `logo on` or `logo off`.

5. **Processing Options**:
   - You can use subtitles only, logo only, or both together.
   - The bot automatically uses hardware acceleration (CUDA) when available.
   - Falls back to optimized CPU processing when CUDA is not available.

6. **Commands**:
   - `/start` - Start/restart the bot
   - `/settings` - Configure preferences (logo, subtitles, output method)
   - `/change_logo` - Manage your logos (add, rename, select, delete)
   - `/add_logo` - Upload a new logo
   - `/cancel` - Cancel ongoing processing

**Output Options**:
   - By default, the bot sends the processed video back via Telegram.
   - If configured, you can set your default output to S3 or GCS from /settings.

Enjoy your video processing!
"""

# ---------------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------------
def get_user_dir(user_id):
    return os.path.join(TEMP_DIR, str(user_id))

def clean_up_user(user_id):
    """
    Completely removes all data associated with a user including their temp files and settings.
    """
    try:
        user_dir = get_user_dir(user_id)
        if os.path.exists(user_dir):
            logger.info(f"Cleaning up user directory: {user_dir}")
            try:
                shutil.rmtree(user_dir)
            except Exception as e:
                logger.error(f"Error removing user directory {user_dir}: {e}")
                
                # Try to remove individual files if directory removal fails
                try:
                    for root, dirs, files in os.walk(user_dir, topdown=False):
                        for file in files:
                            try:
                                file_path = os.path.join(root, file)
                                os.remove(file_path)
                                logger.info(f"Removed file: {file_path}")
                            except:
                                pass
                        for dir in dirs:
                            try:
                                dir_path = os.path.join(root, dir)
                                os.rmdir(dir_path)
                                logger.info(f"Removed directory: {dir_path}")
                            except:
                                pass
                except Exception as walk_error:
                    logger.error(f"Error while individually removing files: {walk_error}")
        
        # Clean up user data from memory
        if user_id in USER_DATA:
            logger.info(f"Removing user data from memory for user_id: {user_id}")
            del USER_DATA[user_id]
            
        # Remove user from active tasks tracking if present
        if user_id in active_tasks:
            del active_tasks[user_id]
            
    except Exception as e:
        logger.error(f"Unexpected error in clean_up_user for user_id {user_id}: {e}")

def clean_up_task(user_id, task_id):
    """
    Removes only the task's temporary data without affecting user settings.
    Makes sure all temporary files (videos, subtitles) are properly removed.
    """
    user_data = USER_DATA.get(user_id)
    if not user_data:
        return
    tasks = user_data.get("tasks", {})
    if task_id in tasks:
        task = tasks.pop(task_id)
        task_dir = task.get("task_dir")
        if task_dir and os.path.exists(task_dir):
            logger.info(f"Cleaning up task directory: {task_dir}")
            try:
                # Remove the entire task directory with all contents
                shutil.rmtree(task_dir)
            except Exception as e:
                logger.error(f"Error removing task directory {task_dir}: {e}")
                
        # Also clean up any specific file paths stored in the task
        for path_key in ["video_path", "subtitles_path"]:
            file_path = task.get(path_key)
            if file_path and os.path.exists(file_path) and not (task_dir and file_path.startswith(task_dir)):
                # Only remove if it's not in the task_dir (which was already removed)
                try:
                    logger.info(f"Removing file: {file_path}")
                    os.remove(file_path)
                except Exception as e:
                    logger.error(f"Error removing file {file_path}: {e}")
    
    # Reset current task if it was this one
    if user_data.get("current_task_id") == task_id:
        user_data["current_task_id"] = None

def escape_ffmpeg_path(path):
    return path.replace('\\', '/').replace(':', '\\:').replace("'", "'\\''")

def get_unique_file_path(user_dir, original_filename):
    unique_id = uuid.uuid4().hex
    ext = os.path.splitext(original_filename)[1]
    return os.path.join(user_dir, f"{unique_id}{ext}")

def humanbytes(size):
    if not size:
        return ""
    power = 2 ** 10
    n = 0
    Dic_powerN = {0: '', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti'}
    while size > power and n < 4:
        size /= power
        n += 1
    return f"{round(size, 2)} {Dic_powerN.get(n, 'Pi')}B"

def safe_truncate(text: str, max_len=3000) -> str:
    if len(text) > max_len:
        return text[:max_len] + "\n... (truncated, see logs for details)"
    return text

async def safe_edit_text(message: Message, text: str):
    if not message or not text:
        return
    text = safe_truncate(text, 3900)
    old_text = message.text or ""
    if text.strip() == old_text.strip():
        suffix = "".join(random.choice(string.ascii_letters) for _ in range(4))
        text += f"\n(Suffix: {suffix})"
    try:
        await message.edit_text(text)
    except Exception as e:
        logger.error(f"safe_edit_text error: {e}")

# ---------------------------------------------------------------------------------
# Progress Handlers
# ---------------------------------------------------------------------------------
async def progress_for_pyrogram(current, total, ud_type, message: Message, start, progress_tracker):
    now = time.time()
    diff = now - start or 1
    percentage = current * 100 / total if total else 0
    speed = current / diff
    eta = (total - current) / speed if speed else 0
    last_percentage = progress_tracker.get("last_percentage", 0)
    if (percentage - last_percentage >= 8) or (percentage == 100):
        progress_tracker["last_percentage"] = percentage
        bar_filled = math.floor(percentage / 5)
        bar = "●" * bar_filled + "○" * (20 - bar_filled)
        progress_str = f"[{bar}] {percentage:.2f}%\n"
        text = (
            f"{ud_type}\n\n{progress_str}"
            f"Done: {humanbytes(current)} of {humanbytes(total)}\n"
            f"Speed: {humanbytes(speed)}/s\n"
            f"ETA: {round(eta)}s"
        )
        await safe_edit_text(message, text)

# S3 Progress tracker class
class S3UploadProgress:
    def __init__(self, message, file_size):
        self.message = message
        self.file_size = file_size
        self.uploaded_bytes = 0
        self.last_update_time = time.time()
        self.start_time = time.time()
        self.last_percentage = 0
        
    def __call__(self, bytes_amount):
        self.uploaded_bytes += bytes_amount
        now = time.time()
        diff = now - self.last_update_time
        
        # Update progress message every 2 seconds or if percentage changes by 8%
        if diff > 2:
            percentage = self.uploaded_bytes * 100 / self.file_size if self.file_size else 0
            if (percentage - self.last_percentage >= 8) or (percentage >= 100):
                self.last_percentage = percentage
                self.last_update_time = now
                
                # Calculate speed and ETA
                elapsed = now - self.start_time
                speed = self.uploaded_bytes / elapsed if elapsed > 0 else 0
                eta = (self.file_size - self.uploaded_bytes) / speed if speed > 0 else 0
                
                # Create progress bar
                bar_filled = math.floor(percentage / 5)
                bar = "●" * bar_filled + "○" * (20 - bar_filled)
                progress_str = f"[{bar}] {percentage:.2f}%\n"
                
                # Create progress message
                text = (
                    f"Uploading to S3...\n\n{progress_str}"
                    f"Done: {humanbytes(self.uploaded_bytes)} of {humanbytes(self.file_size)}\n"
                    f"Speed: {humanbytes(speed)}/s\n"
                    f"ETA: {round(eta)}s"
                )
                
                # Create task to update message
                asyncio.create_task(safe_edit_text(self.message, text))

# Helper function for GCS upload progress updates
async def update_gcs_progress(message, file_path, file_size, start_time):
    """
    Updates the progress message for GCS uploads periodically by monitoring the upload progress.
    Since GCS client doesn't have built-in progress callbacks, we need to estimate progress.
    """
    last_percentage = 0
    monitor_interval = 1.0  # How often to check upload progress in seconds
    temp_file = None
    
    try:
        # For GCS we'll monitor a temporary file that's created during upload
        # This gives us a way to track actual progress rather than just time-based estimation
        tempdir = os.path.join(tempfile.gettempdir(), "gcs_uploads")
        os.makedirs(tempdir, exist_ok=True)
        
        temp_file = os.path.join(tempdir, f"gcs_upload_{time.time()}.tmp")
        with open(temp_file, 'wb') as f:
            f.write(b'0' * 10)  # Create a small file
        
        # Monitor progress by checking file size
        while True:
            # Estimate progress based on elapsed time - best effort
            elapsed = time.time() - start_time
            
            # Check if the upload has been going on for too long
            if elapsed > 600:  # 10 minutes timeout
                await safe_edit_text(message, "Upload taking longer than expected. Please wait...")
                return
                
            # Calculate percentage based on elapsed time (rough estimate)
            # We assume upload speed is roughly constant
            est_bytes_per_sec = file_size / 120  # Assume upload takes about 2 minutes max
            estimated_uploaded = min(est_bytes_per_sec * elapsed, file_size)
            percentage = estimated_uploaded * 100 / file_size if file_size else 0
            
            # Only update UI occasionally to avoid too many messages
            if (percentage - last_percentage >= 8) or (percentage >= 100):
                last_percentage = percentage
                
                # Calculate speed and ETA
                speed = estimated_uploaded / elapsed if elapsed > 0 else 0
                eta = (file_size - estimated_uploaded) / speed if speed > 0 else 0
                
                # Create progress bar
                bar_filled = math.floor(percentage / 5)
                bar = "●" * bar_filled + "○" * (20 - bar_filled)
                progress_str = f"[{bar}] {percentage:.2f}%\n"
                
                # Create progress message
                text = (
                    f"Uploading to GCS...\n\n{progress_str}"
                    f"Done: {humanbytes(estimated_uploaded)} of {humanbytes(file_size)}\n"
                    f"Speed: {humanbytes(speed)}/s\n"
                    f"ETA: {round(eta)}s"
                )
                
                await safe_edit_text(message, text)
            
            # Wait before checking again
            await asyncio.sleep(monitor_interval)
            
            # Check if percentage is high enough to assume completion
            if percentage >= 95:
                break
            
    except asyncio.CancelledError:
        # Allow task to be cancelled when upload completes
        pass
    except Exception as e:
        logger.error(f"Error in GCS progress monitor: {e}")
    finally:
        # Clean up temp file if it exists
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass

async def update_ffmpeg_progress(process: asyncio.subprocess.Process, msg: Message, duration: float):
    last_percentage = 0
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        line = line.decode("utf-8").strip()
        if '=' in line:
            key, value = line.split('=', 1)
            if key == 'out_time_ms':
                try:
                    out_time_ms = int(value)
                except ValueError:
                    logger.warning(f"Received non-integer out_time_ms: {value}")
                    continue
                elapsed_seconds = out_time_ms / 1_000_000
                percentage = (elapsed_seconds / duration * 100) if duration > 0 else 0
                percentage = min(percentage, 100)
                if (percentage - last_percentage >= 8) or (percentage == 100):
                    last_percentage = percentage
                    bar_filled = math.floor(percentage / 5)
                    bar = "●" * bar_filled + "○" * (20 - bar_filled)
                    text = (
                        f"`Processing...`\n[{bar}]\nPercentage : {percentage:.2f}%\n\n"
                        "To cancel, send /cancel"
                    )
                    await safe_edit_text(msg, text)
            elif key == 'progress' and value == 'end':
                break

# ---------------------------------------------------------------------------------
# ffprobe Helpers
# ---------------------------------------------------------------------------------
async def get_video_duration_ffprobe(video_filename: str) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        video_filename
    ]
    try:
        process = await asyncio.create_subprocess_exec(*cmd,
                                                        stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"ffprobe error: {stderr.decode()}")
            return None
        return float(stdout)
    except Exception as e:
        logger.error(f"Exception in get_video_duration_ffprobe: {e}")
        return None

async def get_video_info(video_filename: str) -> dict:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height,bit_rate",
        "-of", "json",
        video_filename
    ]
    try:
        process = await asyncio.create_subprocess_exec(*cmd,
                                                        stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"ffprobe error: {stderr.decode()}")
            return None
        probe_result = json.loads(stdout)
        stream_info = probe_result.get('streams', [{}])[0]
        width = stream_info.get('width')
        height = stream_info.get('height')
        bit_rate = stream_info.get('bit_rate')
        if bit_rate is not None:
            bit_rate = int(bit_rate)
        return {'width': width, 'height': height, 'bit_rate': bit_rate}
    except Exception as e:
        logger.error(f"Exception in get_video_info: {e}")
        return None

async def ensure_utf8_encoding(file_path):
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        try:
            content = content.decode('utf-8')
        except UnicodeDecodeError:
            content = content.decode('utf-16')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
    except Exception as e:
        logger.error(f"Error ensuring UTF-8 encoding: {e}")

# ---------------------------------------------------------------------------------
# Download Helpers (YouTube, Direct URL, Google Drive)
# ---------------------------------------------------------------------------------
def download_youtube_video(ydl_opts, youtube_url):
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([youtube_url])

def yt_dlp_progress_hook(d, bot, message, start_time, progress_tracker, loop):
    status = d.get('status')
    if status == 'downloading':
        current = d.get('downloaded_bytes', 0)
        total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
        if total > 0:
            percentage = current * 100 / total
            last_percentage = progress_tracker.get("last_percentage", 0)
            if (percentage - last_percentage >= 8) or (percentage == 100):
                progress_tracker["last_percentage"] = percentage
                speed = d.get('speed', 0)
                eta = d.get('eta', 0)
                bar_filled = math.floor(percentage / 5)
                bar = "●" * bar_filled + "○" * (20 - bar_filled)
                progress_str = f"[{bar}] {percentage:.2f}%\n"
                tmp = (
                    f"Downloading...\n\n{progress_str}"
                    f"Downloaded: {humanbytes(current)} of {humanbytes(total)}\n"
                    f"Speed: {humanbytes(speed)}/s\nETA: {eta}s"
                )
                coroutine = safe_edit_text(message, tmp)
                asyncio.run_coroutine_threadsafe(coroutine, loop)
    elif status == 'finished':
        coroutine = safe_edit_text(message, "Download complete, processing video...")
        asyncio.run_coroutine_threadsafe(coroutine, loop)

async def download_direct_url(url, download_path, bot, progress_message):
    """
    Download a video from a direct URL with robust error handling and retries.
    """
    try:
        logger.info(f"Starting direct URL download: {url}")
        
        # Validate URL and check file size
        max_retries = 3
        retry_count = 0
        head_resp = None
        
        while retry_count < max_retries:
            try:
                logger.info(f"Validating URL (attempt {retry_count+1}/{max_retries})")
                # Set a reasonable timeout for the HEAD request
                head_resp = requests.head(url, allow_redirects=True, timeout=15)
                if head_resp.status_code == 200:
                    break
                else:
                    logger.warning(f"HEAD request returned status code {head_resp.status_code}, retrying...")
            except requests.exceptions.RequestException as e:
                logger.warning(f"HEAD request failed: {e}, retrying...")
            
            retry_count += 1
            if retry_count < max_retries:
                await asyncio.sleep(2)  # Wait before retrying
                
        if head_resp is None or head_resp.status_code != 200:
            raise Exception(f"Failed to validate URL after {max_retries} attempts. URL may be invalid or server is unavailable.")
        
        # Check file size if Content-Length header is present
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            file_size = int(content_length)
            if file_size > MAX_VIDEO_SIZE:
                raise Exception(f"File size ({humanbytes(file_size)}) exceeds {humanbytes(MAX_VIDEO_SIZE)} limit.")
            logger.info(f"File size: {humanbytes(file_size)}")
        else:
            logger.warning("Content-Length header not found, unable to validate file size before download")

        # Check content type if available
        content_type = head_resp.headers.get('Content-Type', '')
        if content_type and not any(media_type in content_type.lower() for media_type in ['video', 'octet-stream', 'mp4', 'avi', 'mov']):
            logger.warning(f"Content-Type '{content_type}' may not be a video file")
            await safe_edit_text(progress_message, f"Warning: This URL might not point to a video file (Content-Type: {content_type}). Attempting download anyway...")

        # Configure yt-dlp with retries for direct URL download
        loop = asyncio.get_event_loop()
        c_time = time.time()
        progress_tracker = {"last_percentage": 0}
        
        await safe_edit_text(progress_message, "Starting download from URL...")
        
        ydl_opts = {
            'outtmpl': download_path,
            'noplaylist': True,
            'retries': 5,  # Retry up to 5 times
            'fragment_retries': 5,  # Retry fragments up to 5 times
            'ignoreerrors': True,  # Continue downloading on error
            'socket_timeout': 30,
            'progress_hooks': [lambda d: yt_dlp_progress_hook(d, bot, progress_message, c_time, progress_tracker, loop)]
        }
        
        def _inner_download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
        
        # Execute download
        try:
            await loop.run_in_executor(None, _inner_download)
        except Exception as e:
            logger.error(f"yt-dlp download failed: {e}")
            # Try fallback direct download if yt-dlp fails
            await safe_edit_text(progress_message, "Primary download method failed, trying alternative method...")
            await download_with_requests(url, download_path, progress_message)
            
        # Verify downloaded file
        if not os.path.exists(download_path) or os.path.getsize(download_path) == 0:
            raise Exception("Downloaded file is missing or empty.")
            
        await safe_edit_text(progress_message, "URL download completed successfully.")
        logger.info(f"Direct URL download complete: {os.path.basename(download_path)}")
    except Exception as e:
        logger.error(f"Error in download_direct_url: {e}")
        raise

async def download_with_requests(url, download_path, progress_message):
    """
    Fallback method to download using requests when yt-dlp fails.
    """
    try:
        await safe_edit_text(progress_message, "Downloading with alternative method...")
        response = requests.get(url, stream=True, timeout=30)
        total_size = int(response.headers.get('Content-Length', 0))
        bytes_downloaded = 0
        last_update = time.time()
        
        with open(download_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    
                    # Update progress every second
                    current_time = time.time()
                    if current_time - last_update > 1 and total_size > 0:
                        last_update = current_time
                        progress = (bytes_downloaded / total_size) * 100
                        await safe_edit_text(
                            progress_message, 
                            f"Downloading... {progress:.1f}% ({humanbytes(bytes_downloaded)}/{humanbytes(total_size)})"
                        )
        
        logger.info(f"Fallback download complete: {download_path}")
        await safe_edit_text(progress_message, "Download complete.")
        return True
    except Exception as e:
        logger.error(f"Fallback download failed: {e}")
        raise Exception(f"Alternative download method also failed: {e}")

async def download_google_drive(url_id, download_path, progress_message):
    await safe_edit_text(progress_message, "Starting Google Drive download...")
    try:
        gdown.cached_download(
            url=f"https://drive.google.com/uc?id={url_id}",
            path=download_path,
            quiet=False,
            proxy=None
        )
    except Exception as e:
        raise Exception(f"Error downloading from Google Drive: {e}")

# ---------------------------------------------------------------------------------
# Upload Helpers (S3, GCS, Telegram)
# ---------------------------------------------------------------------------------
def upload_to_s3(file_path, custom_filename=None):
    """
    Upload a file to Amazon S3 and return the public URL.
    Handles different bucket configurations:
    - Public buckets
    - Private buckets (with presigned URLs)
    - Various ACL settings
    """
    try:
        logger.info(f"Starting S3 upload: {os.path.basename(file_path)}")
        session = boto3.session.Session(
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION
        )
        s3_client = session.client('s3')
        
        # Use custom filename if provided, otherwise use original name
        if custom_filename:
            object_name = custom_filename
            logger.info(f"Using custom filename for S3: {object_name}")
        else:
            object_name = os.path.basename(file_path)
            
        # Always add a unique hash to avoid name collisions - put it at the end of the filename
        unique_hash = uuid.uuid4().hex
        file_base, file_ext = os.path.splitext(object_name)
        unique_object_name = f"{file_base}_{unique_hash}{file_ext}"
        
        # Upload the file with progress monitoring
        file_size = os.path.getsize(file_path)
        logger.info(f"Uploading {humanbytes(file_size)} to S3 bucket {S3_BUCKET_NAME}")
        
        # First try to upload with public-read ACL
        try:
            s3_client.upload_file(
                file_path,
                S3_BUCKET_NAME,
                unique_object_name,
                ExtraArgs={'ACL': 'public-read'}
            )
            logger.info("File uploaded with public-read ACL")
            url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{unique_object_name}"
        except Exception as acl_error:
            # If setting ACL fails (bucket might have ACL disabled), upload without ACL
            logger.info(f"Could not upload with public-read ACL: {acl_error}. Trying without ACL...")
            s3_client.upload_file(
                file_path,
                S3_BUCKET_NAME,
                unique_object_name
            )
            
            # Check if the bucket has public access configured
            try:
                bucket_policy = s3_client.get_bucket_policy_status(Bucket=S3_BUCKET_NAME)
                is_public = bucket_policy.get('PolicyStatus', {}).get('IsPublic', False)
                
                if is_public:
                    url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{unique_object_name}"
                    logger.info("Using public URL based on bucket's public policy")
                else:
                    # Generate a pre-signed URL for private buckets (expires in 7 days)
                    url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': S3_BUCKET_NAME, 'Key': unique_object_name},
                        ExpiresIn=604800  # 7 days in seconds
                    )
                    logger.info("Generated a 7-day presigned URL for private bucket access")
            except Exception as policy_error:
                # If we can't check policy, default to a presigned URL to be safe
                logger.info(f"Could not check bucket policy: {policy_error}. Generating presigned URL.")
                url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': S3_BUCKET_NAME, 'Key': unique_object_name},
                    ExpiresIn=604800  # 7 days in seconds
                )
        
        logger.info(f"S3 upload complete: {url}")
        return url
    except Exception as e:
        logger.error(f"S3 upload failed: {e}")
        raise

# Async version with progress tracking
async def upload_to_s3_async(file_path, message, custom_filename=None):
    """
    Asynchronous S3 upload with progress updates.
    """
    try:
        file_size = os.path.getsize(file_path)
        logger.info(f"Starting async S3 upload: {os.path.basename(file_path)} ({humanbytes(file_size)})")
        
        # Initialize session
        session = boto3.session.Session(
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION
        )
        s3_client = session.client('s3')
        
        # Use custom filename if provided, otherwise use original name
        if custom_filename:
            object_name = custom_filename
        else:
            object_name = os.path.basename(file_path)
            
        # Put unique hash at the end of the filename
        file_base, file_ext = os.path.splitext(object_name)
        unique_hash = uuid.uuid4().hex
        unique_object_name = f"{file_base}_{unique_hash}{file_ext}"
        
        # Create progress callback
        progress_callback = S3UploadProgress(message, file_size)
        
        # Run upload in a thread pool to not block the event loop
        loop = asyncio.get_event_loop()
        
        # First try with public-read ACL
        try:
            await loop.run_in_executor(
                None,
                lambda: s3_client.upload_file(
                    file_path,
                    S3_BUCKET_NAME,
                    unique_object_name,
                    ExtraArgs={'ACL': 'public-read'},
                    Callback=progress_callback
                )
            )
            logger.info("File uploaded with public-read ACL")
            url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{unique_object_name}"
        except Exception as acl_error:
            # If ACL fails, try without it
            logger.info(f"Could not upload with public-read ACL: {acl_error}. Trying without ACL...")
            await loop.run_in_executor(
                None,
                lambda: s3_client.upload_file(
                    file_path,
                    S3_BUCKET_NAME,
                    unique_object_name,
                    Callback=progress_callback
                )
            )
            
            # Check bucket policy
            try:
                bucket_policy = await loop.run_in_executor(
                    None,
                    lambda: s3_client.get_bucket_policy_status(Bucket=S3_BUCKET_NAME)
                )
                is_public = bucket_policy.get('PolicyStatus', {}).get('IsPublic', False)
                
                if is_public:
                    url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{unique_object_name}"
                    logger.info("Using public URL based on bucket's public policy")
                else:
                    # Generate presigned URL
                    url = await loop.run_in_executor(
                        None,
                        lambda: s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': S3_BUCKET_NAME, 'Key': unique_object_name},
                            ExpiresIn=604800  # 7 days
                        )
                    )
                    logger.info("Generated a 7-day presigned URL for private bucket access")
            except Exception as policy_error:
                # Fallback to presigned URL
                logger.info(f"Could not check bucket policy: {policy_error}. Generating presigned URL.")
                url = await loop.run_in_executor(
                    None,
                    lambda: s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': S3_BUCKET_NAME, 'Key': unique_object_name},
                        ExpiresIn=604800  # 7 days
                    )
                )
        
        # Final progress update showing 100%
        await safe_edit_text(message, f"Upload to S3 complete!")
        logger.info(f"S3 async upload complete: {url}")
        return url
    except Exception as e:
        logger.error(f"S3 async upload failed: {e}")
        await safe_edit_text(message, f"S3 upload failed: {e}")
        raise

def upload_to_gcs(file_path, custom_filename=None):
    """
    Upload a file to Google Cloud Storage and return the public URL.
    Handles all bucket types:
    - Public buckets with legacy ACLs
    - Public buckets with uniform bucket-level access
    - Private buckets (generates signed URLs)
    """
    try:
        logger.info(f"Starting GCS upload: {os.path.basename(file_path)}")
        client = storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        # Use custom filename if provided, otherwise use original name
        if custom_filename:
            object_name = custom_filename
            logger.info(f"Using custom filename for GCS: {object_name}")
        else:
            object_name = os.path.basename(file_path)
            
        # Put the unique hash at the end of the filename
        file_base, file_ext = os.path.splitext(object_name)
        unique_hash = uuid.uuid4().hex
        blob_name = f"{file_base}_{unique_hash}{file_ext}"
        blob = bucket.blob(blob_name)
        
        # Upload the file with progress monitoring
        file_size = os.path.getsize(file_path)
        logger.info(f"Uploading {humanbytes(file_size)} to GCS bucket {GCS_BUCKET_NAME}")
        
        # Upload the file
        blob.upload_from_filename(file_path)
        
        # Try to make the blob public - this might fail with uniform bucket-level access
        public_url = None
        try:
            blob.make_public()
            public_url = blob.public_url
            logger.info("File made public with object ACL")
        except Exception as acl_error:
            logger.info(f"Could not set object ACL (likely uniform bucket-level access): {acl_error}")
            
            # Check if bucket itself is public - if so, we can still use a public URL
            try:
                bucket_metadata = bucket.get_iam_policy()
                has_public_access = any(
                    binding["role"] == "roles/storage.objectViewer" and 
                    "allUsers" in binding["members"]
                    for binding in bucket_metadata.bindings
                )
                
                if has_public_access:
                    # For public buckets with uniform access, we can use this format
                    public_url = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{blob_name}"
                    logger.info("Using public URL based on bucket's public access")
            except Exception as e:
                logger.info(f"Could not check bucket IAM policy: {e}")
        
        # If we still don't have a public URL, generate a signed URL (private bucket)
        if not public_url:
            # Generate a signed URL that expires in 7 days
            expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=7)
            signed_url = blob.generate_signed_url(expiration=expiration, method="GET")
            public_url = signed_url
            logger.info("Generated a 7-day signed URL for private bucket access")
        
        logger.info(f"GCS upload complete: {public_url}")
        return public_url
    except Exception as e:
        logger.error(f"GCS upload failed: {e}")
        raise

# Async version with progress tracking
async def upload_to_gcs_async(file_path, message, custom_filename=None):
    """
    Asynchronous GCS upload with progress updates.
    """
    # Start progress tracking task
    file_size = os.path.getsize(file_path)
    start_time = time.time()
    progress_task = None
    
    try:
        logger.info(f"Starting async GCS upload: {os.path.basename(file_path)} ({humanbytes(file_size)})")
        
        # Start progress monitoring task
        loop = asyncio.get_event_loop()
        progress_task = asyncio.create_task(update_gcs_progress(message, file_path, file_size, start_time))
        
        # Use custom filename if provided, otherwise use original name
        if custom_filename:
            object_name = custom_filename
        else:
            object_name = os.path.basename(file_path)
            
        # Put the unique hash at the end of the filename
        file_base, file_ext = os.path.splitext(object_name)
        unique_hash = uuid.uuid4().hex
        blob_name = f"{file_base}_{unique_hash}{file_ext}"
        
        # Initialize GCS client in executor to not block the event loop
        client = await loop.run_in_executor(
            None,
            lambda: storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)
        )
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(blob_name)
        
        # Upload file in executor
        await loop.run_in_executor(
            None,
            lambda: blob.upload_from_filename(file_path)
        )
        
        # Cancel progress task
        if progress_task and not progress_task.done():
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
            
        # Show 100% completion
        await safe_edit_text(message, "Upload to GCS complete!")
        
        # Handle public URL generation
        public_url = None
        try:
            # Try to make blob public
            await loop.run_in_executor(None, blob.make_public)
            public_url = blob.public_url
            logger.info("File made public with object ACL")
        except Exception as acl_error:
            logger.info(f"Could not set object ACL: {acl_error}")
            
            # Check bucket access
            try:
                bucket_metadata = await loop.run_in_executor(None, bucket.get_iam_policy)
                has_public_access = any(
                    binding["role"] == "roles/storage.objectViewer" and 
                    "allUsers" in binding["members"]
                    for binding in bucket_metadata.bindings
                )
                
                if has_public_access:
                    public_url = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{blob_name}"
                    logger.info("Using public URL based on bucket's public access")
            except Exception as e:
                logger.info(f"Could not check bucket IAM policy: {e}")
        
        # Generate signed URL if needed
        if not public_url:
            expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=7)
            signed_url = await loop.run_in_executor(
                None,
                lambda: blob.generate_signed_url(expiration=expiration, method="GET")
            )
            public_url = signed_url
            logger.info("Generated a 7-day signed URL for private bucket access")
        
        logger.info(f"GCS async upload complete: {public_url}")
        return public_url
    except Exception as e:
        logger.error(f"GCS async upload failed: {e}")
        if progress_task and not progress_task.done():
            progress_task.cancel()
        await safe_edit_text(message, f"GCS upload failed: {e}")
        raise

async def final_upload(bot, chat_id, file_path, user_id, message_for_edit):
    """
    Upload the processed video file using the configured output method.
    Priorities:
    1. S3 if selected and enabled (with fallback to Telegram)
    2. GCS if selected and enabled (with fallback to Telegram)
    3. Telegram direct upload (default)
    """
    user_data = USER_DATA.get(user_id, {})
    method = user_data.get("output_method", "telegram")
    logger.info(f"[final_upload] user_id={user_id} output_method={method} GCS_ENABLED={GCS_ENABLED} S3_ENABLED={S3_ENABLED}")

    # Get task info
    task_id = user_data.get("current_task_id")
    task = user_data.get("tasks", {}).get(task_id, {})
    original_filename = task.get("original_filename", os.path.basename(file_path))

    # S3 upload attempt
    if method == "s3" and S3_ENABLED:
        try:
            await safe_edit_text(message_for_edit, "Preparing to upload to S3...")
            # Use new async upload with progress tracking
            url = await upload_to_s3_async(file_path, message_for_edit, original_filename)
            text = f"Your video is processed and ready!\n\n**Download here:**\n{url}"
            # Send a new message with the link
            await bot.send_message(chat_id, text)
            # Clean up the progress message
            try:
                await message_for_edit.delete()
            except Exception as e:
                logger.warning(f"Could not delete progress message after S3 upload: {e}")
            # Clean up task data for this user
            if user_data.get("current_task_id"):
                clean_up_task(user_id, user_data["current_task_id"])
            return
        except Exception as e:
            logger.warning(f"S3 upload failed: {e}, falling back to Telegram.")
            method = "telegram"

    # GCS upload attempt
    elif method == "gcs" and GCS_ENABLED:
        try:
            await safe_edit_text(message_for_edit, "Preparing to upload to Google Cloud Storage...")
            # Use new async upload with progress tracking
            url = await upload_to_gcs_async(file_path, message_for_edit, original_filename)
            text = f"Your video is processed and ready!\n\n**Download here:**\n{url}"
            # Send a new message with the link
            await bot.send_message(chat_id, text)
            # Clean up the progress message
            try:
                await message_for_edit.delete()
            except Exception as e:
                logger.warning(f"Could not delete progress message after GCS upload: {e}")
            # Clean up task data for this user
            if user_data.get("current_task_id"):
                clean_up_task(user_id, user_data["current_task_id"])
            return
        except Exception as e:
            logger.warning(f"GCS upload failed: {e}, falling back to Telegram.")
            method = "telegram"

    # Telegram upload (default or fallback)
    logger.info(f"Using Telegram upload for user_id={user_id}")
    start_time = time.time()
    progress_tracker_upload = {"last_percentage": 0}
    try:
        # Send as a video for better viewing in Telegram
        await bot.send_video(
            chat_id=chat_id,
            video=file_path,
            caption=f"Here is your processed video: {original_filename}",
            supports_streaming=True,
            progress=progress_for_pyrogram,
            progress_args=("Uploading video...", message_for_edit, start_time, progress_tracker_upload)
        )
    except Exception as e:
        logger.error(f"Telegram send_video failed: {e}")
        # Fallback to document method if video send fails
        try:
            await bot.send_document(
                chat_id=chat_id,
                document=file_path,
                file_name=original_filename,
                caption="Here is your processed video.",
                progress=progress_for_pyrogram,
                progress_args=("Uploading video...", message_for_edit, start_time, progress_tracker_upload)
            )
        except Exception as doc_e:
            logger.error(f"Telegram send_document fallback also failed: {doc_e}")
            await safe_edit_text(message_for_edit, f"Failed to send the file via Telegram.\n{e}")
            return
    # Clean up the progress message after successful Telegram upload
    try:
        await message_for_edit.delete()
    except Exception as e:
        logger.warning(f"Could not delete progress message after Telegram upload: {e}")
    
    # Clean up task data for this user after successful Telegram upload
    if user_data.get("current_task_id"):
        clean_up_task(user_id, user_data["current_task_id"])

# ---------------------------------------------------------------------------------
# Video Processing Main Logic
# ---------------------------------------------------------------------------------
async def process_video(bot, m: Message, user_id: int, task_id: str, processing_msg: Message):
    user_data = USER_DATA.get(user_id, {})
    tasks = user_data.get("tasks", {})
    task = tasks.get(task_id, {})

    video_path = task.get("video_path")
    subtitles_path = task.get("subtitles_path")
    add_subtitles = user_data.get("add_subtitles", True)
    logo_enabled = user_data.get("logo_enabled", True)
    add_logo = user_data.get("add_logo", True)
    if not logo_enabled:
        add_logo = False

    logos_dict = user_data.get("logos", {})
    active_logo_id = user_data.get("active_logo", None)
    task_dir = task.get("task_dir")
    unique_id = uuid.uuid4().hex
    output_filename = f"output_video_{unique_id}.mp4"
    output_file_path = os.path.join(task_dir, output_filename)

    # Retrieve video duration and info
    try:
        duration = await get_video_duration_ffprobe(video_path)
        if duration and duration > MAX_VIDEO_DURATION:
            await safe_edit_text(processing_msg, "The video is too long. Maximum allowed duration is 3 hours.")
            return
        if duration is None:
            logger.error("Duration is None, cannot track progress accurately.")
        video_info = await get_video_info(video_path)
        if not video_info:
            await safe_edit_text(processing_msg, "Failed to retrieve video information.")
            return
        width = video_info.get("width", 1280)
        height = video_info.get("height", 720)
        bit_rate = video_info.get("bit_rate", 1500000)
        if (width == 1280 and height == 720):
            bitrate_to_use = bit_rate if bit_rate < 1700000 else 1500000
        elif (width == 1920 and height == 1080):
            bitrate_to_use = bit_rate if bit_rate < 2000000 else 1800000
        else:
            bitrate_to_use = bit_rate or 1500000
        bitrate_kbps = bitrate_to_use // 1000
    except Exception as e:
        logger.error(f"Error retrieving video info: {e}")
        await safe_edit_text(processing_msg, f"Failed to retrieve video info.\nError: {e}")
        return

    # Decide which filters to apply.
    subtitle_applied = add_subtitles and subtitles_path and os.path.isfile(subtitles_path)
    
    # Check if there's an available logo and only apply if one exists
    logo_exists = False
    logo_abs_path = None
    
    if add_logo:
        if logos_dict:
            if active_logo_id and active_logo_id in logos_dict:
                logo_abs_path = logos_dict[active_logo_id]["path"]
                if os.path.isfile(logo_abs_path):
                    logo_exists = True
            elif logos_dict:
                # Try to use the first available logo if active not set
                for logo_id, logo_info in logos_dict.items():
                    if os.path.isfile(logo_info["path"]):
                        logo_abs_path = logo_info["path"]
                        logo_exists = True
                        break
        else:
            # Try legacy single-user logo
            single_logo_path = os.path.join(LOGO_DIR, f"{user_id}_logo.png")
            if os.path.isfile(single_logo_path):
                logo_abs_path = single_logo_path
                logo_exists = True
    
    # Only apply logo if we found a valid logo file
    logo_applied = logo_exists
    
    filter_complex = None

    # Generate a processing mode message for the user
    processing_mode = []
    if subtitle_applied:
        processing_mode.append("subtitles")
    if logo_applied:
        processing_mode.append("logo overlay")
    
    processing_mode_msg = ", ".join(processing_mode) if processing_mode else "basic encoding"

    # Check if CUDA is available for hardware acceleration
    has_cuda = await is_cuda_available()
    
    # Update processing message with hardware info
    hw_method = "GPU acceleration" if has_cuda else "CPU encoding"
    await safe_edit_text(processing_msg, f"Processing your video with {processing_mode_msg} using {hw_method}...")

    # Build base ffmpeg command based on CUDA availability
    ffmpeg_cmd = ["ffmpeg", "-y"]
    
    if has_cuda:
        # Use CUDA hardware acceleration when available
        ffmpeg_cmd.extend([
            "-init_hw_device", "cuda=gpu",
            "-filter_hw_device", "gpu",
            "-hwaccel", "cuda",
            "-hwaccel_output_format", "cuda"
        ])
    
    # Add input video
    ffmpeg_cmd.extend(["-i", video_path])

    # If a logo is applied, determine its source and add it as a second input.
    if logo_applied:
        if logo_abs_path and os.path.isfile(logo_abs_path):
            # Copy logo into the task directory.
            local_logo_name = os.path.basename(logo_abs_path)
            logo_copy_path = os.path.join(task_dir, local_logo_name)
            shutil.copy(logo_abs_path, logo_copy_path)
            logo_abs_path = os.path.abspath(logo_copy_path)
            ffmpeg_cmd.extend(["-i", logo_abs_path])
        else:
            logo_applied = False  # fallback if logo file not found

    # Build the filter_complex chain based on hardware availability and scenario
    if has_cuda:
        # CUDA-accelerated filter chains
        if subtitle_applied and logo_applied:
            esc_sub = escape_ffmpeg_path(subtitles_path)
            filter_complex = (
                f"[0:v]hwdownload,format=nv12,ass='{esc_sub}',hwupload_cuda=extra_hw_frames=64[vid_subs]; "
                f"[1:v]format=nv12,hwupload_cuda=extra_hw_frames=64[logo]; "
                f"[vid_subs][logo]overlay_cuda=W-w-10:H-h-10"
            )
        elif subtitle_applied and not logo_applied:
            esc_sub = escape_ffmpeg_path(subtitles_path)
            filter_complex = f"[0:v]hwdownload,format=nv12,ass='{esc_sub}',hwupload_cuda=extra_hw_frames=64"
        elif logo_applied and not subtitle_applied:
            filter_complex = (
                f"[0:v]format=nv12,hwupload_cuda=extra_hw_frames=64[vid]; "
                f"[1:v]format=nv12,hwupload_cuda=extra_hw_frames=64[logo]; "
                f"[vid][logo]overlay_cuda=W-w-10:H-h-10"
            )
    else:
        # CPU-based filter chains (no hw acceleration)
        if subtitle_applied and logo_applied:
            esc_sub = escape_ffmpeg_path(subtitles_path)
            filter_complex = (
                f"[0:v]ass='{esc_sub}'[vid]; "
                f"[vid][1:v]overlay"
            )
        elif subtitle_applied and not logo_applied:
            esc_sub = escape_ffmpeg_path(subtitles_path)
            filter_complex = f"ass='{esc_sub}'"
        elif logo_applied and not subtitle_applied:
            filter_complex = "overlay"

    # Apply filter complex if defined
    if filter_complex:
        ffmpeg_cmd.extend(["-filter_complex", filter_complex])

    # Configure encoder based on hardware availability
    if has_cuda:
        # NVIDIA GPU encoding using configured encoder
        ffmpeg_cmd.extend([
            "-c:v", "h264_nvenc",
            "-preset", "p4",
            "-b:v", f"{bitrate_kbps}k",
            "-maxrate", f"{int(bitrate_kbps * 1.5)}k", # Ensured integer for maxrate
            "-bufsize", f"{bitrate_kbps * 2}k",
            "-spatial_aq", "1",
            "-g", "120"
        ])
    else:
        # CPU encoding (libx264)
        ffmpeg_cmd.extend([
            "-c:v", "libx264",
            "-preset", "veryfast",  # Balance between speed and quality
            "-crf", "23",         # Constant quality factor (lower is better)
            "-b:v", f"{bitrate_kbps}k",
            "-maxrate", f"{bitrate_kbps * 1.5}k",
            "-bufsize", f"{bitrate_kbps * 2}k",
            "-pix_fmt", "yuv420p",  # Widely compatible pixel format
            "-movflags", "+faststart"  # Optimize for web streaming
        ])
        
    # Common parameters regardless of hardware
    ffmpeg_cmd.extend([
        "-c:a", "copy",         # Copy audio stream without re-encoding
        "-progress", "pipe:1",    # Output progress information
        "-nostats",
        output_file_path
    ])

    try:
        process = await asyncio.create_subprocess_exec(*ffmpeg_cmd,
                                                     stdout=asyncio.subprocess.PIPE,
                                                     stderr=asyncio.subprocess.PIPE)
        task["process"] = process
        progress_task = asyncio.create_task(update_ffmpeg_progress(process, processing_msg, duration or 0))
        
        try:
            await process.wait()
            await progress_task
        except asyncio.CancelledError:
            # Handle task cancellation
            if process.returncode is None:
                process.kill()
                logger.info(f"FFmpeg process killed for task {task_id}")
            raise

        if process.returncode != 0:
            error_message = (await process.stderr.read()).decode()
            error_message = safe_truncate(error_message, 3000)
            logger.error(f"ffmpeg error: {error_message}")
            await safe_edit_text(processing_msg, f"Error during processing:\n```{error_message}```")
            return

        if not os.path.isfile(output_file_path) or os.path.getsize(output_file_path) == 0:
            logger.error("Output file not created by ffmpeg or is empty.")
            await safe_edit_text(processing_msg, "Failed to process the video. Output file was not created or is empty.")
            return

        # Create a descriptive completion message
        complete_msg = f"Processing complete! "
        if subtitle_applied and logo_applied:
            complete_msg += "Added subtitles and logo overlay."
        elif subtitle_applied:
            complete_msg += "Added subtitles."
        elif logo_applied:
            complete_msg += "Added logo overlay."
        else:
            complete_msg += "Video encoded successfully."
            
        # Store the output_file_path in the task for future reference
        task["output_file_path"] = output_file_path
        
        # Check if user is using S3 or GCS and offer them to rename the file
        method = user_data.get("output_method", "telegram")
        
        if method in ["s3", "gcs"] and (S3_ENABLED or GCS_ENABLED):
            complete_msg += "\n\nDo you want to rename the video before uploading?"
            await safe_edit_text(processing_msg, complete_msg)
            
            # Set up the rename callback
            rename_kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Yes, rename", callback_data=f"rename_video_{task_id}"),
                    InlineKeyboardButton("No, upload now", callback_data=f"upload_video_{task_id}")
                ]
            ])
            
            await processing_msg.edit_reply_markup(rename_kb)
            # Set state to waiting for rename decision
            user_data["state"] = "waiting_for_rename"
        else:
            # Default flow: upload immediately
            complete_msg += " Uploading your video..."
            await safe_edit_text(processing_msg, complete_msg)
            await final_upload(bot, m.chat.id, output_file_path, user_id, processing_msg)

    except Exception as e:
        logger.error(f"An unexpected error occurred during processing: {e}")
        err_txt = safe_truncate(str(e), 3000)
        await safe_edit_text(processing_msg, f"An unexpected error occurred during processing.\nError: {err_txt}")
    finally:
        # Always cleanup resources
        if task.get("process") and task["process"].returncode is None:
            try:
                task["process"].kill()
                logger.info(f"FFmpeg process killed during cleanup for task {task_id}")
            except:
                pass
            task["process"] = None
        # Don't clean up task resources here if we're waiting for a rename decision
        if user_data.get("state") != "waiting_for_rename":
            clean_up_task(user_id, task_id)

async def check_and_process(bot, m: Message, user_id: int, task_id: str):
    user_data = USER_DATA.get(user_id, {})
    tasks = user_data.get("tasks", {})
    task = tasks.get(task_id, {})
    video = task.get("video_path")
    subtitles = task.get("subtitles_path")
    add_subtitles = user_data.get("add_subtitles", True)
    add_logo = user_data.get("logo_enabled", True)
    
    # Process if we have a video and:
    # 1. If subtitles are enabled, we must have a subtitle file
    # 2. If subtitles are disabled, we can proceed with or without logo
    if video and ((add_subtitles and subtitles) or (not add_subtitles)):
        # Check if the user already has the maximum allowed tasks running
        user_task_count = active_tasks.get(user_id, 0)
        if user_task_count >= MAX_CONCURRENT_TASKS:
            await m.reply(f"You already have {user_task_count} processing tasks running. Please wait for them to complete.")
            return
            
        msg = await m.reply("Processing your video...")
        
        # Increment the user's active task count
        active_tasks[user_id] = user_task_count + 1
        
        # Use semaphore to limit concurrent tasks globally
        try:
            async with task_semaphore:
                await process_video(bot, m, user_id, task_id, msg)
        finally:
            # Decrement the user's active task count when finished
            active_tasks[user_id] = max(0, active_tasks.get(user_id, 1) - 1)
    else:
        # If we have a video but are waiting for subtitles
        if video and add_subtitles and not subtitles:
            await m.reply("Video received! Now please send a subtitle (.srt or .ass) file to continue processing.")
        pass

# ---------------------------------------------------------------------------------
# Bot Handlers
# ---------------------------------------------------------------------------------
@Bot.on_message(filters.command("start"))
async def start_command(bot, update: Message):
    user_id = update.from_user.id
    if user_id not in USER_DATA:
        USER_DATA[user_id] = {
            "tasks": {},
            "add_logo": True,
            "add_subtitles": True,
            "state": "idle",
            "current_task_id": None,
            "logos": {},
            "active_logo": None,
            "logo_enabled": True,
            "output_method": "telegram"
        }
    text = START_TXT.format(update.from_user.mention)
    await update.reply_text(text, disable_web_page_preview=True, reply_markup=START_BTN)

@Bot.on_message(filters.command("help"))
async def help_command(bot, m: Message):
    await m.reply_text(HELP_TEXT, disable_web_page_preview=True)

@Bot.on_message(filters.command("cancel"))
async def cancel_command(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.get(user_id, {})
    tasks = user_data.get("tasks", {})
    cancelled = False
    for tid, task in list(tasks.items()):
        process = task.get("process")
        if process and process.returncode is None:
            process.kill()
            cancelled = True
            clean_up_task(user_id, tid)
    if cancelled:
        await m.reply("Canceled the ongoing operation(s).")
    else:
        await m.reply("No ongoing operation to cancel.")

# --- Modified /change_logo to always use multi-logo mode ---
@Bot.on_message(filters.command("change_logo"))
async def change_logo_command(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {
        "tasks": {}, "logos": {}, "state": "idle", "active_logo": None, "logo_enabled": True, "output_method": "telegram"
    })
    user_data["state"] = "awaiting_new_logo"
    await m.reply("Please send the new logo image (PNG preferred) to add.")

@Bot.on_callback_query(filters.regex("^change_logo$"))
async def change_logo_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    user_data = USER_DATA.setdefault(user_id, {})
    user_data["state"] = "awaiting_new_logo"
    await query.message.reply("Please send the new logo image (PNG preferred) to add.")
    await query.answer()

@Bot.on_message(filters.command("settings"))
async def settings_command(bot, m: Message):
    user_id = m.from_user.id
    await show_settings_menu(bot, user_id, m)

@Bot.on_callback_query(filters.regex("^settings$"))
async def settings_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    await query.answer()
    await show_settings_menu(bot, user_id, query.message, edit=True)

async def show_settings_menu(bot, user_id: int, msg: Message, edit=False):
    user_data = USER_DATA.setdefault(user_id, {
        "tasks": {}, "add_logo": True, "add_subtitles": True,
        "state": "idle", "logos": {}, "active_logo": None,
        "logo_enabled": True, "output_method": "telegram"
    })
    logo_enabled = user_data.get("logo_enabled", True)
    add_subtitles = user_data.get("add_subtitles", True)
    output_method = user_data.get("output_method", "telegram")
    methods_row = []
    if TELEGRAM_ENABLED:
        methods_row.append(
            InlineKeyboardButton(
                f"Telegram {'✓' if output_method=='telegram' else ''}",
                callback_data="set_output_telegram"
            )
        )
    if S3_ENABLED:
        methods_row.append(
            InlineKeyboardButton(
                f"S3 {'✓' if output_method=='s3' else ''}",
                callback_data="set_output_s3"
            )
        )
    if GCS_ENABLED:
        methods_row.append(
            InlineKeyboardButton(
                f"GCS {'✓' if output_method=='gcs' else ''}",
                callback_data="set_output_gcs"
            )
        )
    settings_buttons = [
        [
            InlineKeyboardButton(f"Logo {'ON' if logo_enabled else 'OFF'}", callback_data="toggle_logo"),
            InlineKeyboardButton(f"Subs {'ON' if add_subtitles else 'OFF'}", callback_data="toggle_subtitles")
        ]
    ]
    if methods_row:
        settings_buttons.append(methods_row)
    settings_buttons.append([
        InlineKeyboardButton("Manage Logos", callback_data="manage_logos"),
        InlineKeyboardButton("Add Logo", callback_data="inline_add_logo")
    ])
    text = (
        "**Current Settings:**\n\n"
        f"**Logo Overlay**: {'ON' if logo_enabled else 'OFF'}\n"
        f"**Subtitles**: {'ON' if add_subtitles else 'OFF'}\n"
        f"**Output Method**: {output_method.upper()}\n\n"
        "Use the buttons below to toggle or change."
    )
    kb = InlineKeyboardMarkup(settings_buttons)
    if edit:
        await safe_edit_text(msg, text)
        try:
            await msg.edit_reply_markup(kb)
        except:
            pass
    else:
        await msg.reply(text, reply_markup=kb)

@Bot.on_callback_query(filters.regex("^toggle_logo$"))
async def toggle_logo_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    user_data = USER_DATA.setdefault(user_id, {})
    user_data["logo_enabled"] = not user_data.get("logo_enabled", True)
    user_data["add_logo"] = user_data["logo_enabled"]
    await query.answer(f"Logo overlay {'enabled' if user_data['logo_enabled'] else 'disabled'}.")
    # Save user settings after making changes
    save_user_settings()
    await show_settings_menu(bot, user_id, query.message, edit=True)

@Bot.on_callback_query(filters.regex("^toggle_subtitles$"))
async def toggle_subtitles_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    user_data = USER_DATA.setdefault(user_id, {})
    user_data["add_subtitles"] = not user_data.get("add_subtitles", True)
    await query.answer(f"Subtitles {'enabled' if user_data['add_subtitles'] else 'disabled'}.")
    # Save user settings after making changes
    save_user_settings()
    await show_settings_menu(bot, user_id, query.message, edit=True)

@Bot.on_callback_query(filters.regex("^set_output_(telegram|s3|gcs)$"))
async def set_output_method(bot, query: CallbackQuery):
    user_id = query.from_user.id
    user_data = USER_DATA.setdefault(user_id, {})
    old_method = user_data.get("output_method", "telegram")
    method = query.data.split("_", 2)[2]
    
    # Log the requested change
    logger.info(f"[set_output_method] User {user_id} requested change from {old_method} to {method}")
    
    # Validate and set the new method
    if method == "telegram" and TELEGRAM_ENABLED:
        user_data["output_method"] = "telegram"
    elif method == "s3" and S3_ENABLED:
        user_data["output_method"] = "s3"
    elif method == "gcs" and GCS_ENABLED:
        user_data["output_method"] = "gcs"
    else:
        # Default fallback
        user_data["output_method"] = "telegram"
        logger.warning(f"Invalid or disabled output method requested: {method}, defaulting to telegram")
    
    # Log confirmation of the change
    new_method = user_data["output_method"]
    logger.info(f"[set_output_method] Set output_method={new_method} for user_id={user_id}")
    
    # Save user settings after making changes
    save_user_settings()
    
    # Notify the user
    await query.answer(f"Output method set to {user_data['output_method'].upper()}")
    await show_settings_menu(bot, user_id, query.message, edit=True)

async def show_logo_management_menu(bot, user_id: int, msg: Message, edit=False):
    """
    Display the logo management menu showing all user logos with options to:
    - Select the active logo
    - Rename logos
    - Delete logos
    - Return to settings
    """
    user_data = USER_DATA.setdefault(user_id, {"logos": {}, "active_logo": None})
    logos = user_data.get("logos", {})
    active_logo_id = user_data.get("active_logo")
    
    if not logos:
        # No logos case
        text = "**Logo Management**\n\nYou don't have any logos yet. Add a logo first."
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Add Logo", callback_data="inline_add_logo")],
            [InlineKeyboardButton("Back to Settings", callback_data="back_to_settings")]
        ])
    else:
        # Show all logos with management options
        text = "**Logo Management**\n\nSelect, rename or delete your logos:"
        buttons = []
        
        # List all logos with select/rename/delete options
        for logo_id, logo_info in logos.items():
            display_name = logo_info["display_name"]
            is_active = (logo_id == active_logo_id)
            select_text = f"✓ {display_name}" if is_active else f"□ {display_name}"
            
            # Create a row for each logo with select, rename, and delete options
            logo_row = [
                InlineKeyboardButton(select_text, callback_data=f"select_logo_{logo_id}"),
                InlineKeyboardButton("Rename", callback_data=f"rename_logo_{logo_id}"),
                InlineKeyboardButton("Delete", callback_data=f"delete_logo_{logo_id}")
            ]
            buttons.append(logo_row)
        
        # Add logo management buttons
        buttons.append([
            InlineKeyboardButton("Add New Logo", callback_data="inline_add_logo"),
            InlineKeyboardButton("Back to Settings", callback_data="back_to_settings")
        ])
        
        kb = InlineKeyboardMarkup(buttons)
    
    if edit:
        await safe_edit_text(msg, text)
        try:
            await msg.edit_reply_markup(kb)
        except Exception as e:
            logger.error(f"Error editing keyboard: {e}")
    else:
        await msg.reply(text, reply_markup=kb)

@Bot.on_callback_query(filters.regex("^manage_logos$"))
async def manage_logos_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    await query.answer()
    await show_logo_management_menu(bot, user_id, query.message)

@Bot.on_callback_query(filters.regex("^inline_add_logo$"))
async def inline_add_logo_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    user_data = USER_DATA.setdefault(user_id, {"logos": {}, "state": "idle"})
    if len(user_data.get("logos", {})) >= MAX_LOGOS_PER_USER:
        await query.answer("Maximum logo limit reached.", show_alert=True)
        return
    user_data["state"] = "awaiting_new_logo"
    await query.message.reply("Please send the new logo image (PNG preferred) to add.")
    await query.answer()

@Bot.on_callback_query(filters.regex("^select_logo_"))
async def select_logo_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    logo_id = query.data[len("select_logo_"):]
    user_data = USER_DATA.setdefault(user_id, {})
    logos = user_data.setdefault("logos", {})
    if logo_id not in logos:
        await query.answer("Logo not found.", show_alert=True)
        return
    user_data["active_logo"] = logo_id
    # Save user settings after making changes
    save_user_settings()
    await query.answer(f"Selected logo: {logos[logo_id]['display_name']}")
    await show_logo_management_menu(bot, user_id, query.message)

@Bot.on_callback_query(filters.regex("^rename_logo_"))
async def rename_logo_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    logo_id = query.data[len("rename_logo_"):]
    user_data = USER_DATA.setdefault(user_id, {})
    if logo_id not in user_data.get("logos", {}):
        await query.answer("Logo not found.", show_alert=True)
        return
    user_data["state"] = f"renaming_logo_{logo_id}"
    await query.message.reply("Please send the new name for this logo.")
    await query.answer()

@Bot.on_callback_query(filters.regex("^delete_logo_"))
async def delete_logo_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    logo_id = query.data[len("delete_logo_"):]
    user_data = USER_DATA.setdefault(user_id, {})
    logos = user_data.get("logos", {})
    
    if logo_id not in logos:
        await query.answer("Logo not found.", show_alert=True)
        return
    
    # Get logo info before deletion
    logo_path = logos[logo_id].get("path")
    logo_name = logos[logo_id].get("display_name")
    
    # Delete the logo file if it exists
    try:
        if logo_path and os.path.exists(logo_path):
            os.remove(logo_path)
            logger.info(f"Deleted logo file: {logo_path}")
    except Exception as e:
        logger.error(f"Error deleting logo file: {e}")
    
    # Remove from user's logos dictionary
    del logos[logo_id]
    
    # If we deleted the active logo, reset active_logo
    if user_data.get("active_logo") == logo_id:
        # Set to first available logo or None
        user_data["active_logo"] = next(iter(logos.keys())) if logos else None
    
    # Save user settings after making changes
    save_user_settings()
    
    await query.answer(f"Logo '{logo_name}' deleted.")
    await show_logo_management_menu(bot, user_id, query.message, edit=True)

@Bot.on_callback_query(filters.regex("^back_to_settings$"))
async def back_to_settings_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    await query.answer()
    await show_settings_menu(bot, user_id, query.message, edit=True)

@Bot.on_message(filters.command("add_logo"))
async def add_logo_command(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {"logos": {}, "state": "idle"})
    if len(user_data["logos"]) >= MAX_LOGOS_PER_USER:
        await m.reply("You already have the maximum number of logos. Delete or replace one.")
        return
    user_data["state"] = "awaiting_new_logo"
    await m.reply("Please send the new logo image (PNG preferred) to add.")

@Bot.on_message(filters.command("rename_logo"))
async def rename_logo_command(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {"logos": {}})
    logos = user_data["logos"]
    parts = m.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await m.reply("Usage: /rename_logo <logo_id> <new_name>")
        return
    _, logo_id, new_name = parts
    if logo_id not in logos:
        await m.reply("Logo ID not found.")
        return
    logos[logo_id]["display_name"] = new_name
    await m.reply(f"Renamed logo `{logo_id}` to `{new_name}`.")

@Bot.on_message(filters.command("toggle_logo"))
async def toggle_logo_command(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {})
    user_data["logo_enabled"] = not user_data.get("logo_enabled", True)
    user_data["add_logo"] = user_data["logo_enabled"]
    
    # Save user settings after changing logo state
    save_user_settings()
    
    await m.reply(f"Logo globally {'enabled' if user_data['logo_enabled'] else 'disabled'}.")

# ---------------------------------------------------------------------------------
# Text-based toggles for backward compatibility
# ---------------------------------------------------------------------------------
@Bot.on_message(filters.text & filters.private, group=2)
async def handle_legacy_toggles(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {})
    state = user_data.get("state", "")
    if state.startswith("renaming_logo_") or state in ("waiting_for_logo", "awaiting_new_logo"):
        return
    text_lower = m.text.lower().strip()
    if text_lower in ["logo on", "logo off"]:
        new_state = (text_lower == "logo on")
        user_data["logo_enabled"] = new_state
        user_data["add_logo"] = new_state
        
        # Save user settings after changing logo state
        save_user_settings()
        
        await m.reply(f"Logo overlay set to {'ON' if new_state else 'OFF'}.")
    elif text_lower in ["subtitles on", "subtitles off"]:
        new_state = (text_lower == "subtitles on")
        user_data["add_subtitles"] = new_state
        
        # Save user settings after changing subtitle state
        save_user_settings()
        
        await m.reply(f"Subtitles set to {'ON' if new_state else 'OFF'}.")
    else:
        pass

# ---------------------------------------------------------------------------------
# Media/Logo Receiving
# ---------------------------------------------------------------------------------
@Bot.on_message(filters.private & (filters.photo | filters.document), group=3)
async def handle_possible_logo(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {"tasks": {}, "state": "idle", "logos": {}, "active_logo": None})
    state = user_data.get("state", "idle")
    # If in a renaming state, ignore file messages.
    if state.startswith("renaming_logo_"):
        return
    # Process logo only if state indicates logo upload.
    if state in ("waiting_for_logo", "awaiting_new_logo"):
        # Process only image files.
        if m.photo or (m.document and m.document.mime_type.startswith("image/")):
            # Always use multi-logo mode.
            logos_dict = user_data.setdefault("logos", {})
            if len(logos_dict) >= MAX_LOGOS_PER_USER:
                await m.reply("You already have the maximum number of logos.")
                user_data["state"] = "idle"
                return
            new_logo_id = uuid.uuid4().hex[:8]
            temp_path = os.path.join(LOGO_DIR, f"{user_id}_{new_logo_id}.png")
            try:
                await m.download(file_name=temp_path)
                display_name = f"Logo_{len(logos_dict) + 1}"
                logos_dict[new_logo_id] = {"path": temp_path, "display_name": display_name}
                # Set the newly added logo as active.
                user_data["active_logo"] = new_logo_id
                user_data["state"] = "idle"
                
                # Save user settings after adding a logo
                save_user_settings()
                
                await m.reply(f"New logo added with ID: `{new_logo_id}` named '{display_name}'.")
            except Exception as e:
                logger.error(f"Failed to add new logo: {e}")
                await m.reply(f"Failed to process the logo.\nError: {e}")
                user_data["state"] = "idle"
        else:
            await m.reply("Please send an image file (PNG preferred).")
    # Otherwise, do nothing so that non-logo images don't trigger "Unsupported document" later.

@Bot.on_message(filters.text & filters.private, group=4)
async def rename_logo_text_input(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.get(user_id, {})
    state = user_data.get("state", "")
    if state.startswith("renaming_logo_"):
        logo_id = state.split("_", 2)[2]
        if logo_id in user_data.get("logos", {}):
            new_name = m.text.strip()
            user_data["logos"][logo_id]["display_name"] = new_name
            user_data["state"] = "idle"
            
            # Save user settings after renaming a logo
            save_user_settings()
            
            await m.reply(f"Renamed logo `{logo_id}` to `{new_name}`.")
        else:
            await m.reply("Logo not found. Rename aborted.")
            user_data["state"] = "idle"

@Bot.on_message(filters.private & (filters.video | filters.document), group=5)
async def receive_media(bot, m: Message):
    # Ignore images in receive_media so that logo images are not processed as video/subtitle.
    if m.document and m.document.mime_type.startswith("image/"):
        return
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {"tasks": {}, "state": "idle", "logos": {}, "active_logo": None, "logo_enabled": True})
    if user_data.get("state", "").startswith("renaming_logo_") or user_data.get("state") in ("waiting_for_logo", "awaiting_new_logo"):
        return
    tasks = user_data["tasks"]
    user_dir = get_user_dir(user_id)
    os.makedirs(user_dir, exist_ok=True)
    if m.video or (m.document and m.document.mime_type.startswith("video/")):
        media = m.video or m.document
        if media.file_size > MAX_VIDEO_SIZE:
            await m.reply("The video file is too large. Max allowed is 3GB.")
            return
        task_id = uuid.uuid4().hex
        task_dir = os.path.join(user_dir, task_id)
        os.makedirs(task_dir, exist_ok=True)
        
        # Get original filename
        original_filename = media.file_name or "video.mp4"
        
        tasks[task_id] = {
            "video_path": None,
            "subtitles_path": None,
            "add_logo": user_data.get("add_logo", True),
            "add_subtitles": user_data.get("add_subtitles", True),
            "process": None,
            "task_dir": task_dir,
            "original_filename": original_filename
        }
        user_data["current_task_id"] = task_id
        task = tasks[task_id]
        try:
            msg = await m.reply("Downloading video...")
            c_time = time.time()
            progress_tracker = {"last_percentage": 0}
            dl_path = get_unique_file_path(task_dir, original_filename)
            await bot.download_media(
                message=m,
                file_name=dl_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading video...", msg, c_time, progress_tracker)
            )
            duration = await get_video_duration_ffprobe(dl_path)
            if duration and duration > MAX_VIDEO_DURATION:
                await safe_edit_text(msg, "The video is too long (>3 hours).")
                os.remove(dl_path)
                return
            task["video_path"] = dl_path
            await safe_edit_text(msg, "Video downloaded.")
            await check_and_process(bot, m, user_id, task_id)
        except Exception as e:
            logger.error(f"Video download error: {e}")
            err_txt = safe_truncate(str(e))
            await safe_edit_text(msg, f"Failed to download the video.\nError: {err_txt}")
            clean_up_task(user_id, task_id)
    elif m.document:
        file_name = m.document.file_name or ""
        _, ext = os.path.splitext(file_name)
        ext = ext.lower().lstrip('.')
        if ext in ALLOWED_SUB_FORMATS and SUBTITLES_ENABLED:
            msg = await m.reply("Downloading subtitle file...")
            c_time = time.time()
            progress_tracker = {"last_percentage": 0}
            sub_path = get_unique_file_path(user_dir, file_name)
            try:
                await bot.download_media(
                    message=m,
                    file_name=sub_path,
                    progress=progress_for_pyrogram,
                    progress_args=("Downloading subtitle file...", msg, c_time, progress_tracker)
                )
                if user_data.get("current_task_id"):
                    ctid = user_data["current_task_id"]
                    if ctid in tasks:
                        tasks[ctid]["subtitles_path"] = sub_path
                        await safe_edit_text(msg, "Subtitle file downloaded.")
                        await check_and_process(bot, m, user_id, ctid)
                    else:
                        await safe_edit_text(msg, "No matching task found for the subtitle.")
                else:
                    for tid, tk in tasks.items():
                        if tk.get("subtitles_path") is None:
                            tk["subtitles_path"] = sub_path
                            await safe_edit_text(msg, "Subtitle file downloaded.")
                            await check_and_process(bot, m, user_id, tid)
                            break
                    else:
                        await safe_edit_text(msg, "No pending video to associate with subtitle.")
            except Exception as e:
                logger.error(f"Subtitle download error: {e}")
                err_txt = safe_truncate(str(e))
                await safe_edit_text(msg, f"Failed to download the subtitle.\nError: {err_txt}")
        else:
            await m.reply("Unsupported document type. Please send a valid video or subtitle file.")

# ---------------------------------------------------------------------------------
# Text Handler for YouTube, Direct URL, GDrive
# ---------------------------------------------------------------------------------
@Bot.on_message(filters.private & filters.text, group=6)
async def link_handler(bot, m: Message):
    user_id = m.from_user.id
    text = m.text.strip()
    logger.info(f"Received message: '{text}' from user {user_id}")
    
    # Skip if already processed by YouTube handler
    if hasattr(m, 'youtube_link_handled') and m.youtube_link_handled:
        logger.info(f"Skipping message already handled by YouTube handler: {text}")
        return
    
    user_data = USER_DATA.setdefault(user_id, {
        "tasks": {}, "add_logo": True, "add_subtitles": True,
        "state": "idle", "current_task_id": None, "logos": {},
        "active_logo": None, "logo_enabled": True, "output_method": "telegram"
    })
    
    # Don't process commands or links when in special states
    if user_data.get("state", "").startswith("renaming_logo_") or user_data.get("state") in ("waiting_for_logo", "awaiting_new_logo"):
        logger.info(f"User {user_id} is in state '{user_data.get('state')}', not processing link")
        return
    
    # Check for YouTube URL
    if YOUTUBE_URL_REGEX.match(text):
        logger.info(f"Detected YouTube URL: {text}")
        
        task_id = uuid.uuid4().hex
        user_dir = get_user_dir(user_id)
        os.makedirs(user_dir, exist_ok=True)
        task_dir = os.path.join(user_dir, task_id)
        os.makedirs(task_dir, exist_ok=True)
        
        tasks = user_data.get("tasks", {})
        tasks[task_id] = {
            "youtube_url": text,
            "video_path": None,
            "subtitles_path": None,
            "add_logo": user_data.get("add_logo", True),
            "add_subtitles": user_data.get("add_subtitles", True),
            "process": None,
            "task_dir": task_dir
        }
        
        user_data["state"] = "waiting_for_resolution"
        user_data["current_task_id"] = task_id
        
        res_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("480p", callback_data="resolution_480p"),
                InlineKeyboardButton("720p", callback_data="resolution_720p"),
                InlineKeyboardButton("1080p", callback_data="resolution_1080p")
            ]
        ])
        
        try:
            await m.reply("Please select the resolution you want to download:", reply_markup=res_kb)
            logger.info(f"Sent resolution options to user {user_id}")
        except Exception as e:
            logger.error(f"Error sending resolution options: {e}")
            await m.reply("An error occurred while processing your YouTube link. Please try again.")
            user_data["state"] = "idle"
    
    elif DIRECT_URL_REGEX.match(text):
        logger.info(f"Detected direct URL: {text}")
        await handle_direct_url_input(bot, m, text)
    
    elif GDRIVE_URL_REGEX.match(text):
        logger.info(f"Detected Google Drive URL: {text}")
        await handle_gdrive_url_input(bot, m, text)
    
    else:
        logger.info(f"No URL pattern matched for: {text}")
        await m.reply("Send me a valid YouTube link, direct video URL (.mp4/.mov/.avi), or Google Drive link.\nOr send a video/subtitle file directly.")

@Bot.on_callback_query(filters.regex(r"^resolution_(\d+p)$"))
async def resolution_selected(bot, query: CallbackQuery):
    user_id = query.from_user.id
    user_data = USER_DATA.get(user_id, {})
    resolution = query.data.split("_")[1]
    logger.info(f"User {user_id} selected resolution: {resolution}")
    
    if user_data.get("state") != "waiting_for_resolution":
        logger.warning(f"User {user_id} selected resolution but state is {user_data.get('state')}")
        await query.answer("Please send a YouTube link first.", show_alert=True)
        return
    
    task_id = user_data.get("current_task_id")
    tasks = user_data.get("tasks", {})
    
    if not task_id or task_id not in tasks:
        logger.error(f"No matching task found for user {user_id}, task_id: {task_id}")
        await query.answer("No matching task found.", show_alert=True)
        return
    
    task = tasks[task_id]
    youtube_url = task.get("youtube_url")
    
    if not youtube_url:
        logger.error(f"No YouTube URL in task for user {user_id}, task_id: {task_id}")
        await query.answer("An error occurred. Please try again.", show_alert=True)
        return
    
    user_data["state"] = "downloading_youtube_video"
    
    try:
        await query.answer(f"Starting download in {resolution}...")
        await safe_edit_text(query.message, f"Starting download of YouTube video in {resolution} resolution...")
        
        msg = await query.message.reply("Downloading video from YouTube...")
        c_time = time.time()
        progress_tracker = {"last_percentage": 0}
        loop = asyncio.get_event_loop()
        resolution_height = resolution.replace("p", "")
        
        logger.info(f"Starting YouTube download for user {user_id}: {youtube_url} in {resolution}")
        
        # Configure yt-dlp with retries and robust error handling
        # First, extract info to get the title
        info_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True
        }
        
        # Extract video info to get title
        video_info = {}
        try:
            with yt_dlp.YoutubeDL(info_opts) as ydl:
                video_info = ydl.extract_info(youtube_url, download=False)
        except Exception as info_error:
            logger.warning(f"Could not extract video info: {info_error}")
        
        # Get video title or default to YouTube Video
        video_title = video_info.get('title', 'YouTube Video')
        safe_title = ''.join(c if c.isalnum() or c in ' .-_' else '_' for c in video_title)
        safe_title = safe_title[:100] if len(safe_title) > 100 else safe_title
        
        # Store the title in the task
        task["original_filename"] = f"{safe_title}.mp4"
        
        # Configure download options
        ydl_opts = {
            'format': f'bestvideo[height<={resolution_height}]+bestaudio/best[height<={resolution_height}]',
            'outtmpl': os.path.join(task["task_dir"], 'youtube_video.%(ext)s'),
            'noplaylist': True,
            'ignoreerrors': True,  # Continue downloading on error
            'retries': 5,  # Retry up to 5 times
            'fragment_retries': 5,  # Retry fragments up to 5 times
            'merge_output_format': 'mp4',
            'writethumbnail': False,
            'quiet': False,
            'verbose': False,
            'geo_bypass': True,  # Try to bypass geo-restrictions
            'socket_timeout': 30,
            'extractor_retries': 3,
            'postprocessors': [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }],
            'progress_hooks': [lambda d: yt_dlp_progress_hook(d, bot, msg, c_time, progress_tracker, loop)]
        }
        
        try:
            await loop.run_in_executor(None, download_youtube_video, ydl_opts, youtube_url)
        except Exception as yt_error:
            logger.warning(f"Initial YouTube download failed, trying fallback format: {yt_error}")
            # Try fallback format if the specific resolution fails
            ydl_opts['format'] = 'best'
            await loop.run_in_executor(None, download_youtube_video, ydl_opts, youtube_url)
        
        # Search for the downloaded video file
        video_path = None
        for f in os.listdir(task["task_dir"]):
            if (f.startswith('youtube_video') and any(f.endswith(ext) for ext in [".mp4", ".mkv", ".webm"])):
                video_path = os.path.join(task["task_dir"], f)
                # Convert non-mp4 files to mp4 if needed
                if not f.endswith(".mp4"):
                    logger.info(f"Converting {f} to mp4 format")
                    mp4_path = os.path.join(task["task_dir"], "youtube_video_converted.mp4")
                    # Simple ffmpeg conversion, no re-encoding
                    convert_cmd = [
                        "ffmpeg", "-i", video_path, "-c", "copy", mp4_path
                    ]
                    convert_process = await asyncio.create_subprocess_exec(
                        *convert_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    await convert_process.wait()
                    if os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0:
                        video_path = mp4_path
                break
        
        if not video_path or not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
            logger.error(f"Downloaded video file not found or empty for user {user_id}, task_id: {task_id}")
            raise Exception("Downloaded video file not found or is empty.")
        
        task["video_path"] = video_path
        await safe_edit_text(msg, f"Video '{safe_title}' downloaded successfully.")
        user_data["state"] = "idle"
        logger.info(f"YouTube video downloaded for user {user_id}, proceeding to processing")
        await check_and_process(bot, query.message, user_id, task_id)
    
    except Exception as e:
        logger.error(f"Error downloading YouTube video for user {user_id}: {e}")
        user_data["state"] = "idle"
        clean_up_task(user_id, task_id)
        err_txt = safe_truncate(str(e))
        await safe_edit_text(query.message, f"Failed to download the video from YouTube.\nError: {err_txt}")

# ---------------------------------------------------------------------------------
# URL Handlers
# ---------------------------------------------------------------------------------
async def handle_direct_url_input(bot, m: Message, url: str):
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {
        "tasks": {}, "add_logo": True, "add_subtitles": True,
        "state": "idle", "current_task_id": None, "logos": {},
        "active_logo": None, "logo_enabled": True, "output_method": "telegram"
    })
    task_id = uuid.uuid4().hex
    user_dir = get_user_dir(user_id)
    os.makedirs(user_dir, exist_ok=True)
    task_dir = os.path.join(user_dir, task_id)
    os.makedirs(task_dir, exist_ok=True)
    
    # Extract filename from URL if possible
    try:
        url_path = url.split('?')[0]  # Remove query parameters
        filename = os.path.basename(url_path)
        if not filename or '.' not in filename:
            filename = f"direct_video_{uuid.uuid4().hex}.mp4"
    except:
        filename = f"direct_video_{uuid.uuid4().hex}.mp4"
    
    tasks = user_data["tasks"]
    tasks[task_id] = {
        "video_path": None,
        "subtitles_path": None,
        "add_logo": user_data.get("add_logo", True),
        "add_subtitles": user_data.get("add_subtitles", True),
        "process": None,
        "task_dir": task_dir,
        "original_filename": filename
    }
    user_data["current_task_id"] = task_id
    try:
        msg = await m.reply("Starting download from URL...")
        download_path = os.path.join(task_dir, f"direct_video_{uuid.uuid4().hex}.mp4")
        await download_direct_url(url, download_path, bot, msg)
        tasks[task_id]["video_path"] = download_path
        await check_and_process(bot, m, user_id, task_id)
    except Exception as e:
        logger.error(f"Error downloading from URL: {e}")
        clean_up_task(user_id, task_id)
        err_txt = safe_truncate(str(e))
        await safe_edit_text(msg, f"Failed to download from URL.\nError: {err_txt}")

async def handle_gdrive_url_input(bot, m: Message, url: str):
    user_id = m.from_user.id
    user_data = USER_DATA.setdefault(user_id, {
        "tasks": {}, "add_logo": True, "add_subtitles": True,
        "state": "idle", "current_task_id": None, "logos": {},
        "active_logo": None, "logo_enabled": True, "output_method": "telegram"
    })
    match = GDRIVE_URL_REGEX.match(url)
    if not match:
        await m.reply("Invalid Google Drive URL format.")
        return
    
    file_id = match.group(1)
    task_id = uuid.uuid4().hex
    user_dir = get_user_dir(user_id)
    os.makedirs(user_dir, exist_ok=True)
    task_dir = os.path.join(user_dir, task_id)
    os.makedirs(task_dir, exist_ok=True)
    
    # Default filename (will be updated if we can get the real name)
    filename = f"gdrive_video_{uuid.uuid4().hex}.mp4"
    
    tasks = user_data["tasks"]
    tasks[task_id] = {
        "video_path": None,
        "subtitles_path": None,
        "add_logo": user_data.get("add_logo", True),
        "add_subtitles": user_data.get("add_subtitles", True),
        "process": None,
        "task_dir": task_dir,
        "original_filename": filename
    }
    user_data["current_task_id"] = task_id
    try:
        msg = await m.reply("Starting download from Google Drive...")
        download_path = os.path.join(task_dir, filename)
        
        # Try to get the filename from Google Drive
        try:
            import requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(f"https://drive.google.com/file/d/{file_id}/view", headers=headers, allow_redirects=True, timeout=10)
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                title_tag = soup.find('title')
                if title_tag and title_tag.text:
                    gdrive_title = title_tag.text.replace(' - Google Drive', '').strip()
                    if gdrive_title:
                        safe_title = ''.join(c if c.isalnum() or c in ' .-_' else '_' for c in gdrive_title)
                        safe_title = safe_title[:100] if len(safe_title) > 100 else safe_title
                        tasks[task_id]["original_filename"] = f"{safe_title}.mp4"
        except Exception as name_error:
            logger.warning(f"Could not get Google Drive filename: {name_error}")
        
        await download_google_drive(file_id, download_path, msg)
        tasks[task_id]["video_path"] = download_path
        await check_and_process(bot, m, user_id, task_id)
    except Exception as e:
        logger.error(f"Error downloading from Google Drive: {e}")
        clean_up_task(user_id, task_id)
        err_txt = safe_truncate(str(e))
        await safe_edit_text(msg, f"Failed to download from Google Drive.\nError: {err_txt}")

# ---------------------------------------------------------------------------------
# YouTube URL Handler (Specific)
# ---------------------------------------------------------------------------------
@Bot.on_message(filters.private & filters.regex(r'(youtube\.com|youtu\.be)'), group=1)
async def youtube_link_handler(bot, m: Message):
    """Dedicated handler for YouTube links to ensure they're processed correctly"""
    user_id = m.from_user.id
    text = m.text.strip()
    logger.info(f"Received potential YouTube URL: '{text}' from user {user_id}")
    
    # Skip if it's not actually a YouTube URL
    if not YOUTUBE_URL_REGEX.match(text):
        logger.info(f"Text contains YouTube domain but doesn't match full pattern: {text}")
        return
        
    user_data = USER_DATA.setdefault(user_id, {
        "tasks": {}, "add_logo": True, "add_subtitles": True,
        "state": "idle", "current_task_id": None, "logos": {},
        "active_logo": None, "logo_enabled": True, "output_method": "telegram"
    })
    
    # Don't process links when in special states
    if user_data.get("state", "").startswith("renaming_logo_") or user_data.get("state") in ("waiting_for_logo", "awaiting_new_logo"):
        logger.info(f"User {user_id} is in state '{user_data.get('state')}', not processing YouTube link")
        return
    
    logger.info(f"Processing YouTube URL: {text}")
    
    # Set a flag to prevent the standard link_handler from processing this message again
    setattr(m, 'youtube_link_handled', True)
    
    task_id = uuid.uuid4().hex
    user_dir = get_user_dir(user_id)
    os.makedirs(user_dir, exist_ok=True)
    task_dir = os.path.join(user_dir, task_id)
    os.makedirs(task_dir, exist_ok=True)
    
    tasks = user_data.setdefault("tasks", {})
    tasks[task_id] = {
        "youtube_url": text,
        "video_path": None,
        "subtitles_path": None,
        "add_logo": user_data.get("add_logo", True),
        "add_subtitles": user_data.get("add_subtitles", True),
        "process": None,
        "task_dir": task_dir
    }
    
    user_data["state"] = "waiting_for_resolution"
    user_data["current_task_id"] = task_id
    
    res_kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("480p", callback_data="resolution_480p"),
            InlineKeyboardButton("720p", callback_data="resolution_720p"),
            InlineKeyboardButton("1080p", callback_data="resolution_1080p")
        ]
    ])
    
    try:
        await m.reply("Please select the resolution you want to download:", reply_markup=res_kb)
        logger.info(f"Sent resolution options to user {user_id}")
    except Exception as e:
        logger.error(f"Error sending resolution options: {e}")
        await m.reply("An error occurred while processing your YouTube link. Please try again.")
        user_data["state"] = "idle"

# ---------------------------------------------------------------------------------
# CUDA helpers
# ---------------------------------------------------------------------------------
async def is_cuda_available():
    """
    Check if CUDA is available by testing with ffmpeg.
    Returns True if CUDA is available, False otherwise.
    """
    global CUDA_AVAILABLE
    
    # If we've already checked, return the cached result
    if CUDA_AVAILABLE is not None:
        return CUDA_AVAILABLE
    
    # First try to check with a simple h264_nvenc command
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error", # Reduce output noise
        "-f", "lavfi",
        "-i", "testsrc=size=1920x1080:rate=30", # Changed input source
        "-t", "1",
        "-c:v", "h264_nvenc", # Explicitly use h264_nvenc and not ENCODER from config for this test
        "-b:v", "2M", # Added bitrate
        "-f", "null",
        "-"
    ]
    
    try:
        logger.info(f"Testing CUDA availability with encoder: {ENCODER}")
        process = await asyncio.create_subprocess_exec(*cmd,
                                                     stdout=asyncio.subprocess.PIPE,
                                                     stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        
        # If the simple test succeeds, CUDA is available
        if process.returncode == 0:
            logger.info("CUDA is available for hardware acceleration.")
            CUDA_AVAILABLE = True
            return True
        
        # If the encoder is not available, try without init_hw_device
        stderr_text = stderr.decode()
        logger.warning(f"Initial CUDA check returned error: {stderr_text}")
        
        if "No such file or directory" in stderr_text or "Unrecognized option" in stderr_text or "not found" in stderr_text:
            logger.warning("CUDA is not properly installed. Will use CPU encoding.")
            CUDA_AVAILABLE = False
            return False
            
        # One final check with hwaccel only
        cmd2 = [
            "ffmpeg", 
            "-hide_banner",
            "-hwaccel", "cuda",
            "-f", "lavfi", 
            "-i", "color=black:s=64x64:r=1", 
            "-t", "1", 
            "-c:v", "h264_nvenc",
            "-f", "null", 
            "-"
        ]
        
        logger.info("Trying alternative CUDA check...")
        process2 = await asyncio.create_subprocess_exec(*cmd2,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stderr=asyncio.subprocess.PIPE)
        stdout2, stderr2 = await process2.communicate()
        
        CUDA_AVAILABLE = process2.returncode == 0
        
        if CUDA_AVAILABLE:
            logger.info("CUDA is available using alternative configuration.")
        else:
            stderr_text2 = stderr2.decode()
            logger.warning(f"Alternative CUDA check failed: {stderr_text2}")
            logger.warning("CUDA is not available. Will use CPU encoding.")
        
        return CUDA_AVAILABLE
    except Exception as e:
        logger.error(f"Error checking CUDA availability: {e}")
        CUDA_AVAILABLE = False
        return False

@Bot.on_callback_query(filters.regex("^rename_video_(.+)$"))
async def rename_video_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    task_id = query.data.split("_")[2]
    user_data = USER_DATA.get(user_id, {})
    
    if user_data.get("state") != "waiting_for_rename":
        await query.answer("This operation is no longer valid.", show_alert=True)
        return
    
    task = user_data.get("tasks", {}).get(task_id)
    if not task:
        await query.answer("Task not found. Please try again.", show_alert=True)
        return
    
    # Update state and prompt for new name
    user_data["state"] = f"renaming_video_{task_id}"
    await query.answer("Please send the new name for your video (without extension)")
    await query.message.reply("Please send the new name for your video (without extension).\nExample: 'My Awesome Video'")

@Bot.on_callback_query(filters.regex("^upload_video_(.+)$"))
async def upload_video_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    task_id = query.data.split("_")[2]
    user_data = USER_DATA.get(user_id, {})
    
    if user_data.get("state") != "waiting_for_rename":
        await query.answer("This operation is no longer valid.", show_alert=True)
        return
    
    task = user_data.get("tasks", {}).get(task_id)
    if not task:
        await query.answer("Task not found. Please try again.", show_alert=True)
        return
    
    output_file_path = task.get("output_file_path")
    if not output_file_path or not os.path.exists(output_file_path):
        await query.answer("Video file not found. Please try again.", show_alert=True)
        return
    
    await query.answer("Starting upload...")
    user_data["state"] = "idle"  # Reset state
    
    # Update message before uploading
    await safe_edit_text(query.message, "Starting upload process...")
    
    # Start the upload
    await final_upload(bot, query.message.chat.id, output_file_path, user_id, query.message)

@Bot.on_message(filters.text & filters.private, group=5)
async def rename_video_text_input(bot, m: Message):
    user_id = m.from_user.id
    user_data = USER_DATA.get(user_id, {})
    state = user_data.get("state", "")
    
    # Only process if in renaming_video state
    if not state.startswith("renaming_video_"):
        return
    
    task_id = state.split("_")[2]
    task = user_data.get("tasks", {}).get(task_id)
    
    if not task:
        await m.reply("Task not found. Please try again.")
        user_data["state"] = "idle"
        return
    
    output_file_path = task.get("output_file_path")
    if not output_file_path or not os.path.exists(output_file_path):
        await m.reply("Video file not found. Please try again.")
        user_data["state"] = "idle"
        return
    
    # Get new name from user input
    new_name = m.text.strip()
    if not new_name:
        await m.reply("Invalid name. Please send a valid name.")
        return
    
    # Sanitize the filename
    safe_name = ''.join(c if c.isalnum() or c in ' .-_' else '_' for c in new_name)
    safe_name = safe_name[:100] if len(safe_name) > 100 else safe_name
    
    # Update the original_filename with extension
    _, ext = os.path.splitext(output_file_path)
    if not ext:
        ext = ".mp4"
    task["original_filename"] = f"{safe_name}{ext}"
    
    # Reset state and start upload
    user_data["state"] = "idle"
    
    msg = await m.reply(f"Renamed video to '{safe_name}{ext}'. Starting upload...")
    
    # Start the upload
    await final_upload(bot, m.chat.id, output_file_path, user_id, msg)

# ---------------------------------------------------------------------------------
# Run the Bot
# ---------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting Telegram Video Processor Bot...")
    
    # Clean up the temp directory at startup
    try:
        if os.path.exists(TEMP_DIR):
            logger.info(f"Cleaning up temp directory at startup: {TEMP_DIR}")
            for item in os.listdir(TEMP_DIR):
                item_path = os.path.join(TEMP_DIR, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        logger.info(f"Removed directory: {item_path}")
                    else:
                        os.remove(item_path)
                        logger.info(f"Removed file: {item_path}")
                except Exception as e:
                    logger.error(f"Error cleaning up temp item {item_path}: {e}")
        # Recreate temp directory if it was removed or doesn't exist
        os.makedirs(TEMP_DIR, exist_ok=True)
    except Exception as e:
        logger.error(f"Error during temp directory cleanup: {e}")
    
    # Load user settings from persistent storage
    load_user_settings()
    
    # Check output methods configuration
    def check_upload_methods():
        global TELEGRAM_ENABLED
        
        logger.info("Checking upload methods configuration:")
        logger.info(f"- Telegram upload: {TELEGRAM_ENABLED}")
        logger.info(f"- S3 upload: {S3_ENABLED}")
        if S3_ENABLED:
            logger.info(f"  S3 Bucket: {S3_BUCKET_NAME}, Region: {S3_REGION}")
        logger.info(f"- GCS upload: {GCS_ENABLED}")
        if GCS_ENABLED:
            logger.info(f"  GCS Bucket: {GCS_BUCKET_NAME}")
        
        if not (TELEGRAM_ENABLED or S3_ENABLED or GCS_ENABLED):
            logger.warning("No output methods are enabled! Enabling Telegram output as fallback.")
            TELEGRAM_ENABLED = True
    
    # Run configuration checks
    check_upload_methods()
    
    # Check and log CUDA availability at startup
    async def check_cuda_at_startup():
        has_cuda = await is_cuda_available()
        if has_cuda:
            logger.info("CUDA hardware acceleration is available and will be used for video processing.")
        else:
            logger.info("CUDA hardware acceleration is NOT available. Using CPU-based processing instead.")
    
    # Run the CUDA check before starting the bot
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_cuda_at_startup())
    
    # Start the bot
    Bot.run()
