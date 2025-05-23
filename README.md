# Telegram Video Processor Bot

A production-ready Telegram bot for video processing that can burn subtitles and overlay logos into videos.

## Features

- Process videos from multiple sources:
  - YouTube links (Working)
  - Google Drive links
  - Direct URLs (Not Yet)
  - - Direct uploads to Telegram (Not Yet)
- Multiple output methods:
  - Telegram
  - Amazon S3
  - Google Cloud Storage
- Subtitle handling (SRT and ASS formats)
- Logo overlays with multiple logo management
- Real-time progress reporting for downloads and processing

## Setup Instructions

### 1. Prerequisites

- Python 3.7 or higher
- FFmpeg installed and available in PATH
- A Telegram Bot Token (from [@BotFather](https://t.me/botfather))
- API ID and Hash from [my.telegram.org](https://my.telegram.org)

### 2. Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/burn-subs-tg-bot.git
cd burn-subs-tg-bot
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create necessary directories:
```bash
mkdir -p logs logos temp
```

### 3. Configuration

The bot uses a single configuration file `config.ini` for all settings:

1. Open `config.ini` and update all settings as needed
2. Pay special attention to the sensitive settings marked with `(SENSITIVE)`
3. For security in production environments, consider moving these settings to environment variables

```ini
# Example sensitive settings
[Bot]
bot_token = your_bot_token
api_id = your_api_id
api_hash = your_api_hash

[S3]
access_key = your_s3_access_key
secret_key = your_s3_secret_key
```

### 4. Running the Bot

```bash
python burn.py
```

## Using the Bot

1. Send videos, YouTube links, direct URLs, or Google Drive links to the bot
2. Upload subtitle files (.srt or .ass) before or after sending the video
3. Use `/settings` to configure preferences:
   - Toggle logo overlay
   - Toggle subtitles
   - Select output method (Telegram, S3, GCS)
4. Use `/change_logo` to manage multiple logos

## Commands

- `/start` - Start the bot
- `/help` - Display help information
- `/settings` - Configure bot preferences
- `/change_logo` - Manage logos
- `/add_logo` - Add a new logo
- `/toggle_logo` - Toggle logo overlay on/off
- `/cancel` - Cancel ongoing operations

## Requirements
See `requirements.txt` for a complete list of dependencies.

If downloading a YouTube video with yt-dlp fails, create a cookies.txt file by copying your browser cookies and placing it in the root directory of your project.

Note when using a GPU: CUDA overlay is enabled, so ensure your logo is carefully resized to fit properly within the video frame.

burn.py file is very long, consider modularizing it into separate functions or files for better readability, easier maintenance and adding more features.
