"""
本地运行的视频字幕 ETL 脚本，使用 yt-dlp 抓取字幕，上传到 MongoDB。

用法：
    python etl-videos-local.py
    python etl-videos-local.py --json-path data/videos.json --db fsdl-dev --collection ask-fsdl

yt-dlp 遇到机器人检测时，使用本地浏览器 cookie 绕过：
    python etl-videos-local.py --browser chrome   # 从 Chrome 读取 cookie
    python etl-videos-local.py --browser firefox  # 从 Firefox 读取 cookie

安装依赖：
    pip install yt-dlp requests pymongo python-dotenv
"""

import argparse
import hashlib
import json
import os
import urllib.parse
from pathlib import Path

import requests
import yt_dlp
from dotenv import load_dotenv
from pymongo import InsertOne, MongoClient


# ── 配置 ──────────────────────────────────────────────────────────────────────

load_dotenv(".env.dev")

DEFAULT_JSON_PATH = "data/videos.json"
DEFAULT_DB = os.environ.get("MONGODB_DATABASE", "fsdl-dev")
DEFAULT_COLLECTION = os.environ.get("MONGODB_COLLECTION", "ask-fsdl")
CHUNK_SIZE = 250


# ── MongoDB ───────────────────────────────────────────────────────────────────

def connect_mongo():
    user = urllib.parse.quote_plus(os.environ["MONGODB_USER"])
    password = urllib.parse.quote_plus(os.environ["MONGODB_PASSWORD"])
    host = os.environ["MONGODB_HOST"]

    if host.startswith("localhost") or host.startswith("127.0.0.1"):
        uri = f"mongodb://{user}:{password}@{host}/?authSource=admin"
    else:
        uri = f"mongodb+srv://{user}:{password}@{host}/?retryWrites=true&w=majority"

    return MongoClient(uri, appname="etl-videos-local")


def upload_documents(documents, db_name, collection_name):
    client = connect_mongo()
    collection = client[db_name][collection_name]

    requesting = []
    for doc in documents:
        requesting.append(InsertOne(doc))
        if len(requesting) >= CHUNK_SIZE:
            collection.bulk_write(requesting)
            requesting = []
    if requesting:
        collection.bulk_write(requesting)

    print(f"[MongoDB] 写入 {len(documents)} 条文档 -> {db_name}.{collection_name}")


# ── 字幕抓取（yt-dlp）────────────────────────────────────────────────────────

def get_transcript(video_id, browser=None):
    """用 yt-dlp 获取字幕，返回 [{"text", "start", "duration"}, ...] 格式。"""
    url = f"https://www.youtube.com/watch?v={video_id}"

    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "check_formats": False,
        "ignore_no_formats_error": True,
        "js_runtimes": {"node": {}},  # yt-dlp 默认只用 deno，需显式指定 node
    }
    if browser:
        ydl_opts["cookiesfrombrowser"] = (browser,)

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    if info is None:
        raise ValueError(f"无法获取视频信息: {video_id}")

    # 优先人工字幕，再退到自动生成字幕
    captions = info.get("subtitles") or {}
    if not captions:
        captions = info.get("automatic_captions") or {}

    # yt-dlp 自动生成字幕有时用 "a.en" 前缀
    lang_key = next(
        (l for l in ["en", "en-US", "en-GB", "a.en", "a.en-US"] if l in captions),
        None,
    )
    if not lang_key:
        raise ValueError(f"视频 {video_id} 没有英文字幕")

    json3_url = next(
        (c["url"] for c in captions[lang_key] if c.get("ext") == "json3"), None
    )
    if not json3_url:
        raise ValueError(f"视频 {video_id} 无 json3 格式字幕")

    data = requests.get(json3_url).json()

    segments = []
    for event in data.get("events", []):
        if "segs" not in event:
            continue
        text = "".join(seg.get("utf8", "") for seg in event["segs"]).strip()
        if not text:
            continue
        segments.append({
            "text": text,
            "start": event.get("tStartMs", 0) / 1000.0,
            "duration": event.get("dDurationMs", 0) / 1000.0,
        })

    return segments


def get_chapters(video_id):
    """从 yt.lemnoslife.com 获取视频章节信息。"""
    resp = requests.get(
        "https://yt.lemnoslife.com/videos",
        params={"id": video_id, "part": "chapters"},
    )
    resp.raise_for_status()
    chapters = resp.json()["items"][0]["chapters"]["chapters"]
    for chapter in chapters:
        chapter.pop("thumbnails", None)
    return chapters


def add_transcript(chapters, subtitles):
    """将字幕按章节时间段分配到对应章节。"""
    for ii, chapter in enumerate(chapters):
        next_chapter = chapters[ii + 1] if ii < len(chapters) - 1 else {"time": 1e10}
        chapter["text"] = " ".join(
            seg["text"]
            for seg in subtitles
            if chapter["time"] <= seg["start"] < next_chapter["time"]
        )
    return chapters


def create_documents(chapters, video_id, video_title):
    """将章节转换为文档格式。"""
    base_url = f"https://www.youtube.com/watch?v={video_id}"
    documents = []
    for chapter in chapters:
        text = chapter["text"].strip()
        url = f"{base_url}&t={chapter['time']}s"
        doc = {
            "text": text,
            "metadata": {
                "source": url,
                "title": video_title,
                "chapter-title": chapter["title"],
                "full-title": f"{video_title} - {chapter['title']}",
            },
        }
        documents.append(doc)
    return enrich_metadata(documents)


def enrich_metadata(documents):
    """添加 sha256 哈希和 ignore 标记。"""
    for doc in documents:
        m = hashlib.sha256()
        m.update(doc["text"].encode("utf-8", "replace"))
        doc["metadata"]["sha256"] = m.hexdigest()
        doc["metadata"]["ignore"] = False
    return documents


# ── 主流程 ────────────────────────────────────────────────────────────────────

def process_video(video_info, browser=None):
    video_id = video_info["id"]
    video_title = video_info["title"]

    print(f"\n[{video_title}] 开始处理...")

    try:
        subtitles = get_transcript(video_id, browser=browser)
        print(f"  字幕: {len(subtitles)} 段")
    except Exception as e:
        print(f"  ✗ 字幕获取失败: {e}")
        return []

    try:
        chapters = get_chapters(video_id)
        print(f"  章节: {len(chapters)} 个")
    except Exception as e:
        print(f"  ✗ 章节获取失败: {e}")
        return []

    chapters = add_transcript(chapters, subtitles)
    documents = create_documents(chapters, video_id, video_title)
    print(f"  ✓ 生成 {len(documents)} 条文档")
    return documents


def main():
    parser = argparse.ArgumentParser(description="本地视频字幕 ETL")
    parser.add_argument("--json-path", default=DEFAULT_JSON_PATH)
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument(
        "--browser",
        default=None,
        help="从指定浏览器读取 cookie 绕过机器人检测，如 chrome / firefox / safari",
    )
    args = parser.parse_args()

    with open(args.json_path) as f:
        video_infos = json.load(f)

    print(f"共 {len(video_infos)} 个视频，目标: {args.db}.{args.collection}")
    if args.browser:
        print(f"使用 {args.browser} 的 cookie")

    all_documents = []
    for video_info in video_infos:
        docs = process_video(video_info, browser=args.browser)
        all_documents.extend(docs)

    print(f"\n共生成 {len(all_documents)} 条文档，开始上传...")
    if all_documents:
        upload_documents(all_documents, args.db, args.collection)
    print("完成。")


if __name__ == "__main__":
    main()
