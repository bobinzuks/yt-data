#!/usr/bin/env python3
"""
YouTube Channel & Video Information Extractor
Uses yt-dlp (no API key required) to pull full metadata.
Usage:
  python yt_extract.py channel <url_or_handle> [--limit N] [--sort newest|oldest|popular] [--out file.json]
  python yt_extract.py video   <url>                                                        [--out file.json]
  python yt_extract.py search  <query>           [--limit N]                                [--out file.json]
"""

import argparse
import json
import sys
from datetime import datetime

# ── helpers ──────────────────────────────────────────────────────────────────

def _require(pkg, install_hint):
    try:
        return __import__(pkg)
    except ImportError:
        print(f"[error] missing '{pkg}'. Install: pip install {install_hint} --break-system-packages")
        sys.exit(1)

def _iso(ts):
    """yt-dlp upload_date → ISO string."""
    if not ts:
        return None
    try:
        return datetime.strptime(str(ts), "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return str(ts)

def _dur(secs):
    if not secs:
        return None
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

# ── channel ───────────────────────────────────────────────────────────────────

def extract_channel(url, limit=None, sort_by="newest"):
    scrapetube = _require("scrapetube", "scrapetube")

    # normalise handle → channel_url
    if url.startswith("@"):
        url = f"https://www.youtube.com/{url}"
    elif not url.startswith("http"):
        url = f"https://www.youtube.com/@{url}"

    videos = scrapetube.get_channel(
        channel_url=url,
        limit=limit,
        sort_by=sort_by,
        sleep=0.5,
    )

    results = []
    for v in videos:
        vid_id = v.get("videoId", "")
        title  = v.get("title", {}).get("runs", [{}])[0].get("text", "")
        thumb  = (v.get("thumbnail", {}).get("thumbnails") or [{}])[-1].get("url", "")
        length = v.get("lengthText", {}).get("simpleText", "")
        views  = v.get("viewCountText", {}).get("simpleText", "")
        published = (
            v.get("publishedTimeText", {}).get("simpleText")
            or v.get("videoInfo", {}).get("runs", [{}])[0].get("text", "")
        )
        desc = (
            v.get("descriptionSnippet", {})
             .get("runs", [{}])[0]
             .get("text", "")
        )

        results.append({
            "id":          vid_id,
            "url":         f"https://www.youtube.com/watch?v={vid_id}",
            "title":       title,
            "published":   published,
            "duration":    length,
            "views":       views,
            "description": desc,
            "thumbnail":   thumb,
        })

    return {"channel_url": url, "sort_by": sort_by, "count": len(results), "videos": results}


# ── single video ──────────────────────────────────────────────────────────────

def extract_video(url):
    yt_dlp = _require("yt_dlp", "yt-dlp")

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "forcejson": False,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    return {
        "id":           info.get("id"),
        "url":          url,
        "title":        info.get("title"),
        "channel":      info.get("uploader"),
        "channel_url":  info.get("uploader_url"),
        "published":    _iso(info.get("upload_date")),
        "duration":     _dur(info.get("duration")),
        "views":        info.get("view_count"),
        "likes":        info.get("like_count"),
        "comments":     info.get("comment_count"),
        "description":  info.get("description"),
        "tags":         info.get("tags", []),
        "categories":   info.get("categories", []),
        "thumbnail":    info.get("thumbnail"),
        "subtitles":    list((info.get("subtitles") or {}).keys()),
        "chapters":     info.get("chapters"),
    }


# ── search ────────────────────────────────────────────────────────────────────

def search_youtube(query, limit=10):
    yt_dlp = _require("yt_dlp", "yt-dlp")

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
    }
    search_url = f"ytsearch{limit}:{query}"
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(search_url, download=False)

    results = []
    for entry in info.get("entries", []):
        results.append({
            "id":        entry.get("id"),
            "url":       f"https://www.youtube.com/watch?v={entry.get('id')}",
            "title":     entry.get("title"),
            "channel":   entry.get("uploader") or entry.get("channel"),
            "duration":  _dur(entry.get("duration")),
            "views":     entry.get("view_count"),
        })

    return {"query": query, "count": len(results), "results": results}


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="YouTube information extractor (no API key)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # channel
    ch = sub.add_parser("channel", help="List all videos from a channel")
    ch.add_argument("url", help="Channel URL, handle (@name), or username")
    ch.add_argument("--limit",  type=int, default=None, help="Max videos (default: all)")
    ch.add_argument("--sort",   default="newest", choices=["newest","oldest","popular"])
    ch.add_argument("--out",    default=None, help="Save JSON to this file")

    # video
    vi = sub.add_parser("video", help="Full metadata for a single video")
    vi.add_argument("url")
    vi.add_argument("--out", default=None)

    # search
    se = sub.add_parser("search", help="Search YouTube")
    se.add_argument("query", nargs="+")
    se.add_argument("--limit", type=int, default=10)
    se.add_argument("--out", default=None)

    args = parser.parse_args()

    if args.cmd == "channel":
        data = extract_channel(args.url, limit=args.limit, sort_by=args.sort)
    elif args.cmd == "video":
        data = extract_video(args.url)
    else:
        data = search_youtube(" ".join(args.query), limit=args.limit)

    out_str = json.dumps(data, indent=2, ensure_ascii=False)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out_str)
        print(f"[saved] {args.out}  ({data.get('count', 1)} items)")
    else:
        print(out_str)


if __name__ == "__main__":
    main()
