#!/usr/bin/env python3
"""YouTube extractor — uses yt-dlp only, no scrapetube."""
import argparse, json, sys

def _dur(secs):
    if not secs: return None
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def extract_channel(url, limit=None, sort_by="newest"):
    import yt_dlp
    if url.startswith("@"):
        url = f"https://www.youtube.com/{url}/videos"
    elif not url.startswith("http"):
        url = f"https://www.youtube.com/@{url}/videos"
    elif not url.endswith("/videos"):
        url = url.rstrip("/") + "/videos"

    print(f"Fetching: {url}", file=sys.stderr)

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "playlistend": limit,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    entries = info.get("entries") or []
    results = []
    for e in entries:
        results.append({
            "id":          e.get("id"),
            "url":         f"https://www.youtube.com/watch?v={e.get('id')}",
            "title":       e.get("title"),
            "published":   e.get("upload_date") or e.get("timestamp"),
            "duration":    _dur(e.get("duration")),
            "views":       e.get("view_count"),
            "description": e.get("description") or "",
            "thumbnail":   e.get("thumbnail"),
            "channel":     e.get("channel") or info.get("channel"),
        })
        print(f"  {len(results):3}. {e.get('title','?')}", file=sys.stderr)

    return {
        "channel_url": url,
        "channel":     info.get("channel") or info.get("uploader"),
        "sort_by":     sort_by,
        "count":       len(results),
        "videos":      results,
    }

def extract_video(url):
    import yt_dlp
    with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True, "skip_download": True}) as ydl:
        info = ydl.extract_info(url, download=False)
    return {
        "id": info.get("id"), "url": url,
        "title": info.get("title"), "channel": info.get("uploader"),
        "published": info.get("upload_date"),
        "duration": _dur(info.get("duration")),
        "views": info.get("view_count"), "likes": info.get("like_count"),
        "description": info.get("description"),
        "tags": info.get("tags", []), "chapters": info.get("chapters"),
    }

def search_youtube(query, limit=10):
    import yt_dlp
    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
    return {
        "query": query,
        "count": len(info.get("entries", [])),
        "results": [
            {
                "id": e.get("id"),
                "url": f"https://www.youtube.com/watch?v={e.get('id')}",
                "title": e.get("title"),
                "channel": e.get("channel") or e.get("uploader"),
                "duration": _dur(e.get("duration")),
                "views": e.get("view_count"),
            }
            for e in (info.get("entries") or [])
        ],
    }

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ch = sub.add_parser("channel")
    ch.add_argument("url")
    ch.add_argument("--limit", type=int, default=None)
    ch.add_argument("--sort", default="newest")
    ch.add_argument("--out", default=None)

    vi = sub.add_parser("video")
    vi.add_argument("url")
    vi.add_argument("--out", default=None)

    se = sub.add_parser("search")
    se.add_argument("query", nargs="+")
    se.add_argument("--limit", type=int, default=10)
    se.add_argument("--out", default=None)

    args = ap.parse_args()

    if args.cmd == "channel":
        data = extract_channel(args.url, limit=args.limit, sort_by=args.sort)
    elif args.cmd == "video":
        data = extract_video(args.url)
    else:
        data = search_youtube(" ".join(args.query), limit=args.limit)

    out = json.dumps(data, indent=2, ensure_ascii=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"Saved {data.get('count', 1)} items → {args.out}")
    else:
        print(out)

if __name__ == "__main__":
    main()
