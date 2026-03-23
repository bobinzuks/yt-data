#!/usr/bin/env python3
import argparse, json, sys
from datetime import datetime

def _dur(secs):
    if not secs: return None
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def extract_channel(url, limit=None, sort_by="newest"):
    try:
        import scrapetube
    except ImportError:
        print("ERROR: pip install scrapetube", file=sys.stderr); sys.exit(1)

    # normalise handle
    if url.startswith("@"):
        channel_url = f"https://www.youtube.com/{url}"
    elif not url.startswith("http"):
        channel_url = f"https://www.youtube.com/@{url}"
    else:
        channel_url = url

    print(f"Fetching: {channel_url}", file=sys.stderr)
    results = []
    try:
        for v in scrapetube.get_channel(
            channel_url=channel_url,
            limit=limit,
            sort_by=sort_by,
            sleep=0.5,
        ):
            vid_id = v.get("videoId", "")
            title  = v.get("title", {}).get("runs", [{}])[0].get("text", "")
            thumb  = (v.get("thumbnail", {}).get("thumbnails") or [{}])[-1].get("url", "")
            length = v.get("lengthText", {}).get("simpleText", "")
            views  = v.get("viewCountText", {}).get("simpleText", "")
            published = (v.get("publishedTimeText", {}) or {}).get("simpleText") or \
                        (v.get("videoInfo", {}).get("runs") or [{}])[0].get("text", "")
            desc = (v.get("descriptionSnippet", {}).get("runs") or [{}])[0].get("text", "")
            results.append({
                "id": vid_id,
                "url": f"https://www.youtube.com/watch?v={vid_id}",
                "title": title,
                "published": published,
                "duration": length,
                "views": views,
                "description": desc,
                "thumbnail": thumb,
            })
            if len(results) % 10 == 0:
                print(f"  ... {len(results)} videos so far", file=sys.stderr)
    except Exception as e:
        print(f"ERROR during scrape: {e}", file=sys.stderr)
        if not results:
            sys.exit(1)

    return {"channel_url": channel_url, "sort_by": sort_by, "count": len(results), "videos": results}

def extract_video(url):
    try:
        import yt_dlp
    except ImportError:
        print("ERROR: pip install yt-dlp", file=sys.stderr); sys.exit(1)
    ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
    return {
        "id": info.get("id"), "url": url,
        "title": info.get("title"), "channel": info.get("uploader"),
        "channel_url": info.get("uploader_url"),
        "published": info.get("upload_date"),
        "duration": _dur(info.get("duration")),
        "views": info.get("view_count"), "likes": info.get("like_count"),
        "description": info.get("description"),
        "tags": info.get("tags", []), "chapters": info.get("chapters"),
    }

def search_youtube(query, limit=10):
    try:
        import yt_dlp
    except ImportError:
        print("ERROR: pip install yt-dlp", file=sys.stderr); sys.exit(1)
    ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True, "extract_flat": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
    results = []
    for e in (info.get("entries") or []):
        results.append({
            "id": e.get("id"),
            "url": f"https://www.youtube.com/watch?v={e.get('id')}",
            "title": e.get("title"),
            "channel": e.get("uploader") or e.get("channel"),
            "duration": _dur(e.get("duration")),
            "views": e.get("view_count"),
        })
    return {"query": query, "count": len(results), "results": results}

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ch = sub.add_parser("channel")
    ch.add_argument("url")
    ch.add_argument("--limit", type=int, default=None)
    ch.add_argument("--sort", default="newest", choices=["newest","oldest","popular"])
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

    out_str = json.dumps(data, indent=2, ensure_ascii=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out_str)
        items = data.get("count", 1)
        print(f"Saved {items} items to {args.out}")
    else:
        print(out_str)

if __name__ == "__main__":
    main()
