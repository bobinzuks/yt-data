#!/usr/bin/env python3
"""
YouTube channel extractor using InnerTube API directly.
No yt-dlp, no scraping — uses YouTube's own internal API.
Pure stdlib, no pip installs needed beyond requests.
"""
import json, os, re, sys, time
import urllib.request, urllib.error

# ── InnerTube config ──────────────────────────────────────────────────────────

API_URL = "https://www.youtube.com/youtubei/v1/browse?prettyPrint=false"

CONTEXT = {
    "client": {
        "clientName": "ANDROID",
        "clientVersion": "19.09.37",
        "androidSdkVersion": 30,
        "userAgent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11) gzip",
        "hl": "en",
        "gl": "US",
    }
}

REQ_HEADERS = {
    "Content-Type":             "application/json",
    "User-Agent":               "com.google.android.youtube/19.09.37 (Linux; U; Android 11) gzip",
    "X-YouTube-Client-Name":    "3",
    "X-YouTube-Client-Version": "19.09.37",
    "Accept-Language":          "en-US,en;q=0.9",
}

VIDEOS_PARAM = "EgZ2aWRlb3PyBgQKAjoA"  # encodes "videos" tab

# ── helpers ───────────────────────────────────────────────────────────────────

def post(body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(API_URL, data=data, headers=REQ_HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def dur_str(s):
    if not s: return None
    m, sec = divmod(int(s), 60); h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"

def get_text(obj):
    if not obj: return ""
    if isinstance(obj, str): return obj
    runs = obj.get("runs") or []
    return "".join(r.get("text","") for r in runs) or obj.get("simpleText","")

def find_all(obj, key, results=None):
    """Recursively find all values for a key in nested dicts/lists."""
    if results is None: results = []
    if isinstance(obj, dict):
        if key in obj: results.append(obj[key])
        for v in obj.values(): find_all(v, key, results)
    elif isinstance(obj, list):
        for item in obj: find_all(item, key, results)
    return results

def extract_videos_from_response(data):
    """Pull video renderer objects out of any InnerTube browse response."""
    renderers = find_all(data, "richItemRenderer") or find_all(data, "gridVideoRenderer") or []
    # richItemRenderer wraps content
    videos = []
    for r in renderers:
        vr = None
        if "content" in r:
            vr = r["content"].get("videoRenderer") or r["content"].get("reelItemRenderer")
        elif "videoId" in r:
            vr = r
        if vr:
            videos.append(vr)
    # Also try direct videoRenderer search
    if not videos:
        videos = find_all(data, "videoRenderer")
    return videos

def parse_video(vr):
    vid_id = vr.get("videoId","")
    title  = get_text(vr.get("title"))
    views_text = get_text(vr.get("viewCountText") or vr.get("shortViewCountText"))
    published  = get_text(vr.get("publishedTimeText"))
    dur_text   = get_text((vr.get("lengthText") or {}).get("simpleText") and vr.get("lengthText") or vr.get("lengthText"))
    dur_txt    = get_text(vr.get("lengthText")) if vr.get("lengthText") else None
    desc       = get_text(vr.get("descriptionSnippet"))
    thumb      = ((vr.get("thumbnail") or {}).get("thumbnails") or [{}])[-1].get("url","")
    return {
        "id":        vid_id,
        "url":       f"https://www.youtube.com/watch?v={vid_id}",
        "title":     title,
        "published": published,
        "duration":  dur_txt,
        "views":     views_text,
        "description": desc,
        "thumbnail": thumb,
    }

def find_continuation(data):
    tokens = find_all(data, "token")
    # continuation tokens are long base64-ish strings
    for t in tokens:
        if isinstance(t, str) and len(t) > 50:
            return t
    return None

# ── main ──────────────────────────────────────────────────────────────────────

def resolve_channel(handle):
    """Get browseId from @handle by fetching channel page."""
    handle = handle.lstrip("@")
    url = f"https://www.youtube.com/@{handle}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    with urllib.request.urlopen(req, timeout=15) as r:
        html = r.read().decode("utf-8", errors="ignore")
    m = re.search(r'"browseId"\s*:\s*"(UC[^"]{20,})"', html)
    if m: return m.group(1)
    m = re.search(r'"channelId"\s*:\s*"(UC[^"]{20,})"', html)
    if m: return m.group(1)
    raise ValueError(f"Could not find browseId for @{handle}")

def get_all_videos(browse_id):
    videos = []
    page = 0

    # Initial request
    body = {"context": CONTEXT, "browseId": browse_id, "params": VIDEOS_PARAM}
    data = post(body)
    vrs = extract_videos_from_response(data)
    for vr in vrs:
        v = parse_video(vr)
        if v["id"]: videos.append(v)
    print(f"  page {page+1}: +{len(vrs)} videos (total {len(videos)})", flush=True)

    # Paginate
    while True:
        token = find_continuation(data)
        if not token: break
        page += 1
        time.sleep(0.5)
        try:
            body = {"context": CONTEXT, "continuation": token}
            data = post(body)
            vrs = extract_videos_from_response(data)
            if not vrs: break
            for vr in vrs:
                v = parse_video(vr)
                if v["id"]: videos.append(v)
            print(f"  page {page+1}: +{len(vrs)} videos (total {len(videos)})", flush=True)
        except Exception as e:
            print(f"  pagination error: {e}", flush=True)
            break

    return videos


if __name__ == "__main__":
    target = open("target.txt").readline().strip().split()[0]
    print(f"Target: {target}", flush=True)

    print("Resolving channel...", flush=True)
    browse_id = resolve_channel(target)
    print(f"Browse ID: {browse_id}", flush=True)

    print("Fetching all videos via InnerTube...", flush=True)
    videos = get_all_videos(browse_id)

    os.makedirs("output", exist_ok=True)
    result = {
        "channel": target,
        "browse_id": browse_id,
        "count": len(videos),
        "videos": videos,
    }
    with open("output/result_full.json", "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\nDONE: {len(videos)} videos saved", flush=True)
    for v in videos[:20]:
        print(f"  [{v.get('duration','?'):>7}]  {v.get('views','?'):>12}  {v['title']}", flush=True)
