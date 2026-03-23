#!/usr/bin/env python3
"""
YouTube InnerTube extractor - handles both resolve and direct browse.
Falls back gracefully at each step.
"""
import json, os, re, sys, time
import urllib.request, urllib.error

TARGET = open("target.txt").readline().strip().split()[0]
print(f"[1] Target: {TARGET}", flush=True)

API = "https://www.youtube.com/youtubei/v1"
KEY = "AIzaSyA8eiZmM1FaDVjRy-df2KTyQ_vz_yYM394"  # public Android key

CONTEXT = {
    "client": {
        "clientName": "ANDROID",
        "clientVersion": "19.09.37",
        "androidSdkVersion": 30,
        "userAgent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11) gzip",
        "hl": "en", "gl": "US",
    }
}

HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "User-Agent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11) gzip",
    "X-YouTube-Client-Name": "3",
    "X-YouTube-Client-Version": "19.09.37",
}

def post(endpoint, body, retries=3):
    url = f"{API}/{endpoint}?key={KEY}&prettyPrint=false"
    data = json.dumps(body).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except Exception as e:
            print(f"  attempt {attempt+1} failed: {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None

def get_text(obj):
    if not obj: return ""
    if isinstance(obj, str): return obj
    if isinstance(obj, dict):
        if "simpleText" in obj: return obj["simpleText"]
        return "".join(r.get("text","") for r in obj.get("runs",[]))
    return ""

def find_all(obj, key, out=None):
    if out is None: out = []
    if isinstance(obj, dict):
        if key in obj: out.append(obj[key])
        for v in obj.values(): find_all(v, key, out)
    elif isinstance(obj, list):
        for i in obj: find_all(i, key, out)
    return out

def find_continuation(data):
    for t in find_all(data, "token"):
        if isinstance(t, str) and len(t) > 50:
            return t
    return None

def extract_videos(data):
    out = []
    for key in ["richItemRenderer", "gridVideoRenderer", "videoRenderer"]:
        items = find_all(data, key)
        for item in items:
            vr = item.get("content", {}).get("videoRenderer") or item if "videoId" in item else None
            if vr and vr.get("videoId"):
                out.append(vr)
    return out

def parse_video(vr):
    return {
        "id":          vr.get("videoId",""),
        "url":         f"https://www.youtube.com/watch?v={vr.get('videoId','')}",
        "title":       get_text(vr.get("title")),
        "published":   get_text(vr.get("publishedTimeText")),
        "duration":    get_text(vr.get("lengthText")),
        "views":       get_text(vr.get("viewCountText") or vr.get("shortViewCountText")),
        "description": get_text(vr.get("descriptionSnippet")),
        "thumbnail":   ((vr.get("thumbnail") or {}).get("thumbnails") or [{}])[-1].get("url",""),
    }

# ── Step 1: Resolve browse ID ─────────────────────────────────────────────────
print("[2] Resolving channel browse ID...", flush=True)

browse_id = None

# Method A: search InnerTube
handle = TARGET.lstrip("@")
body = {"context": CONTEXT, "query": handle}
resp = post("search", body)
if resp:
    channels = find_all(resp, "channelRenderer")
    for ch in channels:
        ch_id = ch.get("channelId","")
        ch_title = get_text(ch.get("title"))
        print(f"  found channel: {ch_title} ({ch_id})", flush=True)
        if handle.lower() in ch_title.lower() or handle.lower() in ch_id.lower():
            browse_id = ch_id
            break
    if not browse_id and channels:
        browse_id = channels[0].get("channelId")
        print(f"  using first result: {browse_id}", flush=True)

# Method B: fetch channel page HTML
if not browse_id:
    print("  search failed, trying HTML page...", flush=True)
    try:
        req = urllib.request.Request(
            f"https://www.youtube.com/@{handle}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="ignore")
        m = re.search(r'"browseId"\s*:\s*"(UC[^"]{20,})"', html) or \
            re.search(r'"channelId"\s*:\s*"(UC[^"]{20,})"', html)
        if m:
            browse_id = m.group(1)
            print(f"  resolved from HTML: {browse_id}", flush=True)
    except Exception as e:
        print(f"  HTML fetch failed: {e}", flush=True)

if not browse_id:
    print("ERROR: Could not resolve channel ID", flush=True)
    sys.exit(1)

print(f"[3] Browse ID: {browse_id}", flush=True)

# ── Step 2: Fetch all videos ──────────────────────────────────────────────────
print("[4] Fetching videos...", flush=True)
videos = []
page = 0

body = {"context": CONTEXT, "browseId": browse_id, "params": "EgZ2aWRlb3PyBgQKAjoA"}
data = post("browse", body)
if not data:
    print("ERROR: InnerTube browse failed", flush=True)
    sys.exit(1)

vrs = extract_videos(data)
for vr in vrs:
    v = parse_video(vr)
    if v["id"]: videos.append(v)
print(f"  page 1: +{len(vrs)} (total {len(videos)})", flush=True)

while True:
    token = find_continuation(data)
    if not token: break
    page += 1
    time.sleep(0.3)
    data = post("browse", {"context": CONTEXT, "continuation": token})
    if not data: break
    vrs = extract_videos(data)
    if not vrs: break
    for vr in vrs:
        v = parse_video(vr)
        if v["id"]: videos.append(v)
    print(f"  page {page+1}: +{len(vrs)} (total {len(videos)})", flush=True)

# ── Step 3: Save ──────────────────────────────────────────────────────────────
print(f"[5] Saving {len(videos)} videos...", flush=True)
os.makedirs("output", exist_ok=True)
out = {"channel": handle, "browse_id": browse_id, "count": len(videos), "videos": videos}
with open("output/result_full.json", "w") as f:
    json.dump(out, f, indent=2, ensure_ascii=False)

print(f"\nDONE: {len(videos)} videos", flush=True)
for v in videos[:15]:
    print(f"  [{v.get('duration','?'):>7}]  {v.get('views','?'):>12}  {v['title']}", flush=True)
