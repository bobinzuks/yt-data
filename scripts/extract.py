#!/usr/bin/env python3
import json, os, sys, time
import urllib.request, urllib.error

TARGET     = open("target.txt").readline().strip().split()[0]
BROWSE_ID  = "UCDwMsoCCKI_tcXBfgwqqGeg"   # confirmed from last run
API        = "https://www.youtube.com/youtubei/v1"

# WEB client — works for channel browse
WEB_CONTEXT = {
    "client": {
        "clientName": "WEB",
        "clientVersion": "2.20240101.00.00",
        "hl": "en",
        "gl": "US",
    }
}

WEB_HEADERS = {
    "Content-Type":  "application/json; charset=UTF-8",
    "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "X-YouTube-Client-Name":    "1",
    "X-YouTube-Client-Version": "2.20240101.00.00",
    "Origin":  "https://www.youtube.com",
    "Referer": "https://www.youtube.com/",
}

# WEB videos tab params (base64 of protobuf: videos tab)
VIDEOS_PARAMS = "EgZ2aWRlb3M%3D"

def post(endpoint, body, retries=3):
    url = f"{API}/{endpoint}?prettyPrint=false"
    data = json.dumps(body).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=WEB_HEADERS, method="POST")
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} on attempt {attempt+1}", flush=True)
            if e.code == 400 and attempt == 0:
                # Try without params on retry
                pass
            if attempt < retries - 1:
                time.sleep(1.5)
        except Exception as e:
            print(f"  attempt {attempt+1}: {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(1.5)
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

def extract_video_renderers(data):
    out = []
    # richItemRenderer (channel page layout)
    for r in find_all(data, "richItemRenderer"):
        vr = (r.get("content") or {}).get("videoRenderer")
        if vr and vr.get("videoId"): out.append(vr)
    # direct videoRenderer
    if not out:
        for vr in find_all(data, "videoRenderer"):
            if vr.get("videoId"): out.append(vr)
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

# ── Fetch all videos ──────────────────────────────────────────────────────────
print(f"Target:    {TARGET}", flush=True)
print(f"Browse ID: {BROWSE_ID}", flush=True)
print("Fetching videos...", flush=True)

videos = []

# Initial browse — WEB client, videos tab
body = {"context": WEB_CONTEXT, "browseId": BROWSE_ID, "params": "EgZ2aWRlb3M%3D"}
print(f"  POST browse page 1...", flush=True)
data = post("browse", body)

if not data:
    # Retry without params
    print("  retrying without params...", flush=True)
    body = {"context": WEB_CONTEXT, "browseId": BROWSE_ID}
    data = post("browse", body)

if not data:
    print("ERROR: InnerTube browse failed completely", flush=True)
    sys.exit(1)

vrs = extract_video_renderers(data)
for vr in vrs:
    v = parse_video(vr)
    if v["id"]: videos.append(v)
print(f"  page 1: {len(vrs)} videos (total {len(videos)})", flush=True)

# Paginate via continuation tokens
page = 1
while True:
    token = find_continuation(data)
    if not token:
        print("  no more pages", flush=True)
        break
    page += 1
    time.sleep(0.4)
    print(f"  POST browse page {page}...", flush=True)
    data = post("browse", {"context": WEB_CONTEXT, "continuation": token})
    if not data:
        print("  pagination stopped (no response)", flush=True)
        break
    vrs = extract_video_renderers(data)
    if not vrs:
        print("  pagination stopped (no videos in response)", flush=True)
        break
    for vr in vrs:
        v = parse_video(vr)
        if v["id"]: videos.append(v)
    print(f"  page {page}: +{len(vrs)} videos (total {len(videos)})", flush=True)

# ── Save ──────────────────────────────────────────────────────────────────────
os.makedirs("output", exist_ok=True)
result = {"channel": TARGET, "browse_id": BROWSE_ID, "count": len(videos), "videos": videos}
with open("output/result_full.json", "w") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"\nDONE: {len(videos)} videos saved", flush=True)
for v in videos[:20]:
    print(f"  [{v.get('duration','?'):>7}]  {v.get('views','?'):>14}  {v['title']}", flush=True)
