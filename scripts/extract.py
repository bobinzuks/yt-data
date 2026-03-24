#!/usr/bin/env python3
import json, os, sys, time
import urllib.request

BROWSE_ID     = "UCDwMsoCCKI_tcXBfgwqqGeg"
VIDEOS_PARAMS = "EgZ2aWRlb3PyBgQKAjoA"   # exact value from Videos tab endpoint
TARGET        = "@vinnystvincent9788"
API           = "https://www.youtube.com/youtubei/v1/browse?prettyPrint=false"

WEB_CONTEXT = {"client": {"clientName":"WEB","clientVersion":"2.20240101.00.00","hl":"en","gl":"US"}}

HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "X-YouTube-Client-Name":    "1",
    "X-YouTube-Client-Version": "2.20240101.00.00",
    "Origin":  "https://www.youtube.com",
    "Referer": "https://www.youtube.com/",
}

def post(body, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(API, data=json.dumps(body).encode(), headers=HEADERS, method="POST")
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except Exception as e:
            print(f"  attempt {attempt+1} failed: {e}", flush=True)
            if attempt < retries-1: time.sleep(2)
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

def extract_videos(data, seen):
    """Extract all unique video dicts from any response structure."""
    videos = []
    # gridVideoRenderer (Videos tab grid layout)
    for vr in find_all(data, "gridVideoRenderer"):
        vid_id = vr.get("videoId","")
        if vid_id and vid_id not in seen and "title" in vr:
            seen.add(vid_id)
            videos.append({
                "id":          vid_id,
                "url":         f"https://www.youtube.com/watch?v={vid_id}",
                "title":       get_text(vr.get("title")),
                "published":   get_text(vr.get("publishedTimeText")),
                "duration":    get_text(vr.get("lengthText")),
                "views":       get_text(vr.get("viewCountText") or vr.get("shortViewCountText")),
                "description": get_text(vr.get("descriptionSnippet")),
                "thumbnail":   ((vr.get("thumbnail") or {}).get("thumbnails") or [{}])[-1].get("url",""),
            })
    # richItemRenderer > videoRenderer (alternative layout)
    for item in find_all(data, "richItemRenderer"):
        vr = (item.get("content") or {}).get("videoRenderer") or {}
        vid_id = vr.get("videoId","")
        if vid_id and vid_id not in seen and "title" in vr:
            seen.add(vid_id)
            videos.append({
                "id":          vid_id,
                "url":         f"https://www.youtube.com/watch?v={vid_id}",
                "title":       get_text(vr.get("title")),
                "published":   get_text(vr.get("publishedTimeText")),
                "duration":    get_text(vr.get("lengthText")),
                "views":       get_text(vr.get("viewCountText") or vr.get("shortViewCountText")),
                "description": get_text(vr.get("descriptionSnippet")),
                "thumbnail":   ((vr.get("thumbnail") or {}).get("thumbnails") or [{}])[-1].get("url",""),
            })
    return videos

print(f"Target: {TARGET}  Browse: {BROWSE_ID}", flush=True)

seen = set()
videos = []

# Page 1 — use exact params from Videos tab
data = post({"context": WEB_CONTEXT, "browseId": BROWSE_ID, "params": VIDEOS_PARAMS})
if not data:
    print("ERROR: initial browse failed", flush=True)
    sys.exit(1)

os.makedirs("output", exist_ok=True)

page_vids = extract_videos(data, seen)
videos.extend(page_vids)
print(f"  page 1: +{len(page_vids)} → total {len(videos)}", flush=True)

# Paginate
page = 1
while True:
    token = find_continuation(data)
    if not token:
        print("  no continuation token — done", flush=True)
        break
    page += 1
    time.sleep(0.4)
    data = post({"context": WEB_CONTEXT, "continuation": token})
    if not data:
        print("  no response — stopping", flush=True)
        break
    page_vids = extract_videos(data, seen)
    videos.extend(page_vids)
    print(f"  page {page}: +{len(page_vids)} → total {len(videos)}", flush=True)
    if not page_vids and page > 2:
        print("  empty page — done", flush=True)
        break

result = {"channel": TARGET, "browse_id": BROWSE_ID, "count": len(videos), "videos": videos}
with open("output/result_full.json", "w") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"\nDONE: {len(videos)} videos", flush=True)
for v in videos:
    print(f"  [{v.get('duration','?'):>7}]  {v.get('views','?'):>12}  {v['title']}", flush=True)
