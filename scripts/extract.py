#!/usr/bin/env python3
import json, os, sys, time
import urllib.request, urllib.error

BROWSE_ID = "UCDwMsoCCKI_tcXBfgwqqGeg"
TARGET    = "@vinnystvincent9788"
API       = "https://www.youtube.com/youtubei/v1"

WEB_CONTEXT = {
    "client": {
        "clientName": "WEB",
        "clientVersion": "2.20240101.00.00",
        "hl": "en", "gl": "US",
    }
}

HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "X-YouTube-Client-Name":    "1",
    "X-YouTube-Client-Version": "2.20240101.00.00",
    "Origin":  "https://www.youtube.com",
    "Referer": "https://www.youtube.com/",
}

def post(endpoint, body):
    url = f"{API}/{endpoint}?prettyPrint=false"
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())

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

print(f"Browse ID: {BROWSE_ID}", flush=True)
print("Fetching page 1...", flush=True)

data = post("browse", {"context": WEB_CONTEXT, "browseId": BROWSE_ID, "params": "EgZ2aWRlb3M%3D"})

# Dump top-level keys and structure for debugging
print(f"Top keys: {list(data.keys())}", flush=True)
os.makedirs("output", exist_ok=True)

# Save raw response for inspection
with open("output/raw_response.json", "w") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
print("Raw response saved to output/raw_response.json", flush=True)

# Try ALL known video renderer key names
video_keys = ["videoRenderer", "richItemRenderer", "gridVideoRenderer",
              "compactVideoRenderer", "reelItemRenderer", "shortsLockupViewModel"]

videos = []
for key in video_keys:
    found = find_all(data, key)
    if found:
        print(f"  Found {len(found)} '{key}' items", flush=True)
        for item in found:
            # Handle richItemRenderer wrapper
            vr = item
            if key == "richItemRenderer":
                vr = (item.get("content") or {}).get("videoRenderer") or item
            if vr and vr.get("videoId"):
                videos.append(parse_video(vr))

# Also try finding any videoId directly
all_vid_ids = find_all(data, "videoId")
print(f"  Total videoId fields found: {len(all_vid_ids)}", flush=True)
for vid_id in all_vid_ids[:5]:
    print(f"    videoId: {vid_id}", flush=True)

print(f"\nExtracted {len(videos)} videos from page 1", flush=True)

# Paginate
page = 1
while page < 50:
    token = find_continuation(data)
    if not token:
        print("No more pages", flush=True)
        break
    page += 1
    time.sleep(0.4)
    print(f"Fetching page {page}...", flush=True)
    data = post("browse", {"context": WEB_CONTEXT, "continuation": token})
    if not data: break

    new_vids = []
    for key in video_keys:
        for item in find_all(data, key):
            vr = item
            if key == "richItemRenderer":
                vr = (item.get("content") or {}).get("videoRenderer") or item
            if vr and vr.get("videoId"):
                new_vids.append(parse_video(vr))
    videos.extend(new_vids)
    print(f"  page {page}: +{len(new_vids)} (total {len(videos)})", flush=True)
    if not new_vids: break

result = {"channel": TARGET, "browse_id": BROWSE_ID, "count": len(videos), "videos": videos}
with open("output/result_full.json", "w") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"\nDONE: {len(videos)} videos", flush=True)
for v in videos[:10]:
    print(f"  [{v.get('duration','?'):>7}]  {v.get('views','?'):>12}  {v['title']}", flush=True)
