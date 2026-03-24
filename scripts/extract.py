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

def extract_all_videos(data):
    """Extract from ALL renderer types - gridVideoRenderer, videoRenderer, richItemRenderer"""
    seen = set()
    videos = []

    # All known renderer types
    for key in ["gridVideoRenderer", "videoRenderer", "richItemRenderer",
                "compactVideoRenderer", "reelItemRenderer"]:
        for item in find_all(data, key):
            # unwrap richItemRenderer
            vr = (item.get("content") or {}).get("videoRenderer") if key == "richItemRenderer" else item
            if not vr: continue
            vid_id = vr.get("videoId","")
            if not vid_id or vid_id in seen: continue
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

print(f"Browse ID: {BROWSE_ID}", flush=True)

videos = []
seen_ids = set()
page = 0

# Initial request
data = post("browse", {"context": WEB_CONTEXT, "browseId": BROWSE_ID, "params": "EgZ2aWRlb3M%3D"})
page_vids = extract_all_videos(data)
for v in page_vids:
    if v["id"] not in seen_ids:
        seen_ids.add(v["id"])
        videos.append(v)
print(f"  page 1: +{len(page_vids)} → total {len(videos)}", flush=True)

# Save raw first page for inspection
os.makedirs("output", exist_ok=True)
with open("output/raw_response.json","w") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

# Paginate
while True:
    token = find_continuation(data)
    if not token:
        print("  no more pages", flush=True)
        break
    page += 1
    time.sleep(0.4)
    data = post("browse", {"context": WEB_CONTEXT, "continuation": token})
    if not data: break
    page_vids = extract_all_videos(data)
    new = [v for v in page_vids if v["id"] not in seen_ids]
    for v in new:
        seen_ids.add(v["id"])
        videos.append(v)
    print(f"  page {page+1}: +{len(new)} → total {len(videos)}", flush=True)
    if not new: break

result = {"channel": TARGET, "browse_id": BROWSE_ID, "count": len(videos), "videos": videos}
with open("output/result_full.json","w") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"\nDONE: {len(videos)} videos", flush=True)
for v in videos:
    print(f"  [{v.get('duration','?'):>7}]  {v.get('views','?'):>12}  {v['title']}", flush=True)
