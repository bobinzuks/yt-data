#!/usr/bin/env python3
import yt_dlp, json, sys, os

target = open("target.txt").readline().strip().split()[0]
url = f"https://www.youtube.com/{target}/videos" if target.startswith("@") else target
print(f"Extracting ALL videos from: {url}", flush=True)

def dur(s):
    if not s: return None
    m,sec=divmod(int(s),60); h,m=divmod(m,60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"

with yt_dlp.YoutubeDL({"quiet":False,"no_warnings":True,"extract_flat":"in_playlist","ignoreerrors":True}) as ydl:
    info = ydl.extract_info(url, download=False)

entries = [e for e in (info.get("entries") or []) if e and e.get("id")]
channel = info.get("channel") or info.get("uploader") or target
print(f"Found {len(entries)} videos | Channel: {channel}", flush=True)

videos = []
for e in entries:
    videos.append({
        "id":         e.get("id"),
        "url":        f"https://www.youtube.com/watch?v={e.get('id')}",
        "title":      e.get("title"),
        "published":  e.get("upload_date") or str(e.get("timestamp",""))[:10],
        "duration":   dur(e.get("duration")),
        "duration_s": e.get("duration"),
        "views":      e.get("view_count"),
        "description": (e.get("description") or "")[:500],
        "thumbnail":  e.get("thumbnail"),
    })
    print(f"  [{len(videos):3}] [{videos[-1]['duration'] or '?':>7}] {str(e.get('view_count') or ''):>8}  {e.get('title','')}", flush=True)

os.makedirs("output", exist_ok=True)
out = {"channel": channel, "url": url, "count": len(videos), "videos": videos}
with open("output/result_full.json","w") as f:
    json.dump(out, f, indent=2, ensure_ascii=False)

total_s = sum(v.get("duration_s") or 0 for v in videos)
h,rem=divmod(total_s,3600); m,s=divmod(rem,60)
print(f"\nSaved {len(videos)} videos | Total runtime: {h}h {m}m")
