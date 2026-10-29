#!/usr/bin/env python3
"""Автопостинг в Threads через официальный API. Поддерживает цепочки (ветка 1, ответ в ветке 2) и медиа.
Читает все файлы очереди queue*.json (queue.json, queue_ab.json и т.д.).
Часть цепочки: строка или {"text": ..., "media": ["https://...jpg", "https://...mp4"]}.
Одно медиа: пост с фото/видео. Несколько: карусель."""
import argparse, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
STATE = HERE / "state.json"
API = "https://graph.threads.net"
MAX_LATE = timedelta(hours=3)   # опоздавшие больше чем на 3 часа не публикуются
LIMIT = 500

def call(method, path, params):
    data = urllib.parse.urlencode(params).encode()
    url = f"{API}{path}"
    req = urllib.request.Request(url if method == "POST" else f"{url}?{data.decode()}",
                                 data=data if method == "POST" else None, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {e.read().decode()[:200]}")

def is_video(url):
    return url.lower().split("?")[0].endswith((".mp4", ".mov"))

def wait_ready(token, cid):
    for _ in range(40):
        s = call("GET", f"/v1.0/{cid}", {"fields": "status,error_message", "access_token": token})
        if s.get("status") == "FINISHED": return
        if s.get("status") in ("ERROR", "EXPIRED"):
            raise RuntimeError(f"контейнер {cid}: {s}")
        time.sleep(6)
    raise RuntimeError(f"контейнер {cid}: не успел обработаться")

def media_params(token, uid, media):
    """Возвращает параметры контейнера для текста поста с медиа."""
    if len(media) == 1:
        m = media[0]
        return {"media_type": "VIDEO", "video_url": m} if is_video(m) else {"media_type": "IMAGE", "image_url": m}
    kids = []
    for m in media:
        p = {"is_carousel_item": "true", "access_token": token}
        p.update({"media_type": "VIDEO", "video_url": m} if is_video(m) else {"media_type": "IMAGE", "image_url": m})
        kid = call("POST", f"/v1.0/{uid}/threads", p)["id"]
        wait_ready(token, kid)
        kids.append(kid)
    return {"media_type": "CAROUSEL", "children": ",".join(kids)}

def publish_part(token, part, reply_to=None):
    return publish_one(token, part, reply_to)

def publish_one(token, part, reply_to=None):
    text, media = (part, []) if isinstance(part, str) else (part["text"], part.get("media", []))
    uid = call("GET", "/v1.0/me", {"fields": "id", "access_token": token})["id"]
    params = {"media_type": "TEXT", "text": text, "access_token": token}
    if media:
        params.update(media_params(token, uid, media))
    if reply_to:
        params["reply_to_id"] = reply_to
    c = call("POST", f"/v1.0/{uid}/threads", params)
    if media:
        wait_ready(token, c["id"])
    else:
        time.sleep(3)
    return call("POST", f"/v1.0/{uid}/threads_publish", {"creation_id": c["id"], "access_token": token})["id"]

def parts_of(p):
    return p["parts"] if "parts" in p else [p["text"]]

def text_of(part):
    return part if isinstance(part, str) else part["text"]

def load_queue():
    queue = []
    for f in sorted(HERE.glob("queue*.json")):
        queue += json.loads(f.read_text(encoding="utf-8"))
    return queue

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--now", help="ISO-время для теста")
    a = ap.parse_args()
    queue = load_queue()
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    now = datetime.fromisoformat(a.now) if a.now else datetime.now(timezone.utc)
    token = os.environ.get("THREADS_TOKEN", "")

    def done_ids(pid):
        v = state.get(pid)
        return v["ids"] if isinstance(v, dict) else None

    def finished(p):
        v = state.get(p["id"])
        if v is None: return False
        if isinstance(v, dict): return len(v["ids"]) >= len(parts_of(p))
        return True

    due = sorted((p for p in queue if not finished(p) and datetime.fromisoformat(p["at"]) <= now),
                 key=lambda p: p["at"])
    if not due:
        print("Нечего публиковать."); return
    changed = False
    for p in due:
        parts = parts_of(p)
        ids = done_ids(p["id"]) or []
        at = datetime.fromisoformat(p["at"])
        if any(len(text_of(t)) > LIMIT for t in parts):
            print(f"[{p['id']}] ПРОПУСК: часть длиннее {LIMIT} символов"); state[p["id"]] = "too_long"; changed = True; continue
        if not ids and now - at > MAX_LATE:
            print(f"[{p['id']}] ПРОПУЩЕН: опоздание больше 3 ч"); state[p["id"]] = "missed"; changed = True; continue
        if a.dry_run:
            print(f"[{p['id']}] DRY-RUN ({len(parts)} ч.): {text_of(parts[0])[:50]!r}"); continue
        if not token: sys.exit("THREADS_TOKEN не задан")
        try:
            for i in range(len(ids), len(parts)):
                ids.append(publish_part(token, parts[i], ids[-1] if ids else None))
                state[p["id"]] = {"ids": ids}; changed = True
                STATE.write_text(json.dumps(state, indent=1, ensure_ascii=False))
                if i + 1 < len(parts): time.sleep(5)
            print(f"[{p['id']}] ОПУБЛИКОВАН ({len(parts)} ч.)")
        except Exception as e:
            print(f"[{p['id']}] ОШИБКА: {e}"); break
    if changed and not a.dry_run:
        STATE.write_text(json.dumps(state, indent=1, ensure_ascii=False))

main()
