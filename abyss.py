"""
abyss.py — 1080p episode ko Abyss (Hydrax) par upload karta hai, live progress ke saath.

Flow (har episode ke 1080p ke liye ek job):
  Storage channel se download  ->  Abyss API par upload  ->  Firebase (server3 folder) me link save

- Kai episode ek saath bhejo to sab queue me lagte hain; ABYSS_WORKERS (default 2) ek saath chalte hain.
- Har job ka apna status message hota hai (stage + progress bar + speed + ETA) jo live edit hota hai.
- Queue Firebase me persist hoti hai (bot restart / GitHub handover ke baad job wahin se dobara chalta hai).

ENV (sab optional, sirf ABYSS_API_KEY zaroori):
  ABYSS_API_KEY        Abyss dashboard wali API key (GitHub Secret me daalo)
  ABYSS_UPLOAD_URL     default: http://up.hydrax.net/{key}   ({key} khud replace hota hai)
  ABYSS_EMBED_TEMPLATE default: https://abysscdn.com/?v={slug}   (docs/dashboard se confirm karo)
  ABYSS_FIREBASE_FOLDER default: server3
  ABYSS_SERVER_LABEL   default: Server 3
  ABYSS_WORKERS        default: 2    (ek saath kitne episode upload)
  ABYSS_TMP_DIR        default: /tmp/abyss
  ABYSS_ENABLED        0 = band
"""

import asyncio
import json
import logging
import os
import re
import time
import uuid
from typing import Awaitable, Callable, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)

ABYSS_API_KEY = os.getenv("ABYSS_API_KEY", "").strip()
ABYSS_UPLOAD_URL = os.getenv("ABYSS_UPLOAD_URL", "http://up.hydrax.net/{key}")
ABYSS_EMBED_TEMPLATE = os.getenv("ABYSS_EMBED_TEMPLATE", "https://abysscdn.com/?v={slug}")
FIREBASE_FOLDER = os.getenv("ABYSS_FIREBASE_FOLDER", "server3").strip("/ ")
SERVER_LABEL = os.getenv("ABYSS_SERVER_LABEL", "Server 3")
WORKERS = max(1, int(os.getenv("ABYSS_WORKERS", "2")))
TMP_DIR = os.getenv("ABYSS_TMP_DIR", "/tmp/abyss")
ENABLED = os.getenv("ABYSS_ENABLED", "1") != "0" and bool(ABYSS_API_KEY)

QUALITIES = ("480p", "720p", "1080p")      # Abyss khud multi-quality banata hai; teeno key ke neeche same link jaata hai
EDIT_EVERY = 3.0                            # status message itne second me ek baar edit (Telegram limit se bachne ke liye)
UPLOAD_CHUNK = 1024 * 1024
MAX_UPLOAD_TRIES = 3

STAGE_ICON = {
    "queued": "⏳", "downloading": "⬇️", "uploading": "⬆️",
    "saving": "💾", "done": "✅", "failed": "❌", "cancelled": "🚫",
}


# ── chhote helpers ────────────────────────────────────
def _fmt_size(n: float) -> str:
    n = float(n or 0)
    if n >= 1024 ** 3:
        return f"{n / 1024 ** 3:.2f} GB"
    return f"{n / 1024 ** 2:.0f} MB"


def _fmt_speed(bps: float) -> str:
    return f"{bps / 1024 ** 2:.1f} MB/s" if bps else "—"


def _fmt_eta(sec: Optional[float]) -> str:
    if sec is None or sec != sec or sec < 0 or sec > 86400:
        return "—"
    sec = int(sec)
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m:02d}m"
    return f"{m}m {s:02d}s" if m else f"{s}s"


def bar(pct: float, width: int = 12) -> str:
    pct = max(0.0, min(100.0, pct))
    filled = int(round(width * pct / 100))
    return "█" * filled + "░" * (width - filled)


def build_embed_link(slug: str) -> str:
    return ABYSS_EMBED_TEMPLATE.replace("{slug}", slug)


class Progress:
    """Ek phase (download ya upload) ka live number: kitna hua, speed, ETA."""

    def __init__(self):
        self.done = 0
        self.total = 0
        self.t0 = 0.0
        self._last_t = 0.0
        self._last_done = 0
        self.speed = 0.0

    def start(self, total: int):
        self.done, self.total = 0, total or 0
        self.t0 = self._last_t = time.time()
        self._last_done, self.speed = 0, 0.0

    def update(self, done: int, total: Optional[int] = None):
        self.done = done
        if total:
            self.total = total
        now = time.time()
        dt = now - self._last_t
        if dt >= 1.0:                                   # smooth speed (EMA)
            inst = (done - self._last_done) / dt
            self.speed = inst if not self.speed else 0.6 * self.speed + 0.4 * inst
            self._last_t, self._last_done = now, done

    @property
    def pct(self) -> float:
        return (self.done * 100.0 / self.total) if self.total else 0.0

    @property
    def eta(self) -> Optional[float]:
        if not self.speed or not self.total:
            return None
        return max(0.0, (self.total - self.done) / self.speed)


class Job:
    def __init__(self, *, uid: int, chat_id: int, slug: str, season: str, ep: int,
                 sid: int, ext: str, size: int, reply_to: Optional[int] = None, job_id: str = ""):
        self.id = job_id or uuid.uuid4().hex[:8]
        self.uid, self.chat_id = uid, chat_id
        self.slug, self.season, self.ep = slug, season, ep
        self.sid, self.ext, self.size = sid, ext, size
        self.reply_to = reply_to
        self.stage = "queued"
        self.error = ""
        self.attempt = 1
        self.abyss_slug = ""
        self.embed_link = ""
        self.dl = Progress()
        self.up = Progress()
        self.status_msg_id: Optional[int] = None
        self.cancelled = False
        self.task: Optional[asyncio.Task] = None
        self.started = 0.0

    @property
    def title(self) -> str:
        return f"{self.slug} {self.season} E{self.ep}"

    def to_dict(self) -> dict:
        return {"id": self.id, "uid": self.uid, "chat_id": self.chat_id, "slug": self.slug,
                "season": self.season, "ep": self.ep, "sid": self.sid, "ext": self.ext,
                "size": self.size, "reply_to": self.reply_to}

    @classmethod
    def from_dict(cls, d: dict) -> "Job":
        return cls(uid=int(d["uid"]), chat_id=int(d["chat_id"]), slug=d["slug"], season=d["season"],
                   ep=int(d["ep"]), sid=int(d["sid"]), ext=d.get("ext", "mkv"), size=int(d.get("size", 0)),
                   reply_to=d.get("reply_to"), job_id=d.get("id", ""))


# ── upload (streaming multipart, progress ke saath) ───
async def upload_file(path: str, prog: Progress, *, cancelled: Callable[[], bool] = lambda: False) -> str:
    """File ko Abyss par upload karta hai aur video ka slug return karta hai.
    Body hum khud stream karte hain (RAM me poori file nahi aati) aur har chunk par progress update hota hai."""
    url = ABYSS_UPLOAD_URL.replace("{key}", ABYSS_API_KEY)
    size = os.path.getsize(path)
    fname = os.path.basename(path).replace('"', "")
    boundary = uuid.uuid4().hex
    head = (f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{fname}"\r\n'
            f"Content-Type: application/octet-stream\r\n\r\n").encode()
    tail = f"\r\n--{boundary}--\r\n".encode()
    total_len = len(head) + size + len(tail)
    prog.start(size)

    async def body():
        yield head
        sent = 0
        with open(path, "rb") as f:
            while True:
                if cancelled():
                    raise asyncio.CancelledError()
                chunk = await asyncio.to_thread(f.read, UPLOAD_CHUNK)
                if not chunk:
                    break
                sent += len(chunk)
                yield chunk
                prog.update(sent)
        yield tail

    headers = {"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(total_len)}
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=900)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.post(url, data=body(), headers=headers) as r:
            text = await r.text()
            try:
                data = json.loads(text)
            except Exception:
                raise RuntimeError(f"Abyss ne ajeeb reply diya (HTTP {r.status}): {text[:200]}")
    if data.get("status") and data.get("slug"):
        prog.update(size)
        return str(data["slug"])
    raise RuntimeError(f"Abyss error: {data.get('msg') or data}")


# ── Firebase save ─────────────────────────────────────
async def save_to_firebase(firebase_url: str, job: Job) -> bool:
    """server3/{anime}/{season}/E{ep}/{480p|720p|1080p} = {link, slug, server, time}"""
    if not firebase_url:
        return False
    db = firebase_url.rstrip("/")
    base = f"{FIREBASE_FOLDER}/{job.slug}/{job.season}/E{job.ep}"
    now = int(time.time())
    payload = {q: {"link": job.embed_link, "slug": job.abyss_slug, "server": SERVER_LABEL, "time": now}
               for q in QUALITIES}
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
                async with s.patch(f"{db}/{base}.json", json=payload) as r:
                    if r.status == 200:
                        logger.info(f"Abyss Firebase saved: {base}")
                        return True
                    logger.error(f"Abyss Firebase {r.status}: {(await r.text())[:200]}")
        except Exception as e:
            logger.error(f"Abyss Firebase error: {e!r}")
        await asyncio.sleep(2 * (attempt + 1))
    return False


# ── Manager ───────────────────────────────────────────
class AbyssManager:
    """
    get_bot():            PTB Bot object (status message bhejne/edit karne ke liye)
    get_pyro():           Pyrogram Client (storage channel se download)
    storage_channel:      int
    firebase_url:         str
    save_state():         bot ka state save (queue persist hoti hai)
    """

    def __init__(self, *, get_bot: Callable, get_pyro: Callable, storage_channel: int,
                 firebase_url: str, save_state: Callable[[], Awaitable[None]]):
        self.get_bot, self.get_pyro = get_bot, get_pyro
        self.storage_channel, self.firebase_url, self.save_state = storage_channel, firebase_url, save_state
        self.jobs: Dict[str, Job] = {}          # id -> Job (queued + running)
        self.queue: "asyncio.Queue[str]" = None  # type: ignore
        self.workers: List[asyncio.Task] = []
        self.paused = False
        self.recent: List[str] = []             # last finished lines (for /abyss)
        self._idle = asyncio.Event()
        self._idle.set()

    # ---------- public ----------
    def start(self):
        if self.queue is not None:
            return
        self.queue = asyncio.Queue()
        os.makedirs(TMP_DIR, exist_ok=True)
        for i in range(WORKERS):
            self.workers.append(asyncio.create_task(self._worker(i + 1), name=f"abyss-worker-{i + 1}"))
        logger.info(f"Abyss manager start: {WORKERS} worker, folder='{FIREBASE_FOLDER}'")

    async def add(self, job: Job, *, resumed: bool = False):
        self.start()
        self.jobs[job.id] = job
        self._idle.clear()
        await self.queue.put(job.id)
        await self._refresh_status(job, force=True)
        if not resumed:
            await self.save_state()

    def serialize(self) -> list:
        return [j.to_dict() for j in self.jobs.values() if j.stage not in ("done", "failed", "cancelled")]

    async def resume(self, saved: list):
        for d in saved or []:
            try:
                job = Job.from_dict(d)
                if job.id in self.jobs:
                    continue
                job.reply_to = None
                await self.add(job, resumed=True)
            except Exception as e:
                logger.error(f"Abyss resume fail: {e!r}")

    def pause(self):
        self.paused = True

    async def wait_idle(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._idle.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def users(self) -> List[int]:
        return [j.uid for j in self.jobs.values()]

    async def cancel(self, job_id_prefix: str) -> bool:
        for j in list(self.jobs.values()):
            if j.id.startswith(job_id_prefix):
                j.cancelled = True
                if j.task and not j.task.done():
                    j.task.cancel()
                if j.stage == "queued":
                    await self._finish(j, "cancelled")
                return True
        return False

    def dashboard(self) -> str:
        if not self.jobs and not self.recent:
            return "☁️ Abyss queue khaali hai."
        lines = ["☁️ ABYSS QUEUE", "━━━━━━━━━━━━━━━━━━━━━━"]
        running = [j for j in self.jobs.values() if j.stage != "queued"]
        queued = [j for j in self.jobs.values() if j.stage == "queued"]
        for j in running:
            lines.append(f"{STAGE_ICON[j.stage]} {j.title} — {self._short(j)}  (id {j.id})")
        for n, j in enumerate(queued, 1):
            lines.append(f"⏳ #{n} {j.title}  (id {j.id})")
        if self.recent:
            lines.append("━━━━━━━━━━━━━━━━━━━━━━")
            lines += self.recent[-5:]
        lines.append("Cancel: /abyss_cancel <id>")
        return "\n".join(lines)

    # ---------- internals ----------
    @staticmethod
    def _short(j: Job) -> str:
        if j.stage == "downloading":
            return f"download {j.dl.pct:.0f}%"
        if j.stage == "uploading":
            return f"upload {j.up.pct:.0f}%"
        return j.stage

    async def _worker(self, n: int):
        while True:
            jid = await self.queue.get()
            job = self.jobs.get(jid)
            try:
                if not job or job.cancelled:
                    continue
                while self.paused:                       # handover: naya job start nahi (queue Firebase me safe)
                    await asyncio.sleep(2)
                job.task = asyncio.create_task(self._run(job))
                try:
                    await job.task
                except asyncio.CancelledError:
                    if not job.cancelled:
                        raise
            except Exception as e:
                logger.exception(f"worker {n} unexpected: {e!r}")
            finally:
                self.queue.task_done()
                if not self.jobs:
                    self._idle.set()

    async def _run(self, job: Job):
        job.started = time.time()
        path = os.path.join(TMP_DIR, f"{job.id}.{job.ext}")
        ticker = asyncio.create_task(self._ticker(job))
        try:
            # 1) download
            job.stage = "downloading"
            await self._refresh_status(job, force=True)
            await self._download(job, path)
            # 2) upload (retry)
            job.stage = "uploading"
            await self._refresh_status(job, force=True)
            last_err = None
            for attempt in range(1, MAX_UPLOAD_TRIES + 1):
                job.attempt = attempt
                try:
                    job.abyss_slug = await upload_file(path, job.up, cancelled=lambda: job.cancelled)
                    last_err = None
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    last_err = e
                    logger.error(f"Abyss upload try {attempt} fail ({job.title}): {e!r}")
                    await asyncio.sleep(5 * attempt)
            if last_err:
                raise last_err
            job.embed_link = build_embed_link(job.abyss_slug)
            # 3) firebase
            job.stage = "saving"
            await self._refresh_status(job, force=True)
            saved = await save_to_firebase(self.firebase_url, job)
            if not saved:
                job.error = "Firebase save fail (link neeche diya hai, manually daal dena)"
            await self._finish(job, "done")
        except asyncio.CancelledError:
            if job.cancelled:
                await self._finish(job, "cancelled")
            else:
                raise
        except Exception as e:
            job.error = str(e)[:300]
            logger.error(f"Abyss job fail {job.title}: {e!r}")
            await self._finish(job, "failed")
        finally:
            ticker.cancel()
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

    async def _download(self, job: Job, path: str):
        pyro = self.get_pyro()
        msg = await pyro.get_messages(self.storage_channel, job.sid)
        media = None if (not msg or msg.empty) else (msg.video or msg.document or msg.audio)
        if not media:
            raise RuntimeError("Storage channel me ye video nahi mili (delete ho gayi?)")
        total = getattr(media, "file_size", 0) or job.size
        job.dl.start(total)

        def cb(current, tot):                       # pyrogram sync callback
            job.dl.update(current, tot)

        out = await pyro.download_media(msg, file_name=path, progress=cb)
        if not out:
            raise RuntimeError("Download fail hua")
        if os.path.abspath(str(out)) != os.path.abspath(path):
            os.replace(str(out), path)
        job.dl.update(job.dl.total, job.dl.total)

    async def _finish(self, job: Job, stage: str):
        job.stage = stage
        mark = f"{STAGE_ICON[stage]} {job.title}"
        if stage == "done":
            mark += f" — {_fmt_eta(time.time() - job.started)}"
        self.recent.append(mark)
        self.recent = self.recent[-20:]
        await self._refresh_status(job, force=True)
        self.jobs.pop(job.id, None)
        if not self.jobs:
            self._idle.set()
        await self.save_state()

    async def _ticker(self, job: Job):
        while True:
            await asyncio.sleep(EDIT_EVERY)
            await self._refresh_status(job)

    # ---------- status message ----------
    def render(self, job: Job) -> str:
        head = f"☁️ Abyss Upload — {job.title}  (1080p)\n🆔 {job.id}"
        L = [head, "━━━━━━━━━━━━━━━━━━━━━━"]

        def line(icon, name, p: Progress, active, finished):
            if finished:
                return f"{icon} {name}  {bar(100)} 100%  {_fmt_size(p.total)}"
            if active:
                return (f"{icon} {name}  {bar(p.pct)} {p.pct:.0f}%\n"
                        f"     {_fmt_size(p.done)} / {_fmt_size(p.total)} • {_fmt_speed(p.speed)} • ETA {_fmt_eta(p.eta)}")
            if p.done:
                return f"{icon} {name}  {bar(p.pct)} {p.pct:.0f}%  (ruk gaya)"
            return f"{icon} {name}  {bar(0)} 0%"

        st = job.stage
        if st == "queued":
            pos = [j.id for j in self.jobs.values() if j.stage == "queued"]
            n = pos.index(job.id) + 1 if job.id in pos else 0
            L.append(f"⏳ Queue me hai (#{n}) — baari aane par shuru hoga")
        else:
            dl_fin = st in ("uploading", "saving", "done")
            up_fin = st in ("saving", "done")
            L.append(line("⬇️", "Download", job.dl, st == "downloading", dl_fin))
            L.append(line("⬆️", "Upload  ", job.up, st == "uploading", up_fin) +
                     (f"  (try {job.attempt}/{MAX_UPLOAD_TRIES})" if st == "uploading" and job.attempt > 1 else ""))
            L.append(f"{'💾' if st == 'saving' else ('✅' if st == 'done' else '▫️')} Firebase ({FIREBASE_FOLDER})")
        L.append("━━━━━━━━━━━━━━━━━━━━━━")
        L.append(f"Status: {STAGE_ICON[st]} {st.upper()}")
        if st == "done":
            L.append(f"🔗 {job.embed_link}")
            L.append(f"🆔 Abyss slug: {job.abyss_slug}")
            L.append(f"🕒 Total: {_fmt_eta(time.time() - job.started)}")
            if job.error:
                L.append(f"⚠️ {job.error}")
        elif st in ("failed", "cancelled"):
            if job.error:
                L.append(f"⚠️ {job.error}")
            L.append("Dobara bhejna ho to teeno video phir se bhejo.")
        return "\n".join(L)

    async def _refresh_status(self, job: Job, force: bool = False):
        bot = self.get_bot()
        if not bot:
            return
        text = self.render(job)
        if getattr(job, "_last_text", None) == text:
            return
        job._last_text = text  # type: ignore[attr-defined]
        try:
            if job.status_msg_id is None:
                m = await bot.send_message(job.chat_id, text, reply_to_message_id=job.reply_to,
                                           disable_web_page_preview=True)
                job.status_msg_id = m.message_id
            else:
                await bot.edit_message_text(text, chat_id=job.chat_id, message_id=job.status_msg_id,
                                            disable_web_page_preview=True)
        except Exception as e:
            s = str(e)
            if "not modified" in s.lower():
                return
            wait = getattr(e, "retry_after", None)
            if wait is not None:
                w = wait.total_seconds() if hasattr(wait, "total_seconds") else float(wait)
                if force:
                    await asyncio.sleep(min(w + 1, 30))
                return
            logger.warning(f"Abyss status edit fail: {e!r}")
