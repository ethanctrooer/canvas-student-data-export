import asyncio
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime

import requests

# ---------------------------------------------------------------------------
# m3u8 downloader import (submodule)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "m3u8-video-downloader"))
from main import main as _download_m3u8_video  # noqa: E402

# Suppress ffmpeg informational log output.
# Belt-and-suspenders: (1) inject -loglevel error into the command so ffmpeg
# itself stays quiet; (2) redirect stderr to PIPE so any remaining output is
# captured rather than printed. ffmpeg-python may do `from subprocess import
# Popen` internally (local reference), so we patch both subprocess.Popen AND
# the ffmpeg._run module's namespace directly via sys.modules.
_OrigPopen = subprocess.Popen

class _QuietFFmpegPopen(_OrigPopen):
    def __init__(self, args, **kwargs):
        if (isinstance(args, (list, tuple)) and args and
                "ffmpeg" in os.path.basename(str(args[0])).lower()):
            args = list(args)
            if "-loglevel" not in args:
                args = args[:1] + ["-loglevel", "error"] + args[1:]
            kwargs.setdefault("stderr", subprocess.PIPE)
        super().__init__(args, **kwargs)

subprocess.Popen = _QuietFFmpegPopen
# Patch the local reference inside ffmpeg._run if it imported Popen directly.
_ffmpeg_run_mod = sys.modules.get("ffmpeg._run")
if _ffmpeg_run_mod is not None and hasattr(_ffmpeg_run_mod, "Popen"):
    _ffmpeg_run_mod.Popen = _QuietFFmpegPopen


class MultiProgressDisplay:
    """Thread-safe multi-slot progress display for concurrent downloads.

    Maintains one status line per download slot and redraws them in place
    using ANSI cursor-up sequences.
    """

    def __init__(self, num_slots: int):
        self._num_slots = num_slots
        self._lines = [""] * num_slots
        self._lock = threading.Lock()
        self._rendered = False  # True once we've printed at least one frame

    def update_slot(self, slot: int, line: str):
        with self._lock:
            self._lines[slot] = line
            self._redraw()

    def clear(self):
        with self._lock:
            if self._rendered:
                for _ in range(self._num_slots):
                    sys.stderr.write("\033[F\033[2K")  # cursor up + erase line
                sys.stderr.flush()
            self._rendered = False
            self._lines = [""] * self._num_slots

    def _redraw(self):
        if self._rendered:
            for _ in range(self._num_slots):
                sys.stderr.write("\033[F\033[2K")
        for line in self._lines:
            sys.stderr.write(line[:120] + "\n")
        sys.stderr.flush()
        self._rendered = True


MEDIA_GALLERY_TIMEOUT = 15  # seconds to wait for m3u8 URLs after clicking play
GALLERY_CONCURRENCY = 5  # max simultaneous video captures/downloads


def _parse_gallery_date(date_str: str) -> str:
    """Convert e.g. 'December 6th, 2023' → '2023-12-06'. Returns original string on failure."""
    try:
        cleaned = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", date_str).strip()
        return datetime.strptime(cleaned, "%B %d, %Y").strftime("%Y-%m-%d")
    except Exception:
        return date_str



def _load_netscape_cookies(path: str) -> list[dict]:
    """Parse a Netscape-format cookies file into Playwright's add_cookies() format."""
    cookies = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 7:
                    continue
                domain, _, cookie_path, secure, expires_str, name, value = parts[:7]
                try:
                    expires = int(float(expires_str))
                except (ValueError, TypeError):
                    expires = -1
                cookies.append({
                    "domain": domain,
                    "path": cookie_path,
                    "name": name,
                    "value": value,
                    "secure": secure.upper() == "TRUE",
                    "expires": expires,
                })
    except Exception as e:
        print(f"  Warning: could not load cookies from {path}: {e}")
    return cookies


def _get_media_gallery_url(api_url: str, api_key: str, course_id, context_type: str = "courses") -> str | None:
    """Return the full URL of the Media Gallery tab, or None if not found."""
    try:
        resp = requests.get(
            f"{api_url}/api/v1/{context_type}/{course_id}/tabs",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30,
        )
        resp.raise_for_status()
        for tab in resp.json():
            label = tab.get("label", "")
            if "media gallery" in label.lower():
                return tab.get("full_url")
    except Exception as e:
        print(f"  Warning: could not fetch course tabs: {e}")
    return None


async def _enumerate_gallery_videos(page) -> list[tuple[str, str, str]]:
    """Return ordered list of (title, url, date) triples from the Kaltura gallery page."""
    # Gallery content is inside an LTI iframe embedded by Canvas
    frame = page.frame_locator("iframe.tool_launch")

    # Wait for the iframe content to load and first card to appear.
    # Phase 1: short wait — lets us detect an empty gallery quickly.
    # Phase 2: if items still haven't appeared, check for the explicit "No Media" state
    #          before committing to the full 5-minute timeout.
    try:
        await frame.locator("li.galleryItem").first.wait_for(state="attached", timeout=30000)
    except Exception:
        try:
            body_text = (await frame.locator("body").inner_text(timeout=5000)).lower()
            if "no media" in body_text:
                return []
        except Exception:
            pass
        # Items just slow to load — wait the full duration
        await frame.locator("li.galleryItem").first.wait_for(state="attached", timeout=300000)

    # Load-more loop
    for _ in range(50):
        try:
            btn = frame.locator("div.endless-scroll-more button")
            await btn.wait_for(state="visible", timeout=10000)
            await btn.click()
            await page.wait_for_timeout(2000)
        except Exception:
            break

    # Resolve the Kaltura frame's origin so relative hrefs become absolute URLs
    kaltura_origin = ""
    for f in page.frames:
        if f.url and f.url.startswith("http") and ("kaltura" in f.url or "kaf." in f.url):
            parts = f.url.split("/")
            kaltura_origin = "/".join(parts[:3])  # e.g. "https://kaf.berkeley.edu"
            break

    results = []
    cards = frame.locator("li.galleryItem")
    count = await cards.count()
    for i in range(count):
        card = cards.nth(i)
        try:
            link = card.locator(".thumb_name_content_link a.item_link").first
            title = (await link.inner_text()).strip()
            url = await link.get_attribute("href")
            if not title or not url:
                continue
            if url.startswith("/") and kaltura_origin:
                url = kaltura_origin + url
            date_str = ""
            try:
                raw_date = (await card.locator(".thumbTimeAdded").text_content() or "").strip()
                m = re.search(r"[A-Z][a-z]+ \d+(?:st|nd|rd|th)?,? \d{4}", raw_date)
                if m:
                    date_str = _parse_gallery_date(m.group())
            except Exception:
                pass
            results.append((title, url, date_str))
        except Exception:
            continue

    return results


def _download_direct_mp4_sync(url: str, output_path: str, label: str = "",
                               display=None, slot: int = 0) -> None:
    """Streaming download of a direct MP4 from a Kaltura CDN URL.

    Uses requests so we can stream large files without a timeout and show progress.
    Kaltura CDN URLs are self-authenticating via query-string tokens.
    """
    import sys

    chunk_size = 1024 * 1024  # 1 MB
    start = time.monotonic()

    def _render(downloaded: int, total: int):
        elapsed = max(time.monotonic() - start, 0.001)
        speed_mbs = (downloaded / elapsed) / 1024 / 1024
        mb_done = downloaded / 1024 / 1024
        if total:
            pct = downloaded / total * 100
            mb_total = total / 1024 / 1024
            remaining = (total - downloaded) / (downloaded / elapsed) if downloaded else 0
            eta_s = int(remaining)
            eta = f"{eta_s // 60}m {eta_s % 60}s" if eta_s >= 60 else f"{eta_s}s"
            bar_width = 25
            filled = int(bar_width * downloaded / total)
            bar = "█" * filled + "░" * (bar_width - filled)
            line = (f"{label}  {pct:5.1f}%  [{bar}]  "
                    f"{mb_done:.1f}/{mb_total:.1f} MB  {speed_mbs:.1f} MB/s  ETA: {eta}")
        else:
            line = f"{label}  {mb_done:.1f} MB  {speed_mbs:.1f} MB/s"
        if display is not None:
            display.update_slot(slot, line)
        else:
            sys.stderr.write("\r" + line[:120].ljust(120))
            sys.stderr.flush()

    with requests.get(url, stream=True, timeout=(30, None)) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    _render(downloaded, total)
    if display is None:
        sys.stderr.write("\n")
        sys.stderr.flush()


async def _download_direct_mp4(url: str, output_path: str, label: str = "",
                                display=None, slot: int = 0) -> None:
    """Async wrapper — runs the blocking streaming download in a thread executor."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None, _download_direct_mp4_sync, url, output_path, label, display, slot
    )


async def _export_youtube_cookies(context, path: str) -> int:
    """Write YouTube cookies from the browser context to a Netscape cookies file.

    Returns the number of cookies written, or 0 on failure.
    Passing these to youtube-dl lets it authenticate as the browser session,
    bypassing YouTube's bot detection that causes fragment download failures.
    """
    try:
        cookies = await context.cookies(["https://www.youtube.com"])
        with open(path, "w", encoding="utf-8") as f:
            f.write("# Netscape HTTP Cookie File\n")
            for c in cookies:
                domain = c.get("domain", "")
                flag = "TRUE" if domain.startswith(".") else "FALSE"
                path_val = c.get("path", "/")
                secure = "TRUE" if c.get("secure", False) else "FALSE"
                expires = str(int(c["expires"])) if c.get("expires", -1) > 0 else "0"
                name = c.get("name", "")
                value = c.get("value", "")
                f.write(f"{domain}\t{flag}\t{path_val}\t{secure}\t{expires}\t{name}\t{value}\n")
        return len(cookies)
    except Exception as e:
        print(f"    Warning: could not export YouTube cookies: {e}")
        return 0


def _download_youtube_video_sync(youtube_url: str, video_dir: str, index: int,
                                  verbose: bool, cookies_file: str = "",
                                  title: str = "") -> None:
    print(f"    Downloading YouTube video: {youtube_url}")
    if cookies_file and os.path.exists(cookies_file):
        print(f"    Using cookies file ({os.path.getsize(cookies_file)} bytes)")
    else:
        print(f"    No cookies file — may hit bot detection")

    cmd = [
        "yt-dlp",
        "--no-cache-dir",
        "-f", "bestvideo+bestaudio/best",
        "--merge-output-format", "mp4",
        "-o", os.path.join(video_dir, f"perspective_{index}.%(ext)s"),
        "--retries", "10",
        "--fragment-retries", "10",
        # Use the android client — no Node.js needed (REQUIRE_JS_PLAYER=False),
        # so the n-challenge is not required and downloads succeed without it.
        "--extractor-args", "youtube:player_client=android",
    ]
    if cookies_file and os.path.exists(cookies_file):
        cmd += ["--cookies", cookies_file]
    if verbose:
        cmd.append("--verbose")
    cmd.append(youtube_url)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout and verbose:
        for line in result.stdout.splitlines():
            print(f"    [yt-dlp] {line}")
    if result.stderr:
        for line in result.stderr.splitlines():
            if verbose or any(kw in line for kw in ("ERROR", "WARNING", "error", "warning")):
                print(f"    [yt-dlp] {line}")
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp exited with code {result.returncode}")


async def _download_youtube_video(youtube_url: str, video_dir: str, index: int,
                                   verbose: bool, cookies_file: str = "",
                                   title: str = "") -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None, _download_youtube_video_sync, youtube_url, video_dir, index,
        verbose, cookies_file, title
    )


async def _scan_for_youtube(page, seen: set, captured_urls: list) -> None:
    """Detect YouTube videos by querying the watch-URL link inside the YouTube player DOM.

    The Kaltura PlayKit YouTube engine loads the embed as:
        https://www.youtube.com/embed/?controls=0&autoplay=0&...
    The video ID is never placed in the iframe URL path — it is injected via the
    YouTube IFrame API after play.  Once the player has loaded the video, YouTube
    renders a title <a href="https://www.youtube.com/watch?v=VIDEO_ID"> inside the
    iframe DOM.  Reading that link is the reliable way to extract the video ID.
    """
    for frame in page.frames:
        if "youtube.com/embed" not in (frame.url or ""):
            continue
        try:
            href = await frame.evaluate(
                "() => {"
                "  const a = document.querySelector('a[href*=\"youtube.com/watch\"]');"
                "  return a ? a.href : null;"
                "}"
            )
            if href:
                m = re.search(r'[?&]v=([^&]+)', href)
                if m:
                    yt_watch = f"https://www.youtube.com/watch?v={m.group(1)}"
                    if yt_watch not in seen:
                        seen.add(yt_watch)
                        captured_urls.append((yt_watch, "youtube"))
        except Exception:
            pass


async def _capture_video_streams(page, video_url: str, verbose: bool = False) -> list[tuple[str, str]]:
    """Navigate to a video page, play it, and capture stream URLs.

    Returns a list of (url, kind) tuples where kind is "m3u8" or "mp4".
    """
    captured_urls: list[tuple[str, str]] = []
    seen: set[str] = set()

    def handle_request(request):
        url = request.url
        if url in seen:
            return
        # HLS playlist (any Kaltura CDN)
        if "index.m3u8" in url and "kaltura" in url:
            seen.add(url)
            captured_urls.append((url, "m3u8"))
        # Direct MP4 download (cfvod.kaltura.com serves these as a.mp4?...)
        elif "cfvod.kaltura.com" in url and "/a.mp4" in url:
            seen.add(url)
            captured_urls.append((url, "mp4"))
        # YouTube embed — video ID may or may not be in the URL path (Kaltura's
        # PlayKit engine loads the embed without a video ID and injects it later
        # via the IFrame API).  Only capture here if the ID is present; the DOM
        # scan in the polling loop handles the no-ID case after play.
        elif "youtube.com/embed/" in url or "youtube-nocookie.com/embed/" in url:
            m = re.search(r'youtube(?:-nocookie)?\.com/embed/([^/?&#]+)', url)
            if m:
                yt_watch = f"https://www.youtube.com/watch?v={m.group(1)}"
                if yt_watch not in seen:
                    seen.add(yt_watch)
                    captured_urls.append((yt_watch, "youtube"))

    page.on("request", handle_request)

    try:
        # Use "load" instead of "networkidle" — video pages keep the network busy
        await page.goto(video_url, wait_until="load", timeout=60000)
        await page.wait_for_timeout(2000)

        # Selectors to try for the play button (V7 PlayKit, older players, generic)
        play_selectors = [
            ".playkit-pre-playback-play-button",   # V7 PlayKit — large overlay button
            ".playkit-play-pause-control",          # V7 PlayKit — control bar
            "[aria-label='Play']",
            "button[aria-label*='Play']",
            ".mwEmbedPlayer",                       # older Kaltura player (click to play)
            "[class*='playBtn']",
            "[class*='play-btn']",
            "video",
        ]

        clicked = False

        # Try main page first
        for sel in play_selectors:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=1000):
                    await btn.click()
                    clicked = True
                    break
            except Exception:
                continue

        # KAF media pages embed the player inside an iframe — try each frame
        if not clicked:
            for frame in page.frames:
                if frame is page.main_frame:
                    continue
                for sel in play_selectors:
                    try:
                        btn = frame.locator(sel).first
                        if await btn.is_visible(timeout=1000):
                            await btn.click()
                            clicked = True
                            break
                    except Exception:
                        continue
                if clicked:
                    break

        # Poll until at least one URL of any kind is captured.
        # Each tick: check network-captured URLs (via handle_request) AND actively
        # scan frame URLs + DOM — Kaltura injects the YouTube <iframe> via JS after
        # play, so the request event is unreliable; DOM querying is the fallback.
        deadline = time.monotonic() + MEDIA_GALLERY_TIMEOUT
        while time.monotonic() < deadline and len(captured_urls) == 0:
            await page.wait_for_timeout(500)
            await _scan_for_youtube(page, seen, captured_urls)

        if captured_urls:
            await page.wait_for_timeout(2000)  # wait for additional perspectives
        elif verbose:
            frame_urls = [f.url for f in page.frames if f.url and f.url != "about:blank"]
            print(f"    Debug: no streams found; visible frame URLs: {frame_urls}")

    except Exception as e:
        print(f"    Warning: error capturing streams from {video_url}: {e}")
    finally:
        page.remove_listener("request", handle_request)

    return captured_urls


async def _process_one_video(context, title, video_url, video_dir, verbose,
                              display=None, slot=0):
    page = await context.new_page()
    try:
        streams = await _capture_video_streams(page, video_url, verbose=verbose)
        if not streams:
            print(f"    Warning: no streams captured for: {title}")
            return 0
        if verbose:
            print(f"    Captured {len(streams)} stream(s) for: {title}")
        any_failed = False
        # Export browser cookies once for any YouTube downloads in this video
        yt_cookies_file = os.path.join(video_dir, "yt_cookies.txt")
        if any(kind == "youtube" for _, kind in streams):
            n_cookies = await _export_youtube_cookies(context, yt_cookies_file)
            print(f"    Exported {n_cookies} YouTube cookies to {yt_cookies_file}")
        for j, (url, kind) in enumerate(streams, start=1):
            output_file = f"perspective_{j}.mp4"
            output_path = os.path.join(video_dir, output_file)
            if kind == "youtube":
                with open(os.path.join(video_dir, "youtube_url.txt"), "w") as f:
                    f.write(url + "\n")
            try:
                if kind == "mp4":
                    await _download_direct_mp4(url, output_path,
                                               label=title, display=display, slot=slot)
                elif kind == "youtube":
                    await _download_youtube_video(url, video_dir, j, verbose,
                                                  cookies_file=yt_cookies_file,
                                                  title=title)
                else:
                    await _download_m3u8_video(url, output_file, video_dir)
            except Exception as e:
                any_failed = True
                print(f"    Error downloading {output_file} for '{title}': {e}")
        if not any_failed:
            open(os.path.join(video_dir, ".done"), "w").close()
        return 1
    finally:
        await page.close()


async def _process_course_gallery_async(
    context,
    page,
    api_url: str,
    api_key: str,
    course_id,
    course_view,
    dl_location: str,
    verbose: bool,
    context_type: str = "courses",
) -> int:
    """Download all videos for a single course using an already-open browser context."""
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from export import makeValidFilename, shortenFileName, MAX_FOLDER_NAME_SIZE

    gallery_url = _get_media_gallery_url(api_url, api_key, course_id, context_type)
    if gallery_url is None:
        print("  No Media Gallery found for this course, skipping.")
        return 0

    media_gallery_dir = os.path.join(
        dl_location,
        course_view.term,
        course_view.course_code,
        "media_gallery",
    )
    os.makedirs(media_gallery_dir, exist_ok=True)

    videos_processed = 0

    try:
        if verbose:
            print(f"    Navigating to gallery: {gallery_url}")
        await page.goto(gallery_url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(3000)

        videos = await _enumerate_gallery_videos(page)
        if not videos:
            print("  No videos found in Media Gallery.")
            return 0

        print(f"  Found {len(videos)} video(s) in Media Gallery.")

        used_dirs: set[str] = set()

        display = MultiProgressDisplay(GALLERY_CONCURRENCY)
        slot_queue: asyncio.Queue = asyncio.Queue()
        for _s in range(GALLERY_CONCURRENCY):
            slot_queue.put_nowait(_s)

        async def bounded(title, video_url, video_dir):
            slot = await slot_queue.get()
            try:
                print(f"    Processing: {title}")
                return await _process_one_video(
                    context, title, video_url, video_dir, verbose,
                    display=display, slot=slot,
                )
            finally:
                slot_queue.put_nowait(slot)

        def _is_done(vdir: str) -> bool:
            return (os.path.exists(os.path.join(vdir, ".done")) or
                    os.path.exists(os.path.join(vdir, "perspective_1.mp4")))

        # Pre-build full video→dir mapping so we can reuse it in the retry pass
        video_items: list[tuple[str, str, str]] = []
        for title, video_url, date_str in videos:
            safe_title = makeValidFilename(title)
            safe_title = shortenFileName(safe_title, len(safe_title) - MAX_FOLDER_NAME_SIZE)

            folder_label = f"{safe_title} ({date_str})" if date_str else safe_title
            folder_name = makeValidFilename(folder_label)
            folder_name = shortenFileName(folder_name, len(folder_name) - MAX_FOLDER_NAME_SIZE)

            base_dir = os.path.join(media_gallery_dir, folder_name)
            video_dir = base_dir
            counter = 2
            while video_dir in used_dirs:
                video_dir = f"{base_dir}_{counter}"
                counter += 1
            used_dirs.add(video_dir)
            video_items.append((title, video_url, video_dir))

        # Initial download pass
        tasks = []
        for title, video_url, video_dir in video_items:
            # Skip if already downloaded (.done = all perspectives succeeded;
            # perspective_1.mp4 = legacy fallback for runs before .done was introduced)
            if _is_done(video_dir):
                print(f"    Already exists, skipping: {title}")
                continue
            os.makedirs(video_dir, exist_ok=True)
            tasks.append(bounded(title, video_url, video_dir))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        display.clear()
        videos_processed = sum(r for r in results if isinstance(r, int))

        # Confirmation pass: retry any video still missing its .done marker
        retry_items = [(t, u, d) for t, u, d in video_items if not _is_done(d)]
        if retry_items:
            print(f"  Confirmation: {len(retry_items)} video(s) incomplete, retrying...")
            retry_tasks = []
            for title, video_url, video_dir in retry_items:
                os.makedirs(video_dir, exist_ok=True)
                retry_tasks.append(bounded(title, video_url, video_dir))
            retry_results = await asyncio.gather(*retry_tasks, return_exceptions=True)
            display.clear()
            videos_processed += sum(r for r in retry_results if isinstance(r, int))
        else:
            print(f"  Confirmed: all {len(video_items)} video(s) downloaded successfully.")

    except PlaywrightTimeoutError as e:
        print(f"  Error: timed out loading Media Gallery: {e}")
    except Exception as e:
        print(f"  Error during Media Gallery download: {e}")

    return videos_processed


def _build_launch_kwargs(chrome_path: str) -> dict:
    kwargs = {"headless": False, "channel": "chrome"}
    if chrome_path:
        del kwargs["channel"]
        kwargs["executable_path"] = chrome_path
    return kwargs


def _prepare_profile(profile_dir: str) -> str:
    user_profile_dir = profile_dir or os.path.join(
        os.path.expanduser("~"), ".canvas-export", "chrome-profile"
    )
    os.makedirs(user_profile_dir, exist_ok=True)
    first_run = not os.path.exists(os.path.join(user_profile_dir, "Default"))
    if first_run:
        print("  First run: a Chrome window will open. Log in to Canvas, then close the window.")
        print(f"  (Profile will be saved to {user_profile_dir})")
        input("  Press Enter when you have read the above and are ready to continue...")
    return user_profile_dir


async def _download_media_gallery_async(
    api_url: str,
    api_key: str,
    course_id,
    course_view,
    cookies_path: str,
    chrome_path: str,
    dl_location: str,
    verbose: bool,
    profile_dir: str = "",
) -> int:
    """Single-course entry point (opens and closes browser per call)."""
    from playwright.async_api import async_playwright

    user_profile_dir = _prepare_profile(profile_dir)
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_profile_dir, **_build_launch_kwargs(chrome_path)
        )
        page = await context.new_page()
        try:
            return await _process_course_gallery_async(
                context, page, api_url, api_key, course_id, course_view,
                dl_location, verbose,
            )
        finally:
            await page.close()
            await context.close()


async def _download_media_gallery_batch_async(
    courses_data: list[tuple],
    api_url: str,
    api_key: str,
    cookies_path: str,
    chrome_path: str,
    dl_location: str,
    verbose: bool,
    profile_dir: str = "",
) -> int:
    """Multi-course entry point — opens browser once and processes all courses."""
    from playwright.async_api import async_playwright

    user_profile_dir = _prepare_profile(profile_dir)
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_profile_dir, **_build_launch_kwargs(chrome_path)
        )
        page = await context.new_page()
        try:
            total = 0
            for course_id, course_view in courses_data:
                total += await _process_course_gallery_async(
                    context, page, api_url, api_key, course_id, course_view,
                    dl_location, verbose,
                )
            return total
        finally:
            await page.close()
            await context.close()


class MediaGallerySession:
    """
    Holds a single Playwright browser context open across multiple courses.
    Opens once on __enter__, processes each course via download_course(), closes on __exit__.
    Bridges the async Playwright API to synchronous callers via a background thread.
    """

    def __init__(self, api_url, api_key, cookies_path, chrome_path, dl_location, verbose, profile_dir=""):
        self._api_url = api_url
        self._api_key = api_key
        self._cookies_path = cookies_path
        self._chrome_path = chrome_path
        self._dl_location = dl_location
        self._verbose = verbose
        self._profile_dir = profile_dir
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._context = None
        self._page = None
        self._pw_cm = None
        self._pw = None
        self._ready = threading.Event()
        self._setup_error: Exception | None = None

    def open(self):
        # Prepare profile synchronously (may prompt user on first run)
        self._user_profile_dir = _prepare_profile(self._profile_dir)
        self._ready.clear()
        self._setup_error = None
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._ready.wait()
        if self._setup_error:
            raise self._setup_error

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._setup())
        except Exception as e:
            self._setup_error = e
        finally:
            self._ready.set()
        if self._setup_error is None:
            self._loop.run_forever()
        self._loop.close()

    async def _setup(self):
        from playwright.async_api import async_playwright
        self._pw_cm = async_playwright()
        self._pw = await self._pw_cm.__aenter__()
        self._context = await self._pw.chromium.launch_persistent_context(
            self._user_profile_dir, **_build_launch_kwargs(self._chrome_path)
        )
        self._page = await self._context.new_page()

    async def _teardown(self):
        try:
            await self._page.close()
        except Exception:
            pass
        try:
            await self._context.close()
        except Exception:
            pass
        try:
            await self._pw_cm.__aexit__(None, None, None)
        except Exception:
            pass

    def download_course(self, course_id, course_view, context_type: str = "courses") -> int:
        if self._loop is None or not self._loop.is_running():
            return 0
        future = asyncio.run_coroutine_threadsafe(
            _process_course_gallery_async(
                self._context, self._page, self._api_url, self._api_key,
                course_id, course_view, self._dl_location, self._verbose,
                context_type=context_type,
            ),
            self._loop,
        )
        try:
            return future.result()
        except Exception as e:
            print(f"  Fatal error in Media Gallery download: {e}")
            return 0

    def close(self):
        if self._loop is None or not self._loop.is_running():
            return
        future = asyncio.run_coroutine_threadsafe(self._teardown(), self._loop)
        try:
            future.result(timeout=30)
        except Exception:
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()


def downloadMediaGallery(
    api_url: str,
    api_key: str,
    course_id,
    course_view,
    cookies_path: str,
    chrome_path: str,
    dl_location: str,
    verbose: bool = False,
    profile_dir: str = "",
) -> int:
    """Synchronous single-course entry point."""
    try:
        return asyncio.run(
            _download_media_gallery_async(
                api_url, api_key, course_id, course_view,
                cookies_path, chrome_path, dl_location, verbose,
                profile_dir=profile_dir,
            )
        )
    except Exception as e:
        print(f"  Fatal error in Media Gallery download: {e}")
        return 0


def downloadMediaGalleryBatch(
    courses_data: list[tuple],
    api_url: str,
    api_key: str,
    cookies_path: str,
    chrome_path: str,
    dl_location: str,
    verbose: bool = False,
    profile_dir: str = "",
) -> int:
    """Synchronous multi-course entry point — single browser session for all courses."""
    try:
        return asyncio.run(
            _download_media_gallery_batch_async(
                courses_data, api_url, api_key,
                cookies_path, chrome_path, dl_location, verbose,
                profile_dir=profile_dir,
            )
        )
    except Exception as e:
        print(f"  Fatal error in Media Gallery batch download: {e}")
        return 0
