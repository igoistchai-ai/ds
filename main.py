from __future__ import annotations

import asyncio
import logging
import os
import random
import string
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import discord
from aiohttp import web
from discord.ext import commands

# ============================================================
#  CONFIG (всё через Environment Variables на Render)
# ============================================================
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")       # токен бота (команды/сообщения)
USER_TOKEN = os.getenv("DISCORD_USER_TOKEN", "")     # токен аккаунта (проверка юзернеймов)
PORT = int(os.getenv("PORT", "10000"))               # Render задаёт сам
BASE_DELAY = float(os.getenv("CHECK_DELAY", "1.5"))  # пауза между проверками (сек)
TAKEN_BATCH = int(os.getenv("TAKEN_BATCH", "20"))    # сколько занятых кидать одним сообщением
OWNER_IDS = {int(x) for x in os.getenv("OWNER_IDS", "").replace(" ", "").split(",") if x}

CHECK_URL = "https://discord.com/api/v9/users/@me/pomelo-attempt"
CHARSET = string.ascii_lowercase + string.digits + "_."
NAME_LEN = 4

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ztart")


# ============================================================
#  STATE
# ============================================================
@dataclass
class Scan:
    channel_id: int
    stop: bool = False
    checked: int = 0
    taken: int = 0
    invalid: int = 0
    limited: int = 0
    free: list = field(default_factory=list)
    seen: set = field(default_factory=set)
    delay: float = BASE_DELAY
    last: str = "-"
    note: str = ""
    started: float = field(default_factory=time.time)
    task: Optional[asyncio.Task] = None

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()


def random_name(seen: set) -> str:
    while True:
        name = "".join(random.choice(CHARSET) for _ in range(NAME_LEN))
        # правила Discord: нельзя две точки подряд
        if ".." in name or name in seen:
            continue
        return name


async def check_username(session: aiohttp.ClientSession, name: str):
    """Возвращает (status, extra). status: free | taken | invalid | ratelimit | auth | error"""
    headers = {
        "Authorization": USER_TOKEN,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    }
    async with session.post(CHECK_URL, json={"username": name}, headers=headers) as r:
        if r.status == 429:
            try:
                data = await r.json(content_type=None)
                return "ratelimit", float(data.get("retry_after", 5))
            except Exception:
                return "ratelimit", 5.0
        if r.status in (401, 403):
            return "auth", r.status
        if r.status == 400:
            return "invalid", 0
        if r.status == 200:
            data = await r.json(content_type=None)
            return ("taken" if data.get("taken") else "free"), 0
        return "error", r.status


def fmt_time(sec: float) -> str:
    sec = int(sec)
    return f"{sec // 60}m {sec % 60:02d}s"


def make_embed(scan: Scan, state: str) -> discord.Embed:
    elapsed = max(time.time() - scan.started, 1)
    speed = scan.checked / elapsed
    color = {"running": 0x3DBEDC, "stopped": 0xE65A5A, "done": 0x50DCA0}.get(state, 0x3DBEDC)
    e = discord.Embed(title=f"Username scan — {state}", color=color)
    e.add_field(name="Checked", value=f"`{scan.checked}`", inline=True)
    e.add_field(name="Free", value=f"`{len(scan.free)}`", inline=True)
    e.add_field(name="Taken", value=f"`{scan.taken}`", inline=True)
    e.add_field(name="Speed", value=f"`{speed:.2f}/s`", inline=True)
    e.add_field(name="Delay", value=f"`{scan.delay:.2f}s`", inline=True)
    e.add_field(name="Rate limits", value=f"`{scan.limited}`", inline=True)
    e.add_field(name="Last", value=f"`{scan.last}`", inline=True)
    e.add_field(name="Time", value=f"`{fmt_time(elapsed)}`", inline=True)
    if scan.note:
        e.set_footer(text=scan.note)
    return e


# ============================================================
#  SCAN LOOP
# ============================================================
async def scan_loop(bot: commands.Bot, scan: Scan):
    channel = bot.get_channel(scan.channel_id)
    if channel is None:
        return

    status_msg = await channel.send(embed=make_embed(scan, "running"))
    taken_buf: list[str] = []
    last_edit = 0.0
    ok_streak = 0
    final_state = "done"

    async def edit_status(state="running", force=False):
        nonlocal last_edit
        if not force and time.time() - last_edit < 4:
            return
        last_edit = time.time()
        try:
            await status_msg.edit(embed=make_embed(scan, state))
        except discord.HTTPException:
            pass

    async def flush_taken():
        if taken_buf:
            text = "❌ **Taken:** " + " ".join(f"`{n}`" for n in taken_buf)
            taken_buf.clear()
            try:
                await channel.send(text)
            except discord.HTTPException:
                pass

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not scan.stop:
            name = random_name(scan.seen)
            scan.last = name
            try:
                result, extra = await check_username(session, name)
            except Exception as exc:
                log.warning("check error: %s", exc)
                scan.note = "network error, retrying"
                await asyncio.sleep(3)
                continue

            if result == "ratelimit":
                scan.limited += 1
                scan.delay = min(scan.delay * 1.4, 10.0)
                ok_streak = 0
                scan.note = f"rate limited, waiting {extra:.1f}s"
                await edit_status(force=True)
                await asyncio.sleep(extra + 0.5)
                scan.note = ""
                continue

            if result == "auth":
                await channel.send(
                    f"⚠️ Discord отклонил токен (HTTP {extra}). Проверь `DISCORD_USER_TOKEN`."
                )
                final_state = "stopped"
                break

            if result == "error":
                scan.note = f"HTTP {extra}, retrying"
                await asyncio.sleep(2)
                continue

            scan.seen.add(name)
            if result == "invalid":
                scan.invalid += 1
            elif result == "free":
                scan.checked += 1
                scan.free.append(name)
                try:
                    await channel.send(f"✅ `{name}` — **FREE**")
                except discord.HTTPException:
                    pass
            else:  # taken
                scan.checked += 1
                scan.taken += 1
                taken_buf.append(name)
                if len(taken_buf) >= TAKEN_BATCH:
                    await flush_taken()

            # плавно возвращаем скорость после успешных запросов
            ok_streak += 1
            if ok_streak >= 25 and scan.delay > BASE_DELAY:
                scan.delay = max(BASE_DELAY, scan.delay * 0.9)
                ok_streak = 0

            await edit_status()
            await asyncio.sleep(scan.delay + random.uniform(0, 0.3))

    if scan.stop and final_state == "done":
        final_state = "stopped"
    await flush_taken()
    await edit_status(final_state, force=True)
    summary = (
        f"**Scan {final_state}.** Checked `{scan.checked}`, taken `{scan.taken}`, "
        f"free `{len(scan.free)}`."
    )
    if scan.free:
        summary += "\nFree: " + " ".join(f"`{n}`" for n in scan.free)
    try:
        await channel.send(summary)
    except discord.HTTPException:
        pass


# ============================================================
#  BOT
# ============================================================
class ZtartBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix=".", intents=intents, help_command=None)
        self.scan: Optional[Scan] = None

    async def on_ready(self):
        log.info("logged in as %s", self.user)


bot = ZtartBot()


def allowed(ctx: commands.Context) -> bool:
    return not OWNER_IDS or ctx.author.id in OWNER_IDS


@bot.command(name="ztart")
async def ztart(ctx: commands.Context):
    if not allowed(ctx):
        return
    if not USER_TOKEN:
        await ctx.send("⚠️ Не задан `DISCORD_USER_TOKEN` в Environment Variables.")
        return
    if bot.scan and bot.scan.running:
        await ctx.send("Уже работает. `.stop` — остановить, `.status` — статус.")
        return
    scan = Scan(channel_id=ctx.channel.id)
    bot.scan = scan
    scan.task = asyncio.create_task(scan_loop(bot, scan))


@bot.command(name="stop")
async def stop(ctx: commands.Context):
    if not allowed(ctx):
        return
    if bot.scan and bot.scan.running:
        bot.scan.stop = True
        await ctx.send("Останавливаю...")
    else:
        await ctx.send("Сейчас ничего не запущено.")


@bot.command(name="status")
async def status(ctx: commands.Context):
    if not allowed(ctx):
        return
    if not bot.scan:
        await ctx.send("Сканирование ещё не запускалось. `.ztart`")
        return
    await ctx.send(embed=make_embed(bot.scan, "running" if bot.scan.running else "stopped"))


async def start_health_server():
    # мини веб-сервер, чтобы Render видел открытый порт (Web Service)
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="ok"))
    app.router.add_get("/health", lambda r: web.Response(text="ok"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("health server on :%s", PORT)


async def wait_until_not_blocked():
    """Если Discord временно блокирует IP (429 / Cloudflare), не падаем и не
    перезапускаемся в цикле (это только продлевает блок) — ждём с нарастающей паузой."""
    delay = 120
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
                async with s.get("https://discord.com/api/v10/gateway") as r:
                    if r.status != 429:
                        return
                    log.warning("Discord блокирует IP (429). Жду %ss...", delay)
        except Exception as exc:
            log.warning("проверка Discord не удалась: %s", exc)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 1800)


async def main():
    if not BOT_TOKEN:
        raise SystemExit("Set DISCORD_BOT_TOKEN environment variable")
    await start_health_server()
    await wait_until_not_blocked()
    async with bot:
        await bot.start(BOT_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
