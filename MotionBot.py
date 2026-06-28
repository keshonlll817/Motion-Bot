import discord
import csv
import io
import re
import os
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TOKEN = os.getenv("TOKEN")

if not TOKEN:
    raise ValueError("No TOKEN found.")

# ── MAIN SERVER CHANNELS ──
FOUR_PLUS_CHANNEL        = 1497705244220723250   # #tt-4sets — human posts final slate
TOTALS_CHANNEL           = 1497719529571090452   # #tt-totals
MAIN_REMINDER_CHANNEL    = 1497668434677338134   # #train-reminders — alerts WITH ping
MAIN_CHAT_CHANNEL        = 1497740892394623186   # #main-chat — silent copy of alerts
MAIN_CONFIRM_CHANNEL     = 1497721653059129436   # #comfirmation — REMINDERS SET
RECAPS_CHANNEL           = 1497666520015180057   # #tt-recaps — recap commands + output
CSV_CHANNEL              = 1497722290526224545   # #csv — CSV uploads

# ── TEST SERVER CHANNELS ──
TEST_CHANNEL              = 1471792196582637728
TEST_CONFIRMATION_CHANNEL = 1488259145093222522
PROCESSING_CHANNEL        = 1497213517827145728
SLATE_CHANNEL             = 1494963563096313906
REMINDERS_CHANNEL         = 1494963600979394640
TEST_RECAPS_CHANNEL       = 1497215421789638719

# ── ROLE IDs ──
TT_OFFICIAL_ROLE_ID  = 1428404827234504735   # regular TT plays — always pinged
TT_DEGEN_ROLE_ID     = 1428404747085283520   # late night 12AM–5:59AM EST
OTHER_SPORTS_ROLE_ID = 1496207277626101982   # non-TT plays
# ── League notification roles ──
LEAGUE_ROLE_IDS = {
    "ELITE":  1511861995203330129,
    "CZECH":  1511862076384215283,
    "CUP":    1511862130033430609,
    "SETKA":  1511862181904515163,
}

# CSV processing only allowed in csv/processing channels
ALLOWED_CHANNELS = [
    CSV_CHANNEL,
    PROCESSING_CHANNEL,
]

EST = ZoneInfo("America/New_York")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

last_slate_messages = []

# ==============================
# LOCK SYSTEM
# ==============================
# !lock true  → bot goes silent everywhere except test server
# !lock false → bot fully active again
locked = False
TEST_GUILD_ID  = 1471792194963767411  # test server guild ID
MAIN_GUILD_ID  = 1427196726460158056  # main server guild ID

# ==============================
# REMINDER STATE
# ==============================
# Per-guild, per-message task tracking.
# Structure: {guild_id: {message_id: {play_key: asyncio.Task}}}
# A guild_id of 0 is used for DMs / channels with no guild.
scheduled_tasks = {}   # {guild_id: {message_id: {play_key: metadata_dict}}}
pending_alerts  = {}   # {guild_id: [alert_dicts]} — central dispatcher registry
_dispatcher_started = False
_on_ready_done      = False
active_keys     = {}   # {guild_id: set(play_key)} — global dedup per guild
bang_last_fired = {}   # {channel_id: datetime} — per-channel cooldown for "Bang!"


# ==============================
# UTIL FUNCTIONS
# ==============================

def format_units(u):
    if u == 1:    return "1U"
    if u == 1.25: return "1.25U"
    if u == 1.5:  return "1.5U"
    if u == 1.75: return "1.75U"
    if u == 2:    return "2U"
    if u == 2.5:  return "2.5U"
    if u == 3:    return "3U"
    return f"{u}U"

def convert_league(name):
    name = name.lower()
    if "elite" in name: return "ELITE"
    if "setka" in name: return "SETKA"
    if "czech" in name: return "CZECH"
    if "cup"   in name: return "CUP"
    return name.upper()

def parse_time(est_time):
    dt     = datetime.strptime(est_time, "%m/%d %I:%M %p")
    est    = dt.strftime("%I:%M %p")
    pst_dt = dt.replace(hour=(dt.hour - 3) % 24)
    pst    = pst_dt.strftime("%I:%M %p")
    return est, pst

async def send_long_message(channel, text):
    chunks = []
    while len(text) > 2000:
        split_index = text.rfind("\n", 0, 2000)
        if split_index == -1:
            split_index = 2000
        chunks.append(text[:split_index])
        text = text[split_index:]
    chunks.append(text)
    messages = []
    for chunk in chunks:
        msg = await channel.send(chunk.strip())
        messages.append(msg)
    return messages


# ==============================
# REMINDER ENGINE
# ==============================

def _guild_id(message_or_channel):
    """Return the guild id for a message or channel, or 0 if DM."""
    g = getattr(message_or_channel, "guild", None)
    return g.id if g else 0


def make_play_key(league, p1, p2, time_str):
    """
    Unique key for a play.
    Format: "LEAGUE|P1|P2|HH:MM AM" (players sorted alphabetically)
    Sorting prevents reverse duplicates: "A vs B" and "B vs A" at the
    same time produce the same key.
    Keyed on time string (not full ISO datetime) so that
    startup reschedule matches keys created during the session.
    """
    sorted_players = sorted([p1.lower(), p2.lower()])
    return f"{league}|{sorted_players[0]}|{sorted_players[1]}|{time_str}"


def _ensure_guild_structures(guild_id):
    """Create per-guild dicts if they don't exist yet."""
    if guild_id not in scheduled_tasks:
        scheduled_tasks[guild_id] = {}
    if guild_id not in active_keys:
        active_keys[guild_id] = set()


def _pick_role(guild, game_dt):
    """
    Pick the correct ping role based on game time:
    - 12:00 AM – 5:59 AM EST → TT Degen
    - All other times         → TT Official
    Returns (role_or_None, fallback_text).
    """
    hour = game_dt.astimezone(EST).hour if game_dt else datetime.now(EST).hour
    if 0 <= hour < 6:
        role_id   = TT_DEGEN_ROLE_ID
        role_name = "TT Degen"
    else:
        role_id   = TT_OFFICIAL_ROLE_ID
        role_name = "TT Official"

    role = None
    if guild:
        role = guild.get_role(role_id)
    return role, role_name


def build_reminder_text(guild, league, p1, p2, wins, total, tier, label, play_type="", condition=None, game_dt=None):
    """
    Build reminder message. Pings TT Official (always) + the correct league role.
    12AM-5:59AM EST → also pings TT Degen instead of/alongside TT Official.
    Appends conditional note if present.
    """
    if   tier == "nuke":    emoji = " ☢️"
    elif tier == "caution": emoji = " ⚠️"
    else:                   emoji = ""

    play_str = f" {play_type}" if play_type else ""
    body = f"{league} – {p1} vs {p2}{play_str}{emoji} ({wins}/{total}) | {label}"

    if condition:
        body += f"\n*{condition}*"

    if guild:
        # Time-based role: 12AM-5:59AM = Degen, else Official
        hour = game_dt.astimezone(EST).hour if game_dt else datetime.now(EST).hour
        base_role_id = TT_DEGEN_ROLE_ID if 0 <= hour < 6 else TT_OFFICIAL_ROLE_ID
        base_role    = guild.get_role(base_role_id)

        # League-specific role
        league_upper = league.upper()
        league_role  = None
        for key, rid in LEAGUE_ROLE_IDS.items():
            if key in league_upper:
                league_role = guild.get_role(rid)
                break

        mentions = []
        if base_role:   mentions.append(base_role.mention)
        if league_role: mentions.append(league_role.mention)
        if mentions:
            return f"{' '.join(mentions)} {body}"

    return f"@TT Official @{league} {body}"


def _allowed_mentions_for_guild(guild, game_dt=None):
    """Return AllowedMentions for TT Official/Degen + all league roles."""
    if guild:
        hour = game_dt.astimezone(EST).hour if game_dt else datetime.now(EST).hour
        base_role_id = TT_DEGEN_ROLE_ID if 0 <= hour < 6 else TT_OFFICIAL_ROLE_ID
        roles = [guild.get_role(base_role_id)]
        for rid in LEAGUE_ROLE_IDS.values():
            roles.append(guild.get_role(rid))
        roles = [r for r in roles if r is not None]
        if roles:
            return discord.AllowedMentions(roles=roles)
    return discord.AllowedMentions(roles=True)



# ════════════════════════════════════════════════════════════
# CENTRAL REMINDER DISPATCHER (ported from SlateBot)
# ════════════════════════════════════════════════════════════

def _register_alert(guild_id, fire_at, label, play, reminder_channel_id, key, silent_channel_id=None):
    if guild_id not in pending_alerts:
        pending_alerts[guild_id] = []
    pending_alerts[guild_id].append({
        "fire_at": fire_at, "label": label, "play": play,
        "reminder_channel_id": reminder_channel_id,
        "silent_channel_id": silent_channel_id,
        "key": key, "sent": False,
    })


def _cancel_alerts_for_key(guild_id, key):
    if guild_id in pending_alerts:
        pending_alerts[guild_id] = [a for a in pending_alerts[guild_id] if a["key"] != key]


def _get_active_plays(guild_id):
    _ensure_guild_structures(guild_id)
    now = datetime.now(EST)
    pending_now_keys = set()
    for a in pending_alerts.get(guild_id, []):
        if not a["sent"] and a["fire_at"] > now - timedelta(seconds=90):
            pending_now_keys.add(a["key"])
    plays = {}
    for msg_id, msg_meta in scheduled_tasks.get(guild_id, {}).items():
        for key, meta in msg_meta.items():
            if key in pending_now_keys and key not in plays:
                plays[key] = meta
    return sorted(plays.items(), key=lambda kv: kv[1].get("game_dt", now))


async def _fetch_ch_safe(ch_id):
    ch = client.get_channel(ch_id)
    if ch is None:
        try:   ch = await client.fetch_channel(ch_id)
        except: ch = None
    return ch


async def _reminder_dispatcher_loop():
    await client.wait_until_ready()
    print("[DISPATCHER] Reminder dispatcher started.")
    while not client.is_closed():
        try:
            now = datetime.now(EST)
            for guild_id, alerts in list(pending_alerts.items()):
                if locked and guild_id != TEST_GUILD_ID:
                    continue
                due = [a for a in alerts if not a["sent"] and a["fire_at"] <= now]
                # Skip stale alerts (>5min late)
                for a in due:
                    if (now - a["fire_at"]).total_seconds() > 300:
                        a["sent"] = True
                due = [a for a in due if not a["sent"]]
                if not due:
                    continue
                # Group by (channel, label)
                groups = {}
                for a in due:
                    gkey = (a["reminder_channel_id"], a["label"])
                    groups.setdefault(gkey, []).append(a)
                for (ch_id, label), group in groups.items():
                    ch = await _fetch_ch_safe(ch_id)
                    if not ch:
                        continue
                    dest_guild = getattr(ch, "guild", client.get_guild(guild_id))
                    lines = []
                    for a in group:
                        p = a["play"]
                        lines.append(build_reminder_text(
                            dest_guild, p["league"], p["p1"], p["p2"],
                            p["wins"], p["total"], p["tier"], label,
                            p.get("play_type", ""), p.get("condition"),
                            p.get("game_dt")
                        ))
                    if len(lines) > 1:
                        seen_mentions = []
                        for ln in lines:
                            for m in re.findall(r'<@&\d+>', ln):
                                if m not in seen_mentions:
                                    seen_mentions.append(m)
                        clean = [re.sub(r'<@&\d+>\s*', '', ln).strip() for ln in lines]
                        ping_str = " ".join(seen_mentions) + " " if seen_mentions else ""
                        text = ping_str + f"**{label}**\n" + "\n".join(clean)
                    else:
                        text = lines[0]
                    try:
                        await ch.send(text, allowed_mentions=_allowed_mentions_for_guild(dest_guild, group[0]["play"].get("game_dt")))
                        # Silent copy to main-chat if configured
                        silent_ch_id = group[0].get("silent_channel_id")
                        if silent_ch_id:
                            sch = await _fetch_ch_safe(silent_ch_id)
                            if sch:
                                silent_text = re.sub(r'<@&\d+>\s*', '', text).strip()
                                await sch.send(silent_text, allowed_mentions=discord.AllowedMentions.none())
                        for a in group:
                            a["sent"] = True
                        print(f"[DISPATCHER] Sent {label} ({len(group)} play(s)) → ch={ch_id}")
                    except Exception as e:
                        print(f"[DISPATCHER] Send failed: {e}")
                # Clean up sent/expired
                pending_alerts[guild_id] = [
                    a for a in alerts
                    if not a["sent"] and (now - a["fire_at"]).total_seconds() < 600
                ]
        except Exception as e:
            print(f"[DISPATCHER] Loop error: {e}")
            import traceback; traceback.print_exc()
        await asyncio.sleep(10)


def _schedule_play_for_message(guild_id, message_id, guild, play, game_dt, reminder_channel_id, silent_channel_id=None):
    _ensure_guild_structures(guild_id)
    league    = play["league"]
    p1        = play["p1"]
    p2        = play["p2"]
    wins      = play["wins"]
    total     = play["total"]
    tier      = play["tier"]
    time_str  = play["time_str"]
    play_type = play.get("play_type", "")
    condition = play.get("condition", None)
    key       = make_play_key(league, p1, p2, time_str)
    now_est   = datetime.now(EST)
    soon_dt   = game_dt - timedelta(minutes=5)
    if game_dt <= now_est:
        return key, False
    if key in active_keys[guild_id]:
        return key, False
    play_data = {
        "league": league, "p1": p1, "p2": p2, "wins": wins,
        "total": total, "tier": tier, "play_type": play_type,
        "condition": condition, "game_dt": game_dt,
    }
    if (soon_dt - now_est).total_seconds() > -90:
        _register_alert(guild_id, soon_dt, "STARTING SOON", play_data, reminder_channel_id, key, silent_channel_id)
    if (game_dt - now_est).total_seconds() > -90:
        _register_alert(guild_id, game_dt, "STARTING NOW",  play_data, reminder_channel_id, key, silent_channel_id)
    if message_id not in scheduled_tasks[guild_id]:
        scheduled_tasks[guild_id][message_id] = {}
    scheduled_tasks[guild_id][message_id][key] = {"game_dt": game_dt, "league": league, "p1": p1, "p2": p2, "time_str": time_str}
    active_keys[guild_id].add(key)
    print(f"[REMINDERS] Scheduled: {key} @ {game_dt.strftime('%m/%d %I:%M %p')} EST")
    return key, True


def _cancel_message_tasks(guild_id, message_id):
    _ensure_guild_structures(guild_id)
    msg_tasks = scheduled_tasks[guild_id].pop(message_id, {})
    for key in msg_tasks.keys():
        active_keys[guild_id].discard(key)
        _cancel_alerts_for_key(guild_id, key)
        print(f"[REMINDERS] Cancelled: {key}")


def clear_all_reminders():
    scheduled_tasks.clear()
    active_keys.clear()
    pending_alerts.clear()
    print("[REMINDERS] Cleared all stale reminders.")


# ==============================
# STARTUP
# ==============================

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")

    # Wipe any stale in-memory tasks from previous session before rescheduling
    clear_all_reminders()

    # Reschedule reminders still in the future from the slate channel
    # Reschedule from all watched channels — each wrapped so one failure doesn't crash startup
    resched_channels = [
        (SLATE_CHANNEL,      "test slate"),
        (FOUR_PLUS_CHANNEL,  "main 4+"),
        (TOTALS_CHANNEL,     "main totals"),
    ]
    for ch_id, label in resched_channels:
        try:
            ch = client.get_channel(ch_id)
            if ch is None:
                ch = await client.fetch_channel(ch_id)
            if ch:
                await reschedule_from_channel(ch)
                print(f"[REMINDERS] Rescheduled from {label} channel.")
        except discord.Forbidden:
            print(f"[REMINDERS] No access to {label} channel ({ch_id}) — skipping reschedule.")
        except Exception as e:
            print(f"[REMINDERS] Error rescheduling from {label} channel: {e}")


# ==============================
# MESSAGE EDIT HANDLER
# ==============================

@client.event
async def on_message_edit(before, after):
    """
    When a message in 4+/totals/test is edited:
    - Cancel all old tasks for that message.
    - Re-parse and reschedule based on the new content.
    - Other messages are completely unaffected.
    """
    if locked:
        edit_guild_id = after.guild.id if after.guild else None
        if edit_guild_id != TEST_GUILD_ID:
            return  # silently ignore edits when locked

    if after.channel.id != SLATE_CHANNEL:
        return

    # Re-parse the edited message and reschedule reminders
    results = await schedule_message_plays(after)
    if results:
        await send_reminder_confirmation(results)


# ==============================
# MESSAGE DELETE HANDLER
# ==============================

@client.event
async def on_message_delete(message):
    """
    When a message in 4+/totals/test is deleted:
    - Cancel all reminder tasks tied to that message.
    - Remove its keys from the global active_keys set.
    """
    if locked:
        del_guild_id = message.guild.id if message.guild else None
        if del_guild_id != TEST_GUILD_ID:
            return  # silently ignore deletes when locked

    if message.channel.id != SLATE_CHANNEL:
        return

    guild_id = _guild_id(message)
    _cancel_message_tasks(guild_id, message.id)


# ==============================
# MESSAGE HANDLER
# ==============================

@client.event
async def on_message(message):

    global last_slate_messages, locked

    # ── LOCK CHECK — block all activity except test server when locked ──
    if locked:
        msg_guild_id = message.guild.id if message.guild else None
        if msg_guild_id != TEST_GUILD_ID:
            if not message.author.bot and message.content.strip().startswith("!"):
                await message.channel.send(
                    "🔒 **The bot is currently locked and only being used for testing purposes.**\n"
                    "Please ask **Dark** to unlock the bot or try again later."
                )
            return

    # ── Schedule reminders when a HUMAN posts to #slatechannel ──
    if not message.author.bot and message.channel.id in (SLATE_CHANNEL, FOUR_PLUS_CHANNEL, TOTALS_CHANNEL):
        results = await schedule_message_plays(message)
        if results:
            await send_reminder_confirmation(results)

    if message.author.bot:
        return

    content = message.content.lower().strip()
    first_line = content.split("\n")[0].strip()  # use first line for exact-match commands

    # ── "Bang!" response with 1-minute per-channel cooldown ──
    if first_line == "bang" or content == "bang":
        now = datetime.now(EST)
        ch_id = message.channel.id
        last = bang_last_fired.get(ch_id)
        if last is None or (now - last).total_seconds() >= 60:
            bang_last_fired[ch_id] = now
            await message.channel.send("Bang!")
        return

    # ── Ping → pong in any channel ──
    if first_line == "ping" or content == "ping":
        await message.channel.send("pong")
        return

# ==============================
# RECAP COMMANDS
# ==============================

    if content.startswith("!recap"):
        if message.channel.id not in (RECAPS_CHANNEL, TEST_CHANNEL):
            await message.channel.send(f"Head to <#{RECAPS_CHANNEL}> to use recap commands.")
            return

        now=datetime.now(EST)

        if "test" in content:
            start=None
            end=None
            limit=50
            title=f"TEST RECAP — {now.strftime('%b')} {now.day} (EST)"

        elif "today" in content:
            start=now.replace(hour=0,minute=0,second=0,microsecond=0)
            end=now
            title=f"TODAY RECAP — {now.strftime('%b')} {now.day} (EST)"
            limit=None

        elif "lifetime" in content:
            start=None
            end=None
            title="LIFETIME RECAP"
            limit=None

        elif "yesterday" in content:
            start=(now-timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
            end=start+timedelta(days=1)
            title=f"DAILY RECAP — {start.strftime('%b')} {start.day} (EST)"
            limit=None

        elif "last week" in content or "lastweek" in content.replace(" ",""):
            days_since_monday=now.weekday()
            this_monday=now.replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=days_since_monday)
            start=this_monday-timedelta(days=7)
            end=this_monday
            title=f"LAST WEEK RECAP — {start.strftime('%b %-d')} → {end.strftime('%b %-d')} (EST)"
            limit=None

        elif "weekly" in content:
            days_since_monday=now.weekday()
            start=now.replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=days_since_monday)
            end=start+timedelta(days=7)
            title=f"WEEKLY RECAP — {start.strftime('%b %-d')} → {end.strftime('%b %-d')} (EST)"
            limit=None

        elif "monthly" in content:
            start=now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
            end=now
            title=f"MONTHLY RECAP — {now.strftime('%b %Y')}"
            limit=None

        else:
            return

        # Scan correct channels based on which server the command came from
        if message.guild and message.guild.id == TEST_GUILD_ID:
            four_channel   = client.get_channel(SLATE_CHANNEL)
            totals_channel = client.get_channel(SLATE_CHANNEL)
        else:
            four_channel   = client.get_channel(FOUR_PLUS_CHANNEL)
            totals_channel = client.get_channel(TOTALS_CHANNEL)

        # Safety fallback
        if four_channel is None:
            four_channel = message.channel
        if totals_channel is None:
            totals_channel = message.channel

        fw,fl,fwash,nw,nl,cw,cl,kw,kl,league_stats=await parse_four_plus(four_channel,start,end,limit)
        tw,tl,tunits=await parse_totals(totals_channel,start,end,limit)

        four_units=( (nw*0.87)-(nl*3) + (cw*0.435)-(cl*1.5) + (kw*1.74)-(kl*6) )
        total_units = four_units + tunits

        # Sidebar color: green if net positive, red if net negative, grey if zero
        if total_units > 0:
            embed_color = 0x00C853  # green
            result_icon = "🟢"
        elif total_units < 0:
            embed_color = 0xD50000  # red
            result_icon = "🔴"
        else:
            embed_color = 0x607D8B  # grey
            result_icon = "⚪"

        embed = discord.Embed(title=f"📊 {title}", color=embed_color)

        # 4+ PLAYS field
        if fw+fl+fwash==0:
            four_text = "No plays graded."
        else:
            four_text = f"Record: **{fw}-{fl}**"
            if fwash > 0:
                four_text += f" ({fwash} Wash)"
            four_text += f"\nUnits: **{four_units:+.2f}U**"
            four_text += f"\n\nNormal {nw}-{nl}  ⚠️ {cw}-{cl}  ☢️ {kw}-{kl}"

        embed.add_field(name="🏓 4+ PLAYS", value=four_text, inline=False)

        # TOTAL PLAYS field
        if tw+tl==0:
            tot_text = "No plays graded."
        else:
            tot_text = f"Record: **{tw}-{tl}**\nUnits: **{tunits:+.2f}U**"

        embed.add_field(name="🏓 TOTAL PLAYS", value=tot_text, inline=False)

        # Net summary
        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━",
            value=f"{result_icon} **Net Units: {total_units:+.2f}U**  |  Win Rate: {round(fw/(fw+fl)*100) if (fw+fl)>0 else 0}%",
            inline=False
        )

        # Send recap to #recaps channel
        recap_ch = client.get_channel(RECAPS_CHANNEL)
        if recap_ch is None:
            recap_ch = message.channel
        await recap_ch.send(embed=embed)

        # League breakdown as a second embed
        if league_stats:
            sorted_leagues = sorted(league_stats.items(), key=lambda x: x[1]["u"], reverse=True)
            league_embed = discord.Embed(title="🏓 LEAGUE BREAKDOWN", color=embed_color)

            for i,(lg,data) in enumerate(sorted_leagues):
                if i==0:   icon="🔥"
                elif i==1: icon="🟢"
                elif i==2: icon="🟡"
                else:      icon="🔻"
                league_embed.add_field(
                    name=f"{icon} {lg}",
                    value=f"Record: {data['w']}-{data['l']}\nUnits: {data['u']:+.2f}U",
                    inline=True
                )

            await recap_ch.send(embed=league_embed)

        return


# ==============================
# BASIC COMMANDS (work in any channel)
# ==============================

    if content=="ping":
        await message.channel.send("pong")
        return

    if content=="!testreminder":
        now_est = datetime.now(EST)
        fire_dt = now_est + timedelta(minutes=2)
        soon_dt = now_est + timedelta(minutes=1)

        async def _test_task():
            ch = client.get_channel(REMINDERS_CHANNEL)
            await asyncio.sleep((soon_dt - datetime.now(EST)).total_seconds())
            if ch:
                await ch.send(
                    "🧪 **[REMINDER TEST — IGNORE]**\n"
                    "TEST – SlateBot vs Test (25/30) | STARTING SOON\n"
                    "_This is an automated reminder test. No action needed._"
                )
            await asyncio.sleep((fire_dt - datetime.now(EST)).total_seconds())
            if ch:
                await ch.send(
                    "🧪 **[REMINDER TEST — IGNORE]**\n"
                    "TEST – SlateBot vs Test (25/30) | STARTING NOW\n"
                    "_This is an automated reminder test. No action needed._"
                )

        asyncio.ensure_future(_test_task())
        await message.channel.send(
            f"✅ Test reminder scheduled!\n"
            f"**STARTING SOON** → {soon_dt.strftime('%I:%M %p')} EST\n"
            f"**STARTING NOW** → {fire_dt.strftime('%I:%M %p')} EST\n"
            f"Watch <#{REMINDERS_CHANNEL}> for the alerts."
        )
        return

    if first_line == "!reminders" or content == "!reminders":
        guild_id = _guild_id(message)
        _ensure_guild_structures(guild_id)
        now = datetime.now(EST)
        sorted_plays = _get_active_plays(guild_id)
        if not sorted_plays:
            await message.channel.send("⏰ No reminders currently scheduled.")
            return
        out_lines = [f"⏰ **ACTIVE REMINDERS** ({len(sorted_plays)} play(s)) ━━━━━━━━━━━━━━━━━━"]
        for idx, (key, meta) in enumerate(sorted_plays, start=1):
            league_k = meta.get("league", "?")
            p1_k     = meta.get("p1", "?")
            p2_k     = meta.get("p2", "?")
            time_k   = meta.get("time_str", "?")
            game_dt  = meta.get("game_dt", now)
            secs     = (game_dt - now).total_seconds()
            countdown = "starting now" if secs < 0 else (f"in {int(secs//3600)}h {int((secs%3600)//60)}m" if secs >= 3600 else f"in {int(secs//60)}m")
            out_lines.append(f"**{idx}.** {league_k} – {p1_k.title()} vs {p2_k.title()} @ {time_k} EST ({countdown})")
        out_lines.append(f"\n_Use `!reminderremove 1,2,3` to cancel specific reminders._")
        await send_long_message(message.channel, "\n".join(out_lines))
        return

    if content.startswith("!reminderremove"):
        guild_id = _guild_id(message)
        _ensure_guild_structures(guild_id)
        raw_args = content.replace("!reminderremove", "").strip()
        if not raw_args:
            await message.channel.send("Usage: `!reminderremove 1,2,5` — use `!reminders` to see the numbered list.")
            return
        try:
            indexes = [int(x.strip()) for x in raw_args.split(",") if x.strip()]
        except ValueError:
            await message.channel.send("Invalid format. Use numbers separated by commas: `!reminderremove 1,2,5`")
            return
        sorted_plays = _get_active_plays(guild_id)
        if not sorted_plays:
            await message.channel.send("⏰ No reminders currently scheduled.")
            return
        max_idx = len(sorted_plays)
        bad = [i for i in indexes if i < 1 or i > max_idx]
        if bad:
            await message.channel.send(f"Invalid index(es): {', '.join(str(b) for b in bad)}. Valid range: 1–{max_idx}")
            return
        removed = []
        for idx in sorted(set(indexes)):
            key, meta = sorted_plays[idx - 1]
            league_k = meta.get("league", "?")
            p1_k = meta.get("p1", "?").title()
            p2_k = meta.get("p2", "?").title()
            time_k = meta.get("time_str", "?")
            _cancel_alerts_for_key(guild_id, key)
            active_keys[guild_id].discard(key)
            for msg_id, msg_meta in list(scheduled_tasks.get(guild_id, {}).items()):
                if key in msg_meta:
                    del msg_meta[key]
                    break
            print(f"[REMINDERS] Removed by user: {key}")
            removed.append(f"{league_k} – {p1_k} vs {p2_k} @ {time_k} EST")
        out_lines = ["🗑️ **REMINDERS REMOVED** ━━━━━━━━━━━━━━━━━━"] + removed
        out_lines.append(f"\n**{len(removed)} reminder(s) cancelled.**")
        await message.channel.send("\n".join(out_lines))
        return

    if first_line in ("!help", "!commands") or content in ("!help", "!commands"):
        help_msg=(
            "🏓 **SLATEBOT COMMANDS** 🏓\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "📂 **SLATE**\n"
            "Upload a `.csv` file in an allowed channel to post the day's slate.\n"
            "The bot will delete the previous slate and post a fresh one automatically.\n"
            "\n"
            "📊 **RECAP COMMANDS**\n"
            "`!recap today` — Recap from midnight to now\n"
            "`!recap yesterday` — Full recap for yesterday\n"
            "`!recap weekly` — This week Mon → Mon\n"
            "`!recap last week` — Last full week Mon → Mon\n"
            "`!recap monthly` — This month so far\n"
            "`!recap lifetime` — All-time recap\n"
            "`!recap test` — Test recap (last 50 msgs)\n"
            "\n"
            "🎮 **OTHER**\n"
            "`ping` — Check if bot is online (responds with `pong`)\n"
            "`!reminders` — Show all currently active/pending reminders with countdown\n"
            "`!reminderremove 1,2,5` — Cancel specific reminders by index number\n"
            "`!testreminder` — Fire a test alert in 1–2 min (no ping, clearly labeled)\n"
            "`!help` or `!commands` — Show this menu\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "💡 **PLAY TIERS (4+ Channel)**\n"
            "Normal — Standard play\n"
            "⚠️ Caution — Lower confidence play\n"
            "☢️ Nuke — Highest confidence play\n"
            "🧼 Wash — No result counted"
        )
        await message.channel.send(help_msg)
        return

    # ==============================
    # CSV SLATE ENGINE
    # ==============================
    if not message.attachments:
        return

    # Only process CSV uploads in allowed channels.
    # If your slatechannel ID is not listed in ALLOWED_CHANNELS at the top of this file,
    # add it there. Right-click the channel in Discord → Copy Channel ID.
    if message.channel.id not in ALLOWED_CHANNELS:
        return

    attachment = message.attachments[0]

    if not attachment.filename.endswith(".csv"):
        return

    file_bytes = await attachment.read()
    decoded = file_bytes.decode("utf-8")

    reader = csv.DictReader(io.StringIO(decoded))

    # Validate required columns before processing
    required_cols = {"League", "Player 1", "Player 2", "Play", "History", "Time (Eastern)"}
    fieldnames = reader.fieldnames or []
    missing_cols = required_cols - set(fieldnames)
    if missing_cols:
        await message.channel.send(
            f"❌ CSV is missing required columns: `{', '.join(sorted(missing_cols))}`\n"
            f"Expected: `League, Player 1, Player 2, Play, History, Time (Eastern)`"
        )
        return

    four_plus = {}
    totals = {}

    for row in reader:
        try:
            league = convert_league(row["League"])
            p1 = row["Player 1"]
            p2 = row["Player 2"]
            play = row["Play"]
            history = row["History"]
            est_time = row["Time (Eastern)"]
        except KeyError as e:
            print(f"[CSV] Skipping row with missing column: {e}")
            continue

        try:
            est, pst = parse_time(est_time)
        except Exception as e:
            print(f"[CSV] Skipping row — bad time format '{est_time}': {e}")
            continue

        if "4+" in play:
            match = re.search(r"\((\d+)/(\d+)\)", history)
            if not match:
                continue

            losses = int(match.group(1))
            total = int(match.group(2))
            wins = total - losses
            pct = wins / total

            tier = "normal"
            if total >= 40 and pct >= 0.91:
                tier = "nuke"
            elif wins <= 22:
                tier = "caution"

            key = f"{league}{p1}{p2}{est}"
            four_plus[key] = (league, p1, p2, est, pst, wins, total, tier)

        elif "Over/Under" in history:
            match = re.search(r"\((\d+)/(\d+)\)", history)
            if not match:
                continue

            wins = int(match.group(1))
            total = int(match.group(2))
            pct = wins / total

            if total >= 30:
                if pct >= .95: units = 2.5
                elif pct >= .91: units = 2
                elif pct >= .86: units = 1.5
                elif pct >= .81: units = 1.25
                else: units = 1
            else:
                if pct >= .95: units = 2
                elif pct >= .91: units = 1.75
                elif pct >= .86: units = 1.5
                elif pct >= .81: units = 1.25
                else: units = 1

            key = f"{league}{p1}{p2}{est}{play}"
            totals[key] = (league, p1, p2, play, units, est, pst, wins, total)

    # DELETE PREVIOUS SLATE FIRST
    for msg in last_slate_messages:
        try:
            await msg.delete()
        except Exception:
            pass

    last_slate_messages = []
    await message.delete()

    # SEND NEW SLATE
    msg1 = await message.channel.send("🏓 **4+ PLAYS** 🏓")
    last_slate_messages.append(msg1)

    if four_plus:
        text = ""
        for v in four_plus.values():
            league, p1, p2, est, pst, wins, total, tier = v
            emoji = ""
            if tier == "nuke": emoji = " ☢️"
            elif tier == "caution": emoji = " ⚠️"

            text += f"{league} – {p1} vs {p2} @ {est} EST / {pst} PST ({wins}/{total}){emoji}\n\n"

        sent_msgs = await send_long_message(message.channel, text.strip())
        last_slate_messages.extend(sent_msgs)

    msg3 = await message.channel.send("🏓 **TOTAL PLAYS** 🏓")
    last_slate_messages.append(msg3)

    if totals:
        text = ""
        for v in totals.values():
            league, p1, p2, play, units, est, pst, wins, total = v
            text += f"{league} – {p1} vs {p2} {play} {format_units(units)} @ {est} EST / {pst} PST ({wins}/{total})\n\n"

        sent_msgs = await send_long_message(message.channel, text.strip())
        last_slate_messages.extend(sent_msgs)

    # Summary — no reminders here, those fire when human posts to #slatechannel
    total_plays = len(four_plus) + len(totals)
    if total_plays == 0:
        conf = "⚠️ CSV uploaded but **no valid plays were found**. Check your column format and history values."
    else:
        conf_lines = [f"✅ **Slate processed** — {total_plays} play(s) ready."]
        if four_plus:
            conf_lines.append(f"📌 4+ Plays: {len(four_plus)}")
        if totals:
            conf_lines.append(f"📌 Total Plays: {len(totals)}")
        conf_lines.append(f"📋 Review above, edit if needed, then post to <#{SLATE_CHANNEL}>.")
        conf = "\n".join(conf_lines)
    conf_msg = await message.channel.send(conf)
    last_slate_messages.append(conf_msg)


client.run(TOKEN)
