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
FOUR_PLUS_CHANNEL    = 1443356395935240302
TOTALS_CHANNEL       = 1446203029916356649
MAIN_REMINDER_CHANNEL    = 1442258139985608867
MAIN_CONFIRM_CHANNEL     = 1452410545016930335

# ── TEST SERVER CHANNELS (original) ──
TEST_CHANNEL         = 1471792196582637728
TEST_CONFIRMATION_CHANNEL = 1488259145093222522

# ── NEW TEST SERVER CHANNELS ──
PROCESSING_CHANNEL   = 1497213517827145728   # csv uploads go here
SLATE_CHANNEL        = 1494963563096313906   # human posts final slate here
REMINDERS_CHANNEL    = 1494963600979394640   # all reminder output + confirmations
RECAPS_CHANNEL       = 1497215421789638719   # !recap commands and output

# CSV processing is only allowed in the processing channel
ALLOWED_CHANNELS = [
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
MAIN_GUILD_ID  = 1442010466191937660  # main server guild ID

# ==============================
# REMINDER STATE
# ==============================
# Per-guild, per-message task tracking.
# Structure: {guild_id: {message_id: {play_key: asyncio.Task}}}
# A guild_id of 0 is used for DMs / channels with no guild.
scheduled_tasks = {}   # {guild_id: {message_id: {play_key: Task}}}
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


def build_reminder_text(guild, league, p1, p2, wins, total, tier, label, play_type=""):
    """
    Build the reminder message.
    Uses real role mention if the guild and role are available,
    falls back to plain text otherwise.
    Format: @TT Official LEAGUE – P1 vs P2 [OVER/UNDER XU] EMOJI (wins/total) | LABEL
    """
    if   tier == "nuke":    emoji = " ☢️"
    elif tier == "caution": emoji = " ⚠️"
    else:                   emoji = ""

    play_str = f" {play_type}" if play_type else ""
    body = f"{league} – {p1} vs {p2}{play_str}{emoji} ({wins}/{total}) | {label}"

    if guild:
        role = discord.utils.get(guild.roles, name="TT Official")
        if role:
            return f"{role.mention} {body}"

    return f"@TT Official {body}"


def _allowed_mentions_for_guild(guild):
    """
    Return AllowedMentions that explicitly names the 'TT Official' role.
    This forces Discord to ping even when the role is NOT publicly mentionable,
    as long as the bot has the 'Mention Everyone' permission in the channel.
    Falls back to roles=True if the role can't be resolved (shouldn't happen
    in practice, but keeps the bot from crashing).
    """
    if guild:
        role = discord.utils.get(guild.roles, name="TT Official")
        if role:
            return discord.AllowedMentions(roles=[role])
    return discord.AllowedMentions(roles=True)


def parse_play_line_for_reminder(line):
    """
    Parse a slate line into reminder components.

    STRICT VALIDATION — rejects:
      - Graded plays (✅ ❌ 🧼)
      - Non-slate content (recap headers, confirmation messages, bot output)
      - Lines without valid slate format (must have vs, EST time, record)

    Handles:
      4+  format: LEAGUE – P1 vs P2 @ HH:MM AM/PM EST / HH:MM AM/PM PST (W/T) [emoji]
      tot format: LEAGUE – P1 vs P2 PLAY XU @ HH:MM AM/PM EST / ... (W/T)
      Mixed case times: 12:05pm est
      Em-dash or hyphen between league and players.

    Returns dict with keys: league, p1, p2, wins, total, tier, time_str, play_type
    or None if the line cannot be parsed or should be skipped.
    NOTE: game_dt is NOT set here — it is assigned by the batch scheduler
    which resolves the correct calendar date for the whole slate.
    """
    line = re.sub(r'\s+', ' ', line).strip()

    # ── REJECT: empty or too short ──
    if len(line) < 15:
        return None

    # ── REJECT: graded plays (any result emoji anywhere in the line) ──
    if "✅" in line or "❌" in line or "🧼" in line:
        print(f"[REMINDERS] Skipped (graded): {line[:80]}")
        return None

    # ── REJECT: non-slate content (recap, confirmation, bot-generated blocks) ──
    if any(marker in line for marker in (
        "RECAP", "REMINDERS SET", "Record:", "Units:",
        "LEAGUE BREAKDOWN", "━", "ACTIVE REMINDERS",
    )):
        print(f"[REMINDERS] Skipped (non-slate): {line[:80]}")
        return None

    # ── REJECT: lines that start with non-slate emojis / formatting ──
    if line and line[0] in "📊⏰🏓🔥🟢🟡🔻💡🧪":
        print(f"[REMINDERS] Skipped (non-slate prefix): {line[:80]}")
        return None

    # ── REQUIRE: "vs" and an EST time reference ──
    if "vs" not in line:
        return None
    if not re.search(r'est', line, re.IGNORECASE):
        return None

    # ── REQUIRE: valid league keyword ──
    ll = line.lower()
    if   "elite" in ll: league = "ELITE"
    elif "setka" in ll: league = "SETKA"
    elif "czech" in ll: league = "CZECH"
    elif "cup"   in ll: league = "CUP"
    else:
        print(f"[REMINDERS] Skipped (no league): {line[:80]}")
        return None

    # Tier
    if   "☢️" in line: tier = "nuke"
    elif "⚠️" in line: tier = "caution"
    else:              tier = "normal"

    # EST time — handles "@ 12:05 PM EST", "12:05pm est", "12:05PM EST"
    # The @ is optional to handle lines where it was omitted (e.g. "7:20 PM EST")
    time_match = re.search(r'(?:@\s*)?(\d{1,2}:\d{2})\s*([AaPp][Mm])\s*[Ee][Ss][Tt]', line)
    if not time_match:
        print(f"[REMINDERS] Skipped (no valid time): {line[:80]}")
        return None
    time_str = time_match.group(1).strip() + " " + time_match.group(2).strip().upper()

    # ── REQUIRE: record in (wins/total) format ──
    record_match = re.search(r'\((\d+)/(\d+)\)', line)
    if not record_match:
        print(f"[REMINDERS] Skipped (no record): {line[:80]}")
        return None
    wins  = int(record_match.group(1))
    total = int(record_match.group(2))

    # Player names — strip league prefix (em-dash or hyphen), emojis, then grab "P1 vs P2"
    body = re.sub(r'^[A-Z]+\s*[–\-]\s*', '', line).strip()
    body = body.replace("☢️", "").replace("⚠️", "")
    vs_match = re.search(r'^(.+?)\s+vs\s+(.+?)(?:\s+[\d\.]+U|\s+@|\s+\d{1,2}:\d{2}\s*[AaPp]|\s*\()', body, re.IGNORECASE)
    if not vs_match:
        print(f"[REMINDERS] Skipped (bad player format): {line[:80]}")
        return None
    p1 = vs_match.group(1).strip()
    p2 = vs_match.group(2).strip()

    # Extract play type (OVER/UNDER + units) if present — totals lines
    # e.g. "Marcin Marchlewski OVER" → p2="Marcin Marchlewski", play_type="OVER 1.5U"
    play_type = ""
    direction_match = re.search(r'\b(OVER|UNDER)\b', p2, re.IGNORECASE)
    if direction_match:
        direction = direction_match.group(1).upper()
        p2 = p2[:direction_match.start()].strip()
        # Grab units from the original line (e.g. "OVER 1.5U", "UNDER 1.25U")
        units_match = re.search(r'(?:OVER|UNDER)\s+([\d\.]+U)', line, re.IGNORECASE)
        if units_match:
            play_type = f"{direction} {units_match.group(1).upper()}"
        else:
            play_type = direction

    return {
        "league":    league,
        "p1":        p1,
        "p2":        p2,
        "wins":      wins,
        "total":     total,
        "tier":      tier,
        "time_str":  time_str,   # "HH:MM AM" normalised
        "play_type": play_type,  # "OVER 1.5U", "UNDER 1.25U", or "" for 4+ plays
        # game_dt is resolved later by the batch date logic
    }


def _resolve_slate_dates(raw_plays, now_est):
    """
    Given a list of (line, time_str) pairs from one message/text block,
    determine the correct calendar date for each play.

    Rules:
    - The FIRST game in the message anchors the calendar date.
      Slates are always posted in chronological order, so the first game
      is always the earliest upcoming game from the time of posting.
    - Walk through games sequentially. Whenever a game's time is more than
      30 minutes earlier than the previous game's time, it crossed midnight
      and belongs to the next calendar day.
    - If even the first game is more than 1 hour in the past, shift the
      anchor to tomorrow (handles edge case of reposting old slate).

    Returns a list of (line, time_str, game_dt) tuples.
    """
    def to_minutes(ts):
        try:
            dt = datetime.strptime(ts, "%I:%M %p")
            return dt.hour * 60 + dt.minute
        except Exception:
            return None

    if not raw_plays:
        return []

    # Anchor on the FIRST game in the message
    first_time_str = raw_plays[0][1]
    first_mins = to_minutes(first_time_str)
    if first_mins is None:
        return []

    anchor_date = now_est.date()
    try:
        naive_first = datetime.strptime(
            f"{anchor_date.year}/{anchor_date.month}/{anchor_date.day} {first_time_str}",
            "%Y/%m/%d %I:%M %p"
        )
    except ValueError:
        return []

    first_game_dt = naive_first.replace(tzinfo=EST)

    # If the first game is more than 1 hour in the past, anchor to tomorrow
    if first_game_dt < now_est - timedelta(hours=1):
        anchor_date = (now_est + timedelta(days=1)).date()
        try:
            naive_first = datetime.strptime(
                f"{anchor_date.year}/{anchor_date.month}/{anchor_date.day} {first_time_str}",
                "%Y/%m/%d %I:%M %p"
            )
        except ValueError:
            return []
        first_game_dt = naive_first.replace(tzinfo=EST)

    # Walk sequentially — roll to next day whenever time goes backwards
    current_date = anchor_date
    prev_mins    = first_mins
    results      = []

    for line, time_str in raw_plays:
        mins = to_minutes(time_str)
        if mins is None:
            continue

        # If this game's time is more than 30 min earlier than the previous,
        # it crossed midnight into the next calendar day
        if mins < prev_mins - 30:
            current_date = current_date + timedelta(days=1)

        prev_mins = mins

        try:
            naive = datetime.strptime(
                f"{current_date.year}/{current_date.month}/{current_date.day} {time_str}",
                "%Y/%m/%d %I:%M %p"
            )
        except ValueError:
            continue

        game_dt = naive.replace(tzinfo=EST)
        results.append((line, time_str, game_dt))

    return results


async def _reminder_task(guild_id, key, guild, league, p1, p2, wins, total, tier, game_dt, reminder_channel_id, play_type=""):
    """
    Single async task per play.
    Step A: sleep until STARTING SOON (game_dt - 5 min), send alert.
    Step B: recalculate delay to game_dt, send STARTING NOW alert.
    Cleans itself from active_keys when done.
    reminder_channel_id: the channel to fire alerts into (server-specific).
    Channel is fetched fresh at fire time (not at task creation) to avoid
    None returns from get_channel() before guild cache is fully populated.
    """
    try:
        # ── STARTING SOON ──
        soon_dt = game_dt - timedelta(minutes=5)
        now_est = datetime.now(EST)
        pre_delay = (soon_dt - now_est).total_seconds()

        if pre_delay > 0:
            print(f"[REMINDERS] Sleeping {pre_delay:.0f}s for SOON: {key}")
            await asyncio.sleep(pre_delay)
            # Fetch channel fresh at fire time so cache is fully populated
            ch = client.get_channel(reminder_channel_id)
            if ch is None:
                try:
                    ch = await client.fetch_channel(reminder_channel_id)
                except:
                    ch = None
            # Only send if not locked, or if this is the test server (test reminders always fire)
            if ch and (not locked or guild_id == TEST_GUILD_ID):
                # Resolve guild from the destination channel so role lookup
                # matches the server the message is actually being sent to
                dest_guild = getattr(ch, "guild", guild)
                text = build_reminder_text(dest_guild, league, p1, p2, wins, total, tier, "STARTING SOON", play_type)
                await ch.send(text, allowed_mentions=_allowed_mentions_for_guild(dest_guild))
                print(f"[REMINDERS] Sent SOON: {key} → ch={reminder_channel_id}")
            else:
                print(f"[REMINDERS] SOON suppressed: {key} ch={ch} locked={locked} guild_id={guild_id}")

        # ── STARTING NOW ──
        now_est     = datetime.now(EST)
        start_delay = (game_dt - now_est).total_seconds()

        if start_delay > 0:
            print(f"[REMINDERS] Sleeping {start_delay:.0f}s for NOW: {key}")
            await asyncio.sleep(start_delay)
            # Fetch channel fresh at fire time
            ch = client.get_channel(reminder_channel_id)
            if ch is None:
                try:
                    ch = await client.fetch_channel(reminder_channel_id)
                except:
                    ch = None
            # Only send if not locked, or if this is the test server (test reminders always fire)
            if ch and (not locked or guild_id == TEST_GUILD_ID):
                dest_guild = getattr(ch, "guild", guild)
                text = build_reminder_text(dest_guild, league, p1, p2, wins, total, tier, "STARTING NOW", play_type)
                await ch.send(text, allowed_mentions=_allowed_mentions_for_guild(dest_guild))
                print(f"[REMINDERS] Sent NOW: {key} → ch={reminder_channel_id}")
            else:
                print(f"[REMINDERS] NOW suppressed: {key} ch={ch} locked={locked} guild_id={guild_id}")

    except asyncio.CancelledError:
        pass
    finally:
        # Remove from active_keys so future messages can re-register this play
        if guild_id in active_keys:
            active_keys[guild_id].discard(key)


def _schedule_play_for_message(guild_id, message_id, guild, play, game_dt, reminder_channel_id):
    """
    Schedule reminder tasks for a single play within a specific message.
    Returns (key, was_scheduled) tuple.

    - Checks active_keys[guild_id] for cross-message deduplication.
    - Skips plays where both SOON and NOW are already in the past.
    - Stores task under scheduled_tasks[guild_id][message_id][key].
    """
    _ensure_guild_structures(guild_id)

    league    = play["league"]
    p1        = play["p1"]
    p2        = play["p2"]
    wins      = play["wins"]
    total     = play["total"]
    tier      = play["tier"]
    time_str  = play["time_str"]
    play_type = play.get("play_type", "")

    key     = make_play_key(league, p1, p2, time_str)
    now_est = datetime.now(EST)
    soon_dt = game_dt - timedelta(minutes=5)

    # Both alerts already past — nothing to schedule
    if game_dt <= now_est and soon_dt <= now_est:
        return key, False

    # Cross-message duplicate check
    if key in active_keys[guild_id]:
        return key, False

    # Create the task
    task = asyncio.ensure_future(
        _reminder_task(guild_id, key, guild, league, p1, p2, wins, total, tier, game_dt, reminder_channel_id, play_type)
    )

    # Store under this message
    if message_id not in scheduled_tasks[guild_id]:
        scheduled_tasks[guild_id][message_id] = {}

    scheduled_tasks[guild_id][message_id][key] = task
    active_keys[guild_id].add(key)

    print(f"[REMINDERS] Scheduled: {key} @ {game_dt.strftime('%m/%d %I:%M %p')} EST")
    return key, True


def _cancel_message_tasks(guild_id, message_id):
    """
    Cancel all reminder tasks for a specific message and remove their keys
    from active_keys.  Other messages are unaffected.
    """
    _ensure_guild_structures(guild_id)

    msg_tasks = scheduled_tasks[guild_id].pop(message_id, {})
    for key, task in msg_tasks.items():
        task.cancel()
        active_keys[guild_id].discard(key)
        print(f"[REMINDERS] Cancelled: {key}")


def _extract_raw_plays(text):
    """
    Pass 1: extract (line, time_str) pairs from a text block.
    Does not assign dates — that is done by _resolve_slate_dates.
    """
    raw = []
    for raw_line in text.split("\n"):
        play = parse_play_line_for_reminder(raw_line)
        if play:
            raw.append((raw_line.strip(), play["time_str"]))
    return raw


async def schedule_message_plays(message, text=None):
    """
    Main entry point: schedule reminders for all plays in a message.

    Steps:
    1. Reject entire message if it looks like a recap or bot-generated output.
    2. Clear old tasks for this specific message (handles edits/reposts).
    3. Parse all valid play lines from the message text.
    4. Resolve correct calendar dates for the whole slate as a batch.
    5. Apply strict future filter (game_dt must be > now + 2 min buffer).
    6. Schedule tasks for each future play.
    7. Return list of result dicts for confirmation message.

    If text is None, uses message.content.
    """
    if text is None:
        text = message.content

    guild_id   = _guild_id(message)
    message_id = message.id
    guild      = message.guild

    _ensure_guild_structures(guild_id)

    # Step 2: clear this message's old tasks
    _cancel_message_tasks(guild_id, message_id)

    # Step 3: parse raw plays
    raw_plays = _extract_raw_plays(text)
    if not raw_plays:
        return []

    # Step 4: resolve calendar dates as a batch
    now_est  = datetime.now(EST)
    resolved = _resolve_slate_dates(raw_plays, now_est)

    # Step 5 & 6: strict future filter + schedule
    results = []
    future_cutoff = now_est + timedelta(minutes=2)  # 2-minute buffer

    for line, time_str, game_dt in resolved:
        play = parse_play_line_for_reminder(line)
        if not play:
            continue

        # ── STRICT FUTURE FILTER ──
        # Only schedule if the game hasn't started yet (with buffer)
        if game_dt <= future_cutoff:
            print(f"[REMINDERS] Skipped (past/borderline): {play['league']} – {play['p1']} vs {play['p2']} @ {time_str} (game_dt={game_dt.strftime('%m/%d %I:%M %p')})")
            continue

        # All reminder alerts fire to #reminderschannel
        if message.channel.id == SLATE_CHANNEL:
            reminder_channel_id = REMINDERS_CHANNEL
        else:
            reminder_channel_id = MAIN_REMINDER_CHANNEL

        key, was_scheduled = _schedule_play_for_message(
            guild_id, message_id, guild, play, game_dt, reminder_channel_id
        )

        if was_scheduled:
            results.append({
                "play":     play,
                "game_dt":  game_dt,
                "key":      key,
            })

    if results:
        print(f"[REMINDERS] Scheduled {len(results)} play(s) from message {message_id}")
    else:
        print(f"[REMINDERS] No future plays found in message {message_id}")

    return results


def clear_all_reminders():
    """
    Cancel all scheduled reminder tasks across all guilds and wipe state.
    Called on startup before rescheduling to prevent stale task accumulation.
    """
    for guild_id, msg_dict in scheduled_tasks.items():
        for msg_id, key_dict in msg_dict.items():
            for key, task in key_dict.items():
                task.cancel()
    scheduled_tasks.clear()
    active_keys.clear()
    print("[REMINDERS] Cleared all stale reminder tasks.")


async def reschedule_from_channel(channel, lookback_hours=12):
    """
    On startup: scan recent messages in a channel and reschedule any plays
    whose game time is still in the future.
    Only looks back 12 hours to avoid pulling in old slates from previous days.
    Processes both bot and human messages.
    """
    cutoff = datetime.now(EST) - timedelta(hours=12)

    async for msg in channel.history(limit=200):
        msg_time = msg.created_at.astimezone(EST)
        if msg_time < cutoff:
            break
        # schedule_message_plays handles dedup and past-time skipping
        await schedule_message_plays(msg)


async def send_reminder_confirmation(results, override_channel=None):
    """
    Post a confirmation listing every play that had reminders scheduled.
    Sends to CONFIRMATION_CHANNEL by default, or override_channel if provided
    (used when posting from the test channel).
    Shows matchup + tier emoji only — clean and concise.
    """
    if not results:
        return

    # All confirmations go to #reminderschannel
    ch = client.get_channel(REMINDERS_CHANNEL)
    if not ch:
        return

    tier_tag = {"nuke": " ☢️", "caution": " ⚠️", "normal": ""}

    lines = ["⏰ **REMINDERS SET** ━━━━━━━━━━━━━━━━━━"]
    for r in results:
        play = r["play"]
        tag  = tier_tag.get(play["tier"], "")
        lines.append(f"{play['league']} – {play['p1']} vs {play['p2']}{tag}")

    lines.append(f"\n**{len(results)} play(s) queued.**")
    await ch.send("\n".join(lines))


# ==============================
# RECAP PARSERS
# ==============================

async def parse_four_plus(channel, start, end, limit=None, verify=False):

    wins=losses=washes=0
    normal_w=normal_l=0
    nuke_w=nuke_l=0
    caution_w=caution_l=0

    league_stats={}

    seen=set()

    detected_plays=[]
    ignored_lines=[]
    duplicate_lines=[]

    async for msg in channel.history(limit=limit):

        msg_time=msg.created_at.astimezone(EST)

        if start and not(start<=msg_time<end):
            continue

        for raw_line in msg.content.split("\n"):

            line = re.sub(r'\s+', ' ', raw_line).strip()
            line = line.replace(")❌", ") ❌").replace(")✅", ") ✅")

            if not line:
                continue

            if "vs" not in line and " v " not in line:
                continue

            if "U @" in line or "U@" in line:
                continue

            if line in seen:
                if verify:
                    duplicate_lines.append(line)
                continue

            seen.add(line)

            has_result = "✅" in line or "❌" in line or "🧼" in line

            if not has_result:
                if verify:
                    ignored_lines.append(line)
                continue

            line_lower=line.lower()

            if "elite" in line_lower:
                league="ELITE"
            elif "setka" in line_lower:
                league="SETKA"
            elif "czech" in line_lower:
                league="CZECH"
            elif "cup" in line_lower:
                league="CUP"
            else:
                league="OTHER"

            if league not in league_stats:
                league_stats[league]={"w":0,"l":0,"u":0}

            is_nuke="☢️" in line
            is_caution="⚠️" in line

            # Extract clean player names for verify mode
            if verify:
                vs_match = re.search(r'([A-Za-z\u00C0-\u024F\'\-]+)\s+vs\s+([A-Za-z\u00C0-\u024F\'\-]+)', line, re.IGNORECASE)
                if not vs_match:
                    vs_match = re.search(r'([A-Za-z\u00C0-\u024F\'\-]+)\s+v\s+([A-Za-z\u00C0-\u024F\'\-]+)', line, re.IGNORECASE)
                p1_clean = vs_match.group(1).strip() if vs_match else "?"
                p2_clean = vs_match.group(2).strip() if vs_match else "?"
            else:
                p1_clean = ""
                p2_clean = ""

            if "🧼" in line:
                washes+=1
                if verify:
                    detected_plays.append((league, p1_clean, p2_clean, "WASH", False, False))
                continue

            if "✅" in line:

                wins+=1
                league_stats[league]["w"]+=1

                if is_nuke:
                    nuke_w+=1
                    league_stats[league]["u"]+=2.2
                elif is_caution:
                    caution_w+=1
                    league_stats[league]["u"]+=0.55
                else:
                    normal_w+=1
                    league_stats[league]["u"]+=1.1

                if verify:
                    detected_plays.append((league, p1_clean, p2_clean, "WIN", is_nuke, is_caution))

            elif "❌" in line:

                losses+=1
                league_stats[league]["l"]+=1

                if is_nuke:
                    nuke_l+=1
                    league_stats[league]["u"]-=6
                elif is_caution:
                    caution_l+=1
                    league_stats[league]["u"]-=1.5
                else:
                    normal_l+=1
                    league_stats[league]["u"]-=3

                if verify:
                    detected_plays.append((league, p1_clean, p2_clean, "LOSS", is_nuke, is_caution))

    if verify:
        return wins,losses,washes,normal_w,normal_l,caution_w,caution_l,nuke_w,nuke_l,league_stats,detected_plays,ignored_lines,duplicate_lines

    return wins,losses,washes,normal_w,normal_l,caution_w,caution_l,nuke_w,nuke_l,league_stats


async def parse_totals(channel, start, end, limit=None):

    wins=losses=0
    units=0

    seen=set()

    async for msg in channel.history(limit=limit):

        msg_time=msg.created_at.astimezone(EST)

        if start and not(start<=msg_time<end):
            continue

        for raw_line in msg.content.split("\n"):

            line = re.sub(r'\s+', ' ', raw_line).strip()
            line = line.replace(")❌", ") ❌").replace(")✅", ") ✅")

            if not line:
                continue

            if "vs" not in line and " v " not in line:
                continue

            if line in seen:
                continue

            seen.add(line)

            has_result = "✅" in line or "❌" in line or "🪝" in line

            if not has_result:
                continue

            unit_match=re.search(r'(\d+(\.\d+)?)U',line,re.IGNORECASE)

            if not unit_match:
                continue

            stake=float(unit_match.group(1))

            if "✅" in line:
                wins+=1
                units+=stake/1.2

            elif "❌" in line or "🪝" in line:
                losses+=1
                units-=stake

    return wins,losses,units


# ==============================
# STARTUP
# ==============================

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")

    # Wipe any stale in-memory tasks from previous session before rescheduling
    clear_all_reminders()

    # Reschedule reminders still in the future from the slate channel
    slate_ch = client.get_channel(SLATE_CHANNEL)
    if slate_ch:
        await reschedule_from_channel(slate_ch)
        print("[REMINDERS] Rescheduled from slate channel.")

    # Also reschedule from main server channels if accessible
    four_ch = client.get_channel(FOUR_PLUS_CHANNEL)
    if four_ch:
        await reschedule_from_channel(four_ch)
        print("[REMINDERS] Rescheduled from main 4+ channel.")


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
    if not message.author.bot and message.channel.id == SLATE_CHANNEL:
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
        if message.channel.id != RECAPS_CHANNEL:
            return  # recap commands only work in #recaps

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

        # Recap always scans #slatechannel for graded plays
        four_channel = client.get_channel(SLATE_CHANNEL)
        totals_channel = client.get_channel(SLATE_CHANNEL)

        # Safety fallback
        if four_channel is None:
            four_channel = message.channel
        if totals_channel is None:
            totals_channel = message.channel

        fw,fl,fwash,nw,nl,cw,cl,kw,kl,league_stats=await parse_four_plus(four_channel,start,end,limit)
        tw,tl,tunits=await parse_totals(totals_channel,start,end,limit)

        four_units=( (nw*1.1)-(nl*3) + (cw*0.55)-(cl*1.5) + (kw*2.2)-(kl*6) )
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

        guild_tasks = scheduled_tasks.get(guild_id, {})

        # Flatten: collect all (key, task) pairs across all messages
        all_plays = {}
        for msg_id, msg_tasks in guild_tasks.items():
            for key, task in msg_tasks.items():
                if not task.done() and key not in all_plays:
                    all_plays[key] = task

        if not all_plays:
            await message.channel.send("⏰ No reminders currently scheduled.")
            return

        now_est = datetime.now(EST)
        sorted_keys = sorted(all_plays.keys())
        lines   = [f"⏰ **ACTIVE REMINDERS** ({len(sorted_keys)} play(s)) ━━━━━━━━━━━━━━━━━━"]

        for idx, key in enumerate(sorted_keys, start=1):
            # key format: "LEAGUE|P1|P2|HH:MM AM"
            parts = key.split("|")
            if len(parts) < 4:
                continue
            league_k, p1_k, p2_k, time_k = parts[0], parts[1], parts[2], parts[3]

            # Build game_dt from the time string + today/tomorrow logic
            try:
                naive = datetime.strptime(
                    f"{now_est.year}/{now_est.month}/{now_est.day} {time_k}",
                    "%Y/%m/%d %I:%M %p"
                )
                game_dt_k = naive.replace(tzinfo=EST)
                if game_dt_k < now_est:
                    game_dt_k += timedelta(days=1)
            except ValueError:
                continue

            delta   = game_dt_k - now_est
            hours   = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)

            if hours > 0:
                countdown = f"in {hours}h {minutes}m"
            else:
                countdown = f"in {minutes}m"

            lines.append(f"**{idx}.** {league_k} – {p1_k.title()} vs {p2_k.title()} @ {time_k} EST ({countdown})")

        lines.append(f"\n_Use `!reminderremove 1,2,3` to cancel specific reminders._")
        await send_long_message(message.channel, "\n".join(lines))
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

        guild_tasks = scheduled_tasks.get(guild_id, {})
        all_plays = {}
        for msg_id, msg_tasks in guild_tasks.items():
            for key, task in msg_tasks.items():
                if not task.done() and key not in all_plays:
                    all_plays[key] = task

        if not all_plays:
            await message.channel.send("⏰ No reminders currently scheduled.")
            return

        sorted_keys = sorted(all_plays.keys())
        max_idx = len(sorted_keys)

        bad = [i for i in indexes if i < 1 or i > max_idx]
        if bad:
            await message.channel.send(f"Invalid index(es): {', '.join(str(b) for b in bad)}. Valid range: 1–{max_idx}")
            return

        removed = []
        for idx in sorted(set(indexes)):
            key = sorted_keys[idx - 1]
            parts = key.split("|")
            league_k = parts[0] if len(parts) > 0 else "?"
            p1_k     = parts[1].title() if len(parts) > 1 else "?"
            p2_k     = parts[2].title() if len(parts) > 2 else "?"
            time_k   = parts[3] if len(parts) > 3 else "?"

            for msg_id, msg_tasks in list(guild_tasks.items()):
                if key in msg_tasks:
                    msg_tasks[key].cancel()
                    del msg_tasks[key]
                    active_keys[guild_id].discard(key)
                    print(f"[REMINDERS] Removed by user: {key}")
                    break

            removed.append(f"{league_k} – {p1_k} vs {p2_k} @ {time_k} EST")

        lines = ["🗑️ **REMINDERS REMOVED** ━━━━━━━━━━━━━━━━━━"]
        for r in removed:
            lines.append(r)
        lines.append(f"\n**{len(removed)} reminder(s) cancelled.**")
        await message.channel.send("\n".join(lines))
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
