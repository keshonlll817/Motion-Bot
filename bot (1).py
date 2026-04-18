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

ALLOWED_CHANNELS = [
1471792196582637728,
1474078126630768822,
1479241150996152340,
1488259145093222522,
1452410545016930335
]

FOUR_PLUS_CHANNEL    = 1443356395935240302
TOTALS_CHANNEL       = 1446203029916356649
TEST_CHANNEL         = 1471792196582637728
REMINDER_CHANNEL     = 1442258139985608867   # #tabletennis-chat (main server)
TEST_REMINDER_CHANNEL     = 1471792196582637728   # test server STARTING SOON/NOW alerts (same as TEST_CHANNEL)
TEST_CONFIRMATION_CHANNEL = 1488259145093222522  # test server reminders confirmation channel
CONFIRMATION_CHANNEL = 1452410545016930335   # reminders confirmation channel

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

        # Determine which reminder channel to fire into based on guild ID
        if guild and guild.id == TEST_GUILD_ID:
            reminder_channel_id = TEST_REMINDER_CHANNEL
        else:
            reminder_channel_id = REMINDER_CHANNEL

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

    # Route confirmation by guild: test server → test confirmation channel
    #                               main server → main confirmation channel
    if override_channel:
        guild_check = getattr(override_channel, 'guild', None)
        if guild_check and guild_check.id == TEST_GUILD_ID:
            ch = client.get_channel(TEST_CONFIRMATION_CHANNEL)
        else:
            ch = client.get_channel(CONFIRMATION_CHANNEL)
    else:
        ch = client.get_channel(CONFIRMATION_CHANNEL)
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

    # Reschedule any reminders still in the future from today's slate
    four_ch   = client.get_channel(FOUR_PLUS_CHANNEL)
    totals_ch = client.get_channel(TOTALS_CHANNEL)
    test_ch   = client.get_channel(TEST_CHANNEL)

    if four_ch:
        await reschedule_from_channel(four_ch)
        print("[REMINDERS] Rescheduled from 4+ channel.")

    if totals_ch:
        await reschedule_from_channel(totals_ch)
        print("[REMINDERS] Rescheduled from totals channel.")

    if test_ch:
        await reschedule_from_channel(test_ch)
        print("[REMINDERS] Rescheduled from test channel.")


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

    if after.channel.id not in (FOUR_PLUS_CHANNEL, TOTALS_CHANNEL, TEST_CHANNEL):
        return

    results = await schedule_message_plays(after)

    if results:
        conf_ch = after.channel if after.channel.id == TEST_CHANNEL else None
        await send_reminder_confirmation(results, override_channel=conf_ch)


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

    if message.channel.id not in (FOUR_PLUS_CHANNEL, TOTALS_CHANNEL, TEST_CHANNEL):
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

    # ── Schedule reminders for HUMAN posts in reminder-watched channels ──
    # Only human-posted slates trigger reminders.
    # Bot-posted CSV conversion output does NOT trigger reminders.
    if (not message.author.bot and
            message.channel.id in (FOUR_PLUS_CHANNEL, TOTALS_CHANNEL, TEST_CHANNEL)):
        results = await schedule_message_plays(message)
        if results:
            conf_ch = message.channel if message.channel.id == TEST_CHANNEL else None
            await send_reminder_confirmation(results, override_channel=conf_ch)

    if message.author.bot:
        return

    content = message.content.lower().strip()

    # ── "Bang!" response with 1-minute per-channel cooldown ──
    if content == "bang":
        now = datetime.now(EST)
        ch_id = message.channel.id
        last = bang_last_fired.get(ch_id)
        if last is None or (now - last).total_seconds() >= 60:
            bang_last_fired[ch_id] = now
            await message.channel.send("Bang!")
        return

# ==============================
# RECAP COMMANDS
# ==============================

    if content.startswith("!recap"):

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

        elif "verify" in content:

            if message.channel.id==TEST_CHANNEL:
                four_channel=message.channel
            else:
                four_channel=client.get_channel(FOUR_PLUS_CHANNEL)

            result=await parse_four_plus(four_channel,None,None,limit=50,verify=True)
            fw,fl,fwash,nw,nl,cw,cl,kw,kl,league_stats,detected_plays,ignored_lines,duplicate_lines=result

            now_v=datetime.now(EST)
            four_units_v=( (nw*1.1)-(nl*3) + (cw*0.55)-(cl*1.5) + (kw*2.2)-(kl*6) )

            # -- SECTION 1: HEADER --
            verify_out=f"🔍 **RECAP VERIFY — {now_v.strftime('%b')} {now_v.day} (EST)** ━━━━━━━━━━━━━━━━━━\n\n"

            # -- SECTION 2: SUMMARY --
            total_counted=fw+fl+fwash
            total_parsed=total_counted+len(ignored_lines)
            verify_out+=f"📊 **SUMMARY**\n"
            verify_out+=f"Total Parsed: {total_parsed}  |  Counted: {total_counted}  |  Ignored: {len(ignored_lines)}  |  Duplicates Skipped: {len(duplicate_lines)}\n\n"

            # -- SECTION 3: COUNTED PLAYS --
            verify_out+=f"✅ **COUNTED PLAYS**\n"

            display_plays=detected_plays[:40]

            for i,(lg,p1_c,p2_c,outcome,is_nuke,is_caution) in enumerate(display_plays,1):
                tag=""
                if outcome!="WASH":
                    if is_nuke: tag=" ☢️"
                    elif is_caution: tag=" ⚠️"
                verify_out+=f"{i}. {lg} — {p1_c} vs {p2_c} → **{outcome}**{tag}\n"

            if len(detected_plays)>40:
                verify_out+=f"_(... {len(detected_plays)-40} more plays not shown)_\n"

            # -- SECTION 4: IGNORED --
            verify_out+=f"\n❌ **IGNORED (Missing Results)**\n"

            if ignored_lines:
                for ln in ignored_lines:
                    vs_m=re.search(r'([A-Za-z\u00C0-\u024F\'\-]+)\s+vs\s+([A-Za-z\u00C0-\u024F\'\-]+)',ln,re.IGNORECASE)
                    if not vs_m:
                        vs_m=re.search(r'([A-Za-z\u00C0-\u024F\'\-]+)\s+v\s+([A-Za-z\u00C0-\u024F\'\-]+)',ln,re.IGNORECASE)
                    if vs_m:
                        verify_out+=f"• {vs_m.group(1).strip()} vs {vs_m.group(2).strip()}\n"
                    else:
                        verify_out+=f"• {ln}\n"
            else:
                verify_out+="None — all lines had results.\n"

            # -- SECTION 5: DUPLICATES (only if any) --
            if duplicate_lines:
                verify_out+=f"\n⚠️ **DUPLICATES SKIPPED**\n"
                for ln in duplicate_lines:
                    vs_m=re.search(r'([A-Za-z\u00C0-\u024F\'\-]+)\s+vs\s+([A-Za-z\u00C0-\u024F\'\-]+)',ln,re.IGNORECASE)
                    if not vs_m:
                        vs_m=re.search(r'([A-Za-z\u00C0-\u024F\'\-]+)\s+v\s+([A-Za-z\u00C0-\u024F\'\-]+)',ln,re.IGNORECASE)
                    if vs_m:
                        verify_out+=f"• {vs_m.group(1).strip()} vs {vs_m.group(2).strip()}\n"
                    else:
                        verify_out+=f"• {ln}\n"

            # -- SECTION 6: VERIFIED RESULT --
            verify_out+=f"\n━━━━━━━━━━━━━━━━━━ 📊 **VERIFIED RESULT**\n"
            verify_out+=f"Record: {fw}-{fl}"
            if fwash>0:
                verify_out+=f" ({fwash} Wash)"
            verify_out+=f"\nNormal {nw}-{nl}  ⚠️ {cw}-{cl}  ☢️ {kw}-{kl}"
            verify_out+=f"\nUnits: {four_units_v:+.2f}U\n"

            await send_long_message(message.channel, verify_out)
            return

        else:
            return

        if message.channel.id==TEST_CHANNEL:
            four_channel=message.channel
            totals_channel=message.channel
        else:
            four_channel=client.get_channel(FOUR_PLUS_CHANNEL)
            totals_channel=client.get_channel(TOTALS_CHANNEL)

        fw,fl,fwash,nw,nl,cw,cl,kw,kl,league_stats=await parse_four_plus(four_channel,start,end,limit)
        tw,tl,tunits=await parse_totals(totals_channel,start,end,limit)

        four_units=( (nw*1.1)-(nl*3) + (cw*0.55)-(cl*1.5) + (kw*2.2)-(kl*6) )

        recap=f"📊 **{title}**\n\n"

        recap+="🏓 **4+ PLAYS**\n"

        if fw+fl+fwash==0:
            recap+="No plays graded.\n\n"
        else:
            recap+=f"Record: {fw}-{fl}"

            if fwash>0:
                recap+=f" ({fwash} Wash)"

            recap+=f"\nUnits: {four_units:+.2f}U\n\n"

            recap+=f"Normal {nw}-{nl}\n"
            recap+=f"⚠️ {cw}-{cl}\n"
            recap+=f"☢️ {kw}-{kl}\n\n"

        recap+="🏓 **TOTAL PLAYS**\n"

        if tw+tl==0:
            recap+="No plays graded."
        else:
            recap+=f"Record: {tw}-{tl}\n"
            recap+=f"Units: {tunits:+.2f}U"

        await message.channel.send(recap)

        sorted_leagues = sorted(league_stats.items(), key=lambda x: x[1]["u"], reverse=True)

        league_msg="🏓 **LEAGUE BREAKDOWN**\n━━━━━━━━━━━━━━━━━━\n\n"

        for i,(lg,data) in enumerate(sorted_leagues):

            if i==0:
                icon="🔥"
            elif i==1:
                icon="🟢"
            elif i==2:
                icon="🟡"
            else:
                icon="🔻"

            league_msg+=f"{icon} {lg}\nRecord: {data['w']}-{data['l']}\nUnits: {data['u']:+.2f}U\n\n"

        await message.channel.send(league_msg)

        return


# ==============================
# BASIC COMMANDS
# ==============================

    if message.channel.id not in ALLOWED_CHANNELS:
        return

    if content=="ping":
        await message.channel.send("pong")
        return

    if content=="!testreminder":
        now_est  = datetime.now(EST)
        fire_dt  = now_est + timedelta(minutes=2)
        soon_dt  = now_est + timedelta(minutes=1)
        guild    = message.guild
        guild_id = _guild_id(message)

        if guild and guild.id == TEST_GUILD_ID:
            rem_ch_id = TEST_REMINDER_CHANNEL
        else:
            rem_ch_id = REMINDER_CHANNEL

        async def _test_task():
            ch = client.get_channel(rem_ch_id)
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
        rem_ch = client.get_channel(rem_ch_id)
        rem_ch_mention = rem_ch.mention if rem_ch else f"<#{rem_ch_id}>"
        await message.channel.send(
            f"✅ Test reminder scheduled!\n"
            f"**STARTING SOON** → {soon_dt.strftime("%I:%M %p")} EST\n"
            f"**STARTING NOW** → {fire_dt.strftime("%I:%M %p")} EST\n"
            f"Watch {rem_ch_mention} for the alerts."
        )
        return

    if content.startswith("!lock"):
        parts = content.split()
        if len(parts) < 2 or parts[1] not in ("true", "false"):
            await message.channel.send("Usage: `!lock true` or `!lock false`")
            return
        locked = parts[1] == "true"
        status = "🔒 **LOCKED** — bot is now silent on all servers except the test server." if locked else "🔓 **UNLOCKED** — bot is now fully active on all servers."
        await message.channel.send(status)
        return

    if content=="!reminders":
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

    if content=="!help" or content=="!commands":
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
            "🔍 **VERIFY**\n"
            "`!recap verify` — Full audit of last 50 plays in the 4+ channel.\n"
            "Shows counted plays, ignored lines, duplicates, and verified result.\n"
            "\n"
            "🎮 **OTHER**\n"
            "`ping` — Check if bot is online (responds with `pong`)\n"
            "`!reminders` — Show all currently active/pending reminders with countdown\n"
            "`!reminderremove 1,2,5` — Cancel specific reminders by index number\n"
            "`!lock true` — Silence bot everywhere except test server\n"
            "`!lock false` — Re-enable bot on all servers\n"
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

    attachment = message.attachments[0]

    if not attachment.filename.endswith(".csv"):
        return

    file_bytes = await attachment.read()
    decoded = file_bytes.decode("utf-8")

    reader = csv.DictReader(io.StringIO(decoded))

    four_plus = {}
    totals = {}

    for row in reader:
        league = convert_league(row["League"])
        p1 = row["Player 1"]
        p2 = row["Player 2"]
        play = row["Play"]
        history = row["History"]
        est_time = row["Time (Eastern)"]

        est, pst = parse_time(est_time)

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


client.run(TOKEN)
