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
    1494963563096313906  # test-motion-plays
]

SLATE_CHANNEL       = 1494963563096313906  # test-motion-plays
REMINDER_CHANNEL    = 1494963600979394640  # test-motion-reminders
CONFIRMATION_CHANNEL = 1494963621514444870  # test-motion-confirmation

EST = ZoneInfo("America/New_York")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

last_slate_messages = []

# ==============================
# REMINDER STATE
# ==============================
scheduled_tasks = {}   # {guild_id: {message_id: {play_key: asyncio.Task}}}
active_keys     = {}   # {guild_id: set(play_key)} — global dedup per guild


# ==============================
# UTIL FUNCTIONS
# ==============================

def format_units(u):
    """Format units as string (e.g., 0.5u, 1u, 2u)"""
    if u == int(u):
        return f"{int(u)}u"
    return f"{u}u"


def parse_time(est_time_str):
    """
    Parse time string and return EST and PST formatted times.
    Input: "HH:MM AM/PM" or "HH:MM AM/PM EST"
    Returns: (est_str, pst_str) both formatted as "HH:MM AM/PM"
    """
    est_time_str = est_time_str.strip().upper().replace(" EST", "").replace("EST", "")
    
    try:
        dt = datetime.strptime(est_time_str, "%I:%M %p")
        est = dt.strftime("%I:%M %p")
        
        # PST is 3 hours behind EST
        pst_dt = dt.replace(hour=(dt.hour - 3) % 24)
        pst = pst_dt.strftime("%I:%M %p")
        
        return est, pst
    except ValueError:
        return est_time_str, est_time_str


def _guild_id(message_or_channel):
    """Return the guild id for a message or channel, or 0 if DM."""
    g = getattr(message_or_channel, "guild", None)
    return g.id if g else 0


def make_play_key(sport, league, match, bet, time_str, confidence):
    """
    Unique key for a play to prevent duplicate reminders.
    Format: "SPORT|LEAGUE|MATCH|BET|HH:MM AM|CONFIDENCE"
    """
    return f"{sport}|{league}|{match}|{bet}|{time_str}|{confidence}".lower()


def _ensure_guild_structures(guild_id):
    """Create per-guild dicts if they don't exist yet."""
    if guild_id not in scheduled_tasks:
        scheduled_tasks[guild_id] = {}
    if guild_id not in active_keys:
        active_keys[guild_id] = set()


def build_reminder_text(guild, sport, league, match, bet, confidence, label, time_str):
    """
    Build the reminder message.
    Format: @TT Official SPORT | LEAGUE | MATCH | BET | TIME | CONFIDENCE [EMOJI]
    """
    if confidence.lower() == "nuke":
        emoji = " ☢️"
    elif confidence.lower() == "cautious":
        emoji = " ⚠️"
    else:
        emoji = ""

    body = f"{sport} | {league} | {match} | {bet} | {time_str} | {confidence}{emoji}"

    if guild:
        role = discord.utils.get(guild.roles, name="TT Official")
        if role:
            return f"{role.mention} {body}"

    return f"@TT Official {body}"


def _allowed_mentions_for_guild(guild):
    """
    Return AllowedMentions that explicitly names the 'TT Official' role.
    """
    if guild:
        role = discord.utils.get(guild.roles, name="TT Official")
        if role:
            return discord.AllowedMentions(roles=[role])
    return discord.AllowedMentions(roles=True)


async def send_long_message(channel, text):
    """Send text, splitting by 2000 char Discord limit."""
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

async def schedule_reminder_for_play(guild_id, message, sport, league, match, bet, time_str, confidence):
    """
    Calculate time until match and schedule a reminder.
    Assumes time_str is in format "HH:MM AM/PM EST" and uses current date.
    """
    _ensure_guild_structures(guild_id)
    
    # Parse the time
    try:
        now = datetime.now(EST)
        time_obj = datetime.strptime(time_str, "%I:%M %p").replace(tzinfo=EST)
        
        # If time is in the past today, schedule for tomorrow
        if time_obj.time() <= now.time():
            time_obj = time_obj.replace(year=now.year, month=now.month, day=now.day) + timedelta(days=1)
        else:
            time_obj = time_obj.replace(year=now.year, month=now.month, day=now.day)
        
        seconds_until = (time_obj - now).total_seconds()
        
        if seconds_until <= 0:
            return
        
        play_key = make_play_key(sport, league, match, bet, time_str, confidence)
        
        # Don't schedule duplicate
        if play_key in active_keys[guild_id]:
            return
        
        active_keys[guild_id].add(play_key)
        
        async def reminder_task():
            await asyncio.sleep(seconds_until)
            
            # Send reminder
            try:
                reminder_text = build_reminder_text(
                    message.guild, sport, league, match, bet, confidence, 
                    "MATCH STARTING SOON", time_str
                )
                await message.channel.send(
                    reminder_text,
                    allowed_mentions=_allowed_mentions_for_guild(message.guild)
                )
            except Exception as e:
                print(f"[REMINDER] Error sending reminder: {e}")
            finally:
                # Clean up
                if guild_id in scheduled_tasks:
                    for msg_id in list(scheduled_tasks[guild_id].keys()):
                        if play_key in scheduled_tasks[guild_id][msg_id]:
                            del scheduled_tasks[guild_id][msg_id][play_key]
                if guild_id in active_keys:
                    active_keys[guild_id].discard(play_key)
        
        task = asyncio.create_task(reminder_task())
        
        if message.id not in scheduled_tasks[guild_id]:
            scheduled_tasks[guild_id][message.id] = {}
        
        scheduled_tasks[guild_id][message.id][play_key] = task
        print(f"[REMINDER] Scheduled: {play_key}")
    
    except Exception as e:
        print(f"[REMINDER] Error scheduling reminder: {e}")


# ==============================
# RECAP PARSERS
# ==============================

async def parse_plays_with_results(channel, start, end, limit=None):
    """
    Parse all graded plays (✅ ❌ 🧼) from a channel.
    Handles multiple sports and confidence levels.
    Returns breakdown by confidence level and sport/league.
    """
    wins = losses = washes = 0
    cautious_w = cautious_l = 0
    normal_w = normal_l = 0
    nuke_w = nuke_l = 0
    
    sport_league_stats = {}  # {sport: {league: {"w": X, "l": Y, "u": Z}}}
    
    seen = set()
    detected_plays = []

    async for msg in channel.history(limit=limit):
        msg_time = msg.created_at.astimezone(EST)

        if start and not(start <= msg_time < end):
            continue

        for raw_line in msg.content.split("\n"):
            line = re.sub(r'\s+', ' ', raw_line).strip()
            line = line.replace(")❌", ") ❌").replace(")✅", ") ✅")

            if not line:
                continue

            # Skip lines without vs
            if "vs" not in line and " v " not in line:
                continue

            # Skip duplicates
            if line in seen:
                continue
            seen.add(line)

            # Must have a result emoji
            has_result = "✅" in line or "❌" in line or "🧼" in line
            if not has_result:
                continue

            # Extract confidence level
            is_nuke = "☢️" in line
            is_cautious = "⚠️" in line

            # Extract units from line (e.g., "0.5u", "1u", "2u")
            unit_match = re.search(r'(\d+(?:\.\d+)?)u', line, re.IGNORECASE)
            units = float(unit_match.group(1)) if unit_match else 1.0

            # Extract sport and league from line
            # Format: SPORT | LEAGUE | ... | UNITS | RESULT
            pipe_parts = line.split("|")
            sport = pipe_parts[0].strip() if len(pipe_parts) > 0 else "UNKNOWN"
            league = pipe_parts[1].strip() if len(pipe_parts) > 1 else "UNKNOWN"

            if sport not in sport_league_stats:
                sport_league_stats[sport] = {}
            if league not in sport_league_stats[sport]:
                sport_league_stats[sport][league] = {"w": 0, "l": 0, "u": 0.0}

            # Extract player names for display
            vs_match = re.search(r'([A-Za-z\u00C0-\u024F\'\-\s]+)\s+vs\s+([A-Za-z\u00C0-\u024F\'\-\s]+)', line, re.IGNORECASE)
            if not vs_match:
                vs_match = re.search(r'([A-Za-z\u00C0-\u024F\'\-\s]+)\s+v\s+([A-Za-z\u00C0-\u024F\'\-\s]+)', line, re.IGNORECASE)
            p1_clean = vs_match.group(1).strip() if vs_match else "?"
            p2_clean = vs_match.group(2).strip() if vs_match else "?"

            # Process wash
            if "🧼" in line:
                washes += 1
                detected_plays.append((sport, league, p1_clean, p2_clean, "WASH", False, False, units))
                continue

            # Process win
            if "✅" in line:
                wins += 1
                sport_league_stats[sport][league]["w"] += 1

                if is_nuke:
                    nuke_w += 1
                    sport_league_stats[sport][league]["u"] += units
                elif is_cautious:
                    cautious_w += 1
                    sport_league_stats[sport][league]["u"] += units
                else:
                    normal_w += 1
                    sport_league_stats[sport][league]["u"] += units

                detected_plays.append((sport, league, p1_clean, p2_clean, "WIN", is_nuke, is_cautious, units))

            # Process loss
            elif "❌" in line:
                losses += 1
                sport_league_stats[sport][league]["l"] += 1

                if is_nuke:
                    nuke_l += 1
                    sport_league_stats[sport][league]["u"] -= (units * 2)
                elif is_cautious:
                    cautious_l += 1
                    sport_league_stats[sport][league]["u"] -= units
                else:
                    normal_l += 1
                    sport_league_stats[sport][league]["u"] -= (units * 3)

                detected_plays.append((sport, league, p1_clean, p2_clean, "LOSS", is_nuke, is_cautious, units))

    return wins, losses, washes, normal_w, normal_l, cautious_w, cautious_l, nuke_w, nuke_l, sport_league_stats, detected_plays


# ==============================
# DISCORD EVENT HANDLERS
# ==============================

@client.event
async def on_ready():
    print(f"✅ Motion Bot logged in as {client.user}")


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    content = message.content.strip()

    # ── PING ──
    if content.lower() == "ping":
        await message.channel.send("🏓 pong")
        return

    # ── HELP ──
    if content == "!help" or content == "!commands":
        help_msg = (
            "🏓 **MOTION BOT COMMANDS** 🏓\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "📂 **SLATE**\n"
            "Upload a `.csv` file to post the day's slate.\n"
            "CSV format: Sport | League | Match | Bet | Time | Confidence | Units\n"
            "\n"
            "⏰ **REMINDERS**\n"
            "`!reminders` — Show all currently active/pending reminders with countdown\n"
            "`!reminderremove 1,2,5` — Cancel specific reminders by index number\n"
            "\n"
            "📊 **RECAP COMMANDS**\n"
            "`!recap today` — Recap from midnight to now\n"
            "`!recap yesterday` — Full recap for yesterday\n"
            "`!recap weekly` — This week\n"
            "`!recap monthly` — This month so far\n"
            "`!recap lifetime` — All-time recap\n"
            "\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "💡 **CONFIDENCE LEVELS**\n"
            "Cautious (0.5u) — Lower confidence play ⚠️\n"
            "Normal (1u) — Standard play\n"
            "Nuke (2u) — Highest confidence play ☢️\n"
            "\n"
            "**GRADING PLAYS:**\n"
            "Add ✅ for wins, ❌ for losses, 🧼 for washes\n"
        )
        await message.channel.send(help_msg)
        return

    # ── REMINDERS ──
    if content == "!reminders":
        guild_id = _guild_id(message)
        _ensure_guild_structures(guild_id)

        guild_tasks = scheduled_tasks.get(guild_id, {})
        all_plays = {}
        for msg_id, msg_tasks in guild_tasks.items():
            for key, task in msg_tasks.items():
                if not task.done() and key not in all_plays:
                    all_plays[key] = task

        if not all_plays:
            await message.channel.send("⏰ No reminders currently scheduled.")
            return

        lines = ["⏰ **ACTIVE REMINDERS** ━━━━━━━━━━━━━━━━━━"]
        sorted_keys = sorted(all_plays.keys())
        for i, key in enumerate(sorted_keys, 1):
            lines.append(f"{i}. {key}")
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
            await message.channel.send("⏰ No remengers currently scheduled.")
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
            removed.append(key)
            
            for msg_id, msg_tasks in list(guild_tasks.items()):
                if key in msg_tasks:
                    msg_tasks[key].cancel()
                    del msg_tasks[key]
                    active_keys[guild_id].discard(key)
                    print(f"[REMINDERS] Removed by user: {key}")
                    break

        lines = ["🗑️ **REMINDERS REMOVED** ━━━━━━━━━━━━━━━━━━"]
        for r in removed:
            lines.append(r)
        lines.append(f"\n**{len(removed)} reminder(s) cancelled.**")
        await message.channel.send("\n".join(lines))
        return

    # ── RECAP ──
    if content.startswith("!recap"):
        now = datetime.now(EST)

        if "test" in content:
            start = None
            end = None
            limit = 50
            title = f"TEST RECAP — {now.strftime('%b')} {now.day} (EST)"

        elif "today" in content:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = now
            title = f"TODAY RECAP — {now.strftime('%b')} {now.day} (EST)"
            limit = None

        elif "lifetime" in content:
            start = None
            end = None
            title = "LIFETIME RECAP"
            limit = None

        elif "yesterday" in content:
            start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
            title = f"DAILY RECAP — {start.strftime('%b')} {start.day} (EST)"
            limit = None

        elif "last week" in content or "lastweek" in content.replace(" ", ""):
            days_since_monday = now.weekday()
            this_monday = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
            start = this_monday - timedelta(days=7)
            end = this_monday
            title = f"LAST WEEK RECAP — {start.strftime('%b %-d')} → {end.strftime('%b %-d')} (EST)"
            limit = None

        elif "weekly" in content:
            days_since_monday = now.weekday()
            start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
            end = start + timedelta(days=7)
            title = f"WEEKLY RECAP — {start.strftime('%b %-d')} → {end.strftime('%b %-d')} (EST)"
            limit = None

        elif "monthly" in content:
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end = now
            title = f"MONTHLY RECAP — {now.strftime('%b %Y')}"
            limit = None

        else:
            return

        # Get channel
        if message.channel.id == SLATE_CHANNEL:
            recap_channel = message.channel
        else:
            recap_channel = client.get_channel(SLATE_CHANNEL)

        if not recap_channel:
            await message.channel.send("❌ Could not find recap channel.")
            return

        # Parse plays
        w, l, wsh, nw, nl, cw, cl, kw, kl, sport_league_stats, detected_plays = await parse_plays_with_results(
            recap_channel, start, end, limit
        )

        # Calculate units
        total_units = (nw * 1) - (nl * 3) + (cw * 0.5) - (cl * 1) + (kw * 2) - (kl * 2)

        # Build recap message
        recap = f"📊 **{title}**\n━━━━━━━━━━━━━━━━━━\n\n"

        total_graded = w + l + wsh
        if total_graded == 0:
            recap += "No plays graded yet.\n"
        else:
            recap += f"**Record:** {w}-{l}"
            if wsh > 0:
                recap += f" ({wsh} Wash)"
            recap += f"\n**Units:** {total_units:+.2f}u\n\n"

            recap += f"🔷 Normal: {nw}-{nl}\n"
            recap += f"⚠️ Cautious: {cw}-{cl}\n"
            recap += f"☢️ Nuke: {kw}-{kl}\n\n"

        # Sport/League breakdown
        recap += "**SPORT BREAKDOWN**\n"
        for sport in sorted(sport_league_stats.keys()):
            sport_totals = {"w": 0, "l": 0, "u": 0.0}
            for league_data in sport_league_stats[sport].values():
                sport_totals["w"] += league_data["w"]
                sport_totals["l"] += league_data["l"]
                sport_totals["u"] += league_data["u"]
            
            recap += f"🏆 {sport}: {sport_totals['w']}-{sport_totals['l']} ({sport_totals['u']:+.2f}u)\n"
            
            # League breakdown under sport
            for league in sorted(sport_league_stats[sport].keys()):
                league_data = sport_league_stats[sport][league]
                recap += f"  • {league}: {league_data['w']}-{league_data['l']} ({league_data['u']:+.2f}u)\n"
            recap += "\n"

        await send_long_message(message.channel, recap)
        return

    # ── CSV SLATE ──
    if not message.attachments:
        return

    attachment = message.attachments[0]

    if not attachment.filename.endswith(".csv"):
        return

    # Check if in allowed channel
    if message.channel.id not in ALLOWED_CHANNELS:
        return

    file_bytes = await attachment.read()
    decoded = file_bytes.decode("utf-8")

    reader = csv.DictReader(io.StringIO(decoded))

    plays = []

    for row in reader:
        try:
            league = row["League"].strip()
            player1 = row["Player 1"].strip()
            player2 = row["Player 2"].strip()
            play_type = row["Play"].strip()
            history = row["History"].strip()
            time_eastern = row["Time (Eastern)"].strip()
            
            # Extract sport from league name
            if any(tt_league in league.lower() for tt_league in ["setka", "czech", "elite", "cup"]):
                sport = "TT"
            else:
                sport = "TT"
            
            # Parse time using the full eastern time string
            # Extract just the time portion (HH:MM AM/PM) from "04/17 07:30 PM"
            time_parts = time_eastern.split()
            time_only = f"{time_parts[-2]} {time_parts[-1]}"  # "07:30 PM"
            
            # Parse and reformat
            try:
                dt = datetime.strptime(time_only, "%I:%M %p")
                time_str = dt.strftime("%I:%M %p").lstrip('0').replace(' 0', ' ')  # Remove leading zeros
            except:
                time_str = time_only
            
            # Create match and bet strings
            match = f"{player1} vs {player2}"
            
            # Determine bet from play type
            if "4+" in play_type:
                bet = "4+ SET"
            elif "OVER" in play_type:
                bet = "OVER"
            elif "UNDER" in play_type:
                bet = "UNDER"
            else:
                bet = play_type
            
            # Default confidence and units
            confidence = "Normal"
            units = 1
            
            plays.append({
                "sport": sport,
                "league": league,
                "match": match,
                "bet": bet,
                "time": time_str,
                "confidence": confidence,
                "units": units
            })
        except (KeyError, ValueError, IndexError) as e:
            print(f"[CSV] Error parsing row: {e}")
            continue

    # Delete previous slate messages
    for msg in last_slate_messages:
        try:
            await msg.delete()
        except Exception:
            pass

    last_slate_messages = []
    await message.delete()

    # Post new slate
    if plays:
        msg_header = await message.channel.send("🏓 **TODAY'S PLAYS** 🏓")
        last_slate_messages.append(msg_header)

        text = ""
        for play in plays:
            confidence_lower = play["confidence"].lower()
            
            if confidence_lower == "nuke":
                emoji = "☢️"
                confidence_text = "Nuke"
            elif confidence_lower == "cautious":
                emoji = "⚠️"
                confidence_text = "Cautious"
            else:
                emoji = "🔷"
                confidence_text = "Normal"

            play_block = (
                f"{play['sport']} | {play['league']}\n"
                f"{play['match']}\n"
                f"{play['bet']}\n"
                f"{play['time']}\n"
                f"{emoji} {confidence_text} | {format_units(play['units'])}\n\n"
            )
            text += play_block

            # Schedule reminder
            try:
                await schedule_reminder_for_play(
                    _guild_id(message),
                    message,
                    play["sport"],
                    play["league"],
                    play["match"],
                    play["bet"],
                    play["time"],
                    play["confidence"]
                )
            except Exception as e:
                print(f"[REMINDER] Error scheduling: {e}")

        sent_msgs = await send_long_message(message.channel, text.strip())
        last_slate_messages.extend(sent_msgs)
        
        recap_msg = await message.channel.send(f"✅ **{len(plays)} plays posted.** Use `!reminders` to see scheduled alerts.")
        last_slate_messages.append(recap_msg)


client.run(TOKEN)
