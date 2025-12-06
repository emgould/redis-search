from datetime import UTC, datetime, timedelta
from enum import Enum
from zoneinfo import ZoneInfo

# ----------------------------------------------------------
# Channel Utilities
# ----------------------------------------------------------
from api.schedulesdirect.models import LineupStation


class ChannelType(str, Enum):
    """Channel type classification for filtering."""

    BROADCAST = "broadcast"
    PREMIUM_CABLE = "premium-cable"
    NON_PREMIUM_CABLE = "non-premium-cable"


JUNK_KEYWORDS = [
    # Shopping
    "QVC",
    "HSN",
    "SHOPHQ",
    "SHOP LC",
    "EVINE",
    "JEWELRY",
    "JTV",
    "SHOPPING",
    "HOME SHOP",
    # Religious
    "TBN",
    "EWTN",
    "INSP",
    "DAYSTAR",
    "CBN",
    "SBN",
    "THE WORD",
    "TRINITY",
    "FAITH",
    "GOD TV",
    "REJOICE",
    # Low-value / filler / diginets
    "GRIT",
    "LAFF",
    "COMET",
    "CHARGE",
    "BOUNCE",
    "START TV",
    "ION MYSTERY",
    "ION PLUS",
    "SCRIPPS",
    "NEWSY",
    "COURT TV",
    "DABL",
    "TRUE CRMZ",
    "COZI",
    "LOCALISH",
    # Public/gov/edu
    "CSPAN",
    "C-SPAN",
    "PUBLIC ACCESS",
    "GOV",
    "PEG",
    "LOCAL ACCESS",
    "EDU",
    # Audio/music-only
    "STINGRAY",
    "MUSIC CHOICE",
    "MC ",
    "AUDIO ONLY",
    "MUSIC ONLY",
    # Preview/barker/promotional channels
    "PREVIEW",
    "FREE PREVIEW",
    "BARKER",
    "PROMO",
    "TRIAL",
    # Duplicates (local/SD/low-quality feeds)
    " SD",
    "480",
    "STREAM",
    "WEST",
    "PACIFIC",
    "(PACIFIC)",
    "(WEST)",
    # YouTube TV internal / ephemeral channels
    "YOUTUBE",  # catches: YouTube Coach View, WatchWith, League Pass
    "SUNDAY TICKET",  # YTTV-only NFL feed variations
    "MAX LIVE EVENT",  # part of YTTV Max integration
    "SPORTS ON MAX",  # more YTTV Max placeholders
    "4K EVENTS",  # Fox Sports Events 1/2/3
    "ALTERNATE",  # NFL Network Alternate feeds
    "COACH VIEW",
    "WATCHWITH",
    "ZEN",  # YouTube TV Zen mood channel
    # Foreign-language packs (optional unless user opts in)
    "UNIVISION",
    "UNIMAS",
    "GALAVISION",
    "TELEMUNDO",
    "AZTECA",
    "PASIONES",
    "CINE LATINO",
    "SONY CINE",
    "TELEHIT",
    "TELEHIT MÚSICA",
    "ZEE",
    "PINOY",
    "GMA",
    "ANTENA 3",
    "NTN24",
    "NUESTRA TELE",
    "BANDAMAX",
    "TUDN",
    # Optional: kids filler
    "BABY FIRST",
    "SPROUT",
    "BOOMERANG",
    # Optional: adult
    "XXX",
    "HUSTLER",
    "PLAYBOY",
]

SAFE_KEYWORDS = [
    "NBC NEWS NOW",  # must NOT be removed even though it contains "NOW"
    "HBO",
    "SHOWTIME",
    "STARZ",
    "CINEMAX",
    "MGM+",  # premium networks
    "ESPN",
    "FOX",
    "FS1",
    "FS2",
    "NFL",
    "NBA",
    "MLB",
    "NHL",  # sports core
]
SPORTS_KEYWORDS = [
    "ESPN",
    "ESPNU",
    "ESPNEWS",
    "ESPAN",
    "ESPN 2",
    "ESPN2",
    "FS1",
    "FS2",
    "FOX SPORTS",
    "FOX DEPORTES",
    "FOX SOCCER",
    "NBC SPORTS",
    "NBCU 4K",
    "SOCCER",
    "BIG TEN",
    "BTN",
    "SEC NETWORK",
    "ACC NETWORK",
    "NHL",
    "MLB",
    "NBA",
    "NFL",
    "SNY",
    "YES",
    "MSG",
    "SPORTSNET",
    "SPORTSNET",
    "SNY",
    "TENNIS CHANNEL",
    "GOLF CHANNEL",
    "MOTORTREND",
    "RACER",
    "BILLIARD",
    # beIN
    "BEIN",
    "XTRA",
    "BEN IN SPORTS",
    # 4K / overflow / alternates / special sports feeds
    "4K",
    "ALT",
    "ALTERNATE",
    "EVENT",
    "LIVE EVENT",
    # League Pass channels
    "LEAGUE PASS",
    "WNBA",  # all WNBA alternates
]

SPORTS_EXACT = {
    "NFL NETWORK",
    "NFL REDZONE",
    "NBA TV",
    "MLB NETWORK",
    "NHL NETWORK",
    "CBS SPORTS NETWORK",
    "FOX SPORTS 1",
    "FOX SPORTS 2",
    "GOLF CHANNEL",
    "TENNIS CHANNEL",
    "BIG TEN NETWORK",
    "SEC NETWORK",
    "ACC NETWORK",
    "SNY",
    "YES NETWORK",
    "MSG",
}

NOT_SPORTS_KEYWORDS = [
    "FOX NEWS",
    "FOX BUSINESS",
    "FX ",
    "FXX",
    "FREEFORM",
    "NBC NEWS",
    "MSNBC",
    "CNBC",
]

NOT_SPORTS_KEYWORDS = [
    "FOX NEWS",
    "FOX BUSINESS",
    "FX ",
    "FXX",
    "FREEFORM",
    "NBC NEWS",
    "MSNBC",
    "CNBC",
]

NEWS_KEYWORDS = [
    "NEWS",
    "NEWSHD",
    "NEWS HD",
    "NEWS NOW",
    "CHANNEL NEWS",
]

NEWS_EXACT = {
    "CNN",
    "CNN HD",
    "CNN INTERNATIONAL",
    "CNN EN ESPAÑOL",
    "MSNBC",
    "FOX NEWS",
    "FOX NEWS CHANNEL",
    "FOX WEATHER",
    "NEWSNATION",
    "NEWSMAX",
    "HLN",
    "BBC NEWS",
    "BBC AMERICA",  # note: includes some entertainment but widely considered news/information
    "BLOOMBERG",
    "CHEddar",  # spelled inconsistently across lineups
    "CNBC",
    "CNBC WORLD",
    "NBC NEWS NOW",
    "ABC NEWS LIVE",
    "LOCAL NOW",
}

NEWS_NEGATIVE_KEYWORDS = [
    "FOX SPORTS",
    "FS1",
    "FS2",
    "ESPN",
    "ESPN2",
    "ESPNU",
    "ESPNEWS",  # ← despite name, this is NOT a news channel
    "SPORTS",
    "SPORT",
    "BTN",
    "SEC",
    "ACC",
    "GOLF",
    "SOCCER",
    "NBA",
    "NFL",
    "MLB",
    "NHL",
    "TENNIS",
    "MOTOR",
    "RACING",
]
BROADCAST_KEYWORDS = [
    # ABC
    "ABC",
    "WABC",
    "KABC",
    # CBS
    "CBS",
    "WCBS",
    "KCBS",
    # NBC
    "NBC",
    "WNBC",
    "KNBC",
    # FOX
    "FOX",
    "WNYW",
    "KFOX",
    "KTVU",
    "WFLD",
    # CW
    "CW",
    "WPIX",
    "KTLA",
    "WDCW",
    # PBS
    "PBS",
    "WNET",
    "WLIW",
    "KPBS",
]

# Broadcast network priority order (index = priority, lower = higher priority)
# Sort order: CBS, NBC, ABC, FOX, CW, PBS
BROADCAST_PRIORITY = ["CBS", "NBC", "ABC", "FOX", "CW", "PBS"]

# Premium cable channels - sorted last
PREMIUM_KEYWORDS = [
    "HBO",
    "MAX",  # HBO Max became just "Max"
    "SHOWTIME",
    "SHO",
    "STARZ",
    "CINEMAX",
    "EPIX",
    "MGM+",
    "PARAMOUNT+",
    "PARAMOUNT PLUS",
    "AMC+",
    "AMC PLUS",
]

# Exact matches for premium to avoid false positives
PREMIUM_EXACT = {
    "HBO",
    "HBO HD",
    "HBO 2",
    "HBO COMEDY",
    "HBO FAMILY",
    "HBO LATINO",
    "HBO SIGNATURE",
    "HBO ZONE",
    "MAX",
    "SHOWTIME",
    "SHOWTIME 2",
    "SHOWTIME BEYOND",
    "SHOWTIME EXTREME",
    "SHOWTIME FAMILY",
    "SHOWTIME NEXT",
    "SHOWTIME SHOWCASE",
    "SHOWTIME WOMEN",
    "SHO 2",
    "STARZ",
    "STARZ CINEMA",
    "STARZ COMEDY",
    "STARZ EDGE",
    "STARZ ENCORE",
    "STARZ KIDS",
    "CINEMAX",
    "MOREMAX",
    "ACTIONMAX",
    "THRILLMAX",
    "EPIX",
    "EPIX 2",
    "EPIX HITS",
    "MGM+",
    "MGM+ HD",
}

channel_map: set = set()


def is_broadcast_network(channel) -> bool:
    """
    Returns True if the channel is a major U.S. broadcast network affiliate
    (ABC, CBS, NBC, FOX, CW, PBS).
    """
    name = channel.name.upper()
    callsign = channel.callsign.upper() if channel.callsign else ""

    # === EXCLUSIONS FIRST ===
    # Exclude news channels (ABC News Live, NBC News Now, etc.)
    if "NEWS" in name and any(x in name for x in ["LIVE", "NOW"]):
        return False

    # Exclude FOX cable networks ("FOX BUSINESS", "FOX NEWS", "FS1", etc.)
    FOX_EXCLUDE = ["NEWS", "BUSINESS", "SPORTS", "SOUL", "DEPORTES", "WEATHER"]
    if name.startswith("FOX") and any(x in name for x in FOX_EXCLUDE):
        return False

    # === ABC ===
    # Match: "ABC", "ABC National Feed", "ABC HD", "WABC", "KABC", etc.
    if "ABC" not in channel_map and (name == "ABC" or name.startswith(("ABC ", "ABC-"))):
        channel_map.add("ABC")
        return True

    if "ABC" not in channel_map and (
        name.startswith(("WABC", "KABC")) or callsign.startswith(("WABC", "KABC"))
    ):
        channel_map.add("ABC")
        return True

    # === CBS ===
    # Match: "CBS", "CBS National Feed", "CBS HD", "WCBS", "KCBS", etc.
    if "CBS" not in channel_map and (name == "CBS" or name.startswith(("CBS ", "CBS-"))):
        channel_map.add("CBS")
        return True
    if "CBS" not in channel_map and (
        name.startswith(("WCBS", "KCBS")) or callsign.startswith(("WCBS", "KCBS"))
    ):
        channel_map.add("CBS")
        return True

    # === NBC ===
    # Match: "NBC", "NBC National Feed", "NBC HD", "WNBC", "KNBC", etc.
    if "NBC" not in channel_map and (name == "NBC" or name.startswith(("NBC ", "NBC-"))):
        channel_map.add("NBC")
        return True
    if "NBC" not in channel_map and (
        name.startswith(("WNBC", "KNBC")) or callsign.startswith(("WNBC", "KNBC"))
    ):
        channel_map.add("NBC")
        return True

    # === FOX ===
    # Match: "FOX", "FOX National Feed", "FOX HD", "WNYW", "KFOX", etc.
    # But NOT "FOX NEWS", "FOX BUSINESS", etc. (excluded above)
    if "FOX" not in channel_map and (name == "FOX" or name.startswith(("FOX ", "FOX-"))):
        channel_map.add("FOX")
        return True
    if "FOX" not in channel_map and (
        name.startswith(("WNYW", "KFOX", "KTVU")) or callsign.startswith(("WNYW", "KFOX", "KTVU"))
    ):
        channel_map.add("FOX")
        return True

    # === CW ===
    # Match: "CW", "CW TV", "CW HD", "WPIX", "KTLA", etc.
    # But NOT "CW PLUS", "CW STREAM"
    if (
        "CW" not in channel_map
        and name.startswith("CW")
        and "PLUS" not in name
        and "STREAM" not in name
    ):
        channel_map.add("CW")
        return True
    if "CW" not in channel_map and (
        name.startswith(("WPIX", "KTLA", "WDCW")) or callsign.startswith(("WPIX", "KTLA", "WDCW"))
    ):
        channel_map.add("CW")
        return True

    # === PBS ===
    if "PBS" not in channel_map and ("PBS" in name or name.startswith(("WNET", "WLIW"))):
        channel_map.add("PBS")
        return True
    if "PBS" not in channel_map and (
        "PBS" in name or name.startswith(("WNET", "WLIW")) or callsign.startswith(("WNET", "WLIW"))
    ):
        channel_map.add("PBS")
        return True

    return False


channel_name_map: dict[str, str] = {
    "ABC": "ABC",
    "CBS": "CBS",
    "NBC": "NBC",
    "FOX": "FOX",
    "CW": "CW",
    "PBS": "PBS",
    "WABC": "ABC",
    "WCBS": "CBS",
    "WNBC": "NBC",
    "WFOX": "FOX",
    "WNYW": "FOX",
    "WPIX": "CW",
    "WNET": "PBS",
    "WLIW": "PBS",
    "KABC": "ABC",
    "KCBS": "CBS",
    "KNBC": "NBC",
    "KFOX": "FOX",
    "KTLA": "CW",
    "KPBS": "PBS",
    "CW TV": "CW",
    "HBO": "HBO",
    "HBO HD": "HBO",
    "HBO 2": "HBO",
    "HBO COMEDY": "HBO",
    "HBO FAMILY": "HBO",
    "HBO LATINO": "HBO",
    "HBO SIGNATURE": "HBO",
    "HBO ZONE": "HBO",
    "MAX": "MAX",
    "STARZ": "STARZ",
    "STARZ CINEMA": "STARZ",
    "STARZ COMEDY": "STARZ",
    "STARZ EDGE": "STARZ",
    "STARZ ENCORE": "STARZ",
    "STARZ KIDS": "STARZ",
    "CINEMAX": "CINEMAX",
    "MOREMAX": "CINEMAX",
    "ACTIONMAX": "CINEMAX",
    "THRILLMAX": "CINEMAX",
    "EPIX": "EPIX",
    "EPIX 2": "EPIX",
    "EPIX HITS": "EPIX",
    "MGM+": "MGM+",
    "MGM+ HD": "MGM+",
}


def is_broadcast_network_by_name(channel_name: str) -> bool:
    """
    Returns True if the channel name matches a major U.S. broadcast network
    (ABC, CBS, NBC, FOX, CW, PBS).

    Args:
        channel_name: Channel name as a string

    Returns:
        bool: True if the channel is a broadcast network
    """
    name = channel_name.upper()

    # === EXCLUSIONS FIRST ===
    # Exclude news channels (ABC News Live, NBC News Now, etc.)
    if "NEWS" in name and any(x in name for x in ["LIVE", "NOW"]):
        return False

    # Exclude FOX cable networks ("FOX BUSINESS", "FOX NEWS", "FS1", etc.)
    FOX_EXCLUDE = ["NEWS", "BUSINESS", "SPORTS", "SOUL", "DEPORTES", "WEATHER"]
    if name.startswith("FOX") and any(x in name for x in FOX_EXCLUDE):
        return False

    # === ABC ===
    # Check for mapped name "ABC" or original callsigns
    if name == "ABC" or name == "WABC" or name.startswith(("ABC ", "ABC-", "WABC", "KABC")):
        return True

    # === CBS ===
    # Check for mapped name "CBS" or original callsigns
    if name == "CBS" or name == "WCBS" or name.startswith(("CBS ", "CBS-", "WCBS", "KCBS")):
        return True

    # === NBC ===
    # Check for mapped name "NBC" or original callsigns
    if name == "NBC" or name == "WNBC" or name.startswith(("NBC ", "NBC-", "WNBC", "KNBC")):
        return True

    # === FOX ===
    # Check for mapped name "FOX" or original callsigns
    if name == "FOX" or name == "WFOX" or name.startswith(("FOX ", "FOX-", "WNYW", "KFOX", "KTVU")):
        return True

    # === CW ===
    # Check for mapped name "CW" or original callsigns
    if name == "CW" or (name.startswith("CW") and "PLUS" not in name and "STREAM" not in name):
        return True
    if name.startswith(("WPIX", "KTLA", "WDCW")):
        return True

    # === PBS ===
    # Check for mapped name "PBS" or original callsigns
    return name == "PBS" or "PBS" in name or name.startswith(("WNET", "WLIW"))


def is_premium_channel(channel_name: str) -> bool:
    """
    Returns True if the channel is a premium cable network (HBO, Showtime, Starz, etc.).
    """
    name = channel_name.upper()

    # Check exact matches first (most reliable)
    if name in PREMIUM_EXACT:
        return True

    # Check keyword matches
    return any(keyword in name for keyword in PREMIUM_KEYWORDS)


def get_channel_type(channel_name: str) -> ChannelType:
    """
    Determine the channel type classification.

    Args:
        channel_name: Name of the channel

    Returns:
        ChannelType enum value: BROADCAST, PREMIUM_CABLE, or NON_PREMIUM_CABLE
    """
    if is_broadcast_network_by_name(channel_name):
        return ChannelType.BROADCAST
    elif is_premium_channel(channel_name):
        return ChannelType.PREMIUM_CABLE
    else:
        return ChannelType.NON_PREMIUM_CABLE


def get_broadcast_priority(channel_name: str) -> int:
    """
    Returns the priority index for a broadcast network (0=ABC, 1=CBS, etc.).
    Returns -1 if not a broadcast network.
    """
    name = channel_name.upper()

    # Exclude news channels first
    if "NEWS" in name and any(x in name for x in ["LIVE", "NOW"]):
        return -1

    # ABC
    if name == "ABC" or name.startswith(("ABC ", "ABC-", "WABC", "KABC")):
        return BROADCAST_PRIORITY.index("ABC")
    # CBS
    if name == "CBS" or name.startswith(("CBS ", "CBS-", "WCBS", "KCBS")):
        return BROADCAST_PRIORITY.index("CBS")
    # NBC
    if name == "NBC" or name.startswith(("NBC ", "NBC-", "WNBC", "KNBC")):
        return BROADCAST_PRIORITY.index("NBC")

    # FOX but exclude cable
    FOX_EXCLUDE = ["NEWS", "BUSINESS", "SPORTS", "SOUL", "DEPORTES", "WEATHER"]
    if name.startswith("FOX") and not any(x in name for x in FOX_EXCLUDE):
        return BROADCAST_PRIORITY.index("FOX")
    if name.startswith(("WNYW", "KFOX", "KTVU")):
        return BROADCAST_PRIORITY.index("FOX")

    # CW
    if name.startswith("CW") and "PLUS" not in name and "STREAM" not in name:
        return BROADCAST_PRIORITY.index("CW")
    if name.startswith(("WPIX", "KTLA", "WDCW")):
        return BROADCAST_PRIORITY.index("CW")

    # PBS
    if "PBS" in name or name.startswith(("WNET", "WLIW")):
        return BROADCAST_PRIORITY.index("PBS")

    return -1  # Not a broadcast network


def get_schedule_sort_key(item) -> tuple:
    """
    Returns a sort key tuple for schedule items.

    Sort order:
    1. Air time (ascending) - 8pm before 8:30pm
    2. Channel type priority:
       - Broadcast networks first (in order: ABC, CBS, NBC, FOX, CW, PBS)
       - Cable (non-premium) second, alphabetically by channel name
       - Premium channels last, alphabetically by channel name

    Args:
        item: MCBaseItem with metrics["schedule"] containing channel info

    Returns:
        Tuple for sorting: (air_datetime, type_priority, sub_priority, channel_name)
    """
    schedule = item.metrics.get("schedule", {})
    air_datetime = schedule.get("air_datetime_utc", "")
    channel_name = schedule.get("channel_name", "") or ""

    # Determine channel type and priority
    broadcast_priority = get_broadcast_priority(channel_name)

    if broadcast_priority >= 0:
        # Broadcast network: type_priority=0, sub_priority=broadcast order
        return (air_datetime, 0, broadcast_priority, channel_name)
    elif is_premium_channel(channel_name):
        # Premium channel: type_priority=2 (last), alphabetical
        return (air_datetime, 2, 0, channel_name)
    else:
        # Cable (non-premium): type_priority=1, alphabetical
        return (air_datetime, 1, 0, channel_name)


def is_news_channel(c: LineupStation) -> bool:
    name = c.name.upper()

    # Rule 1 — exclude sports channels first
    if any(bad in name for bad in NEWS_NEGATIVE_KEYWORDS):
        return False

    # Rule 2 — exact matches (strongest signal)
    for exact in NEWS_EXACT:
        if name == exact or name.startswith(exact):
            return True

    # Rule 3 — keyword-based match
    return bool(any(k in name for k in NEWS_KEYWORDS))


def is_sports_channel(c: LineupStation) -> bool:
    name = c.name.upper()

    # Rule 1 — if explicitly non-sports, bail early
    if any(bad in name for bad in NOT_SPORTS_KEYWORDS):
        return False

    # Rule 2 — exact matches
    for exact in SPORTS_EXACT:
        if name == exact or name.startswith(exact):
            return True

    # Rule 3 — keyword presence
    return bool(any(k in name for k in SPORTS_KEYWORDS))


def get_base_channel_name(channel_name: str) -> str:
    """
    Get the base channel name by stripping -DT and -TV suffixes.

    These suffixes indicate digital/TV variants of the same channel:
    - WNET and WNET-DT are the same channel
    - WABC-TV and WABC are the same channel

    Args:
        channel_name: The full channel name

    Returns:
        The base channel name without suffix
    """
    name = channel_name.upper().strip()
    # Strip common digital/TV suffixes
    for suffix in ["-DT", "-TV", " DT", " TV"]:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def filter_channels(channels: list[LineupStation]) -> list[LineupStation]:
    """Filter out junk channels and deduplicate -DT/-TV variants."""
    valid_channels = []

    for c in channels:
        # Skip channels with no name
        if not c.name:
            continue

        name = c.name.upper()

        # keep if it matches a SAFE keyword
        if any(s in name for s in SAFE_KEYWORDS):
            valid_channels.append(c)
            continue

        # remove if matches ANY junk keyword
        if any(k in name for k in JUNK_KEYWORDS):
            continue

        # otherwise keep
        valid_channels.append(c)

    # Deduplicate channels with -DT/-TV suffixes
    # Keep the base channel (without suffix) and drop the suffixed variant
    deduplicated: list[LineupStation] = []
    seen_base_names: set[str] = set()

    # First pass: identify all base channel names (channels without -DT/-TV suffix)
    base_channels_present: set[str] = set()
    for c in valid_channels:
        base_name = get_base_channel_name(c.name)
        # If this channel IS the base name (no suffix stripped), record it
        if base_name == c.name.upper().strip():
            base_channels_present.add(base_name)

    # Second pass: keep channels, but skip -DT/-TV variants if base exists
    for c in valid_channels:
        name_upper = c.name.upper().strip()
        base_name = get_base_channel_name(c.name)

        # If this is a -DT/-TV variant and the base channel exists, skip it
        if base_name != name_upper and base_name in base_channels_present:
            continue

        # Skip if we've already seen this base channel name
        if base_name in seen_base_names:
            continue

        seen_base_names.add(base_name)
        deduplicated.append(c)

    return deduplicated


def get_upper_bound_utc():
    local_now = datetime.now()
    utc_now = datetime.now(UTC)
    local_midnight = datetime.combine(local_now.date() + timedelta(days=1), datetime.min.time())
    seconds_until_local_midnight = (local_midnight - local_now).total_seconds()
    return utc_now + timedelta(seconds=seconds_until_local_midnight)


def convert_airdatetime_to_est(raw_schedule_results: list[dict]) -> list[dict]:
    """
    Convert airDateTime values in raw_schedule_results to EST-aware datetimes.

    SchedulesDirect airDateTime values are naive datetimes representing Eastern time.
    This function parses them and assigns Eastern timezone (America/New_York),
    which automatically handles EST/EDT based on the date.

    Args:
        raw_schedule_results: List of station schedule blocks with structure:
            [
                {
                    'stationID': str,
                    'programs': [
                        {
                            'programID': str,
                            'airDateTime': str,  # ISO format naive datetime
                            'duration': int,
                            ...
                        },
                        ...
                    ],
                    'metadata': dict
                },
                ...
            ]

    Returns:
        List of station schedule blocks with airDateTime converted to EST-aware ISO strings.
        Original structure is preserved, only airDateTime values are modified.

    Example:
        >>> raw = [{
        ...     'stationID': '101103',
        ...     'programs': [{'airDateTime': '2025-12-08T00:00:00Z', ...}],
        ...     'metadata': {...}
        ... }]
        >>> result = convert_airdatetime_to_est(raw)
        >>> result[0]['programs'][0]['airDateTime']
        '2025-12-08T00:00:00-05:00'  # EST-aware
    """
    eastern_tz = ZoneInfo("America/New_York")
    converted_results = []

    for station_block in raw_schedule_results:
        converted_block = dict(station_block)
        converted_programs = []

        for program in station_block.get("programs", []):
            converted_program = dict(program)
            air_dt_str = program.get("airDateTime")

            if air_dt_str:
                try:
                    # Parse UTC datetime (Z indicates UTC)
                    # Replace Z with +00:00 to make it UTC-aware
                    dt_utc_str = air_dt_str.replace("Z", "+00:00")
                    dt_utc = datetime.fromisoformat(dt_utc_str)
                    # Convert UTC to Eastern timezone
                    dt_eastern = dt_utc.astimezone(eastern_tz)
                    # Convert back to ISO string with timezone
                    converted_program["airDateTime"] = dt_eastern.isoformat()
                except (ValueError, TypeError) as e:
                    # If parsing fails, keep original value and log warning
                    import logging

                    logger = logging.getLogger(__name__)
                    logger.warning(
                        f"Failed to parse airDateTime '{air_dt_str}': {e}. Keeping original value."
                    )

            converted_programs.append(converted_program)

        converted_block["programs"] = converted_programs
        converted_results.append(converted_block)

    return converted_results


def filter_out_past_programs(schedule_results: list[dict]):
    """
    Filter programs to only include those airing between now and the cutoff time.

    SD uses multiple timestamp formats. This normalizes ALL timestamps to UTC
    and filters correctly.

    Args:
        schedule_results: List of station schedule blocks from SchedulesDirect
        cutoff_time: Upper bound datetime (UTC). Programs at or after this time
                    are excluded. If None, no upper bound is applied.
    """
    import logging

    logger = logging.getLogger(__name__)

    now = datetime.now(UTC)
    logger.debug(f"filter_out_past_programs: now={now.isoformat()} ")
    filtered = []

    for st in schedule_results:
        programs = st.get("programs", [])
        future_programs = []

        for p in programs:
            adt = p.get("airDateTime")
            if not adt:
                continue

            try:
                # SchedulesDirect airDateTime is local time (Eastern), not UTC
                # Parse as naive datetime and assign Eastern timezone
                dt_naive = datetime.fromisoformat(adt.replace("Z", ""))
                eastern_tz = ZoneInfo("America/New_York")
                dt_eastern = dt_naive.replace(tzinfo=eastern_tz)
                # Convert to UTC for comparison with now (UTC)
                dt = dt_eastern.astimezone(UTC)
            except Exception:
                continue

            # Keep programs that are >= now AND (no cutoff OR < cutoff)
            if dt >= now:
                future_programs.append(p)

        if future_programs:
            new_block = dict(st)
            new_block["programs"] = future_programs
            filtered.append(new_block)

    total_input = sum(len(st.get("programs", [])) for st in schedule_results)
    total_output = sum(len(st.get("programs", [])) for st in filtered)
    logger.debug(f"filter_out_past_programs: {total_input} programs in, {total_output} kept")

    return filtered


def filter_programs_by_time_of_day(
    schedule_results: list[dict],
    start_time: str | None = None,
    end_time: str | None = None,
):
    """
    Filter programs to only include those airing within a specific time-of-day window.

    This filters by time of day (e.g., 20:00 to 23:00) regardless of date.
    Useful for filtering to primetime hours, morning shows, etc.

    Args:
        schedule_results: List of station schedule blocks from SchedulesDirect
        start_time: Start time in HH:MM format (e.g., "20:00"). If None, no lower bound.
        end_time: End time in HH:MM format (e.g., "23:00"). If None, no upper bound.
        timezone_str: IANA timezone string for interpreting the time window (default: "UTC").
                     Examples: "America/New_York", "America/Los_Angeles", "UTC"

    Returns:
        Filtered schedule results containing only programs within the time window.

    Note:
        - Times are inclusive of start_time and exclusive of end_time
        - If end_time < start_time, the window wraps around midnight
          (e.g., "22:00" to "02:00" means 10 PM to 2 AM next day)
    """
    import logging

    logger = logging.getLogger(__name__)

    if start_time is None and end_time is None:
        logger.debug("filter_programs_by_time_of_day: no time bounds, returning all")
        return schedule_results

    # Parse time strings to hour/minute
    def parse_time(time_str: str) -> tuple[int, int]:
        parts = time_str.split(":")
        return int(parts[0]), int(parts[1])

    start_hour, start_min = parse_time(start_time) if start_time else (0, 0)
    end_hour, end_min = parse_time(end_time) if end_time else (23, 59)

    # Check if window wraps around midnight
    wraps_midnight = False
    if start_time and end_time:
        wraps_midnight = (start_hour, start_min) > (end_hour, end_min)

    logger.debug(
        f"filter_programs_by_time_of_day: {start_time or '00:00'} to {end_time or '23:59'} "
    )

    filtered = []

    for st in schedule_results:
        programs = st.get("programs", [])
        filtered_programs = []

        for p in programs:
            adt = p.get("airDateTime")
            if not adt:
                continue

            # Handle UTC datetime strings and EST-aware datetime strings
            # Parse the datetime (handles both formats)
            try:
                # Replace Z with +00:00 to parse as UTC-aware if needed
                dt_str = adt.replace("Z", "+00:00") if adt.endswith("Z") else adt
                dt = datetime.fromisoformat(dt_str)

                # Convert to Eastern timezone to get the local hour
                eastern_tz = ZoneInfo("America/New_York")
                if dt.tzinfo is None:
                    # Naive datetime - assume UTC and convert to Eastern
                    dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
                    dt_eastern = dt_utc.astimezone(eastern_tz)
                else:
                    # Already timezone-aware, convert to Eastern
                    dt_eastern = dt.astimezone(eastern_tz)

                program_hour = dt_eastern.hour
                program_min = dt_eastern.minute
            except (ValueError, TypeError):
                continue

            # Check if program time falls within window
            in_window = False

            if wraps_midnight:
                # Window wraps midnight: include if >= start OR < end
                if (start_time and (program_hour, program_min) >= (start_hour, start_min)) or (
                    end_time and (program_hour, program_min) < (end_hour, end_min)
                ):
                    in_window = True
            else:
                # Normal window: include if >= start AND < end
                passes_start = not start_time or (program_hour, program_min) >= (
                    start_hour,
                    start_min,
                )
                passes_end = not end_time or (program_hour, program_min) < (end_hour, end_min)
                in_window = passes_start and passes_end

            if in_window:
                filtered_programs.append(p)

        if filtered_programs:
            new_block = dict(st)
            new_block["programs"] = filtered_programs
            filtered.append(new_block)

    total_input = sum(len(st.get("programs", [])) for st in schedule_results)
    total_output = sum(len(st.get("programs", [])) for st in filtered)
    logger.debug(f"filter_programs_by_time_of_day: {total_input} programs in, {total_output} kept")

    return filtered
