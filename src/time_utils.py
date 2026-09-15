"""Formatting helpers for runtime statistics."""


def format_duration_clock(seconds: float) -> str:
    total_seconds = int(round(max(0, seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_duration_human(seconds: float) -> str:
    total_seconds = int(round(max(0, seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours} giờ")
    if minutes:
        parts.append(f"{minutes} phút")
    if secs or not parts:
        parts.append(f"{secs} giây")
    return " ".join(parts)
