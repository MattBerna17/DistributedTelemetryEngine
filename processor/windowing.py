from datetime import datetime


def get_window_start(event_time: datetime) -> datetime:
    """
    Returns the start of the 1-minute window containing event_time.
    """
    return event_time.replace(
        second=0,
        microsecond=0,
    )
    