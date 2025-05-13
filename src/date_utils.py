from datetime import datetime, timedelta

def is_weekday(date: datetime) -> bool:
    """Check if the given date is a weekday (Monday to Friday)."""
    return date.weekday() < 5

def get_last_weekday(date: datetime) -> datetime:
    """Get the last weekday before the given date."""
    current_date = date - timedelta(days=1)
    while not is_weekday(current_date):
        current_date -= timedelta(days=1)
    return current_date

def get_url_id(date: datetime, base_date: datetime, base_url_id: int) -> int:
    """Calculate URL ID based on the number of weekdays from the base date."""
    if date == base_date:
        return base_url_id
    
    delta = 0
    if date > base_date:
        current_date = base_date + timedelta(days=1)
        while current_date <= date:
            if is_weekday(current_date):
                delta += 1
            current_date += timedelta(days=1)
        return base_url_id + delta
    
    current_date = base_date - timedelta(days=1)
    while current_date >= date:
        if is_weekday(current_date):
            delta -= 1
        current_date -= timedelta(days=1)
    return base_url_id + delta
