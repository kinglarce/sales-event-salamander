"""
Utilities for event processing
"""
from enum import Enum

class TicketCategory(Enum):
    SINGLE = "single"
    DOUBLES = "double"
    RELAY = "relay"
    SPECTATOR = "spectator"
    EXTRA = "extra"
    
class TicketEventDay(Enum):
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"

def determine_ticket_group(ticket_name: str) -> TicketCategory:
    """Determine basic ticket group (single, double, relay, spectator, extra)"""
    name_lower = ticket_name.lower()
    if 'friend' in name_lower or 'sportograf' in name_lower or 'transfer' in name_lower or 'complimentary' in name_lower:
        return TicketCategory.EXTRA 
    elif 'double' in name_lower:
        return TicketCategory.DOUBLES
    elif 'relay' in name_lower:
        return TicketCategory.RELAY
    elif 'spectator' in name_lower:
        return TicketCategory.SPECTATOR
    return TicketCategory.SINGLE

def determine_ticket_event_day(ticket_name: str) -> TicketEventDay:
    """Determine basic ticket group (thursday, friday, saturday, sunday)"""
    name_lower = ticket_name.lower()
    if 'sunday' in name_lower:
        return TicketEventDay.SUNDAY 
    elif 'saturday' in name_lower:
        return TicketEventDay.SATURDAY
    elif 'friday' in name_lower:
        return TicketEventDay.FRIDAY
    elif 'thursday' in name_lower:
        return TicketEventDay.THURSDAY
    return TicketEventDay.SATURDAY 