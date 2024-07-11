class Event:
    ID_PREFIX = "EVENT:"
    LABEL = "event"

    class PropertyKey:
        UUID = "uuid"  # same as name?
        NAME = "name"  # all lowercase for standardization
        DISPLAY_NAME = "display_name"
        DESCRIPTION = "description"
        EVENT_TYPE = "event_type"
        YEAR = "year"
        COUNTRY = "country"
        STATE = "state"
        REGION = "region"
        URL = "url"
        KEYWORDS = "keywords"
        NOTES = "notes"
