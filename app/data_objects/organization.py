class Organization:
    ID_PREFIX = "ORGANIZATION:"
    LABEL = "organization"

    class PropertyKey:
        UUID = "uuid"  # same as name
        NAME = "name"  # all lowercase for standardization
        DISPLAY_NAME = "display_name"
        INDUSTRY = "industry"
        DESCRIPTION = "description"
        LOCATION = "location"
        MAILING_ADDRESS = "mailing_address"
        WEBSITE = "website"
        DOMAIN = "domain"
        LINKEDIN_URL = "linkedin_url"
        SPECIALTIES = "specialties"
        SIZE = "size"
