class Person:
    ID_PREFIX = "PERSON:"
    LABEL = "person"

    class PropertyKey:
        UUID = "uuid"  # via random UUID generator
        CUSTOMER_ID = "customer_id"
        FIRST_NAME = "first_name"
        LAST_NAME = "last_name"
        FULL_NAME = "full_name"
        EMAIL = "email"
        PHONE = "phone"
        SOCIAL_MEDIA = "social_media"
        DOB = "dob"
        TITLE = "title"
        PREVIOUS_TITLES = "previous_titles"
        ORGANIZATION = "organization"
        PREVIOUS_ORGANIZATIONS = "previous_organizations"
        TENTATIVE_ORGANIZATIONS = "tentative organizations"
        MAILING_ADDRESS = "mailing_address"
        INTEREST_AREAS = "interest_areas"
        LEAD_SOURCE = "lead_source"
        EVENT_NAME = "event_name"
        PURCHASING_AGENT = "purchasing_agent"
        VALIDATED_LEAD_STATUS = "validated_lead_status"
        STATUS = "status"  # LEAD-0, CUSTOMER-5, etc
        INGESTION_TAG = "ingestion_tag"
        DATA_SOURCE = "data_source"
