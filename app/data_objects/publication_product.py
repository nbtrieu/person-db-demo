class PublicationProduct:
    ID_PREFIX = "PUBLICATION_PRODUCT:"
    LABEL = "publication_product"

    class PropertyKey:
        UUID = "uuid"  # objectId from mongoDB
        NAME = "name"
        COMPANY = "company"
        DISPLAY_NAME = "display_name"
        PUBLICATIONS = "publications" # list type
