class PublicationProduct:
    ID_PREFIX = "PUBLICATION_PRODUCT:"
    LABEL = "publication_product"

    class PropertyKey:
        UUID = "uuid"  # objectId from mongoDB
        NAME = "name"
        DOI = "doi" # list type
