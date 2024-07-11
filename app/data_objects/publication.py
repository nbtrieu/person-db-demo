class Publication:
    ID_PREFIX = "PUBLICATION:"
    LABEL = "publication"

    class PropertyKey:
        UUID = "uuid"  # same as identifier
        TITLE = "title"
        ABSTRACT = "abstract"
        PUBLICATION_DATE = "publication_date"
        SOURCE_NAME = "source_name"
        VOLUME = "volume"
        ISSUE = "issue"
        PAGES = "pages"
        IDENTIFIER = "identifier"  # DOI? PMID? PMC ID?
        KEYWORDS = "keywords"
        URL = "url"
        PUBLICATION_TYPE = "publication_type"
        NOTES = "notes"
