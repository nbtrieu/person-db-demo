class Publication:
    ID_PREFIX = "PUBLICATION:"
    LABEL = "publication"

    class PropertyKey:
        UUID = "uuid"  # same as url?
        TITLE = "title"
        DOI = "doi"
        PUBLICATION_DATE = "publication_date"
        AFFILIATIONS = "affiliations"  # array
        ABSTRACT = "abstract"
        SOURCE_NAME = "source_name"
        VOLUME = "volume"
        ISSUE = "issue"
        START_PAGE = "start_page"
        END_PAGE = "end_page"
        KEYWORDS = "keywords"
        PUBLICATION_TYPE = "publication_type"
        CITATIONS = "citations"  # int
        REFERENCES = "references"  # array
        URL = "url"
        NOTES = "notes"
