# %%
import pandas as pd
import numpy as np
import database
import ssl
import pickle
import os
import json
from gremlin_python.driver.client import Client
from gremlin_python.process.graph_traversal import GraphTraversalSource, __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import (
    Barrier,
    Bindings,
    Cardinality,
    Column,
    Direction,
    Operator,
    Order,
    P,
    Pop,
    Scope,
    T,
    WithOptions,
)
from tqdm import tqdm
from datetime import datetime, timezone
from itertools import islice
from database import BulkQueryExecutor
from data_objects import Person, Organization, Keyword, ZymoProduct, Publication, PublicationProduct

import nest_asyncio
nest_asyncio.apply()

# # Path to the exported server certificate
# cert_path = '/Users/nicoletrieu/localhost_cert.pem'

# # Create a default client-side SSL context
# ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
# ssl_context.load_verify_locations(cert_path)

# # Use the SSL context in the DriverRemoteConnection
# connection = DriverRemoteConnection('wss://localhost:8182/gremlin', 'g', ssl_context=ssl_context)
# # connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
# g = Graph().traversal().withRemote(connection)

# os.environ[
#     "DB_URL"
# ] = "wss://db-bio-annotations.cluster-cu9wyuyqqen8.ap-southeast-1.neptune.amazonaws.com:8182/gremlin"

# db_url = os.environ[
#     "DB_URL"
# ]
# connection = database.Connection(db_url)
# connection.log_graph_status()
# g = connection.traversal_source


# %% IMPORTING DATA:
def count_nodes_in_db(g: GraphTraversalSource, label: str):
    node_count = g.V().hasLabel(label).count().next()
    return node_count


def count_edges_in_db(g: GraphTraversalSource, label: str):
    edge_count = g.E().hasLabel(label).count().next()
    return edge_count


def drop_nodes(g: GraphTraversalSource, label: str):
    print("Initiating drop_nodes...")
    g.V().hasLabel(label).drop().iterate()
    print(f"{label} nodes dropped")
    return


def drop_edges(g: GraphTraversalSource, label: str):
    g.E().hasLabel(label).drop().iterate()
    print(f"'{label}' edges dropped")
    return


def get_names(g: GraphTraversalSource, label: str):
    name_list = g.V().hasLabel(label).values('name').toList()
    return name_list


def check_node_properties(g: GraphTraversalSource, label: str, property_key: str, property_value: str):
    properties = (
        g.V()
        .hasLabel(label)
        .has(property_key, property_value)
        .project('properties', 'connected_nodes')
        .by(__.valueMap())
        .by(__.both().valueMap().fold())
        .toList()
    )
    return properties


def add_people(g: GraphTraversalSource, contact_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        contact_df.iterrows(),
        total=contact_df.shape[0],
        desc="Importing People"
    ):
        person_properties = {
            Person.PropertyKey.UUID: row.get("UUID"),
            ** ({Person.PropertyKey.CUSTOMER_ID: row["Customer ID"]} if "Customer ID" in contact_df.columns and not pd.isnull(row.get("Customer ID")) else {}),
            ** ({Person.PropertyKey.FIRST_NAME: row["First Name"]} if "First Name" in contact_df.columns and not pd.isnull(row.get("First Name")) else {}),
            ** ({Person.PropertyKey.LAST_NAME: row["Last Name"]} if "Last Name" in contact_df.columns and not pd.isnull(row.get("Last Name")) else {}),
            ** ({Person.PropertyKey.FULL_NAME: row["Full Name"]} if "Full Name" in contact_df.columns and not pd.isnull(row.get("Full Name")) else {}),
            ** ({Person.PropertyKey.EMAIL: row["Email"]} if "Email" in contact_df.columns and not pd.isnull(row.get("Email")) else {}),
            ** ({Person.PropertyKey.PHONE: row["Phone"]} if "Phone" in contact_df.columns and not pd.isnull(row.get("Phone")) else {}),
            ** ({Person.PropertyKey.SOCIAL_MEDIA: row["Social Media"]} if "Social Media" in contact_df.columns and not pd.isnull(row.get("Social Media")) else {}),
            ** ({Person.PropertyKey.DOB: row["DOB"]} if "DOB" in contact_df.columns and not pd.isnull(row.get("DOB")) else {}),
            ** ({Person.PropertyKey.TITLE: row["Title"]} if "Title" in contact_df.columns and not pd.isnull(row.get("Title")) else {}),
            ** ({Person.PropertyKey.PREVIOUS_TITLES: row["Previous Titles"]} if "Previous Titles" in contact_df.columns and not pd.isnull(row.get("Previous Titles")) else {}),
            ** ({Person.PropertyKey.ORGANIZATION: row["Organization"]} if "Organization" in contact_df.columns and not pd.isnull(row.get("Organization")) else {}),
            ** ({Person.PropertyKey.PREVIOUS_ORGANIZATIONS: row["Previous Organizations"]} if "Previous Organizations" in contact_df.columns and not pd.isnull(row.get("Previous Organizations")) else {}),
            ** ({Person.PropertyKey.TENTATIVE_ORGANIZATIONS: row["Tentative Organizations"]} if "Tentative Organizations" in contact_df.columns and not pd.isnull(row.get("Tentative Organizations")) else {}),
            ** ({Person.PropertyKey.INTEREST_AREAS: row["Keywords"]} if "Keywords" in contact_df.columns and not pd.isnull(row.get("Keywords")) else {}),
            ** ({Person.PropertyKey.LEAD_SOURCE: row["Lead Source"]} if "Lead Source" in contact_df.columns and not pd.isnull(row.get("Lead Source")) else {}),
            ** ({Person.PropertyKey.EVENT_NAME: row["Event Name"]} if "Event Name" in contact_df.columns and not pd.isnull(row.get("Event Name")) else {}),
            ** ({Person.PropertyKey.MAILING_ADDRESS: row["Mailing Address"]} if "Mailing Address" in contact_df.columns and not pd.isnull(row.get("Mailing Address")) else {}),
            ** ({Person.PropertyKey.PURCHASING_AGENT: row["Purchasing Agent"]} if "Purchasing Agent" in contact_df.columns and not pd.isnull(row.get("Purchasing Agent")) else {}),
            ** ({Person.PropertyKey.VALIDATED_LEAD_STATUS: row["Validated Lead Status"]} if "Validated Lead Status" in contact_df.columns and not pd.isnull(row.get("Validated Lead Status")) else {}),
            ** ({Person.PropertyKey.STATUS: row["Status"]} if "Status" in contact_df.columns and not pd.isnull(row.get("Status")) else {}),
            Person.PropertyKey.INGESTION_TAG: row.get("Ingestion Tag", None),
            Person.PropertyKey.DATA_SOURCE: row.get("Data Source", None)
        }

        query_executor.add_vertex(
            label=Person.LABEL,
            properties=person_properties
        )

    query_executor.force_execute()


def add_organizations(g: GraphTraversalSource, unique_organizations_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        unique_organizations_df.iterrows(),
        total=unique_organizations_df.shape[0],
        desc="Importing Organizations"
    ):
        organization_properties = {
            Organization.PropertyKey.UUID: row.get("Organization Standardized Name"),
            Organization.PropertyKey.NAME: row.get("Organization Standardized Name"),
            Organization.PropertyKey.DISPLAY_NAME: row.get("Display Name"),
            **({Organization.PropertyKey.INDUSTRY: row["Industry"]} if "Industry" in unique_organizations_df.columns and not pd.isnull(row.get("Industry")) else {}),
            **({Organization.PropertyKey.DESCRIPTION: row["Description"]} if "Description" in unique_organizations_df.columns and not pd.isnull(row.get("Description")) else {}),
            **({Organization.PropertyKey.LOCATION: row["Location"]} if "Location" in unique_organizations_df.columns and not pd.isnull(row.get("Location")) else {}),
            **({Organization.PropertyKey.MAILING_ADDRESS: row["Organization's Mailing Address"]} if "Organization's Mailing Address" in unique_organizations_df.columns and not pd.isnull(row.get("Organization's Mailing Address")) else {}),
            **({Organization.PropertyKey.WEBSITE: row["Website"]} if "Website" in unique_organizations_df.columns and not pd.isnull(row.get("Website")) else {}),
            **({Organization.PropertyKey.DOMAIN: row["Domain"]} if "Domain" in unique_organizations_df.columns and not pd.isnull(row.get("Domain")) else {}),
            **({Organization.PropertyKey.LINKEDIN_URL: row["LinkedIn URL"]} if "LinkedIn URL" in unique_organizations_df.columns and not pd.isnull(row.get("LinkedIn URL")) else {}),
            **({Organization.PropertyKey.SPECIALTIES: row["Specialties"]} if "Specialties" in unique_organizations_df.columns and not pd.isnull(row.get("Specialties")) else {}),
            **({Organization.PropertyKey.SIZE: row["Size"]} if "Size" in unique_organizations_df.columns and not pd.isnull(row.get("Size")) else {})
        }

        query_executor.add_vertex(
            label=Organization.LABEL,
            properties=organization_properties
        )

    query_executor.force_execute()


def add_keywords(g: GraphTraversalSource, unique_keywords_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        unique_keywords_df.iterrows(),
        total=unique_keywords_df.shape[0],
        desc="Importing Keywords"
    ):
        current_time = datetime.now(timezone.utc).isoformat()

        keyword_properties = {
            Keyword.PropertyKey.UUID: row.get("Keywords"),
            Keyword.PropertyKey.NAME: row.get("Keywords"),
            **({Keyword.PropertyKey.TYPE: row["Type"]} if "Type" in unique_keywords_df.columns and not pd.isnull(row.get("Type")) else {}),
            **({Keyword.PropertyKey.DESCRIPTION: row["Description"]} if "Description" in unique_keywords_df.columns and not pd.isnull(row.get("Description")) else {}),
            **({Keyword.PropertyKey.SYNONYMS: row["Synonyms"]} if "Synonyms" in unique_keywords_df.columns and not pd.isnull(row.get("Synonyms")) else {}),
            **({Keyword.PropertyKey.INDUSTRY: row["Industry"]} if "Industry" in unique_keywords_df.columns and not pd.isnull(row.get("Industry")) else {}),
            Keyword.PropertyKey.CREATED_AT: current_time,
            Keyword.PropertyKey.LAST_UPDATED_AT: current_time
        }

        query_executor.add_vertex(
            label=Keyword.LABEL,
            properties=keyword_properties
        )

    query_executor.force_execute()

def add_zymo_products(g: GraphTraversalSource, zymo_products_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        zymo_products_df.iterrows(),
        total=zymo_products_df.shape[0],
        desc="Importing Zymo Products"
    ):
        product_properties = {
            ZymoProduct.PropertyKey.UUID: row.get("Item name/SKU#"),
            ZymoProduct.PropertyKey.NAME: row.get("Item name/SKU#"),
            ZymoProduct.PropertyKey.CATEGORY: row.get("Product Category"),
            ZymoProduct.PropertyKey.PRODUCT_CLASS: row.get("Class (no hierarchy)"),
            ZymoProduct.PropertyKey.ITEM_TYPE: row.get("Type"),
            ZymoProduct.PropertyKey.DESCRIPTION: row.get("Description"),
            ZymoProduct.PropertyKey.SHORT_DESCRIPTION: row.get("Short Description"),
            ZymoProduct.PropertyKey.BASE_PRICE: row.get("Base Price"),
            ZymoProduct.PropertyKey.SKU: row.get("Item name/SKU#"),
            ZymoProduct.PropertyKey.INACTIVE: row.get("Inactive"),
            ZymoProduct.PropertyKey.SHELF_LIFE: row.get("Shelf Life (Months)"),
            ZymoProduct.PropertyKey.STORAGE_TEMPERATURE: row.get("Storage Temperature"),
            ZymoProduct.PropertyKey.SHIPPING_TEMPERATURE: row.get("Shipping Temperature"),
            ZymoProduct.PropertyKey.FEATURES: row.get("Features"),
            ZymoProduct.PropertyKey.LENGTH: row.get("Length"),
            ZymoProduct.PropertyKey.WIDTH: row.get("Width"),
            ZymoProduct.PropertyKey.HEIGHT: row.get("Height"),
            ZymoProduct.PropertyKey.WEIGHT: row.get("Weight"),
            ZymoProduct.PropertyKey.AVAILABLE_STOCK: row.get("Available"),
            ZymoProduct.PropertyKey.SAFETY_INFORMATION: row.get("Safety Data Sheet/ MSDS URL"),
            ZymoProduct.PropertyKey.PRODUCT_URL: row.get("Product URL"),
            ZymoProduct.PropertyKey.IMAGE_URL: row.get("Link for Main image of item")
        }

        query_executor.add_vertex(
            label=ZymoProduct.LABEL,
            properties=product_properties
        )

    query_executor.force_execute()

def load_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def add_publications(g: GraphTraversalSource, publications: list):
    query_executor = BulkQueryExecutor(g, 100)

    for publication in tqdm(
        publications,
        total=len(publications),
        desc="Importing Publications"
    ):
        publication_properties = {
            Publication.PropertyKey.UUID: publication['_id']['$oid'],
            Publication.PropertyKey.TITLE: publication.get('title'),
            Publication.PropertyKey.DOI: publication.get('doi'),
            Publication.PropertyKey.PUBLICATION_DATE: publication.get('publication_date'),
            Publication.PropertyKey.AFFILIATIONS: json.dumps(publication.get('affiliations')),  # Serialize to JSON string
            Publication.PropertyKey.ABSTRACT: publication.get('abstract'),
            Publication.PropertyKey.SOURCE_NAME: publication.get('source_name'),
            Publication.PropertyKey.VOLUME: publication.get('volume'),
            Publication.PropertyKey.ISSUE: publication.get('issue'),
            Publication.PropertyKey.START_PAGE: publication.get('start_page'),
            Publication.PropertyKey.END_PAGE: publication.get('end_page'),
            Publication.PropertyKey.KEYWORDS: json.dumps(publication.get('keywords')),  # Serialize to JSON string if not None
            Publication.PropertyKey.PUBLICATION_TYPE: publication.get('publication_type'),
            Publication.PropertyKey.CITATIONS: publication.get('citations'),
            Publication.PropertyKey.REFERENCES: json.dumps(publication.get('references')),  # Serialize to JSON string
            Publication.PropertyKey.URL: publication.get('url'),
            Publication.PropertyKey.NOTES: None  # Assuming no notes for now
        }

        query_executor.add_vertex(
            label=Publication.LABEL,
            properties=publication_properties
        )

    query_executor.force_execute()

def add_publication_products(g: GraphTraversalSource, publication_products: list):
    query_executor = BulkQueryExecutor(g, 100)

    for product in tqdm(
        publication_products,
        total=len(publication_products),
        desc="Importing Publication Products"
    ):
        publication_product_properties = {
            PublicationProduct.PropertyKey.UUID: product['_id']['$oid'],
            PublicationProduct.PropertyKey.NAME: product.get("product"),
            PublicationProduct.PropertyKey.COMPANY: product.get("company"),
            PublicationProduct.PropertyKey.DISPLAY_NAME: product.get("display_name"),
            PublicationProduct.PropertyKey.PUBLICATIONS: product.get("publications"),
        }

        query_executor.add_vertex(
            label=PublicationProduct.LABEL,
            properties=publication_product_properties
        )
    
    query_executor.force_execute()

def create_id_dict(g: GraphTraversalSource, vertex_label: str, primary_property_label: str):
    node_list = (
        g.V()
        .has_label(vertex_label)
        .project("id", primary_property_label)
        .by(__.id_())
        .by(__.values(primary_property_label))
        .toList()
    )
    id_dict = {
        node[primary_property_label]: node["id"] for node in node_list
    }

    return id_dict

def add_edges_publication_product(g: GraphTraversalSource, publication_products: list):
    query_executor = BulkQueryExecutor(g, 100)

    publication_id_dict = create_id_dict(g, "publication", "doi")
    publication_product_id_dict = create_id_dict(g, "publication_product", "uuid")

    for product in tqdm(
        publication_products,
        total=len(publication_products),
        desc="Adding Publication-Products Edges"
    ):
        for doi in product["publications"]:
            publication_doi_value = doi
            publication_product_uuid_value = product['_id']['$oid']

            publication_graph_id = publication_id_dict.get(publication_doi_value)
            publication_product_graph_id = publication_product_id_dict.get(publication_product_uuid_value)

            g.V(publication_graph_id).addE("mentions").to(__.V(publication_product_graph_id)).iterate()
    
    query_executor.force_execute()

def add_edges_person_organization(
    g: GraphTraversalSource,
    prepped_person_df: pd.DataFrame,
):
    query_executor = BulkQueryExecutor(g, 100)

    person_id_dict = create_id_dict(g, "person", "uuid")
    organization_id_dict = create_id_dict(g, "organization", "uuid")

    prepped_person_df['Organization'] = prepped_person_df['Organization'].str.strip().str.lower()

    for _, row in tqdm(
        prepped_person_df.iterrows(),
        total=prepped_person_df.shape[0],
        desc="Adding person-organization edges"
    ):
        person_uuid_value = row.get("UUID")
        organization_uuid_value = row["Organization"]

        person_graph_id = person_id_dict.get(person_uuid_value)
        organization_graph_id = organization_id_dict.get(organization_uuid_value)

        g.V(organization_graph_id).addE("affiliated_with").to(__.V(person_graph_id)).iterate()

    query_executor.force_execute()


def add_edges_person_keyword(
    g: GraphTraversalSource,
    cleaned_interests_contact_df: pd.DataFrame,
):
    query_executor = BulkQueryExecutor(g, 100)
    person_id_dict = create_id_dict(g, "person", "uuid")
    keyword_id_dict = create_id_dict(g, "keyword", "uuid")

    for _, row in tqdm(
        cleaned_interests_contact_df.iterrows(),
        total=cleaned_interests_contact_df.shape[0],
        desc="Adding person-keyword edges"
    ):
        person_uuid_value = row.get("UUID")
        person_graph_id = person_id_dict.get(person_uuid_value)

        # This should be within the row loop to process each row's interests
        interests = row["Keywords"]
        keywords = [keyword.strip() for keyword in interests.split(',')]

        for keyword in keywords:
            keyword_uuid_value = keyword
            keyword_graph_id = keyword_id_dict.get(keyword_uuid_value)

            # You should check if the graph IDs exist before trying to create edges
            if person_graph_id is not None and keyword_graph_id is not None:
                g.V(person_graph_id).addE("interested_in").to(__.V(keyword_graph_id)).iterate()

    query_executor.force_execute()


def add_property_same_value(
    g: GraphTraversalSource,
    node_label: str,
    property_key: str,
    property_value: any
):
    g.V().hasLabel(node_label).property(property_key, property_value).iterate()

def add_standardized_name(g: GraphTraversalSource):
    names_and_ids = g.V().hasLabel("publication_product") \
                   .project("id", "name") \
                   .by(__.id()) \
                   .by(__.values("name")) \
                   .toList()
    
    for entry in names_and_ids:
        g.V(entry["id"]).property("standardized_name", entry["name"].lower()).iterate()


# %% QUERYING DATA:
def get_people_by_keyword(g: GraphTraversalSource, keyword: str):
    return (
        g.V()
        .has("keyword", "name", keyword)
        .bothE()
        .outV()
        .hasLabel("person")
        .valueMap()
        .dedup()
        .toList()
    )


def get_organization_by_person_name(g: GraphTraversalSource, person_name: str):
    return (
        g.V()
        .has("person", "name", person_name)
        .bothE()
        .outV()
        .hasLabel("organization")
        .valueMap(True)
        .dedup()
        .toList()
    )


# %%
# test_org = get_organization_by_person_name(g, "Gour Digpal")
# print(test_org)


# %%
def get_people_from_organization(g: GraphTraversalSource, organization_name: str):
    return (
        g.V()
        .has("organization", "name", organization_name)
        .bothE()
        .inV()
        .hasLabel("person")
        .valueMap()
        .dedup()
        .toList()
    )


# %%
def get_publications_by_product(g: GraphTraversalSource, product_name: str):
    return (
        g.V()
        .has("publication_product", "name", product_name)
        .bothE()
        .outV()
        .hasLabel("publication")
        .valueMap()
        .dedup()
        .toList()
    )


# %%
def get_people_by_publication_product(g: GraphTraversalSource, product_name: str):
    publications = (
        g.V()
        .has("publication_product", "name", product_name)
        .inE("mentions")
        .outV()
        .hasLabel("publication")
        .valueMap()
        .dedup()
        .toList()
    )

    affiliations = []

    for publication in publications:
        if 'affiliations' in publication:
            # Deserialize the JSON string if 'affiliations' is stored as a string
            if isinstance(publication['affiliations'], list) and len(publication['affiliations']) > 0:
                affiliations_json_str = publication['affiliations'][0]
                deserialized_affiliations = json.loads(affiliations_json_str)
                affiliations.extend(deserialized_affiliations)

    return affiliations


# %% Search by name for possibly multiple people to get all property values:
def get_people_by_full_name(g: GraphTraversalSource, name_value: str):
    return (
        g.V()
        .has("person", "full_name", name_value)
        .valueMap()
        .dedup()
        .toList()
    )


# %%
'''
property_label could be one of the following options:
    "name"
    "phone"
    "title"
    "mailing_address"
    "interest_areas"
    "lead_source"
    "event_name"
'''


# Search by UID for a unique person to get a specific property value:
def get_unique_person_property(g: GraphTraversalSource, uuid_value: str, property_label: str):
    return (
        g.V()
        .has("person", "uuid", uuid_value)
        .values(property_label)
        .next()
    )


# %%
# test_unique_person = get_unique_person_property(g, "craig.lower@truepill.com", "lead_source")
# print(test_unique_person)


# %% Search by name for possibly multiple people to get a specific property value:
def get_peoples_property_by_name(g: GraphTraversalSource, name_value: str, property_label: str):
    return (
        g.V()
        .has("person", "name", name_value)
        .values(property_label)
        .dedup()
        .toList()
    )


# # %%
# test_people = get_peoples_property_by_name(g, "Suisha T", "email")
# print(test_people)


# %%
def fix_property_value(
    g: GraphTraversalSource,
    label: str,
    uuid_value: str,
    target_property_key: str,
    new_value: str
):
    print(f"fixing {target_property_key} for {label}")
    return (
        g.V()
        .has(label, 'uuid', uuid_value)
        .property(Cardinality.single, target_property_key, new_value)
        .iterate()
    )


# %%
def get_path(g: GraphTraversalSource):
    return (
        g.V()
        .hasLabel("person")
        .project('person', 'keywords')
        .by('name')
        .by(__.out('interested_in')
        .hasLabel('keyword')
        .values('name')
        .fold())
    )


# # %%
# file_path = 'data/2019-2023_Leads_List_Test.csv'
# df = pd.read_csv(file_path)

# # Find duplicates in the "Full Name" column
# duplicates = df['Full Name'].duplicated(keep=False)  # Marks all duplicates as True
# duplicate_names = df.loc[duplicates, 'Full Name'].unique()  # Get the unique duplicate names

# # Print or return the list of duplicate "Full Name" values
# print(duplicate_names)


# # %% Search by UID for a unique person to get all property values:
# def get_unique_person_all_properties(g: GraphTraversalSource, uuid_value: str):
#     return (
#         g.V()
#         .has("person", "uuid", uuid_value)
#         .valueMap(True)
#         .toList()
#     )


# # %%
# test_unique_person_all_props = get_unique_person_all_properties(g, "suishal@connect.hku.hk")
# print(test_unique_person_all_props)


# # %%
# def count_unique_people(g: GraphTraversalSource):
#     return g.V().hasLabel('person').dedup().count().next()


# # %%
# person_count = count_unique_people(g)
# print(person_count)

# # %%
