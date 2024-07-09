# %%
import pandas as pd
import numpy as np
import database
import ssl
import pickle
import os
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
from data_objects import Person, Organization, Keyword

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


def create_id_dict(g: GraphTraversalSource, vertex_label: str):
    node_list = (
        g.V()
        .has_label(vertex_label)
        .project("id", "uuid")
        .by(__.id_())
        .by(__.values("uuid"))
        .toList()
    )
    id_dict = {
        node["uuid"]: node["id"] for node in node_list
    }

    return id_dict


def add_edges_person_organization(
    g: GraphTraversalSource,
    prepped_person_df: pd.DataFrame,
):
    query_executor = BulkQueryExecutor(g, 100)

    person_id_dict = create_id_dict(g, "person")
    organization_id_dict = create_id_dict(g, "organization")

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
    person_id_dict = create_id_dict(g, "person")
    keyword_id_dict = create_id_dict(g, "keyword")

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
# test_people = get_people_from_organization(g, "Truepill")
# print(test_people)


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
