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


def create_id_dict(g: GraphTraversalSource, vertex_label: str):
    node_list = (
        g.V()
        .has_label(vertex_label)
        .project("id", "uid")
        .by(__.id_())
        .by(__.values("uid"))
        .to_list()
    )
    id_dict = {
        node["uid"]: node["id"] for node in node_list
    }

    return id_dict


def add_people(g: GraphTraversalSource, contact_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        contact_df.iterrows(),
        total=contact_df.shape[0],
        desc="Importing People"
    ):
        person_uid_value = row["Email"] if pd.notnull(row["Email"]) else "_".join([row["Full Name"], row["Organization"]]).replace(" ", "_").lower()

        person_properties = {
            Person.PropertyKey.UUID: person_uid_value,
            Person.PropertyKey.FIRST_NAME: row.get("First Name", None),
            Person.PropertyKey.LAST_NAME: row.get("Last Name", None),
            Person.PropertyKey.FULL_NAME: row.get("Full Name", None),
            Person.PropertyKey.EMAIL: row.get("Email", None),
            Person.PropertyKey.PHONE: row.get("Phone", None),
            Person.PropertyKey.TITLE: row.get("Title", None),
            Person.PropertyKey.ORGANIZATION: row.get("Organization", None),
            Person.PropertyKey.INTEREST_AREAS: row.get("Area of Interests", None),
            ** ({Person.PropertyKey.LEAD_SOURCE: row["Lead Source"]} if "Lead Source" in contact_df.columns and not pd.isnull(row.get("Lead Source")) else {}),
            ** ({Person.PropertyKey.EVENT_NAME: row["Event Name"]} if "Event Name" in contact_df.columns and not pd.isnull(row.get("Event Name")) else {}),
            ** ({Person.PropertyKey.MAILING_ADDRESS: row["Mailing Address"]} if "Mailing Address" in contact_df.columns and not pd.isnull(row.get("Mailing Address")) else {}),
            Person.PropertyKey.INGESTION_TAG: row.get("Ingestion Tag", None)
        }

        query_executor.add_vertex(
            label=Person.LABEL,
            properties=person_properties
        )

    query_executor.force_execute()


def extract_unique_organizations(contact_df: pd.DataFrame):
    contact_df['Organization'] = contact_df['Organization'].str.strip().str.lower().str.title()
    unique_organizations_series = contact_df['Organization'].dropna().drop_duplicates().reset_index(drop=True)
    unique_organizations_df = unique_organizations_series.to_frame()
    print("unique_organizations_df:\n", unique_organizations_df)
    return unique_organizations_df


def add_organizations(g: GraphTraversalSource, unique_organizations_df: pd.DataFrame):  # organization_df derived from 'Organization' column of contact_df
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        unique_organizations_df.iterrows(),
        total=unique_organizations_df.shape[0],
        desc="Importing Organizations"
    ):
        organization_uid_value = row.get("Organization").strip().lower().capitalize()  # Normalize the organization name

        organization_properties = {
            Organization.PropertyKey.UID: organization_uid_value,
            Organization.PropertyKey.NAME: row.get("Organization"),
            # Organization.PropertyKey.DOMAIN: domain_value,
            # Organization.PropertyKey.ACRONYM: acronym_value,
            ** ({Organization.PropertyKey.MAILING_ADDRESS: row["Organization's Mailing Address"]} if "Organization's Mailing Address" in unique_organizations_df.columns and not pd.isnull(row.get("Organization's Mailing Address")) else {})
        }

        query_executor.add_vertex(
            label=Organization.LABEL,
            properties=organization_properties
        )

    query_executor.force_execute()


def process_keywords(interests_row_value: str) -> list:
    keywords_to_be_added = []

    if ',' in interests_row_value:
        keywords_to_be_added.extend([keyword.strip() for keyword in interests_row_value.split(',')])
    else:
        keywords_to_be_added(interests_row_value.strip())

    return keywords_to_be_added


def extract_unique_keywords(contact_df: pd.DataFrame):
    ignore_list = ["- None -", "N/A", "null"]

    all_keywords = []
    for interests in contact_df["Area of Interests"].dropna():
        keywords = [keyword.strip() for keyword in interests.split(',') if keyword.strip() not in ignore_list]
        all_keywords.extend(keywords)

    unique_keywords = list(set(all_keywords))

    unique_keywords_df = pd.DataFrame(unique_keywords, columns=["Keyword"])

    return unique_keywords_df


def add_keywords(g: GraphTraversalSource, unique_keywords_df: pd.DataFrame):  # keyword_df derived from 'Area of Interests' column of contact_df
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        unique_keywords_df.iterrows(),
        total=unique_keywords_df.shape[0],
        desc="Importing Keywords"
    ):
        keyword_name_value = row.get("Keyword")
        keyword_properties = {
            Keyword.PropertyKey.UID: keyword_name_value,
            Keyword.PropertyKey.NAME: keyword_name_value
        }

        query_executor.add_vertex(
            label=Keyword.LABEL,
            properties=keyword_properties
        )

    query_executor.force_execute()


def add_edges_person_organization(
    g: GraphTraversalSource,
    cleaned_org_contact_df: pd.DataFrame,
):
    query_executor = BulkQueryExecutor(g, 100)

    person_id_dict = create_id_dict(g, "person")
    organization_id_dict = create_id_dict(g, "organization")

    for _, row in tqdm(
        cleaned_org_contact_df.iterrows(),
        total=cleaned_org_contact_df.shape[0],
        desc="Adding person-organization edges"
    ):
        person_uid_value = row["Email"] if pd.notnull(row["Email"]) else "_".join([row["Full Name"], row["Organization"]]).replace(" ", "_").lower()
        organization_uid_value = row.get("Organization").strip().lower().capitalize()

        person_graph_id = person_id_dict.get(person_uid_value)
        organization_graph_id = organization_id_dict.get(organization_uid_value)

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
        person_uid_value = row["Email"] if pd.notnull(row["Email"]) else "_".join([row["Full Name"], row["Organization"]]).replace(" ", "_").lower()
        person_graph_id = person_id_dict.get(person_uid_value)

        # This should be within the row loop to process each row's interests
        interests = row["Area of Interests"]
        keywords = [keyword.strip() for keyword in interests.split(',')]

        for keyword in keywords:
            keyword_uid_value = keyword
            keyword_graph_id = keyword_id_dict.get(keyword_uid_value)

            # You should check if the graph IDs exist before trying to create edges
            if person_graph_id is not None and keyword_graph_id is not None:
                g.V(person_graph_id).addE("interested_in").to(__.V(keyword_graph_id)).iterate()

    query_executor.force_execute()


# # %%
# contact_df = pd.read_csv("data/2019-2023_Leads_List_Test_deduped.csv")

# # %%
# keywords = contact_df["Area of Interests"][18]
# # print(keywords)
# # print(type(keywords))
# processed_keywords = process_keywords(keywords)
# print(processed_keywords)

# # %%
# test_contact_df = pd.DataFrame({
#     "Area of Interests": [
#         "Sample Collection",
#         "Assay Development, Epigenetics/NGS/RNA-Seq, Microbiomics, Sample Collection",
#         None,  # Example of a row with no interests
#         "- None -",
#         "Microbiomics, Epigenetics/NGS/RNA-Seq",  # Duplicates to show removal
#     ]
# })

# unique_keywords_df = extract_unique_keywords(test_contact_df)
# print(unique_keywords_df)

# # %%
# unique_keywords_df = extract_unique_keywords(contact_df)
# print(unique_keywords_df)

# # %%
# contact_df['Organization'] = contact_df['Organization'].str.strip().str.lower().str.title()

# # Remove duplicates and drop rows with missing 'Organization' values
# unique_organizations_series = contact_df['Organization'].dropna().drop_duplicates().reset_index(drop=True)

# # Convert Pandas Series to DataFrame
# unique_organizations_df = unique_organizations_series.to_frame()
# print("unique_organizations_df:\n", unique_organizations_df)

# # %% DataFrame containing non-null organization names for the purpose of edge creation between "person" and "organization" nodes:
# cleaned_org_contact_df = contact_df.dropna(subset=['Organization']).reset_index(drop=True)
# print("cleaned_org_contact_df:\n", cleaned_org_contact_df)

# # %%
# # %% DataFrame containing non-null "Area of Interests" values for the purpose of edge creation between "person" and "keyword" nodes:
# # Replace "- None -", "N/A", "null" with numpy.nan
# contact_df['Area of Interests'].replace(["- None -", "N/A", "null"], np.nan, inplace=True)
# cleaned_interests_contact_df = contact_df.dropna(subset=['Area of Interests']).reset_index(drop=True)
# print("cleaned_interests_contact_df:\n", cleaned_interests_contact_df)

# # %% VERTEX CREATION:
# person_id_dict = add_people(g, contact_df)

# # %%
# file_path = './dicts/person_id_dict.pickle'
# with open(file_path, 'wb') as handle:
#     pickle.dump(person_id_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

# # %%
# organization_id_dict = add_organizations(g, unique_organizations_df)

# # %%
# file_path = './dicts/organization_id_dict.pickle'
# with open(file_path, 'wb') as handle:
#     pickle.dump(organization_id_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

# # %%
# keyword_id_dict = add_keywords(g, unique_keywords_df)
# print(keyword_id_dict)

# # %%
# file_path = './dicts/keyword_id_dict.pickle'
# with open(file_path, 'wb') as handle:
#     pickle.dump(keyword_id_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

# # %% EDGE CREATION:
# add_edges_person_keyword(g, cleaned_interests_contact_df, person_id_dict, keyword_id_dict)

# # %%
# add_edges_person_organization(g, cleaned_org_contact_df, person_id_dict, organization_id_dict)

# # %% TESTING NODES:
# # g.V().drop().iterate()
# g.E().drop().iterate()

# # %%
# # g.V().count().next()
# g.E().count().next()

# # %%
# check_node_properties(g, "person", "name", "Baback Gharizadeh")

# # %%
# check_node_properties(g, "organization", "name", "Truepill")

# # %%
# check_node_properties(g, "keyword", "name", "Sample Collection")  # DEBUG NEEDED: 'connected_nodes': []


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
def get_unique_person_property(g: GraphTraversalSource, uid_value: str, property_label: str):
    return (
        g.V()
        .has("person", "uid", uid_value)
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
    uid_value: str,
    target_property_key: str,
    new_value: str
):
    print(f"fixing {target_property_key} for {label}")
    return (
        g.V()
        .has(label, 'uid', uid_value)
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
# def get_unique_person_all_properties(g: GraphTraversalSource, uid_value: str):
#     return (
#         g.V()
#         .has("person", "uid", uid_value)
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
