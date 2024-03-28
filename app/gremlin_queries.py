# %%
import pandas as pd
import database
import ssl
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
from data_objects import Person, Organization

import nest_asyncio
nest_asyncio.apply()

# Path to the exported server certificate
cert_path = '/Users/nicoletrieu/server-cert.pem'

# Create an SSL context that trusts the server's certificate
ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
ssl_context.load_verify_locations(cert_path)

# Use the SSL context in the DriverRemoteConnection
connection = DriverRemoteConnection('wss://localhost:8182/gremlin', 'g', ssl_context=ssl_context)
g = Graph().traversal().withRemote(connection)


# %% IMPORTING DATA:
def count_nodes_in_db(g: GraphTraversalSource, label: str):
    node_count = g.V().hasLabel(label).count().next()
    return node_count


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
        person_uid_value = row["Email"] if pd.notnull(row["Email"]) else "_".join([row["Full Name"], row["Organization"]]).replace(" ", "_").lower()

        person_properties = {
            Person.PropertyKey.UID: person_uid_value,
            Person.PropertyKey.NAME: row.get("Full Name", None),
            Person.PropertyKey.EMAIL: row.get("Email", None),
            Person.PropertyKey.PHONE: row.get("Phone", None),
            Person.PropertyKey.TITLE: row.get("Title", None),
            Person.PropertyKey.INTEREST_AREAS: row.get("Area of Interests", None),
            Person.PropertyKey.LEAD_SOURCE: row.get("Lead Source", None),
            Person.PropertyKey.EVENT_NAME: row.get("Event Name", None),
            ** ({Person.PropertyKey.MAILING_ADDRESS: row["Lead's Mailing Address"]} if "Lead's Mailing Address" in contact_df.columns and not pd.isnull(row.get("Lead's Mailing Address")) else {})
        }

        query_executor.add_vertex(
            label=Person.LABEL,
            properties=person_properties
        )

    query_executor.force_execute()

    person_node_list = (
        g.V()
        .has_label("person")
        .project("id", "uid")
        .by(__.id_())
        .by(__.values("uid"))
        .to_list()
    )
    person_id_dict = {
        person_node["uid"]: person_node["id"] for person_node in person_node_list
    }

    return person_id_dict


def add_organizations(g: GraphTraversalSource, organization_df: pd.DataFrame):  # organization_df derived from 'Organization' column of contact_df
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        organization_df.iterrows(),
        total=organization_df.shape[0],
        desc="Importing Organizations"
    ):
        # if pd.notnull(row["Email"]):
        #     domain_value = row["Email"].split("@")[-1].lower()
        #     acronym_value = domain_value.split(".")[0]
        # else:
        #     domain_value = None
        #     acronym_value = None

        # if pd.notnull(row["Organization"]):
        #     organization_name_value = row["Organization"].strip().lower().capitalize()
        # else:
        #     organization_name_value = "N/A"

        organization_name_value = row.get("Organization").strip().lower().capitalize()  # Normalize the organization name

        organization_properties = {
            Organization.PropertyKey.UID: organization_name_value,
            Organization.PropertyKey.NAME: organization_name_value,
            # Organization.PropertyKey.DOMAIN: domain_value,
            # Organization.PropertyKey.ACRONYM: acronym_value,
            ** ({Organization.PropertyKey.MAILING_ADDRESS: row["Organization's Mailing Address"]} if "Organization's Mailing Address" in organization_df.columns and not pd.isnull(row.get("Organization's Mailing Address")) else {})
        }

        query_executor.add_vertex(
            label=Organization.LABEL,
            properties=organization_properties
        )

    query_executor.force_execute()

    organization_node_list = (
        g.V()
        .has_label("organization")
        .project("id", "uid")
        .by(__.id_())
        .by(__.values("uid"))
        .to_list()
    )
    organization_id_dict = {
        organization_node["uid"]: organization_node["id"] for organization_node in organization_node_list
    }

    return organization_id_dict


def add_keywords(g: GraphTraversalSource, keyword_df: pd.DataFrame):  # keyword_df derived from 'Area of Interests' column of contact_df
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        keyword_df.iterrows(),
        total=keyword_df.shape[0],
        desc="Importing Keywords"
    ):
        keyword_name_value = row.get("Area of Interests")


def add_edges_person_organization(
    g: GraphTraversalSource,
    cleaned_df: pd.DataFrame,
    person_id_dict: dict,
    organization_id_dict: dict
):
    query_executor = BulkQueryExecutor(g, 100)

    for _, row in tqdm(
        cleaned_df.iterrows(),
        total=cleaned_df.shape[0],
        desc="Adding edges"
    ):
        person_uid_value = row["Email"] if pd.notnull(row["Email"]) else "_".join([row["Full Name"], row["Organization"]]).replace(" ", "_").lower()
        organization_uid_value = row.get("Organization").strip().lower().capitalize()

        person_graph_id = person_id_dict.get(person_uid_value)
        organization_graph_id = organization_id_dict.get(organization_uid_value)

        g.V(organization_graph_id).addE("affiliated with").to(__.V(person_graph_id)).iterate()

    query_executor.force_execute()


# %%
contact_df = pd.read_csv("data/2019-2023_Leads_List_Test_deduped.csv")

# %%
keywords = contact_df["Area of Interests"][18]
print(keywords)
print(type(keywords))

# %%
contact_df['Organization'] = contact_df['Organization'].str.strip().str.lower().str.title()

# Remove duplicates and drop rows with missing 'Organization' values
unique_organizations_series = contact_df['Organization'].dropna().drop_duplicates().reset_index(drop=True)

# Convert Pandas Series to DataFrame
unique_organizations_df = unique_organizations_series.to_frame()

# Display the DataFrame with unique organization names
print("unique_organizations_df:\n", unique_organizations_df)

# %% DataFrame containing organization names for the purpose of edge creation between "person" and "organization" nodes:
cleaned_contact_df = contact_df.dropna(subset=['Organization']).reset_index(drop=True)
print("cleaned_contact_df:\n", cleaned_contact_df)

# %%
person_id_dict = add_people(g, contact_df)

# %%
organization_id_dict = add_organizations(g, unique_organizations_df)

# %%
add_edges_person_organization(g, cleaned_contact_df, person_id_dict, organization_id_dict)

# %% TESTING NODES:
g.V().drop().iterate()
# g.E().drop().iterate()

# %%
g.V().count().next()
# g.E().count().next()

# %%
check_node_properties(g, "person", "name", "Baback Gharizadeh")

# %%
check_node_properties(g, "organization", "name", "Truepill")


# %% QUERYING DATA:
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
test_org = get_organization_by_person_name(g, "Gour Digpal")
print(test_org)


# %%
def get_people_from_organization(g: GraphTraversalSource, organization_name: str):
    return (
        g.V()
        .has("organization", "name", organization_name)
        .bothE()
        .inV()
        .hasLabel("person")
        .valueMap(True)
        .dedup()
        .toList()
    )


# %%
test_people = get_people_from_organization(g, "Truepill")
print(test_people)


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
test_unique_person = get_unique_person_property(g, "craig.lower@truepill.com", "lead_source")
print(test_unique_person)


# %% Search by name for possibly multiple people to get a specific property value:
def get_peoples_property_by_name(g: GraphTraversalSource, name_value: str, property_label: str):
    return (
        g.V()
        .has("person", "name", name_value)
        .values(property_label)
        .dedup()
        .toList()
    )


# %%
test_people = get_peoples_property_by_name(g, "Suisha T", "email")
print(test_people)

# %%
file_path = 'data/2019-2023_Leads_List_Test.csv'
df = pd.read_csv(file_path)

# Find duplicates in the "Full Name" column
duplicates = df['Full Name'].duplicated(keep=False)  # Marks all duplicates as True
duplicate_names = df.loc[duplicates, 'Full Name'].unique()  # Get the unique duplicate names

# Print or return the list of duplicate "Full Name" values
print(duplicate_names)


# %% Search by UID for a unique person to get all property values:
def get_unique_person_all_properties(g: GraphTraversalSource, uid_value: str):
    return (
        g.V()
        .has("person", "uid", uid_value)
        .valueMap(True)
        .toList()
    )


# %%
test_unique_person_all_props = get_unique_person_all_properties(g, "suishal@connect.hku.hk")
print(test_unique_person_all_props)


# %%
def count_unique_people(g: GraphTraversalSource):
    return g.V().hasLabel('person').dedup().count().next()


# %%
person_count = count_unique_people(g)
print(person_count)

# %%
