# %%
from typing import List
import pandas as pd
import database
from gremlin_python.process.graph_traversal import GraphTraversalSource, __
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


# %%
def count_nodes_in_db(g: GraphTraversalSource, label: str):
    node_count = g.V().hasLabel(label).count().next()
    return node_count


# %%
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


# %%
def add_people(g: GraphTraversalSource, contact_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        contact_df.iterrows,
        total=contact_df.shape[0],
        desc="Importing People"
    ):
        uid_value = row["Email"] if pd.notnull(row["Email"]) else "_".join([row["Full Name"], row["Organization"]]).replace(" ", "_").lower()
        
        person_properties = {
            Person.PropertyKey.UID: uid_value,
            Person.PropertyKey.NAME: row.get("Full Name", None),
            Person.PropertyKey.EMAIL: row.get("Email", None),
            Person.PropertyKey.PHONE: row.get("Phone", None),
            Person.PropertyKey.TITLE: row.get("Title", None),
            Person.PropertyKey.INTEREST_AREAS: row.get("Areas of Interests", None),
            Person.PropertyKey.LEAD_SOURCE: row.get("Lead Source", None),
            Person.PropertyKey.EVENT_NAME: row.get("Event Name", None)
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


def add_organization(g: GraphTraversalSource, contact_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        contact_df.iterrows,
        total=contact_df.shape[0],
        desc="Importing Organization"
    ):
        if pd.notnull(row["Email"]):
            domain_value = row["Email"].split("@")[-1].lower()
            acronym_value = domain_value.split(".")[0]
        else:
            domain_value = None
            acronym_value = None

        organization_properties = {
            Organization.PropertyKey.UID: acronym_value,
            Organization.PropertyKey.NAME: row["Organization"],
            Organization.PropertyKey.DOMAIN: domain_value,
            Organization.PropertyKey.ACRONYM: acronym_value,
            ** ({Organization.PropertyKey.MAILING_ADDRESS: row["Organization's Mailing Address"]} if "Organization's Mailing Address" in contact_df.columns and not pd.isnull(row.get("Organization's Mailing Address")) else {})
        }

        query_executor.add_vertex(
            label=Organization.LABEL,
            properties=organization_properties
        )
    
