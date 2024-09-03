from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from gremlin_queries import load_json_file, add_edges_publication_keyword, add_individual_keyword, add_people, add_keywords, add_organizations, add_zymo_products, add_publications, add_publication_products, add_edges_publication_product, add_standardized_name, get_publication_products_by_keyword, get_publications_by_keyword, get_organizations_by_keyword, get_publications_by_product, get_people_by_publication_product, get_people_from_organization, get_people_by_full_name, count_nodes_in_db, drop_nodes, drop_edges, drop_specific_node, drop_specific_edge, check_node_properties, add_edges_person_keyword, add_edges_person_organization, count_edges_in_db, count_specific_nodes_in_db, count_specific_edges_in_db, get_names, get_people_by_keyword
import asyncio
import database_connection
import pandas as pd
# import numpy as np
# import requests

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.on_event("startup")
async def app_startup():
    # Async operations that should be run when the app starts
    # Connect to local Gremlin Server:
    # g = database_connection.init_gremlin_client()
    # Connect to AWS Neptune:
    g = database_connection.get_gremlin_client()
    
    # NODE CREATION:
    file_name = 'lead_scoring_data_2021-24.csv'
    
    # person_df = pd.read_csv("data/prepped_" + file_name)
    # add_people(g, person_df)

    # unique_organizations_df = pd.read_csv('data/organization_list_' + file_name)
    # add_organizations(g, unique_organizations_df)

    # unique_keywords_df = pd.read_csv('data/keyword_list_' + file_name)
    # add_keywords(g, unique_keywords_df)

    # zymo_products_df = pd.read_csv('data/merged_netsuite_products.csv')
    # add_products(g, zymo_products_df)

    # file_path = 'data/article_metadata.json'
    # publications = load_json_file(file_path)
    # add_publications(g, publications)

    # file_path = 'data/lowercase_product_data.json'
    # publication_products = load_json_file(file_path)
    # add_publication_products(g, publication_products)

    # add_individual_keyword(g, "Lead Scores")

    # EDGE CREATION:
    # cleaned_keyword_person_df = pd.read_csv('data/cleaned_keyword_' + file_name)
    # print(cleaned_keyword_person_df)
    # add_edges_person_keyword(g, cleaned_keyword_person_df)

    keyword_added_person_df = pd.read_csv('data/keyword_added_' + 'keyword' + '_' + file_name)
    print (keyword_added_person_df)
    add_edges_person_keyword(g, keyword_added_person_df)

    # cleaned_organization_person_df = pd.read_csv('data/cleaned_organization_' + file_name)
    # add_edges_person_organization(g, cleaned_organization_person_df)

    # file_path = 'data/lowercase_product_data.json'
    # publication_products = load_json_file(file_path)
    # add_edges_publication_product(g, publication_products)

    # file_path = 'data/article_metadata.json'
    # publications = load_json_file(file_path)
    # add_edges_publication_keyword(g, publications)

    # QUERYING/TESTING:
    # global node_count
    # global edge_count
    # global name_list
    # global query_result
    # global function_result

    # function_result = await asyncio.to_thread(
    #     fix_property_value, g, 'person', 'ngreenidge26@gmail.com', 'title', 'Sr. Manager Quality Assurance and Regulatory Affair'
    # )
    # function_result = await asyncio.to_thread(
    #     add_standardized_name, g
    # )
    # query_result = await asyncio.to_thread(
    #     get_publications_by_product, g, "qiagen rneasy mini kit"
    # )
    # query_result = await asyncio.to_thread(
    #     get_publication_products_by_keyword, g, "Microbiomics"
    # )
    # query_result = await asyncio.to_thread(
    #     get_people_from_organization, g, "Ambry Genetics"
    # )
    # name_list = await asyncio.to_thread(
    #     get_names, g, 'publication_product'
    # )

    # DROP NODES/EDGES BY LABEL:
    # await asyncio.to_thread(
    #     drop_nodes, g, 'publication'
    # )
    # await asyncio.to_thread(
    #     drop_edges, g, 'relates to'
    # )
    # await asyncio.to_thread(
    #     drop_specific_node, g, 'person', 'ingestion_tag', 'Lead Scores'
    # )
    # await asyncio.to_thread(
    #     drop_specific_edge, g, 'interested_in', 'has_lead_scores', 'yes'
    # )

    # TESTING:
    # node_count = await asyncio.to_thread(
    #     count_nodes_in_db, g, 'person'
    # )
    # edge_count = await asyncio.to_thread(
    #     count_edges_in_db, g, 'relates to'
    # )
    # edge_count = await asyncio.to_thread(
    #     count_edges_in_db, g, 'interested_in'
    # )
    # edge_count = await asyncio.to_thread(
    #     count_edges_in_db, g, 'mentions'
    # )
    specific_edge_count = await asyncio.to_thread(
        count_specific_edges_in_db, g, "interested_in", "has_lead_scores", "yes"
    )
    node_properties = await asyncio.to_thread(
        check_node_properties, g, 'person', 'email', 'liangqiongqiong@excellbio.com'
    )
    # node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'organization', 'display_name', 'SML Genetree Co. Ltd'
    # )
    # node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'keyword', 'name', "Lead Scores"
    # )
    # node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'zymo_product', 'name', 'A4001-50'
    # )
    # publication_node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'publication', 'doi', '10.1038/s41589-024-01685-3'
    # )
    # publication_product_node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'publication_product', 'name', 'qiagen rneasy mini kit'
    # )
    # print('QUERY RESULT:', query_result)
    # print('NODE COUNT:', node_count)
    # print('EDGE COUNT:', edge_count)
    print('SPECIFIC EDGE COUNT:', specific_edge_count)
    # print('NAME LIST:', name_list)
    print('NODE PROPERTIES:', node_properties)
    # print('NODE PROPERTIES:', publication_node_properties)
    # print('NODE PROPERTIES:', publication_product_node_properties)

@app.on_event("shutdown")
async def shutdown():
    database_connection.close_gremlin_client()
