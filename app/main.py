from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from gremlin_queries import load_json_file, add_edges_person_marketing_campaign, add_edges_publication_keyword, add_edges_person_keyword_parallel, add_edges_marketing_campaign_keyword, add_individual_keyword, add_marketing_campaigns, add_people, add_keywords, add_organizations, add_zymo_products, add_publications, add_publication_products, add_edges_publication_product, add_standardized_name, get_marketing_campaigns_by_keyword, get_publication_products_by_keyword, get_publications_by_keyword, get_organizations_by_keyword, get_publications_by_product, get_people_by_publication_product, get_people_from_organization, get_people_by_full_name, count_nodes_in_db, count_people_by_keyword, drop_nodes, drop_edges, drop_specific_node, drop_specific_edge, check_node_properties, add_edges_person_keyword, add_edges_person_organization, count_edges_in_db, count_specific_nodes_in_db, count_specific_edges_in_db, get_names, get_people_by_keyword
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
    file_path = 'data/klaviyo'
    file_name = 'recipients_30YA_sep_promo.csv'

    # marketing_campaigns_df = pd.read_csv('data/klaviyo/prepped_all_sent_campaigns.csv')
    # print(marketing_campaigns_df)
    # add_marketing_campaigns(g, marketing_campaigns_df)
    
    person_df = pd.read_csv(f'{file_path}/campaign_added_person_node_{file_name}')
    # test_row = person_df.iloc[0:1]
    # print(test_row)
    # print(person_df)
    # add_people(g, person_df)
    # add_people(g, test_row)

    # unique_organizations_df = pd.read_csv(file_path + 'organization_list_' + file_name)
    # add_organizations(g, unique_organizations_df)

    # unique_keywords_df = pd.read_csv(file_path + 'keyword_list_' + file_name)
    # add_keywords(g, unique_keywords_df)

    # zymo_products_df = pd.read_csv(file_path + 'merged_netsuite_products.csv')
    # add_products(g, zymo_products_df)

    # file_path = 'data/alps_scraping/article_metadata.json'
    # publications = load_json_file(file_path)
    # add_publications(g, publications)

    # file_path = 'data/alps_scraping/lowercase_product_data.json'
    # publication_products = load_json_file(file_path)
    # add_publication_products(g, publication_products)

    # new_keywords = ["Klaviyo", "Emails", "Marketing", "Marketing Campaigns", "Email Marketing Data"]
    # for keyword in new_keywords:
    #     add_individual_keyword(g, keyword)

    # EDGE CREATION:

    # person_marketing_campaign_df = pd.read_csv(f'{file_path}/campaign_added_person_node_{file_name}')
    # print(person_marketing_campaign_df)
    # add_edges_person_marketing_campaign(g, person_marketing_campaign_df)
    # test_row = person_marketing_campaign_df.iloc[0:1]  # testing with individual row(s)
    # print(test_row)
    # add_edges_person_marketing_campaign(g, test_row)

    # marketing_campaign_keyword_df = pd.read_csv(f'{file_path}/keyword_added_marketing_campaign_node_{file_name}')
    # print(marketing_campaign_df)
    # add_edges_marketing_campaign_keyword(g, marketing_campaign_keyword_df)

    # cleaned_keyword_person_df = pd.read_csv(file_path + 'cleaned_keyword_' + file_name)
    # print(cleaned_keyword_person_df)
    # add_edges_person_keyword(g, cleaned_keyword_person_df)

    add_edges_person_keyword(g, person_df)
    # add_edges_person_keyword(g, test_row)
    # add_edges_person_keyword_parallel(g, test_row)

    # cleaned_organization_person_df = pd.read_csv(file_path + 'cleaned_organization_' + file_name)
    # add_edges_person_organization(g, cleaned_organization_person_df)

    # file_path = 'data/alps_scraping/lowercase_product_data.json'
    # publication_products = load_json_file(file_path)
    # add_edges_publication_product(g, publication_products)

    # file_path = 'data/alps_scraping/article_metadata.json'
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
    #     get_marketing_campaigns_by_keyword, g, "Klaviyo"
    # )
    # query_result = await asyncio.to_thread(
    #     get_people_by_keyword, g, "Lead Scores"
    # )
    # name_list = await asyncio.to_thread(
    #     get_names, g, 'publication_product'
    # )

    # DROP NODES/EDGES BY LABEL:
    # await asyncio.to_thread(
    #     drop_nodes, g, 'marketing_campaign'
    # )
    # await asyncio.to_thread(
    #     drop_edges, g, 'is_recipient_of'
    # )
    # await asyncio.to_thread(
    #     drop_specific_node, g, 'person', 'ingestion_tag', 'klaviyo_30YA_sep_promo'
    # )
    # await asyncio.to_thread(
    #     drop_specific_edge, g, 'interested_in', 'has_klaviyo_data', 'yes'
    # )

    # TESTING:
    # node_count = await asyncio.to_thread(
    #     count_nodes_in_db, g, 'marketing_campaign'
    # )
    # edge_count = await asyncio.to_thread(
    #     count_edges_in_db, g, 'relates to'
    # )
    # edge_count = await asyncio.to_thread(
    #     count_edges_in_db, g, 'interested_in'
    # )
    # edge_count = await asyncio.to_thread(
    #     count_edges_in_db, g, 'is_recipient_of'
    # )

    specific_node_count = count_specific_nodes_in_db(g, "person", "ingestion_tag", "klaviyo_30YA_sep_promo")

    # people_by_lead_scores_count = count_people_by_keyword(g, "Lead Scores")

    specific_edge_count = await asyncio.to_thread(
        count_specific_edges_in_db, g, "interested_in", "has_klaviyo_data", "yes"
    )
    node_properties = await asyncio.to_thread(
        check_node_properties, g, 'person', 'email', '004953230@coyote.csusb.edu'
    )
    # node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'organization', 'display_name', 'SML Genetree Co. Ltd'
    # )
    # node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'keyword', 'name', "Klaviyo"
    # )
    # marketing_campaign_node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'marketing_campaign', 'uuid', '01J722YH98T4N9Y67S65HBDKGE'
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
    # print('LEAD SCORES PEOPLE COUNT:', people_by_lead_scores_count)
    print('\nSPECIFIC EDGE COUNT:', specific_edge_count)
    print('\nSPECIFIC NODE COUNT:', specific_node_count)
    # print('NAME LIST:', name_list)
    print('\nNODE PROPERTIES:', node_properties)
    # print('\nMARKETING CAMPAIGN NODE PROPERTIES:', marketing_campaign_node_properties)
    # print('NODE PROPERTIES:', publication_product_node_properties)

@app.on_event("shutdown")
async def shutdown():
    database_connection.close_gremlin_client()
