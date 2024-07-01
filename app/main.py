from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from gremlin_queries import get_path, fix_property_value, add_people, get_people_from_organization, get_people_by_full_name, count_nodes_in_db, drop_nodes, drop_edges, check_node_properties, add_edges_person_keyword, add_edges_person_organization, count_edges_in_db, get_names, get_people_by_keyword
import asyncio
import database_connection
# import pandas as pd
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
    # contact_df = pd.read_csv("data/updated_2019-2023_Leads_List_Test_deduped.csv")
    # NODE CREATION:
    # add_people(g, contact_df)

    # contact_df['Organization'] = contact_df['Organization'].str.strip().str.lower().str.title()
    # unique_organizations_series = contact_df['Organization'].dropna().drop_duplicates().reset_index(drop=True)
    # unique_organizations_df = unique_organizations_series.to_frame()
    # add_organizations(g, unique_organizations_df)

    # unique_keywords_df = extract_unique_keywords(contact_df)
    # add_keywords(g, unique_keywords_df)

    # EDGE CREATION:
    # contact_df['Area of Interests'].replace(["- None -", "N/A", "null"], np.nan, inplace=True)
    # cleaned_interests_contact_df = contact_df.dropna(subset=['Area of Interests']).reset_index(drop=True)
    # print(cleaned_interests_contact_df)
    # cleaned_interests_contact_df.to_csv('./data/cleaned_interests_contacts.csv', index=False)
    # add_edges_person_keyword(g, cleaned_interests_contact_df)

    # cleaned_org_contact_df = contact_df.dropna(subset=['Organization']).reset_index(drop=True)
    # add_edges_person_organization(g, cleaned_org_contact_df)

    # global node_count
    # global edge_count
    # global name_list
    # global query_result
    # global function_result

    # function_result = await asyncio.to_thread(
    #     fix_property_value, g, 'person', 'ngreenidge26@gmail.com', 'title', 'Sr. Manager Quality Assurance and Regulatory Affair'
    # )
    query_result = await asyncio.to_thread(
        get_path, g
    )
    # name_list = await asyncio.to_thread(
    #     get_names, g, 'organization'
    # )

    # DROP NODES/EDGES BY LABEL:
    # await asyncio.to_thread(
    #     drop_nodes, g, 'person'
    # )
    # await asyncio.to_thread(
    #     drop_edges, g, 'affiliated_with'
    # )

    node_count = await asyncio.to_thread(
        count_nodes_in_db, g, 'person'
    )
    edge_count = await asyncio.to_thread(
        count_edges_in_db, g, 'affiliated_with'
    )
    node_properties = await asyncio.to_thread(
        check_node_properties, g, 'person', 'uid', 'ngreenidge26@gmail.com'
    )
    # node_properties = await asyncio.to_thread(
    #     check_node_properties, g, 'organization', 'name', 'Seegene Inc.'
    # )
    print('QUERY RESULT:', query_result)
    print('NODE COUNT:', node_count)
    print('EDGE COUNT:', edge_count)
    # print('NAME LIST:', name_list)
    print('NODE PROPERTIES:', node_properties)


@app.on_event("shutdown")
async def shutdown():
    database_connection.close_gremlin_client()
