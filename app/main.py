from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from gremlin_queries import count_nodes_in_db, check_node_properties, add_edges_person_keyword
import asyncio
import database_connection
import pandas as pd
import numpy as np
import requests

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
    # g = database_connection.init_gremlin_client()
    g = database_connection.get_gremlin_client()
    # contact_df = pd.read_csv("data/2019-2023_Leads_List_Test_deduped.csv")
    # contact_df['Area of Interests'].replace(["- None -", "N/A", "null"], np.nan, inplace=True)
    # cleaned_interests_contact_df = contact_df.dropna(subset=['Area of Interests']).reset_index(drop=True)
    # await add_edges_person_keyword(g, cleaned_interests_contact_df)
    # cleaned_org_contact_df = contact_df.dropna(subset=['Organization']).reset_index(drop=True)
    # await add_edges_person_organization(g, cleaned_org_contact_df)
    # contact_df['Organization'] = contact_df['Organization'].str.strip().str.lower().str.title()
    # unique_organizations_series = contact_df['Organization'].dropna().drop_duplicates().reset_index(drop=True)
    # unique_organizations_df = unique_organizations_series.to_frame()
    # await add_organizations(g, unique_organizations_df)
    # unique_keywords_df = extract_unique_keywords(contact_df)
    # await add_keywords(g, unique_keywords_df)
    global node_count
    node_count = await asyncio.to_thread(
        count_nodes_in_db, g, 'keyword'
    )
    node_properties = await asyncio.to_thread(
        check_node_properties, g, 'keyword', 'uid', 'Microbiomics'
    )
    print('NODE COUNT:', node_count)
    print('NODE PROPERTIES:', node_properties)


@app.on_event("shutdown")
async def shutdown():
    database_connection.close_gremlin_client()
