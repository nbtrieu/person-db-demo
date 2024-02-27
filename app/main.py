from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from gremlin_queries import count_nodes_in_db, check_node_properties
import asyncio
import database_connection
import pandas as pd
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
    global node_count
    node_count = await asyncio.to_thread(
        count_nodes_in_db, g, 'person'
    )
    node_properties = await asyncio.to_thread(
        check_node_properties, g, 'person', 'uid', 'xtzhou@g.ucla.edu'
    )
    print('NODE COUNT:', node_count)
    print('NODE PROPERTIES:', node_properties)
