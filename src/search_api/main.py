import os

import uvicorn
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from src.services.search_service import autocomplete, full_search

app = FastAPI(
    title="Redis Search API",
    description="Search and autocomplete service backed by Redis",
    version="1.0.0",
)

# CORS - allow all origins for public API
# Mobile apps, Firebase Functions, and local dev all need access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/autocomplete")
async def autocomplete_endpoint(q: str = Query(...)):
    return await autocomplete(q)

@app.get("/search")
async def search_endpoint(q: str = Query(...)):
    return await full_search(q)

@app.get("/health")
async def health():
    return {"status":"ok"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("src.search_api.main:app", host="0.0.0.0", port=port)
