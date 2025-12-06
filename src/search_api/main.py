
import uvicorn
from fastapi import FastAPI, Query

from src.services.search_service import autocomplete, full_search

app = FastAPI()

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
    uvicorn.run("src.search_api.main:app", host="0.0.0.0", port=8080)
