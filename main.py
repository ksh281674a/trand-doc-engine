import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from trend_engine import TrendEngine

engine = TrendEngine()

@asynccontextmanager
async def lifespan(app: FastAPI):
    engine.start()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health_check():
    return {"status": "ok"}

@app.get("/trends")
def all_trends():
    return JSONResponse(engine.get_snapshot())

@app.get("/trends/category/{category}")
def by_category(category: str):
    snap = engine.get_snapshot()
    filtered = {k: v for k, v in snap.items() if v["category"] == category}
    if not filtered:
        return JSONResponse({"error": "카테고리 없음"}, status_code=404)
    return JSONResponse(filtered)

@app.get("/trends/{ticker}")
def one_trend(ticker: str):
    snap = engine.get_snapshot()
    if ticker not in snap:
        return JSONResponse({"error": "종목 없음"}, status_code=404)
    return JSONResponse(snap[ticker])

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
