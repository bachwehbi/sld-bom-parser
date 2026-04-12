import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from server.routes import chat, diagrams, upload, matching

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("SLD BOM Parser app starting up")
    yield
    logger.info("SLD BOM Parser app shutting down")


app = FastAPI(title="SLD BOM Parser", lifespan=lifespan)

app.include_router(chat.router)
app.include_router(diagrams.router)
app.include_router(upload.router)
app.include_router(matching.router)

app.mount("/", StaticFiles(directory="static", html=True), name="static")
