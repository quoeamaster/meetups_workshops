from sentence_transformers import SentenceTransformer
import clickhouse_connect
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

# --------------------
# Config (from .env)
# --------------------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))
DATABASE = os.getenv("CLICKHOUSE_DATABASE")
TABLE = os.getenv("CLICKHOUSE_TABLE")
USER = os.getenv("CLICKHOUSE_USER")
PASSWORD = os.getenv("CLICKHOUSE_PWD")

TENANT_ID = 88
SOURCE = "meetup_workshop"

# print(f"CLICKHOUSE_HOST: {CLICKHOUSE_HOST}")
# print(f"CLICKHOUSE_PORT: {CLICKHOUSE_PORT}")
# print(f"DATABASE: {DATABASE}")
# print(f"TABLE: {TABLE}")
# print(f"USER: {USER}")
# print(f"PASSWORD: {PASSWORD}")
# print(f"TENANT_ID: {TENANT_ID}")
# print(f"SOURCE: {SOURCE}")

# --------------------
# Sample chunks
# --------------------
texts = [
    "ClickHouse is a column-oriented database designed for analytics.",
    "Vector search enables semantic similarity instead of keyword matching.",
    "LLMs require external context to avoid hallucination.",
    "ClickHouse can combine metadata filtering with vector search."
]

# --------------------
# Load embedding model
# --------------------
model = SentenceTransformer("all-MiniLM-L6-v2")  # 384-dim (not 1586 compared with openAI)
embeddings = model.encode(texts)

print(f"Generated {len(embeddings)} embeddings")
print(f"Embedding dimension: {len(embeddings[0])}")

# --------------------
# ClickHouse client
# --------------------
ch = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    secure=True,
    verify=False, # only for demo...
)

# --------------------
# Prepare rows
# --------------------
# print(f"current time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}")
rows = []
for i, (text, emb) in enumerate(zip(texts, embeddings)):
    rows.append((
        1,                      # doc_id
        i,                      # chunk_id
        text,
        emb.tolist(),           # embedding
        SOURCE,
        TENANT_ID,
        datetime.now(),
    ))

# --------------------
# Insert
# --------------------
ch.insert(
    TABLE,
    rows,
    column_names=[
        "doc_id",
        "chunk_id",
        "content",
        "embedding",
        "source",
        "tenant_id",
        "ts"
    ]
)

print("Chunks successfully inserted into ClickHouse")