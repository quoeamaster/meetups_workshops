from sentence_transformers import SentenceTransformer
import clickhouse_connect
from dotenv import load_dotenv
import os
# from datetime import datetime

load_dotenv()

# --------------------
# Config
# --------------------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))
DATABASE = os.getenv("CLICKHOUSE_DATABASE")
TABLE = os.getenv("CLICKHOUSE_TABLE")
USER = os.getenv("CLICKHOUSE_USER")
PASSWORD = os.getenv("CLICKHOUSE_PWD")

TENANT_ID = 88
SOURCE = "meetup_workshop"
TOP_K = 5

QUESTION = "How does ClickHouse support vector search?"
# QUESTION = "how do I look today??? cool or not?"
# QUESTION = "I want to know how this db works with LLM"

# --------------------
# Load model
# --------------------
model = SentenceTransformer("all-MiniLM-L6-v2")

query_embedding = model.encode([QUESTION])[0]
query_vector_sql = "[" + ",".join(map(str, query_embedding.tolist())) + "]"

# --------------------
# ClickHouse client
# --------------------
ch = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
    secure=True,
    verify=False, # only for demo...
)

# --------------------
# KNN query
# --------------------
sql = f"""
SELECT
    content,
    cosineDistance(embedding, {query_vector_sql}) AS score
FROM {TABLE}
WHERE
    tenant_id = {TENANT_ID}
    AND source = '{SOURCE}'
ORDER BY score ASC
LIMIT {TOP_K}
"""

result = ch.query(sql)

# --------------------
# Output
# --------------------
print(f"\nQuestion: {QUESTION}\n")
for i, row in enumerate(result.result_rows, start=1):
    content, score = row
    print(f"{i}. score={score:.4f}")
    print(f"   {content}\n")