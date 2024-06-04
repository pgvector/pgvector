'''
test reduce
'''
import json
import sys
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
import requests

engine = create_engine("postgresql+psycopg2://localhost/pgv")
ollama_url = "http://localhost:11434/api/embeddings"
session_maker = sessionmaker(bind=engine)


def embedding(segment, model='llama3:8b'):
    doc = {
        "model": model,
        "prompt": segment
    }
    resp = requests.post(ollama_url, json=doc)
    return resp.json()["embedding"]


question = sys.argv[1].strip()

vector = embedding(question)


def query(session, vector, scale, method):
    m = f"vector_{method}_reduce"
    c = method + "_" + str(scale)
    query = f"select id, {c} <-> {m}(:emb, {scale}), content from items order by {c} <-> {m}(:emb, {scale}) limit 5"
    return session.execute(text(query), {"emb": json.dumps(vector)}).fetchall()

def print_vs(idx, data, row):
    print(data[idx][0], row[0], row[1], row[2])

with session_maker() as session:
    data = []
    for row in session.execute(text("SELECT id, embedding <-> :emb, content FROM items ORDER BY embedding <-> :emb LIMIT 5;"),
                               {"emb": json.dumps(vector)}).fetchall():
        print(row)
        data.append((row[0], row[1]))
    print("==============")
    print(">> integral reduce to 256")
    for idx, row in enumerate(query(session, vector, 256, 'norm')):
        print_vs(idx, data, row)

    print(">> integral reduce to 512")
    for idx, row in enumerate(query(session, vector, 512, 'norm')):
        print_vs(idx, data, row)

    print(">> integral reduce to 1024")
    for idx, row in enumerate(query(session, vector, 1024, 'norm')):
        print_vs(idx, data, row)

