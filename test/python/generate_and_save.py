'''
generate test dataset in pg

you need create extension vector at first;

the table is ```
create table items
(
    id            bigserial
        primary key,
    content       text,
    embedding     vector(4096),
    norm_256  vector(256),
    norm_512  vector(512),
    norm_1024 vector(1024)
);
```

requirements:
 * sqlalchemy
 * psycopg2
 * requests

$un it as:

```
python generate_and_save.py xxx.txt
```

The script will insert content, origin vector and reduced vectors into items table line by line.

'''
import sys

from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
import re, json
import requests

engine = create_engine("postgresql+psycopg2://localhost/pgv")

# doc_path = "/Users/mars/jobs/blue-pro/postgresql-16.1/doc/src/sgml"
doc = sys.argv[1]

exp = re.compile(r"<para>(.*?)</para>")
ollama_url = "http://localhost:11434/api/embeddings"


def embedding(segment, model='llama3:8b'):
    doc = {
        "model": model,
        "prompt": segment
    }
    resp = requests.post(ollama_url, json=doc)
    return resp.json()["embedding"]


session_maker = sessionmaker(bind=engine)

with open(doc) as f, session_maker() as session:
    for l in f:
        line=l.strip()
        if not line:
            continue

        vector = embedding(line.strip())
        session.execute(text("insert into items(content, embedding, norm_256, norm_512, norm_1024) "
                             "select :line, :emb, "
                             "vector_norm_reduce(:emb, 256), "
                             "vector_norm_reduce(:emb, 512), "
                             "vector_norm_reduce(:emb, 1024)"),
                        {"emb": json.dumps(vector), "line": line})
        print(line)

    session.commit()
