import os
import json
import tqdm
import urllib3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


def download_dataset(json_filename):
    data = []
    with open(json_filename,'r') as jfile:
        for line in jfile:
            data.append(json.loads(line))
    return(data)

def create_index(client):
    
    client.indices.create(
        index="hcov19",
        body={
            "settings": {"number_of_shards": 1},
            "mappings": {
                "properties": {
                    "country": {"type": "keyword"},
                    "division": {"type": "keyword"},
                    "location": {"type": "keyword"},
                }
            },
        },
        ignore=400,
    )


def generate_actions(data):
    for row in data:
        doc = {
            "_id": row["strain"],
            "country": row["country"],
            "division": row["division"],
            "division": row["location"],
        }
        yield doc

def main():
    print("Loading dataset...")
    number_of_docs = download_dataset()

    client = Elasticsearch()
    print("Creating an index...")
    create_index(client)

    print("Indexing documents...")
    progress = tqdm.tqdm(unit="docs", total=number_of_docs)
    successes = 0
    for ok, action in streaming_bulk(
        client=client, index="hcov19", actions=generate_actions(),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, number_of_docs))


if __name__ == "__main__":
    main()

