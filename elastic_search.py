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
                "type": {
                        "type": "keyword"
                        },
                    "mutation": {
                        "type": "keyword",
                        "normalizer": "keyword_lowercase_normalizer"
                        },
                    "gene": {
                        "type": "keyword"
                        },
                    "ref_codon": {
                        "type": "keyword"
                        },
                    "pos": {
                        "type": "keyword"
                        },
                    "alt_codon": {
                        "type": "keyword"
                        },
                    "is_synonymous": {
                        "type": "keyword"
                        },
                    "ref_aa": {
                        "type": "keyword"
                        },
                    "codon_num": {
                        "type": "keyword"
                        },
                    "alt_aa": {
                        "type": "keyword"
                        },
                    "absolute_coords": {
                        "type": "keyword"
                        },
                    "change_length_nt": {
                        "type": "keyword"
                        },
                    "nt_map_coords": {
                        "type": "keyword"
                        },
                    "aa_map_coords": {
                        "type": "keyword"
                        }
                    }
                },
            "division": {
                "type": "keyword",
                },
            "division_lower": {
                "type": "keyword",
                "normalizer": "keyword_lowercase_normalizer"

                },
        "country": {
                "type": "keyword",
                },
        "country_lower": {
                "type": "keyword",
                "normalizer": "keyword_lowercase_normalizer"

                },
        "date_submitted": {
                "type": "keyword"
                },
        "date_collected": {
                "type": "keyword"
                },
        "date_modified": {
                "type": "keyword"
                },
        "country_id": {
                "type": "keyword"
                },
        "authors": {
                "type": "keyword"
                },
        "pangolin_lineage": {
                "type": "keyword",
                "normalizer": "keyword_lowercase_normalizer"

                },
        "location": {
                "type": "keyword"
                },
        "location_lower": {
                "type": "keyword",
                "normalizer": "keyword_lowercase_normalizer"
                },
        "location_id": {
                "type": "keyword"
                },
        "division_id": {
                "type": "keyword"
                },
        "accession_id": {
                "type": "keyword"
                },
        "clade": {
                "type": "keyword"
                },
        "pango_version": {
                "type": "keyword"
                },
        "country_normed": {
                "type": "keyword"
                }                   
                }
            },
        },
        ignore=400,
    )


def generate_actions(data):
    for row in data:
        print(row)
        row['_id'] = row['strain']
        yield row

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

