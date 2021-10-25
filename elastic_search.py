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
            "settings": {"number_of_shards": 1,
                "analysis": {
                    "normalizer": {
                        "keyword_lowercase": {
                        "type": "custom",
                        "filter": ["lowercase"]
                        }
                    }
                }
            },            
            "mappings": {
            "properties": {
                 "@timestamp" : {"type" : "date", "format": "date_optional_time||epoch_millis" },
                 "strain" :{"type":"keyword"},
                 "country": {"type":"keyword"},
                 "country_id" : {"type":"keyword"},
                 "country_lower": {"type":"keyword", "normalizer":"keyword_lowercase"},
                 "division": {"type":"keyword"},
                 "division_id": {"type": "keyword"},
                 "division_lower": {"type": "keyword", "normalizer":"keyword_lowercase"},
                 "location": {"type":"keyword"},
                 "location_id": {"type": "keyword"},
                 "location_lower": {"type": "keyword", "normalizer":"keyword_lowercase"},
                 "accession_id": {"type": "keyword"},
                 
                 "mutations" : {"type" : "nested",
                    "properties":{
                        "mutation" : {"type":"keyword"},
                        "type" : {"type":"keyword"},
                        "gene" : {"type":"keyword"},
                        "ref_codon" : {"type":"keyword"},
                        "pos" : {"type":"keyword"},
                        "alt_codon" : {"type":"keyword"},
                        "is_synonymous" : {"type":"keyword"},
                        "ref_aa" : {"type":"keyword"},
                        "codon_num" : {"type":"keyword"},
                        "alt_aa" : {"type":"keyword"},
                        "absolute_coords" : {"type": "keyword"},
                        "change_length_nt" : {"type": "keyword"},
                        "nt_map_coords" : {"type": "keyword"},
                        "aa_map_coords" : {"type": "keyword"},
                   },
                   },
                   "pangolin_lineage" : {"type": "keyword", "normalizer":"keyword_lowercase"},
                   "pango_version" : {"type": "keyword"},
                   "clade" : {"type":"keyword"},
                   "date_collected" : {"type":"keyword"},
                   "date_modified" : {"type":"keyword"},
                   "date_submitted" : {"type":"keyword"},
            },
            },
        },
        ignore=400,)


def generate_actions(data):
    for i,row in enumerate(data):
        new_dict = {}
        new_dict['_id'] = i
        new_dict['strain'] = row['strain']
        new_dict['country'] = str(row['country'])
        new_dict['country_id'] = str(row['country_id'])
        new_dict['country_lower'] = str(row['country_lower'])
        new_dict['division'] = str(row['division'])
        new_dict['division_id'] = str(row['division_id'])
        new_dict['division_lower'] = str(row['division_lower'])
        new_dict['location'] = str(row['location'])
        new_dict['location_id'] = str(row['location_id'])
        new_dict['location_lower'] = str(row['location_lower'])
        new_dict['accession_id'] = str(row['accession_id'])
        new_dict['pangolin_lineage'] = str(row['pangolin_lineage'])
        if 'pango_version' in row:
            new_dict['pango_version'] = str(row['pango_version'])
        if 'clade' in row:
            new_dict['clade'] = str(row['clade'])
        new_dict['date_submitted'] = str(row['date_submitted'])
        new_dict['date_collected'] = str(row['date_collected'])
        new_dict['date_modified'] = str(row['date_modified'])
          
        temp_list = []
        
        if row['mutations'] != None:
            temp = {}
            for mut in row['mutations']:
                temp['mutation'] = mut['mutation']
                temp['type'] = mut['type']
                temp['gene'] = mut['gene']
                temp['ref_codon'] = mut['ref_codon']
                temp['pos'] = mut['pos']
                if 'alt_codon' in mut:
                    temp['alt_codon'] = mut['alt_codon']
                temp['is_synonymous'] = mut['is_synonymous']
                if 'ref_aa' in mut:
                    temp['ref_aa'] = mut['ref_aa']
                temp['codon_num'] = mut['codon_num']
                if 'alt_aa' in mut:
                    temp['alt_aa'] = mut['alt_aa']
                if 'absolute_coords' in mut:
                    temp['absolute_coords'] = mut['absolute_coords']
                if 'change_length_nt' in mut:
                    temp['change_length_nt'] = mut['change_length_nt']
                if 'nt_map_coords' in mut:
                    temp['nt_map_coords'] = mut['nt_map_coords']
                if 'aa_map_coords'  in mut:
                    temp['aa_map_coords'] = mut['aa_map_coords']
            temp_list.append(temp) 
       
        new_dict['mutations'] = temp_list
        yield new_dict

def main():
    json_filename = 'new_api_data.json'
    print("Loading dataset...")
    data = download_dataset(json_filename)
    
    for d in data:
        if 'pangolin_lineage' in d:
            print(d['pangolin_lineage'])
    

    client = Elasticsearch()
    print("Creating an index...")
    create_index(client)

    print("Indexing documents...")
    progress = tqdm.tqdm(unit="docs", total=len(data))
    successes = 0
    for ok, action in streaming_bulk(
        client=client, index="hcov19", actions=generate_actions(data),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, len(data)))


if __name__ == "__main__":
    main()
