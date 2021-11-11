import os
import io
import sys
import json
import tqdm
import urllib3
import requests
import zipfile

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


def get_gpkg(countries):
    """
    Parameters
    ----------
    country : str
        The name of the country to download information for.
    """
    for country in countries:
        print(country)
        response = requests.get('https://biogeo.ucdavis.edu/data/gadm3.6/shp/gadm36_%s_shp.zip' %country)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall("./shapefiles/")
        #find and delete all non shape files
        #os.system("find ./shapefiles -type f  ! -name '*.shp'  -delete")

def simplify_gpkg():
    location = './shapefiles'
    all_shp_files = os.listdir(location)
    all_shp_files = [os.path.join(location,filename) for filename in all_shp_files if filename.endswith(".shp")]
    print(all_shp_files)

    from shapely.geometry import shape as sh
    import shapely
    import shapefile
    count=0
    for shp in all_shp_files:
        shape = shapefile.Reader(shp)
        for c,feature in enumerate(shape.shapeRecords()): 
            new_dict = {}
            geojson = { "type": "Feature"}
             
            #print(feature.record, shp) 
            country=feature.record[1]
            try:
                if '1' in shp or '2' in shp:
                    division=feature.record[3]
                    division_id=feature.record[-1].split('.')[1]
                else:
                    division='None'
                    division_id='None'
            except:
                division='None'
                division_id='None'
            try:
                if '2' in shp:
                    location=feature.record[6]
                else:
                    location='None'
            except:
                location='None'

            #print(country, division, location)
            first = feature.shape.__geo_interface__  
            
            def recursive_len(item):
                if type(item) == list:
                    return sum(recursive_len(subitem) for subitem in item)
                else:
                    return 1
            total_coordinates = recursive_len(first['coordinates'])
            #print(total_coordinates) 

            if total_coordinates > 80000:
                sim = 0.4
            elif 10000 < total_coordinates <= 80000:
                sim = 0.019
            elif 1500 < total_coordinates <= 10000:
                sim = 0.01
            elif 500 < total_coordinates <= 1500:
                sim = 0.01
            elif 250 < total_coordinates <= 500:
                sim = 0.005
            else:
                sim = 0.0001

            shp_geom = sh(first)
            #print('shp', shp_geom.__dict__)
            if sim != None:
                s = shp_geom.simplify(sim, preserve_topology=False)
            else:
                s = shp_geom
            new_dict['_id']=count
            #print(count)
            count += 1
            new_dict['country'] = country
            new_dict['country_lower'] = country.lower()
            new_dict['country_id'] = feature.record[0]
            new_dict['division'] = division
            new_dict['division_lower'] = division.lower()
            new_dict['division_id']=division_id
            new_dict['location'] = location
            new_dict['location_lower'] = location.lower()
            new_dict['location_id'] = 'None'
            geojson['geometry'] = shapely.geometry.mapping(s)
            #print(geojson)
            new_dict['shape'] = json.dumps(geojson)
            #print(new_dict)
            if feature.record[0] == 'USA' and division_id == 'CA':
                print(total_coordinates, sim)
 
            yield new_dict


def download_dataset(json_filename):
    data = []
    with open(json_filename,'r') as jfile:
        for line in jfile:
            data.append(json.loads(line))
    return(data)

def create_polygon(client):
    client.indices.create(
        index="shape",
        body={
            "settings": {"number_of_shards": 100,
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
                "country": {"type":"keyword"},
                "country_lower" : {"type":"keyword", "normalizer":"keyword_lowercase"},
                "country_id" : {'type': "keyword"},
                "division": {"type":"keyword"},
                "division_lower": {"type":"keyword", "normalizer":"keyword_lowercase"},
                "division_id": {"type":"keyword"},
                "location": {"type":"keyword"},
                "location_lower": {"type":"keyword", "normalizer":"keyword_lowercase"},
                "location_id" : {"type":"keyword"},
                "shape": {"type": "keyword"},
                },
            },
        },
        ignore=400,)

def create_index(client):
    client.indices.create(
        index="hcov19",
        body={
            "settings": {"number_of_shards": 100,
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
    test_mut_count = 0
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
            for mut in row['mutations']:
                temp = {}
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
        #print(temp_list)
        new_dict['mutations'] = temp_list
        #print(test_mut_count)    
        yield new_dict

def main():
    json_filename = 'new_api_data.json'
    print("Loading dataset...")
    data = download_dataset(json_filename)
    
    unique_countries = []
    unique_divisions = []
    for item in data:
        if item['country_id'] not in unique_countries and item['country_id'] != 'None':
            unique_countries.append(item['country_id'])
        if item['division_id'] not in unique_divisions:
            unique_divisions.append(item['division_id'])
    #get_gpkg(unique_countries) 
    client = Elasticsearch()
    create_polygon(client)
    #simplify_gpkg()
    
    print("Indexing shapes...")
    progress = tqdm.tqdm(unit="docs", total=5342)
    successes = 0
    
    for ok, action in streaming_bulk(
        client=client, index="shape", actions=simplify_gpkg(),
    ):
        progress.update(1)
        successes += ok
        
    print("Indexed %d/%d documents" % (successes, 5342))
 
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
