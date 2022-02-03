"""
Script parallel ingests data into ElasticSearch database.
"""
import os
import io
import sys
import ast
import json
import argparse
import shapely
import datetime
import shapefile
import tqdm
import urllib3
import requests
import zipfile
import pandas as pd
import numpy as np
from collections.abc import Iterable
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, parallel_bulk
from shapely.geometry import shape as sh
from shapely.geometry import GeometryCollection

countries = []

def test_epi_availability(epi_location, zipcodes):
    """
    Given a zipcode epi filepath or url, determine whether 
    (1) the resource exists
    (2) if it matches the geojson data available for the zipcode
    
    If both conditions are met, return the loaded file.
    
    Parameters
    ----------
    epi_location : str
        The url or filepath to json epi data by zipcode.
    zipcodes : str
        The filepath to the zipcode geojson.
    
    Returns
    -------
    epi_data : dict
        The epi data by zipcode.
    """
    print("Testing the availability of epi data")
    #if the resource exists
    have_resource = False  
    #if the zipcode match out geojson data  
    correct_zipcodes = False    
    epi_data = {}

    #figure out if we have epi data
    #try and get url epi
    response = requests.get(epi_location)
    if response.status_code == 200:
        have_resource = True
        resource_load = response.json()

    #if its not a url try and find it locally
    else:
        if os.path.isfile(epi_location):
            #make sure we can open it
            try:
                with open(epi_location, 'r') as jsonfile:
                    resource_load = json.load(jsonfile)
                have_resource = True

            except:
                print("loading epi data failed: data file isn't in json format.")

    #if we have epi data make sure it's correct
    if have_resource:
        #now we load the zipcode data for comparison
        geojson_zipcodes = []
        with open(zipcodes, "r") as jsonfile:
            zip_data = json.load(jsonfile)
            for zd in zip_data["features"]:
                geojson_zipcodes.append(zd["properties"]["zip"])
        for location_data in resource_load["features"]:
            temp_zip = str(location_data["attributes"]["zipcode_zip"])
            #we have this zipcode geojson data
            if temp_zip in geojson_zipcodes: 
                case_count = location_data["attributes"]["case_count"]
                case_rate = location_data["attributes"]["rate_100k"]
                update_date = location_data["attributes"]["updatedate"]
                
                epi_data[temp_zip] = {"case_count":case_count, "rate_100k":case_rate, \
                    "updatedate":update_date}
        if len(epi_data) > 0:
            return(epi_data)
        else:
            return(None)
    #we don't have epi data
    else:
        return(None)
    
def create_snapshot(es):
    snapshot_body = {
    "type": "fs",
    "settings": {
            "location": "backup"
        }
    }
    index_body = {
    "indices": "hcov19,zipcodes,shape"
    }
    es.snapshot.create_repository(repository='backup', body=snapshot_body)
    index_body = {
    "indices": "shape,zipcodes,hcov19"
    }
    es.snapshot.create(repository='backup', snapshot='test_snapshot', body=index_body)

def get_gpkg(ucountries):
    """
    Parameters
    ----------
    country : str
        The name of the country to download information for.
    """
    for country in ucountries:
        print(country)
        response = requests.get('https://biogeo.ucdavis.edu/data/gadm3.6/shp/gadm36_%s_shp.zip' %country)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall("./shapefiles/")
        #find and delete all non shape files
        #os.system("find ./shapefiles -type f  ! -name '*.shp'  -delete")

def simplify_gpk_zipcode(location, epi_data=None):
    """
    Generator function for adding zipcode related data to ElasticSearch.

    Parameters
    ----------
    location : str
        Path to zipcode geojson.
    epi_data : dict, Opt.
        Dictionary with epi data per zipcode.
    """   

    count=0
    doc=''
    
    with open(location, "r") as gjson:
        for line in gjson:
            doc += line.strip()
    geojson_list = ast.literal_eval(doc)['features']

    for c,feature in enumerate(geojson_list): 
        new_dict = {}
        geojson_temp = { "type": "Feature"}
        zipcode = feature['properties']['zip']
        zipcode_name = feature['properties']['community']
      
        def flatten(l):
            for el in l:
                if isinstance(el, Iterable) and not isinstance(el, (str, bytes)):
                    yield from flatten(el)
                else:
                    yield el
        
        def recursive_len(item):
            if type(item) == list:
                return sum(recursive_len(subitem) for subitem in item)
            else:
                return 1

        def recur(lst):
            if isinstance(lst,float):
                return (lst,4) #use round(lst,2) if you want float instead of string.
            else:
                if isinstance(lst, list):
                    return [recur(i) for i in lst]
                elif type(lst) == tuple:
                    return ((recur(i for i in lst)))
        
        
        total_coordinates = recursive_len(feature['geometry']['coordinates']) 
        flat_earth = list(flatten(feature['geometry']['coordinates']))
        total_bytes = sys.getsizeof(flat_earth)
        total_coordinates = recursive_len(feature['geometry']['coordinates'])
        if total_bytes > 50000:
            sim = 0.0025
        elif 30000 < total_bytes <= 50000:
            sim = 0.0001
        elif 20000 < total_bytes <= 30000:
            sim = 0.0001
        elif 10000 < total_bytes <= 20000:
            sim = 0.00005
        elif 5000 < total_bytes <= 10000:
            sim = 0.000001
        else:
            sim = 0.0000001
        
        shp_geom = sh(feature["geometry"]).buffer(0)
        
        s = shp_geom.simplify(sim, preserve_topology=False)
        
       
        new_dict['_id']=count
        count += 1
        new_dict['zipcode'] = zipcode
        new_dict['zipcode_name'] = zipcode_name
        geojson_temp={}
        geojson_temp['geometry'] = shapely.geometry.mapping(s)
        new_dict['shape'] = json.dumps(geojson_temp,separators=(',', ':'))
        
        #add epi data if applicable
        if epi_data is not None:
            new_dict['case_count'] = epi_data[zipcode]["case_count"]
            new_dict['updatedate'] = epi_data[zipcode]["updatedate"]
            new_dict['rate_100k'] = epi_data[zipcode]["rate_100k"]     
        else:       
            new_dict['case_count'] = "None"
            new_dict['updatedate'] = "None"
            new_dict['rate_100k'] = "None"
        
        yield new_dict
     
def simplify_gpkg():
    location = './shapefiles'
    all_shp_files = os.listdir(location)
    all_shp_files = [os.path.join(location,filename) for filename in all_shp_files if filename.endswith(".shp")]
    count=0
    for shp in all_shp_files:
        shape = shapefile.Reader(shp)
        for c,feature in enumerate(shape.shapeRecords()): 
            new_dict = {}
            geojson = { "type": "Feature"}
             
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

            yield new_dict


def create_zipcode(client):
    client.indices.create(
        index="zipcodes",
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
                "zipcode" : {"type":"keyword"},
                "zipcode_name" : {"type":"keyword"},
                "shape": {"type": "keyword"},
                },
            },
        },
        ignore=400,)


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
            "settings": {"number_of_shards": 1000,
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
                 "zipcode" : {"type": "keyword"},
                 "region": {"type": "keyword"},
                 
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


def generate_actions(json_filename, region_df=None):
    test_mut_count = 0
   
    with open(json_filename,'r') as jfile:
        for i, line in enumerate(jfile):
            row = json.loads(line)
            currentDT = datetime.datetime.now()
            new_dict = {}
            new_dict['@timestamp'] = currentDT.strftime("%Y-%m-%dT%H:%M:%SZ")
            #new_dict['_id'] = i
            new_dict['strain'] = row['strain']
            new_dict['country'] = str(row['country'])
            if str(row['country']) not in countries:
                countries.append(str(row['country_id']))
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
            if 'zipcode' in row:
                if str(row['zipcode']).isdigit() and int(row['zipcode']) > 0:     
                    new_dict['zipcode'] = str(row['zipcode'])
                    if region_df != None:
                        new_dict['region'] = region_df.loc[region_df['ZIP'] == int(row['zipcode'])]['Region']
                else:
                    new_dict['zipcode'] = 'None'
                    new_dict['region'] = "None"
            else:
                new_dict['region'] = "None"
                new_dict['zipcode'] = "None"
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
           
            new_dict['mutations'] = temp_list
            yield new_dict

def main():
    """
    Script takes in a json file containing metadata processed
    according to https://github.com/andersen-lab/bjorn and ingests
    it into an elastic search database.
    
    Parameters
    ----------
    json : str
        Path to the metadata file.
    zipcode :str
        Path to the file with zipcode geojson data.
    config : str
        Path to the config file.
    hostname : str
        The host address.
    """
    
    #tutorial on how to connect this to docker es
    #https://github.com/davidefiocco/dockerized-elasticsearch-indexer
    
    #parse out args
    parser = argparse.ArgumentParser(description='Bulk elasticsearch ingest.')
    parser.add_argument('-j','--json', help='Full path to json metadata.', required=True)
    parser.add_argument('-z','--zipcode', help='Full path to config file.', required=False)
    parser.add_argument('-c', '--config', help="Full path to config file.", required=False)
    parser.add_argument('--hostname', nargs="?",const="es",help='Hostname in case not being run via docker.', required=False)
    
    args = parser.parse_args()
    
    zipcodes = args.zipcode
    json_filename = args.json
    hostname = args.hostname
    config_filename = args.config
    print("Hostname ", hostname)
    print("Zipcodes ", zipcodes)
    
    client = Elasticsearch(hosts=[{'host': '%s' %hostname}], retry_on_timeout=True, maxsize=25)
    
    #default epi data to None
    epi_data = None

    #check for a zipcode config file and epi information
    if zipcodes is None:
        with open(config_filename,'r') as json_file:
            config = json.load(json_file)
            param = config['pathToZipcodes']
            #make sure the file exists          
            if os.path.isfile(param):
                zipcodes = param
                   
            #only bother with epi if we have the geojson for zipcodes
            if zipcodes is not None:    
                epi_location = config['zipcodeEpi']
                
                #make sure we can access the epi data and it matches our geojson
                epi_data = test_epi_availability(epi_location, zipcodes)
            
    #if we have a zipcode file provided we process it
    if zipcodes is not None: 
        create_zipcode(client)
        progress = tqdm.tqdm(unit="docs", total=123)
        successes = 0
            
        for ok, action in streaming_bulk(
            client=client, index="zipcodes", actions=simplify_gpk_zipcode(zipcodes, epi_data),
        ):
            progress.update(1)
            successes += ok
        
        print("Indexed %d/%d documents", successes, 123)
    
    create_index(client)
    client.indices.put_settings(index="hcov19", body={
    "index.refresh_interval": "1s",
    })    
    
    #parallel bulk
    progress = tqdm.tqdm(unit="docs", total=5000000)
    successes = 0
    for ok, action in parallel_bulk(
        client=client, index="hcov19", actions=generate_actions(json_filename), thread_count=8, chunk_size=5000,\
        queue_size=5
    ):  
        progress.update(1)
        if ok:
            successes += ok
   
    create_polygon(client)
    
    #handle geojson shapes not related to zipcode
    unique_countries = np.unique(countries)   
    get_gpkg(unique_countries)
    
    progress = tqdm.tqdm(unit="docs", total=5342)
    successes = 0
    
    for ok, action in streaming_bulk(
        client=client, index="shape", actions=simplify_gpkg(),
    ):
        progress.update(1)
        successes += ok
        
     
    if config['esSnapshot'] == 'True':
        create_snapshot(client)

if __name__ == "__main__":
    main()
