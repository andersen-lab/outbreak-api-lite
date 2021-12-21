import tornado.ioloop
import tornado.web
from general import LocationHandler, Shape, Zipcode, ShapeByZipcode
from elasticsearch import AsyncElasticsearch, Elasticsearch
from lineage import LineageByCountryHandler, LineageByDivisionHandler, LineageAndCountryHandler, LineageAndDivisionHandler, LineageHandler, LineageMutationsHandler, MutationDetailsHandler, MutationsByLineage
from prevalence import GlobalPrevalenceByTimeHandler, PrevalenceByLocationAndTimeHandler, CumulativePrevalenceByLocationHandler, PrevalenceAllLineagesByLocationHandler, PrevalenceByAAPositionHandler
from general import LocationHandler, LocationDetailsHandler, MetadataHandler, MutationHandler, SubmissionLagHandler, SequenceCountHandler, MostRecentSubmissionDateHandler, MostRecentCollectionDateHandler, GisaidIDHandler


es = AsyncElasticsearch(hosts=[{'host': 'es'}], retry_on_timeout=True)
na = Elasticsearch(hosts=[{'host': 'es'}], retry_on_timeout=True)
if __name__ == "__main__":
    application = tornado.web.Application([
        (r"/shape/shape", Shape, dict(db=es, db2=na)),
        (r"/sdzipcode/shape", ShapeByZipcode, dict(db=es,db2=na)),
        (r"/hcov19/get-zipcodes", Zipcode, dict(db=es, db2=na)),
        (r"/hcov19/location", LocationHandler, dict(db=es,db2=na)),
        (r"/hcov19/lineage-by-country", LineageByCountryHandler, dict(db=es,db2=na)),
        (r"/hcov19/lineage-and-country", LineageAndCountryHandler, dict(db=es,db2=na)),
        (r"/hcov19/lineage-by-division", LineageByDivisionHandler, dict(db=es,db2=na)),
        (r"/hcov19/lineage-and-division", LineageAndDivisionHandler, dict(db=es,db2=na)),
        (r"/hcov19/sequence-count", SequenceCountHandler, dict(db=es,db2=na)),
        (r"/hcov19/global-prevalence", GlobalPrevalenceByTimeHandler, dict(db=es,db2=na)),
        (r"/hcov19/prevalence-by-location", PrevalenceByLocationAndTimeHandler, dict(db=es, db2=na)),
        (r"/hcov19/prevalence-by-location-all-lineages", PrevalenceAllLineagesByLocationHandler, dict(db=es,db2=na)),
        (r"/hcov19/prevalence-by-position", PrevalenceByAAPositionHandler, dict(db=es, db2=na)),
        (r"/hcov19/lineage-by-sub-admin-most-recent", CumulativePrevalenceByLocationHandler, dict(db=es,db2=na)),
        (r"/hcov19/most-recent-collection-date-by-location", MostRecentCollectionDateHandler, dict(db=es,db2=na)),
        (r"/hcov19/most-recent-submission-date-by-location", MostRecentSubmissionDateHandler, dict(db=es,db2=na)),
        (r"/hcov19/mutation-details", MutationDetailsHandler, dict(db=es,db2=na)),
        (r"/hcov19/mutations-by-lineage", MutationsByLineage, dict(db=es,db2=na)),
        (r"/hcov19/lineage-mutations", LineageMutationsHandler, dict(db=es,db2=na)),
        (r"/hcov19/collection-submission", SubmissionLagHandler, dict(db=es,db2=na)),
        (r"/hcov19/lineage", LineageHandler, dict(db=es,db2=na)),
        (r"/hcov19/location", LocationHandler, dict(db=es,db2=na)),
        (r"/hcov19/location-lookup", LocationDetailsHandler, dict(db=es,db2=na)),
        (r"/hcov19/mutations", MutationHandler, dict(db=es,db2=na)),
        (r"/hcov19/metadata", MetadataHandler, dict(db=es,db2=na)),
        (r"/hcov19/gisaid-id-lookup", GisaidIDHandler, dict(db=es)),
    ])
    application.listen(8000)
    tornado.ioloop.IOLoop.current().start()
