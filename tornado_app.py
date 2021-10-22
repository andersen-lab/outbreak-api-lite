import tornado.ioloop
import tornado.web
from general import LocationHandler
from elasticsearch import Elasticsearch
from lineage import LineageByCountryHandler, LineageByDivisionHandler, LineageAndCountryHandler, LineageAndDivisionHandler, LineageHandler, LineageMutationsHandler, MutationDetailsHandler, MutationsByLineage
from prevalence import GlobalPrevalenceByTimeHandler, PrevalenceByLocationAndTimeHandler, CumulativePrevalenceByLocationHandler, PrevalenceAllLineagesByLocationHandler, PrevalenceByAAPositionHandler
from general import LocationHandler, LocationDetailsHandler, MetadataHandler, MutationHandler, SubmissionLagHandler, SequenceCountHandler, MostRecentSubmissionDateHandler, MostRecentCollectionDateHandler, GisaidIDHandler


es = Elasticsearch('http://localhost:9200')

if __name__ == "__main__":
    application = tornado.web.Application([
        (r"/hcov19/location", LocationHandler, dict(db=es)),
        (r"/hcov19/lineage-by-country", LineageByCountryHandler, dict(db=es)),
        (r"/hcov19/lineage-and-country", LineageAndCountryHandler, dict(db=es)),
        (r"/hcov19/lineage-by-division", LineageByDivisionHandler, dict(db=es)),
        (r"/hcov19/lineage-and-division", LineageAndDivisionHandler, dict(db=es)),
        (r"/hcov19/sequence-count", SequenceCountHandler, dict(db=es)),
        (r"/hcov19/global-prevalence", GlobalPrevalenceByTimeHandler, dict(db=es)),
        (r"/hcov19/prevalence-by-location", PrevalenceByLocationAndTimeHandler, dict(db=es)),
        (r"/hcov19/prevalence-by-location-all-lineages", PrevalenceAllLineagesByLocationHandler, dict(db=es)),
        (r"/hcov19/prevalence-by-position", PrevalenceByAAPositionHandler, dict(db=es)),
        (r"/hcov19/lineage-by-sub-admin-most-recent", CumulativePrevalenceByLocationHandler, dict(db=es)),
        (r"/hcov19/most-recent-collection-date-by-location", MostRecentCollectionDateHandler, dict(db=es)),
        (r"/hcov19/most-recent-submission-date-by-location", MostRecentSubmissionDateHandler, dict(db=es)),
        (r"/hcov19/mutation-details", MutationDetailsHandler, dict(db=es)),
        (r"/hcov19/mutations-by-lineage", MutationsByLineage, dict(db=es)),
        (r"/hcov19/lineage-mutations", LineageMutationsHandler, dict(db=es)),
        (r"/hcov19/collection-submission", SubmissionLagHandler, dict(db=es)),
        (r"/hcov19/lineage", LineageHandler, dict(db=es)),
        (r"/hcov19/location", LocationHandler, dict(db=es)),
        (r"/hcov19/location-lookup", LocationDetailsHandler, dict(db=es)),
        (r"/hcov19/mutations", MutationHandler, dict(db=es)),
        (r"/hcov19/metadata", MetadataHandler, dict(db=es)),
        (r"/hcov19/gisaid-id-lookup", GisaidIDHandler, dict(db=es)),
    ])
    application.listen(8000)
    tornado.ioloop.IOLoop.current().start()
