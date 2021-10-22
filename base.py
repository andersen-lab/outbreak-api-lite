import tornado.web
import asyncio


class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "content-type,Authorization")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS, PATCH, PUT')

    size = 10000

    def initialize(self, db):
        self.es = db

    async def asynchronous_fetch(self, query):
        response = await self.es.search(index='hcov19', body=query, size=0)
        return response

    
    async def asynchronous_fetch_count(self, query):
        response = await self.es.search.count(
            index="hcov19",
            body=query)
        return response

    async def get_mapping(self):
        response = await self.es.search.indices.get_mapping("hcov19")
        return response

    def post(self):
        pass
    
