import tornado.web
import asyncio


class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "content-type,Authorization")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS, PATCH, PUT')

    size = 10000

    def initialize(self, db, db2):
        self.es = db
        self.na = db2

    async def asynchronous_fetch_shape(self, query):
        response = await self.es.search(index='shape', body=query)
        return response

    async def asynchronous_fetch(self, query):
        response = await self.es.search(index='hcov19', body=query)
        return response

    
    async def asynchronous_fetch_count(self, query):
        response = await self.es.count(
            index="hcov19",
            body=query)
        return response

    async def get_mapping(self):
        response = self.na.indices.get_mapping("hcov19")
        return response

    def post(self):
        pass
    
