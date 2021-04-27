# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo

class MongoPipeline:
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(spider.settings.get('MONGO_URI'))
        self.collection = self.client[spider.settings.get('MONGO_DB')][spider.collection]
        self.collection.create_index('url', unique=True)

    def close_spider(self, _):
        self.client.close()

    def process_item(self, item, _):
        self.collection.replace_one({'url': item['url']}, item, upsert=True)
        return item
