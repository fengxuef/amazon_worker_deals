from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)
from celery import Celery
app = Celery()
app.config_from_object('settings')

from datetime import datetime
from pymongo import MongoClient
@app.task
def list_amazon_deal(uri, db_n, c_n, en_id=None, d_id=None):
  """
  uri: URI of the mongodb(e.g. mongodb://localhost:27017)
  db_n: db name
  c_n:collection name
  en_id: data entry identifier
  d_id: identifier of the deal in form of a dict, most likely to be deal url (e.g. {'link':'http:/....'})
  """
  #logger.info("%s/%s/%s, %s %s"%(uri, db_n, c_n, en_id, d_id))
  clnt = MongoClient(uri)
  db = clnt[db_n]
  collection = db[c_n]
  entry = None
  if en_id:
    entry = collection.find_one({'_id':en_id})
  elif d_id:
    entry = collection.find_one(d_id)
  if not entry:
    # log a ERROR
    return
  # got the entry,
  # now process the entry
  # ...
  # TODO: categorize urls to product links, searches, browse nodes, gp
  # TODO: extract ASIN from product links
  # ...
  # mark the entry PROCESSED
  entry['lister_metadata'] = {'ts': datetime.utcnow(),'status':'PROCESSED'}
  #logger.info("entry %s"%(entry))
  collection.save(entry)

from tasks import list_amazon_deal
from scrapy.utils.project import get_project_settings
class AmazonDealsListerPipeline(object):
    def process_item(self, item, spider):
        settings = get_project_settings()
        list_amazon_deal.delay(settings['MONGODB_URI'],settings['MONGODB_DATABASE'],settings['MONGODB_COLLECTION'],d_id={'link':item['link']})
        return item
