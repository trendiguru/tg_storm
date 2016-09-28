from __future__ import absolute_import, print_function, unicode_literals

from streamparse.spout import Spout
import rq, time
from trendi.constants import redis_conn, db


class NewImageSpout(Spout):

    def initialize(self, stormconf, context):
        rq.push_connection(redis_conn)
        self.q = rq.Queue("start_pipeline")
        self.db = db

    def next_tuple(self):
        time.sleep(0.1)
        job = self.q.dequeue()
        if not job:
            return
        page_url, image_url, products, method = job.args
        self.emit([page_url, image_url, products, method], tup_id=image_url)

    def fail(self, tup_id):
        deleted = db.iip.delete_one({'image_urls': tup_id}).deleted_count
        var = deleted * 'and have' + (1-deleted) * 'but have not'
        self.log("OMG {0} FAILED {1} been deleted from db.iip".format(tup_id, var))

    def ack(self, tup_id):
        self.log("HEYYY!.. {0} ACKNOWLEGED".format(tup_id))
