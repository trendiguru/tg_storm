from __future__ import absolute_import, print_function, unicode_literals

from streamparse.spout import Spout
import rq
from trendi.constants import redis_conn


class NewImageSpout(Spout):

    def initialize(self, stormconf, context):
        rq.push_connection(redis_conn)
        self.q = rq.Queue("storm_pipeline")

    def next_tuple(self):
        job = self.q.dequeue()
        if not job:
            return
        page_url, image_url, lang = job.args  # TODO - cancel the lang..
        self.log("spouts page_url: {0}, image_url: {1}".format(page_url, image_url))
        self.emit([page_url, image_url], tup_id=image_url)

    def fail(self, tup_id):
        self.log("OMG {0} FAILED".format(tup_id))

    def ack(self, tup_id):
        self.log("HEYYY!.. {0} ACKNOWLEGED".format(tup_id))



"""
JSON serializable:
    +-------------------+---------------+
    | Python            | JSON          |
    +===================+===============+
    | dict              | object        |
    +-------------------+---------------+
    | list, tuple       | array         |
    +-------------------+---------------+
    | str, unicode      | string        |
    +-------------------+---------------+
    | int, long, float  | number        |
    +-------------------+---------------+
    | True              | true          |
    +-------------------+---------------+
    | False             | false         |
    +-------------------+---------------+
    | None              | null          |
    +-------------------+---------------+
"""
