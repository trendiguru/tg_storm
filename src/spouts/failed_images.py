from __future__ import absolute_import, print_function, unicode_literals

import itertools
from streamparse.spout import Spout
import rq
from trendi.constants import redis_conn

class FailedImageSpout(Spout):

    def initialize(self, stormconf, context):
        rq.push_connection(redis_conn)
        self.q = rq.registry.FailedQueue()

    def next_tuple(self):
        job = self.q.dequeue()
	if not job: 
            return
        image_url = job.args[0]
        origin = job.origin
        self.emit([image_url, origin])
