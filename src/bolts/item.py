

from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import numpy as np
import time
from trendi import find_similar_mongo, constants


class ItemBolt(Bolt):

    def initialize(self, conf, ctx):
        self.db = constants.db

    def process(self, tup):
        item, person_id = tup.values
        item['mask'] = np.array(item['mask'], dtype=np.uint8)
        item['image'] = np.array(item['image'], dtype=np.uint8)
        try:
            out_item = {}
            start = time.time()
            out_item['fp'], out_item['similar_results'] = find_similar_mongo.find_top_n_results(item['image'],
                                                                                                item['mask'],
                                                                                                100, item['category'],
                                                                                                'products')
            self.log("back from find_top_n after {0} secs..".format(time.time() - start))
            out_item['category'] = item['category']
            self.log("person_id: {0}".format(person_id))
            self.log("out_item['category']: {0}".format(out_item['category']))
            self.log("similar_item: {0}".format(out_item['similar_results'][0]))
            self.emit([out_item, str(person_id)])
        except Exception as e:
            self.log(e.message, e.args)
            # TODO - tell someone we have an item down