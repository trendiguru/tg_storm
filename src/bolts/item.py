
from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import numpy as np
import time
from trendi import find_similar_mongo
from trendi.constants import db, products_per_site


class ItemBolt(Bolt):

    # outputs = ["item", "person_id"]

    def initialize(self, conf, ctx):
        self.db = db

    def process(self, tup):
        self.log("Got into ITEM-BOLT")
        item, person_id = tup.values
        item['mask'] = np.array(item['mask'], dtype=np.uint8)
        item['image'] = np.array(item['image'], dtype=np.uint8)
        if 'gender' in item.keys():
            gender = item['gender'] or 'Female'
        else:
            gender = "Female"
        out_item = {'similar_results': {}}
        start = time.time()
        coll = item['products_collection']
        prod = coll + '_' + gender
        self.log("Sending to FIND_TOP_N")
        out_item['fp'], out_item['similar_results'][coll] = find_similar_mongo.find_top_n_results(item['image'],
                                                                                                  item['mask'], 100,
                                                                                                  item['category'],
                                                                                                  prod)
        self.log("back from find_top_n after {0} secs, collection = {1}".format((time.time() - start), prod))
        for feature in out_item['fp'].keys():
            if isinstance(out_item['fp'][feature], np.ndarray):
                out_item['fp'][feature] = out_item['fp'][feature].tolist()
        out_item['category'] = item['category']
        self.emit([out_item, person_id])

