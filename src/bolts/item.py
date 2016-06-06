
from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import numpy as np
import time
from trendi import find_similar_mongo
from trendi.constants import db, products_per_site


class ItemBolt(Bolt):

    def initialize(self, conf, ctx):
        self.db = db

    def process(self, tup):
        item, person_id = tup.values
        domain = item['domain']
        item['mask'] = np.array(item['mask'], dtype=np.uint8)
        item['image'] = np.array(item['image'], dtype=np.uint8)
        if 'gender' in item.keys():
            gender = item['gender']
        else:
            gender = "Female"
        out_item = {'similar_results': {}}
        start = time.time()
        if domain in products_per_site.keys():
            coll = products_per_site[domain]
            prod = coll + '_' + gender
        else:
            prod = "ShopStyle_" + gender
        out_item['fp'], out_item['similar_results'][prod] = find_similar_mongo.find_top_n_results(item['image'],
                                                                                                  item['mask'], 100,
                                                                                                  item['category'],
                                                                                                  prod)
        self.log("back from find_top_n after {0} secs..".format(time.time() - start))
        out_item['category'] = item['category']
        self.emit([out_item, person_id])
