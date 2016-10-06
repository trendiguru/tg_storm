
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
            gender = item['gender'] or 'Female'
        else:
            gender = "Female"
        out_item = {'similar_results': {}}
        start = time.time()
        coll = item['products_collection']
        prod = coll + '_' + gender
        out_item['fp'], out_item['similar_results'][coll] = find_similar_mongo.find_top_n_results(item['image'],
                                                                                                  item['mask'], 100,
                                                                                                  item['category'],
                                                                                                  prod)
        for feature in out_item['fp'].keys():
            if isinstance(out_item['fp'][feature], np.ndarray):
                out_item['fp'][feature] = out_item['fp'][feature].tolist()
        self.log("find_top_n took {0} secs, {1} , {2}: ".format(time.time() - start, prod, item['category']))
        out_item['category'] = item['category']
        self.emit([out_item, person_id])
