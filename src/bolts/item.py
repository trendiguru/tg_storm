
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
        # if tup.stream == "item_args":
        #     item, person_id = tup.values
        #     image_id = None
        # else:
        #     item, image_id = tup.values
        #     person_id = None
        item, person_id, image_id = tup.values
        # domain = item['domain']
        item['mask'] = np.array(item['mask'], dtype=np.uint8)
        item['image'] = np.array(item['image'], dtype=np.uint8)
        if 'gender' in item.keys():
            gender = item['gender']
        else:
            gender = "Female"
        out_item = {'similar_results': {}}
        start = time.time()
        coll = item['products_collection']
        prod = coll + '_' + gender
        # if domain in products_per_site.keys():
        #     coll = products_per_site[domain]
        #     prod = coll + '_' + gender
        # else:
        #     coll = products_per_site['default']
        #     prod = coll + '_' + gender
        out_item['fp'], out_item['similar_results'][coll] = find_similar_mongo.find_top_n_results(item['image'],
                                                                                                  item['mask'], 100,
                                                                                                  item['category'],
                                                                                                  prod)
        self.log("back from find_top_n after {0} secs..".format(time.time() - start))
        out_item['category'] = item['category']
        self.emit([out_item, person_id, image_id], stream='to_merge_items')
        # if tup.stream == "item_args":
        #     self.emit([out_item, person_id], stream='to_merge_items')
        # else:
        #     self.emit([out_item, image_id], stream='to_merge_objects')
