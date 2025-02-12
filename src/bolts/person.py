from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
# from streamparse import Stream
import bson
import numpy as np
import time
import traceback
from trendi import constants
from trendi import whitelist, page_results, Utils, background_removal, pipeline, constants
from trendi.paperdoll import pd_falcon_client, neurodoll_falcon_client


class PersonBolt(Bolt):

    # outputs = [Stream(fields=["item" "person_id", "image_id"], name='item_args'),
    #            Stream(fields=["person_obj", "person_id", "image_id"], name='person_obj'),
    #            Stream(fields=[], name='default')]

    def initialize(self, conf, ctx):
        self.db = constants.db

    def process(self, tup):
        image_id = tup.values[0].pop('image_id')
        image = np.array(tup.values[0].pop('image'), dtype=np.uint8)
        person = tup.values[0]
        person['_id'] = str(bson.ObjectId())
        person['items'] = []

        start = time.time()
        try:
            if person['segmentation_method'] == 'pd':
                seg_res = pd_falcon_client.pd(image)

            else:
                seg_res = neurodoll_falcon_client.pd(image)
        except Exception:
            self.log(traceback.format_exc())
            return

        self.log("{0} took {1} seconds..".format(person['segmentation_method'], time.time() - start))
        if 'success' in seg_res and seg_res['success']:
            mask = seg_res['mask']
            if person['segmentation_method'] == 'pd':
                labels = seg_res['label_dict']
            else:
                labels = constants.ultimate_21_dict
        else:
            return
        final_mask = pipeline.after_pd_conclusions(mask, labels, person['face'])
        idx = 0
        items = []
        for num in np.unique(final_mask):
            pd_category = list(labels.keys())[list(labels.values()).index(num)]
            if pd_category in constants.paperdoll_relevant_categories:
                item_mask = 255 * np.array(final_mask == num, dtype=np.uint8)
                if person['gender'] == 'Male':
                    category = constants.paperdoll_paperdoll_men[pd_category]
                else:
                    category = pd_category
                item_args = {'mask': item_mask.tolist(), 'category': category, 'image': image.tolist(),
                             'domain': person['domain'], 'gender': person['gender'],
                             'products_collection': person['products_collection']}
                items.append(item_args)
                idx += 1
        person['num_of_items'] = idx
        person.pop('domain')
        self.emit([person, person['_id'], image_id], stream='person_obj')
        for item in items:
            self.log("emitting {0}".format(item['category']))
            self.emit([item, person['_id']], stream='item_args')


class MergeItems(Bolt):

    # outputs = ["person", "image_id"]

    def initialize(self, conf, ctx):
        self.bucket = {}

    def process(self, tup):
        if tup.stream == "person_obj":
            person_obj, person_id, image_id = tup.values
            self.bucket[person_id] = {'image_id': image_id, 'item_stack': 0, 'person_obj': person_obj}
        else:
            item, person_id = tup.values
            self.bucket[person_id]['person_obj']['items'].append(item)
            self.bucket[person_id]['item_stack'] += 1
            self.log("so far {0}/{2} items was saved for person {1}".format(self.bucket[person_id]['item_stack'],
                                                                            person_id,
                                                                            self.bucket[person_id]['person_obj']['num_of_items']))
            if self.bucket[person_id]['item_stack'] == self.bucket[person_id]['person_obj']['num_of_items']:
                self.emit([self.bucket[person_id]['person_obj'], self.bucket[person_id]['image_id']])
                del self.bucket[person_id]
