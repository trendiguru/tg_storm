from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import bson
import numpy as np
import time
from trendi import constants
from trendi import whitelist, page_results, Utils, background_removal, pipeline, constants
from trendi.paperdoll import paperdoll_parse_enqueue


class PersonBolt(Bolt):

    # auto_ack = False
    auto_fail = False

    # def fail(self, tup):
    #     tup_id = tup.id if isinstance(tup, Tuple) else tup
    #     self.send_message({'command': 'ack', 'id': tup_id})

    def initialize(self, conf, ctx):
        self.db = constants.db

    # def process(self, tup):
    #     self.log("got into person-bolt! :)")
    #     image_id = tup.values[0].pop('image_id')
    #     image_url = tup.values[0].pop('image_url')
    #     person = tup.values[0]
    #     person['_id'] = str(bson.ObjectId())
    #     person['items'] = []
    #     image = background_removal.person_isolation(Utils.get_cv2_img_array(image_url), person['face'])
    #     # TODO - serialize image obj or .tolist() it
    #     self.log("sending to Herr paperdoll")
    #     start = time.time()
    #     paper_job = paperdoll_parse_enqueue.paperdoll_enqueue(image, str(person['_id']))
    #     while not paper_job.is_finished or paper_job.is_failed:
    #         time.sleep(0.5)
    #     self.log("back from paperdoll after {0} seconds..".format(time.time() - start))
    #     # if paper_job.is_failed:
    #     #     raise SystemError("Paper-job has failed!")
    #     #     # TODO - update someone that we got a man down !
    #     # elif not paper_job.result:
    #     #     elapsed = time.time()-start_time
    #     #     raise SystemError("Paperdoll has returned empty results ({0} elapsed,timeout={1} )!".format(elapsed,paper_job.timeout))
    #     mask, labels = paper_job.result[:2]
    #     final_mask = pipeline.after_pd_conclusions(mask, labels)
    #     idx = 0
    #     items = []
    #     for num in np.unique(final_mask):
    #         category = list(labels.keys())[list(labels.values()).index(num)]
    #         if category in constants.paperdoll_shopstyle_women.keys():
    #             item_mask = 255 * np.array(final_mask == num, dtype=np.uint8)
    #             item_args = {'mask': item_mask.tolist(), 'category': category, 'image': image.tolist()}
    #             items.append(item_args)
    #             idx += 1
    #     person['num_of_items'] = idx
    #     id1 = self.emit([person, person['_id'], image_id], stream='person_obj')
    #     self.log("emitted person {0} to merge, task id is {1}".format(person['_id'], id1))
    #     for item in items:
    #         self.log("emitting {0}".format(item['category']))
    #         id2 = self.emit([item, person['_id']], stream='item_args')
    #         self.log("AFTER ITEM {id} EMIT".format(id=id2))

    def process(self, tup):
        self.log("got into person-bolt! :)")
        image_id = tup.values[0].pop('image_id')
        image_url = tup.values[0].pop('image_url')
        person = tup.values[0]
        person['_id'] = str(bson.ObjectId())
        person['items'] = []
        self.log("sending to Herr paperdoll")
        start = time.time()
        time.sleep(15)
        self.log("back from paperdoll after {0} seconds..".format(time.time() - start))
        idx = 0
        items = []
        for cat in ['dress', 'coat']:
            item_args = {'item_id': str(bson.ObjectId()), 'category': cat}
            items.append(item_args)
            idx += 1
        person['num_of_items'] = idx
        self.log("gonna emit person {0} to merge..".format(person['_id']))
        self.emit([person, person['_id'], image_id], stream='person_obj')
        for item in items:
            self.log("emits the {0}".format(item['category']))
            self.emit([item, person['_id']], stream='item_args')


class MergeItems(Bolt):

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
                self.log("Done! all items for person {0} arrived, ready to Merge! :)".format(person_id))
                self.emit([self.bucket[person_id]['person_obj'], self.bucket[person_id]['image_id']])
                del self.bucket[person_id]
