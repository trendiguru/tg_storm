
from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import time
import bson
import numpy as np

from trendi import whitelist, page_results, Utils, background_removal, pipeline, constants
from trendi.paperdoll import paperdoll_parse_enqueue


class PersonBolt(Bolt):

    def initialize(self, conf, ctx):
        self.db = constants.db

    def process(self, tup):
        image_id, image_url = tup[1:]
        person = tup[0]
        person['_id'] = bson.ObjectId()
        image = background_removal.person_isolation(Utils.get_cv2_img_array(image_url), person['face'])
        # TODO - serialize image obj or .tolist() it
        start_time = time.time()
        paper_job = paperdoll_parse_enqueue.paperdoll_enqueue(image, str(person['_id']))
        while not paper_job.is_finished or paper_job.is_failed:
            time.sleep(0.5)
        if paper_job.is_failed:
            raise SystemError("Paper-job has failed!")
            # TODO - update someone that we got a man down !
        elif not paper_job.result:
            elapsed = time.time()-start_time
            raise SystemError("Paperdoll has returned empty results ({0} elapsed,timeout={1} )!".format(elapsed,paper_job.timeout))
        mask, labels = paper_job.result[:2]
        final_mask = pipeline.after_pd_conclusions(mask, labels)
        for num in np.unique(final_mask):
            category = list(labels.keys())[list(labels.values()).index(num)]
            if category in constants.paperdoll_shopstyle_women.keys():
                item_mask = 255 * np.array(final_mask == num, dtype=np.uint8)
                item_args = {'mask': item_mask, 'category': category, 'image': image}
                self.emit([item_args, person['_id']], stream='item_args')
                self.emit([person, person['_id'], image_id], stream='person_obj')


class MergeItems(Bolt):

    def initialize(self, conf, ctx):
        self.db = constants.db

    def process(self, tup):
        origin = tup.values[1]
        self.counts[origin] += 1
        self.emit([origin, self.counts[origin]])
        self.log('%s: %d' % (origin, self.counts[origin]))