
from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import tldextract
import datetime
import bson

from trendi.constants import db
from trendi import whitelist, page_results, Utils, background_removal


class NewImageBolt(Bolt):

    def initialize(self, conf, ctx):
        self.db = db

    def process(self, tup):
        page_url, image_url = tup.values
        if not tldextract.extract(page_url).registered_domain(page_url) in whitelist.all_white_lists:
            return

        images_by_url = db.images.find_one({"image_urls": image_url})
        if images_by_url:
            return

        images_obj_url = db.irrelevant_images.find_one({"image_urls": image_url})
        if images_obj_url:
            return

        image_hash = page_results.get_hash_of_image_from_url(image_url)
        images_obj_hash = db['images'].find_one_and_update({"image_hash": image_hash},
                                                           {'$addToSet': {'image_urls': image_url}})
        if images_obj_hash:
            return

        image = Utils.get_cv2_img_array(image_url)
        if image is None:
            raise IOError("'get_cv2_img_array' has failed. Bad image!")

        relevance = background_removal.image_is_relevant(image, use_caffe=False, image_url=image_url)
        image_dict = {'image_urls': [image_url], 'relevant': relevance.is_relevant, 'views': 1,
                      'saved_date': datetime.datetime.utcnow(), 'image_hash': image_hash, 'page_urls': [page_url],
                      'people': [], 'image_id': bson.ObjectId()}
        if relevance.is_relevant:
            # There are faces
            idx = 0
            for face in relevance.faces:
                x, y, w, h = face
                person_bb = [int(round(max(0, x - 1.5 * w))), str(y), int(round(min(image.shape[1], x + 2.5 * w))),
                             min(image.shape[0], 8 * h)]
                person_args = {'face': face.tolist(), 'person_bb': person_bb}
                self.emit([person_args, image_dict['image_id'], image_url], stream='person_args')
                idx += 1
                self.log('{idx} people from {url} has been emitted'.format(idx=idx, url=image_url))
            self.emit([image_dict, image_dict['image_id']], stream='image_obj')
        else:
            db.irrelevant_images.insert_one(image_dict)
            self.log('{url} stored as irrelevant'.format(url=image_url))


class MergePeople(Bolt):

    def initialize(self, conf, ctx):
        self.db = db

    def process(self, tup):
        origin = tup.values[1]
        self.counts[origin] += 1
        self.emit([origin, self.counts[origin]])
        self.log('%s: %d' % (origin, self.counts[origin]))