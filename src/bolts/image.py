
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
        # check if page domain is in our white-list
        if not tldextract.extract(page_url).registered_domain in whitelist.all_white_lists:
            return
        # check if image is already in some collection in our db
        # I'm not sure we need to check if it in db.images, I think we check it in page_results.py
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
        # check if image is valid
        image = Utils.get_cv2_img_array(image_url)
        if image is None:
            raise IOError("'get_cv2_img_array' has failed. Bad image!")
        # get faces and relevancy
        relevance = background_removal.image_is_relevant(image, use_caffe=False, image_url=image_url)
        image_dict = {'image_urls': [image_url], 'relevant': relevance.is_relevant, 'views': 1,
                      'saved_date': str(datetime.datetime.utcnow()), 'image_hash': image_hash, 'page_urls': [page_url],
                      'people': [], 'image_id': str(bson.ObjectId())}
        if relevance.is_relevant:
            # There are faces
            people = []
            idx = 0
            for face in relevance.faces:
                x, y, w, h = face
                person_bb = [int(round(max(0, x - 1.5 * w))), str(y), int(round(min(image.shape[1], x + 2.5 * w))),
                             min(image.shape[0], 8 * h)]
                person_args = {'face': face.tolist(), 'person_bb': person_bb, 'image_id': image_dict['image_id'],
                               'image_url': image_url}
                idx += 1
                people.append(person_args)
            image_dict['num_of_people'] = idx
            self.log("gonna emit {0} as image_id".format(image_dict['image_id']))
            self.emit([image_dict, image_dict['image_id']], stream='image_obj')
            self.log('gonna emit {idx} people from {id}'.format(idx=idx, id=image_dict['image_id']))
            self.emit_many([people], stream='person_args')
        else:
            db.irrelevant_images.insert_one(image_dict)
            self.log('{url} stored as irrelevant'.format(url=image_url))


class MergePeople(Bolt):

    def initialize(self, conf, ctx):
        self.db = db
        self.bucket = {}

    def process(self, tup):
        if tup.stream == "image_obj":
            self.log("got to MergePeople from image_obj-stream")
            image_dict, image_id = tup.values
            self.bucket[image_id] = {'person_stack': 0, 'image_obj': image_dict}
        else:
            self.log("got to MergePeople from MergeItems bolt")
            person, image_id = tup.values
            self.bucket[image_id]['image_obj']['people'].append(person)
            self.bucket[image_id]['person_stack'] += 1
            self.log("so far {0}/{2} persons was saved for image {1}".format(self.bucket[image_id]['person_stack'],
                                                                             image_id,
                                                                             self.bucket[image_id]['image_obj']['num_of_people']))
            if self.bucket[image_id]['person_stack'] == self.bucket[image_id]['image_obj']['num_of_people']:
                insert_result = db.storm_test.insert_one(self.bucket[image_id]['image_obj'])
                self.log("Done! all people for image {0} arrived, Inserting! :)".format(image_id))
                del self.bucket[image_id]
                if not insert_result.acknowledged:
                    self.log("Insert failed")