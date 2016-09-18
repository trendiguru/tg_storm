
from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import tldextract
import datetime
import bson
import time
from trendi.constants import db
from trendi import whitelist, page_results, Utils, background_removal
GENDERATOR_PATH = 'http://extremeli.trendi.guru/demo/genderator'
YONATANS_PATH = 'http://extremeli.trendi.guru/demo/yonatan_gender'


class NewImageBolt(Bolt):

    def initialize(self, conf, ctx):
        self.db = db

    def process(self, tup):
        page_url, image_url, products_collection, method = tup.values

        if db.images.find_one({'image_urls': image_url}):
            return

        domain = tldextract.extract(page_url).registered_domain

        image = Utils.get_cv2_img_array(image_url)
        if image is None:
            self.log("'get_cv2_img_array' has failed. Bad image!")
            return

        image_hash = page_results.get_hash(image)

        gender_obj = db.genderator.find_one({'image_url': image_url})
        image_dict = {'image_urls': [image_url], 'relevant': True, 'views': 1,
                      'saved_date': str(datetime.datetime.utcnow()), 'image_hash': image_hash, 'page_urls': [page_url],
                      'people': gender_obj['people'], 'image_id': str(bson.ObjectId()), 'domain': domain}
        idx = 0
        people_to_emit = []
        for person in image_dict['people']:
            face = person['face']
            isolated_image = background_removal.person_isolation(image, face)

            person_bb = Utils.get_person_bb_from_face(face, image.shape)
            person_args = {'face': face, 'person_bb': person_bb, 'image_id': image_dict['image_id'],
                           'image': isolated_image.tolist(), 'gender': person['gender'], 'domain': domain,
                           'products_collection': products_collection, 'segmentation_method': method}
            people_to_emit.append(person_args)
            idx += 1

        image_dict['num_of_people'] = idx
        image_dict['people'] = []
        self.emit([image_dict, image_dict['image_id']], stream='image_obj')
        # self.log('gonna emit {idx} people from {id}'.format(idx=idx, id=image_dict['image_id']))
        for person in people_to_emit:
            self.emit([person], stream='person_args')
        if not idx:
            db.irrelevant_images.insert_one(image_dict)
            db.iip.delete_one({'image_urls': image_url})
            # self.log('{url} stored as irrelevant, wrong face was found'.format(url=image_url))


class MergePeople(Bolt):

    def initialize(self, conf, ctx):
        self.db = db
        self.bucket = {}

    def process(self, tup):
        if tup.stream == "image_obj":
            # self.log("got to MergePeople from image_obj-stream")
            image_dict, image_id = tup.values
            self.bucket[image_id] = {'person_stack': 0, 'image_obj': image_dict}
        else:
            # self.log("got to MergePeople from MergeItems bolt")
            person, image_id = tup.values
            self.bucket[image_id]['image_obj']['people'].append(person)
            self.bucket[image_id]['person_stack'] += 1
            self.log("so far {0}/{2} persons was saved for image {1}".format(self.bucket[image_id]['person_stack'],
                                                                             image_id,
                                                                             self.bucket[image_id]['image_obj']['num_of_people']))
            if self.bucket[image_id]['person_stack'] == self.bucket[image_id]['image_obj']['num_of_people']:
                image_obj = self.bucket[image_id]['image_obj']
                image_obj['saved_date'] = datetime.datetime.strptime(image_obj['saved_date'], "%Y-%m-%d %H:%M:%S.%f")
                db.images.find_one_and_replace({'image_urls': image_obj['image_urls'][0]}, image_obj, upsert=True)
                db.genderator.delete_one({'image_urls': image_obj['image_urls'][0]})
                db.permanent_images.insert_one(image_obj)
                db.iip.delete_one({'image_urls': image_obj['image_urls'][0]})
                self.log("Done! all people for image {0} arrived, Inserting! :)".format(image_id))
                del self.bucket[image_id]
