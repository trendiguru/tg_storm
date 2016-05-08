
from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt
import tldextract
import datetime
import bson
import time
from trendi import monitoring
from trendi.constants import db, manual_gender_domains
from trendi import whitelist, page_results, Utils, background_removal
GENDERATOR_PATH = 'http://extremeli.trendi.guru/demo/genderator'
YONATANS_PATH = 'http://extremeli.trendi.guru/demo/yonatan_gender'


class NewImageBolt(Bolt):

    def initialize(self, conf, ctx):
        self.db = db
        self.stats = {'massege': "Hey! there's a new image waiting in " + GENDERATOR_PATH + ' to be gender-classified !',
                      'date': time.ctime()}
        self.yonatans = {'massege': "Hey Yonatan! there's a new image waiting in " + YONATANS_PATH +
                                    ' to be gender-classified !', 'date': time.ctime()}

    def process(self, tup):
        page_url, image_url = tup.values

        domain = tldextract.extract(page_url).registered_domain

        image = Utils.get_cv2_img_array(image_url)
        if image is None:
            self.log("'get_cv2_img_array' has failed. Bad image!")
            return

        image_hash = page_results.get_hash(image)

        gender_obj = db.genderator.find_one({'image_url': image_url})
        image_dict = {'image_urls': [image_url], 'relevant': True, 'views': 1,
                      'saved_date': str(datetime.datetime.utcnow()), 'image_hash': image_hash, 'page_urls': [page_url],
                      'people': gender_obj['people'], 'image_id': str(bson.ObjectId())}
        idx = 0
        people_to_emit = []
        for person in image_dict['people']:
            face = person['face']
            x, y, w, h = face
            isolated_image = background_removal.person_isolation(image, face)
            person_bb = [int(round(max(0, x - 1.5 * w))), str(y), int(round(min(image.shape[1], x + 2.5 * w))),
                         min(image.shape[0], 8 * h)]
            # INSERT TO YONATAN'S COLLECTION
            # db.yonatan_gender.insert_one({'url': image_url, 'face': face, 'status': 'fresh',
            #                              'person_id': str(bson.ObjectId())})
            # monitoring.email(self.yonatans, 'New image to genderize!', ['yonatanguy@gmail.com'])
            # if 'gender' in person.keys():
            #     gender = person['gender']
            # else:
            #     gender = 'Female'
            #
            # if gender != "not_relevant":
            person_args = {'face': face, 'person_bb': person_bb, 'image_id': image_dict['image_id'],
                           'image': isolated_image.tolist(), 'gender': person['gender'], 'domain': domain}
            people_to_emit.append(person_args)
            idx += 1

        db.genderator.delete_one({'image_url': image_url})
        image_dict['num_of_people'] = idx
        self.emit([image_dict, image_dict['image_id']], stream='image_obj')
        self.log('gonna emit {idx} people from {id}'.format(idx=idx, id=image_dict['image_id']))
        for person in people_to_emit:
            self.emit([person], stream='person_args')
        if not idx:
            db.irrelevant_images.insert_one(image_dict)
            db.iip.delete_one({'image_url': image_url})
            self.log('{url} stored as irrelevant, wrong face was found'.format(url=image_url))


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
                image_obj = self.bucket[image_id]['image_obj']
                image_obj['saved_date'] = datetime.datetime.strptime(image_obj['saved_date'], "%Y-%m-%d %H:%M:%S.%f")
                insert_result = db.images.insert_one(image_obj)
                db.iip.delete_one({'image_url': image_obj['image_urls'][0]})
                self.log("Done! all people for image {0} arrived, Inserting! :)".format(image_id))
                del self.bucket[image_id]
                if not insert_result.acknowledged:
                    self.log("Insert failed")
