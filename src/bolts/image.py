
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


class NewImageBolt(Bolt):

    def initialize(self, conf, ctx):
        self.db = db
        self.stats = {'massege': "Hey! there's a new image waiting in " + GENDERATOR_PATH + ' to be gender-classified !',
                      'date': time.ctime()}

    def process(self, tup):
        page_url, image_url = tup.values

        # check if image is in-process by url
        tempo = db.iip.find_one({'image_url': image_url})
        if tempo and time.time()-tempo['insert_time'] < 3600:
            return

        # check if page domain is in our white-list
        domain = tldextract.extract(page_url).registered_domain
        if not db.whitelist.find_one({'domain': domain}):
            return

        if image_url[:4] == "data":
            image = Utils.data_url_to_cv2_img(image_url)
            image_url = "data"
            if image is None:
                self.log("'data_url_to_cv2_img' has failed. Bad image!")
                return
        else:
            images_by_url = db.images.find_one({"image_urls": image_url})
            if images_by_url:
                return

            images_obj_url = db.irrelevant_images.find_one({"image_urls": image_url})
            if images_obj_url:
                return

            image = Utils.get_cv2_img_array(image_url)
            if image is None:
                self.log("'get_cv2_img_array' has failed. Bad image!")
                return

        image_hash = page_results.get_hash(image)
        images_obj_hash = db['images'].find_one_and_update({"image_hash": image_hash},
                                                           {'$addToSet': {'image_urls': image_url}})
        if images_obj_hash:
            return

        # get faces and relevancy
        relevance = background_removal.image_is_relevant(image, use_caffe=False, image_url=image_url)
        image_dict = {'image_urls': [image_url], 'relevant': relevance.is_relevant, 'views': 1,
                      'saved_date': str(datetime.datetime.utcnow()), 'image_hash': image_hash, 'page_urls': [page_url],
                      'people': [], 'image_id': str(bson.ObjectId())}
        if relevance.is_relevant:
            db.iip.insert_one({'image_url': image_url, 'insert_time': datetime.datetime.utcnow()})
            # There are faces
            people = []
            idx = 0
            for face in relevance.faces:
                x, y, w, h = face
                isolated_image = background_removal.person_isolation(image, face)
                person_bb = [int(round(max(0, x - 1.5 * w))), str(y), int(round(min(image.shape[1], x + 2.5 * w))),
                             min(image.shape[0], 8 * h)]
                if domain in manual_gender_domains:
                    person_id = str(bson.ObjectId())
                    if not db.genderator.count():
                        monitoring.email(self.stats, 'New image to genderize!', ['nadav@trendiguru.com',
                                                                                 'yonatanguy@gmail.com'])
                                                                                 # 'lior@trendiguru.com',
                                                                                 # 'kyle@trendiguru.com'])
                    db.genderator.insert_one({'url': image_url, 'face': face.tolist(), 'status': 'fresh',
                                             'person_id': person_id})
                    pers = db.genderator.find_one({'person_id': person_id})
                    while pers['status'] != 'done':
                        time.sleep(2)
                        self.log(pers)
                        pers = db.genderator.find_one({'person_id': person_id})
                    gender = pers['gender']
                    db.genderator.delete_one({'person_id': person_id})
                    if gender != "not_relevant":
                        person_args = {'face': face.tolist(), 'person_bb': person_bb, 'image_id': image_dict['image_id'],
                                       'image': isolated_image.tolist(), 'gender': gender, 'domain': domain}
                        people.append(person_args)
                        idx += 1
                else:
                    person_args = {'face': face.tolist(), 'person_bb': person_bb, 'image_id': image_dict['image_id'],
                                   'image': isolated_image.tolist(), 'domain': domain}
                    idx += 1
                    people.append(person_args)
            image_dict['num_of_people'] = idx
            self.log("gonna emit {0} as image_id".format(image_dict['image_id']))
            self.emit([image_dict, image_dict['image_id']], stream='image_obj')
            self.log('gonna emit {idx} people from {id}'.format(idx=idx, id=image_dict['image_id']))
            for person in people:
                self.emit([person], stream='person_args')
            if not idx:
                db.irrelevant_images.insert_one(image_dict)
                db.iip.delete_one({'image_url': image_url})
                self.log('{url} stored as irrelevant, wrong face was found'.format(url=image_url))
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
                image_obj = self.bucket[image_id]['image_obj']
                image_obj['saved_date'] = datetime.datetime.strptime(image_obj['saved_date'], "%Y-%m-%d %H:%M:%S.%f")
                insert_result = db.images.insert_one(image_obj)
                db.iip.delete_one({'image_url': image_obj['image_urls'][0]})
                self.log("Done! all people for image {0} arrived, Inserting! :)".format(image_id))
                del self.bucket[image_id]
                if not insert_result.acknowledged:
                    self.log("Insert failed")
