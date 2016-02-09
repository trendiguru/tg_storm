

from __future__ import absolute_import, print_function, unicode_literals
from streamparse.bolt import Bolt

from trendi import find_similar_mongo, constants


class ItemBolt(Bolt):

    def initialize(self, conf, ctx):
        self.db = constants.db

    def process(self, tup):
        item, person_id = tup.values
        try:
            item['fp'], item['similar_results'] = find_similar_mongo.find_top_n_results(item['image'], item['mask'],
                                                                                        100, item['category'],
                                                                                        'products')
            self.emit([item, person_id])
        except Exception as e:
            self.log(e.message, e.args)
            # TODO - tell someone we have an item down