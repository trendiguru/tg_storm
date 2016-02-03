from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt


class OriginCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()

    def process(self, tup):
        origin = tup.values[1]
        self.counts[origin] += 1
        self.emit([origin, self.counts[origin]])
        self.log('%s: %d' % (origin, self.counts[origin]))
