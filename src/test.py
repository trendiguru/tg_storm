__author__ = 'Nadav Paz'

from rq import push_connection, Queue
from trendi.constants import redis_conn
page_url = 'http://www.vogue.com/13396259/super-bowl-50-performers-choose-italian-brands/'
image_url = 'http://cdn.fashionisers.com/wp-content/uploads/2015/03/fall_winter_2015_2016_fashion_trends_eighties_fashion.jpg'
push_connection(redis_conn)
storm_pipeline = Queue('storm_pipeline')


def run():
    storm_pipeline.enqueue_call(func='pass', args=[page_url, image_url, None])


if __name__ == "__main__":
    run()

