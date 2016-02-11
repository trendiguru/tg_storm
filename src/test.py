__author__ = 'Nadav Paz'

from rq import push_connection, Queue
from trendi.constants import redis_conn
page_url = 'http://www.vogue.com/13396259/super-bowl-50-performers-choose-italian-brands/'
image_url = 'http://ell.h-cdn.co/assets/15/39/980x625/gallery-1443036533-victorian-inspired-collage.jpg'
push_connection(redis_conn)
storm_pipeline = Queue('storm_pipeline')


def run():
    storm_pipeline.enqueue_call(func='pass', args=[page_url, image_url, None])


if __name__ == "__main__":
    run()

