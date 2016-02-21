__author__ = 'Nadav Paz'

from rq import push_connection, Queue
from trendi.constants import redis_conn
page_url = 'http://www.vogue.com/13396259/super-bowl-50-performers-choose-italian-brands/'
image_urls = ['http://ell.h-cdn.co/assets/cm/15/02/980x490/54ac3a68ad2b5_-_elle-00-rainbow-opener-h-elh.jpg',
              'http://botyanszky.hu/wp-content/uploads/2015/02/colour-block.jpg',
              'http://slideshow.tcmwebcorp.com/slideshow/1/fr/21312312321/medias/slide/27278',
              'http://media.style.com/image/slideshows/trends/fashion/2014/11-november/spring-2015-trends/800/530/O-spring-2015-trends.jpg',
              'http://www.fashiongonerogue.com/wp-content/uploads/2014/09/spring-2015-new-york-trends.jpg',
              'http://www.fashiongonerogue.com/wp-content/uploads/2014/10/milan-spring-2015-trends.jpg',
              'http://www.fashiongonerogue.com/wp-content/uploads/2014/10/paris-fashion-week-spring-2015-trends.jpg']
push_connection(redis_conn)
storm_pipeline = Queue('storm_pipeline')


def run():
    for image_url in image_urls:
        storm_pipeline.enqueue_call(func='pass', args=[page_url, image_url, None])


if __name__ == "__main__":
    run()
