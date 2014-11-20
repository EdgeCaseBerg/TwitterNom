import json
import pymongo
import tweepy
import logging
import os,sys
import time

# Go Ahead and change this to something else if desired
config_file = 'conf.json'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class NomStream(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        self.db = pymongo.MongoClient().test

    def on_data(self, tweet):
        logging.debug("New Tweet! %s" % tweet)
        self.db.tweets.insert(json.loads(tweet))
        time.sleep(2)

    def on_error(self, status_code):
        logging.warn("Error! Status Code: %s" % status_code)
        if status_code == 420:
            time.sleep(5)
            pass
        return True # Don't kill the stream

    def on_timeout(self):
        logging.warn("Timeout!")
        return True # Don't kill the stream


if __name__ == '__main__':
    conf = None
    basedir = os.path.realpath(".")
    with open(os.path.join(basedir, config_file)) as fp:
        conf = json.load(fp)

    if not conf:
        logging.error("Could not load configuration")
        raise 'Could not load configuration'

    consumer_key = conf['consumer_key']
    consumer_secret = conf['consumer_secret']
    access_key = conf['access_key']
    access_secret = conf['access_secret']
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)
    sapi = tweepy.streaming.Stream(auth, NomStream(api))
    logging.info("Tracking Hashtags: %s" % str(sys.argv) )
    sapi.filter(track=(sys.argv + conf['hashtags']) )