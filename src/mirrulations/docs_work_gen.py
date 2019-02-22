#!/usr/bin/env python
import requests
import os
import random
import string
import redis
import mirrulations.redis_manager as redis_manager
import mirrulations.endpoints as endpoints
import logging

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'redis_log.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': 'REDIS'}
logger = logging.getLogger('tcpserver')


def monolith():
    """
    Runs the script. This is one monolithic function (aptly named) as the script just needs to be run; however, there is a certain
    point where I need to break out of the program if an error occurs, and I wasn't sure how exactly sys.exit() would work and whether
    or not it would mess with things outside of / calling this script, so I just made one giant method so I can return when needed.
    :return:
    """

    url_base = "https://api.data.gov/regulations/v3/documents.json?rpp=1000"

    r = redis_manager.RedisManager(redis.Redis())

    home = os.getenv("HOME")
    with open(home + '/.env/regulationskey.txt') as f:
        regulations_key = f.readline().strip()

    current_page = 0

    if regulations_key != "":
        # Gets number of documents available to download
        try:
            record_count = requests.get("https://api.data.gov/regulations/v3/documents.json?api_key=" + regulations_key
                                        + "&countsOnly=1").json()["totalNumRecords"]
        except:
            logger.error('Error occured with API request')
            print("Error occurred with docs_work_gen regulations API request.")
            return 0

        # Gets the max page we'll go to; each page is 1000 documents
        max_page_hit = record_count // 1000

        # This loop generates lists of URLs, sending out a job and writing them to the work server every 1000 URLs.
        # It will stop and send whatever's left if we hit the max page limit.
        while current_page < max_page_hit:
            url_list = []
            for i in range(1000):
                current_page += 1
                url_full = url_base + "&po=" + str(current_page * 1000)

                url_list.append(url_full)

                if current_page == max_page_hit:
                    break

            # Makes a JSON from the list of URLs and send it to the queue as a job
            docs_work = [''.join(random.choices(string.ascii_letters + string.digits, k=16)), "docs", url_list]
            r.add_to_queue(endpoints.generate_json(docs_work))

    else:
        print("No API Key!")


if __name__ == '__main__':
    monolith()
