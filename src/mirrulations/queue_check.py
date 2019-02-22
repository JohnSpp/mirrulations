import redis, logging
from mirrulations.redis_manager import RedisManager

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'redis_log.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': 'REDIS'}
logger = logging.getLogger('tcpserver')


def queue_check(r):
    logger.info('Checking queue...')
    return r.get_all_items_in_progress_no_lock() , r.get_all_items_in_queue_no_lock()


if __name__ == '__main__':
    r = RedisManager(redis.Redis())
    progress,queue = queue_check(r)
    print(progress)
    print(queue)
