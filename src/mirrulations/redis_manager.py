from ast import literal_eval
import redis_lock
import json
import time
import logging

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'redis_manager.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': 'REDIS'}
logger = logging.getLogger('tcpserver')


class RedisManager:

    def __init__(self, database):
        """
        Initialize the database and create the lock
        """
        logger.info('Initializing redis database...')
        logger.debug('REDIS Setup Successful: %s', '__init___: redis setup', extra=d)
        logger.debug('Assign Variable: %s', '__init__: assign the redis database', extra=d)
        self.r = database
        logger.debug('Variable Success: %s', '__init__: redis database successfully assigned', extra=d)
        logger.debug('Calling Function: %s', '__init__: init attempting to reset locks', extra=d)
        logger.info('Resetting locks...')
        reset_lock(self.r)
        logger.debug('Function Successful: %s', '__init__: init reset the locks', extra=d)
        logger.debug('Assign Variable: %s', '__init__: attempting assign the lock', extra=d)
        logger.debug('Calling Function: %s', '__init__: attempting to set a lock', extra=d)
        self.lock = set_lock(self.r)
        logger.info('Locks reset')
        logger.info('Redis database initialized')

    def get_work(self):
        """
        Gets a single job from the queue
        :return: Returns the work to be done from the queue
        """
        logger.info("Locking database to get a job from the queue...")
        logger.debug('Call Successful: %s', 'get_work: get_work call successful', extra=d)
        logger.debug('Locking: %s', 'get_work: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'get_work: lock retrieved successful', extra=d)
            logger.debug('Assign Variable: %s', 'get_work: attempting to pop item from queue', extra=d)
            item_from_queue = self.r.lpop("queue")
            logger.debug('Variable Success: %s', 'get_work: item_from_queue retrieved an item', extra=d)

            if item_from_queue is None:
                logger.debug('Assign Variable: %s', 'get_work: assign work to have a type of none', extra=d)
                work = {"type": "none"}
                logger.debug('Variable Success: %s', 'get_work: type none assigned to work', extra=d)
                logger.info('Work type was none')
            else:
                logger.debug('Assign Variable: %s', 'get_work: assign work to the item retrieved from the queue', extra=d)
                work = literal_eval(item_from_queue.decode('utf-8'))
                logger.debug('Variable Success: %s', 'get_work: work assigned to item_from_queue', extra=d)
                logger.debug('Queue Add Attempt: %s', 'get_work: attempting to add the work gotten to the progress queue', extra=d)
                self.r.hset("progress", get_curr_time(), json.dumps(work))
                logger.debug('Queue Add Success: %s', 'get_work: work added to the progress queue', extra=d)
            logger.debug("Returning: %s", 'get_work: returning the work to do', extra=d)
            logger.info('Work acquired')
            return work

    def add_to_queue(self, work):
        """
        Adds work to the queue
        :param work: The word to be added to the queue
        :return:
        """
        logger.info('Adding work to queue...')
        logger.debug('Call Successful: %s', 'add_to_queue: add_to_queue call successful', extra=d)
        logger.debug('Locking: %s', 'add_to_queue: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'add_to_queue: lock retrieved successful', extra=d)
            logger.debug('Queue Add Attempt: %s', 'add_to_queue: attempting to push item to queue', extra=d)
            self.r.rpush("queue", work)
            logger.info('Work added to queue')

    def add_to_progress(self, work):
        """
        Adds work to progress queue
        :param work: The work that is in progress
        :return:
        """
        logger.info('Adding work to the progress queue...')
        logger.debug('Call Successful: %s', 'add_to_progress: add_to_progress call successful', extra=d)
        logger.debug('Locking: %s', 'add_to_progress: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'add_to_progress: lock retrieved successful', extra=d)
            logger.debug('Queue Add Attempt: %s', 'add_to_progress: attempting to add item to progress queue', extra=d)
            self.r.hset("progress", get_curr_time(), work)
            logger.info('Work added to progress queue')

    def get_all_items_in_queue(self):
        """
        Returns all the items currently in the queue
        :return: The list of items in the queue
        """
        logger.info('Gathering all items in queue (with lock)...')
        logger.debug('Call Successful: %s', 'get_all_items_in_queue: get_all_items_in_queue call successful', extra=d)
        logger.debug('Locking: %s', 'get_all_items_in_queue: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'get_all_items_in_queue: lock retrieved successful', extra=d)
            logger.debug("Returning: %s", 'get_all_items_in_queue: returning the list of all items in queue', extra=d)
            return self.r.lrange("queue", 0, -1)

    def get_all_items_in_queue_no_lock(self):
        """
        Returns all the items currently in the queue
        :return: The list of items in the queue
        """
        logger.info('Gathering all items in queue (no lock)...')
        logger.debug('Call Successful: %s', 'get_all_items_in_queue_no_lock: get_all_items_in_queue call successful', extra=d)
        logger.debug("Returning: %s", 'get_all_items_in_queue_no_lock: returning the list of all items in queue', extra=d)
        return self.r.lrange("queue", 0, -1)

    def get_all_items_in_progress(self):
        """
        Returns all the items currently in progress
        :return: The list of items currently in progress
        """
        logger.info('Gathering items in progress queue (with lock)...')
        logger.debug('Call Successful: %s', 'get_all_items_in_progress: get_all_items_in_progress call successful', extra=d)
        logger.debug('Locking: %s', 'get_all_items_in_progress: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'get_all_items_in_progress: lock retrieved successful', extra=d)
            logger.debug("Returning: %s", 'get_all_items_in_progress: returning the list of all items in progress', extra=d)
            return self.r.hgetall("progress")

    def get_all_items_in_progress_no_lock(self):
        """
        Returns all the items currently in progress
        :return: The list of items currently in progress
        """
        logger.info('Gathering all items in progress queue (no lock)...')
        logger.debug('Call Successful: %s', 'get_all_items_in_progress_no_lock: get_all_items_in_progress_no_lock call successful', extra=d)
        logger.debug("Returning: %s", 'get_all_items_in_progress_no_lock: returning the list of all items in progress',extra=d)
        return self.r.hgetall("progress")

    def find_expired(self):
        """
        Searches through the work in progress, checks which works have been in progress for over 6 hours,
        and moves those that have been in progress for over 6 hours back to the queue
        :return:
        """
        logger.info('Finding expired jobs...')
        logger.debug('Call Successful: %s','find_expired: find_expired call successful', extra=d)
        logger.debug('Locking: %s', 'find_expired: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'find_expired: lock retrieved successful', extra=d)
            for item in self.r.hgetall('progress'):
                if (float(time.time()) - float(item.decode('utf-8')) > 21600):
                    logger.debug('Queue Remove Attempt: %s', 'find_expired: attmept to remove expired item from progress', extra=d)
                    self.r.hdel('progress',item)
                    logger.debug('Queue Remove Success: %s', 'find_expired: item removed from progress', extra=d)
                    logger.debug('Queue Add Attempt: %s', 'find_expired: attempt to add item to queue', extra=d)
                    self.r.rpush("queue", item)
                    logger.info('Expired work removed from progress queue')

    def delete_all(self):
        """
        Delete everything from the database
        """
        logger.info('Flushing database...')
        logger.debug('Call Successful: %s', 'delete_all: delete_all call successful', extra=d)
        logger.debug('Flushall Attempt: %s', 'delete_all: attempting to flush all items from queue', extra=d)
        self.r.flushall()
        logger.info('Database emptied')

    def get_specific_job_from_queue(self, job_id):
        """
        Gets a job from the "queue" queue using its job_id
        :param job_id: The id for the job in question
        :return: Returns the job of the given job_id or '' if the job does not exist
        """
        logger.info('Retrieving specific job (with lock)...')
        logger.debug('Call Successful: %s', 'get_specific_job_from_queue: get_specific_job_from_queue call successful', extra=d)
        logger.debug('Locking: %s', 'get_specific_job_from_queue: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'get_specific_job_from_queue: lock retrieved successful', extra=d)
            for element in range(self.r.llen('queue')):
                logger.debug('Assign Variable: %s', 'get_specific_job_from_queue: get a certain item from queue', extra=d)
                current = (self.r.lindex('queue', element)).decode("utf-8")
                logger.debug('Variable Success: %s', 'get_specific_job_from_queue: item retrieved', extra=d)

                logger.debug('Assign Variable: %s', 'get_specific_job_from_queue: load the json information', extra=d)
                info = json.loads(current)
                logger.debug('Variable Success: %s', 'get_specific_job_from_queue: json successfully loaded', extra=d)

                if job_id == info['job_id']:
                    logger.debug("Returning: %s", 'get_specific_job_from_queue: returning json information as a string', extra=d)
                    logger.info('Specific job found')
                    return current

            logger.debug("Returning: %s", 'get_specific_job_from_queue: returning nothing if the item wasnt found', extra=d)
            logger.info('Specific job not found')
            return '{"job_id":"null", "type":"none"}'

    def get_specific_job_from_queue_no_lock(self, job_id):
        """
        Gets a job from the "queue" queue using its job_id
        :param job_id: The id for the job in question
        :return: Returns the job of the given job_id or '' if the job does not exist
        """
        logger.info('Retrieving specific job (no lock)...')
        for element in range(self.r.llen('queue')):

            logger.debug('Assign Variable: %s', 'get_specific_job_from_queue_no_lock: get a certain item from queue', extra=d)
            current = (self.r.lindex('queue', element)).decode("utf-8")
            logger.debug('Variable Success: %s', 'get_specific_job_from_queue_no_lock: item retrieved', extra=d)

            logger.debug('Assign Variable: %s', 'get_specific_job_from_queue_no_lock: load the json information', extra=d)
            info = json.loads(current)
            logger.debug('Variable Success: %s', 'get_specific_job_from_queue_no_lock: json successfully loaded', extra=d)

            if job_id == info['job_id']:
                logger.debug("Returning: %s", 'get_specific_job_from_queue_no_lock: returning json information as a string',extra=d)
                logger.info('Specific job found')
                return current
        logger.debug("Returning: %s", 'get_specific_job_from_queue_no_lock: returning nothing if the item wasnt found', extra=d)
        logger.info('Specific job not found')
        return '{"job_id":"null", "type":"none"}'

    def does_job_exist_in_queue(self, job_id):
        """
        Verifies that a given job is in the "queue" queue exists
        :param job_id: The id for the job in question
        :return: True if the job is in the "queue", False if it is not in the "queue"
        """
        logger.info('Searching for job in queue...')
        logger.debug('Call Successful: %s', 'does_job_exist_in_queue: call successful',extra=d)
        logger.debug('Locking: %s', 'does_job_exist_in_queue: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'does_job_exist_in_queue: lock retrieved successful', extra=d)

            logger.debug('Assign Variable: %s', 'does_job_exist_in_queue: assign job a specific item from queue', extra=d)
            job = self.get_specific_job_from_queue_no_lock(job_id)
            logger.debug('Variable Success: %s', 'does_job_exist_in_queue: job assignment successful',extra=d)
            if job == '':
                logger.debug("Returning: %s", 'does_job_exist_in_queues: returning False if the job does not exists', extra=d)
                logger.info('Job not found in queue')
                return False
            else:
                logger.debug("Returning: %s", 'does_job_exist_in_queue: returning True if the job exists', extra=d)
                logger.info('Job found in queue')
                return True

    def remove_specific_job_from_queue(self, job_id):
        """
        Removes a job from the "queue" queue
        :param job_id: The id for the job in question
        """
        logger.info('Removing job from queue...')
        logger.debug('Call Successful: %s', 'remove_specific_job_from_queue: call successful', extra=d)
        logger.debug('Locking: %s', 'remove_specific_job_from_queue: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'remove_specific_job_from_queue: lock retrieved successful', extra=d)

            logger.debug('Assign Variable: %s', 'remove_specific_job_from_queue: assign job a specific item from queue',extra=d)
            job = self.get_specific_job_from_queue_no_lock(job_id)
            logger.debug('Variable Success: %s', 'remove_specific_job_from_queue: job assignment successful', extra=d)

            logger.debug('Queue Remove Attempt: %s', 'remove_specific_job_from_queue: attmept to remove item from queue',extra=d)
            self.r.lrem('queue', 1, job)
            logger.info('Job removed from queue')

    def does_job_exist_in_progress(self, job_id):
        """
        Verifies that a given job is in the "progress" queue exists
        :param job_id: The key of the job
        :return: True if the job is in progress, False if it is not in progress
        """
        logger.info('Searching fro job in progress queue...')
        logger.debug('Call Successful: %s', 'does_job_exist_in_progress: call successful', extra=d)
        logger.debug('Locking: %s', 'does_job_exist_in_progress: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'does_job_exist_in_progress: lock retrieved successful', extra=d)
            logger.debug('Assign Variable: %s', 'does_job_exist_in_progress: get the key of a job', extra=d)
            key = self.get_keys_from_progress_no_lock(job_id)
            logger.debug('Variable Success: %s', 'does_job_exist_in_progress: key has been received', extra=d)
            logger.debug('KEY VALUE: %s', 'does_job_exist_in_progress: ' + str(key), extra=d)
            if float(key) > -1:
                logger.debug('Assign Variable: %s', 'does_job_exist_in_progress: attempt to get the job from the key', extra=d)
                job = self.get_specific_job_from_progress_no_lock(key)
                logger.debug('Variable Success: %s', 'does_job_exist_in_progress: job has been received from the key', extra=d)
                if job == '{"job_id":"null", "type":"none"}':
                    logger.debug("Returning: %s", 'does_job_exist_in_progress: returning False if the job does not exist', extra=d)
                    logger.info('Job not found in progress queue')
                    return False
                else:
                    logger.debug("Returning: %s",'does_job_exist_in_progress: returning True if the job exists', extra=d)
                    logger.info('Job found in progress queue')
                    return True
        logger.debug("Returning: %s", 'does_job_exist_in_progress: returning False if the job does not exist',extra=d)
        logger.info('Job does not exist')
        return False

    def get_specific_job_from_progress(self, key):
        """
        Get a specific job that is in the "progress" queue
        :param key: The key of the job requested
        :return: '' if the job does not exist, otherwise returns the data for the job
        """
        logger.info('Retrieving specific job (with lock)...')
        logger.debug('Call Successful: %s', 'get_specific_job_from_progress: call successful', extra=d)
        logger.debug('Locking: %s', 'get_specific_job_from_progress: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'get_specific_job_from_progress: lock retrieved successful', extra=d)
            logger.debug('Assign Variable: %s', 'get_specific_job_from_progress: attempt to get the job from the hash', extra=d)
            job = self.r.hget('progress', key)
            logger.debug('Variable Success %s', 'get_specific_job_from_progress: job was received from the hash', extra=d)

            if job is not None:
                logger.debug('Variable Assign: %s', 'get_specific_job_from_progress: attempt to decode the job', extra=d)
                data = job.decode("utf-8")
                logger.debug('Variable Success: %s', 'get_specific_job_from_progress: job was successfully decoded', extra=d)
                logger.debug("Returning: %s",'get_specific_job_from_progress: return the decoded job', extra=d)
                logger.info('Specific job found')
                return data
            logger.debug("Returning: %s", 'get_specific_job_from_progress: returning nothing if the item wasnt found', extra=d)
            logger.info('Specific job not found')
            return '{"job_id":"null", "type":"none"}'

    def get_specific_job_from_progress_no_lock(self, key):
        """
        Get a specific job that is in the "progress" queue
        :param key: The key of the job requested
        :return: '' if the job does not exist, otherwise returns the data for the job
        """
        logger.info('Retrieving specific job (no lock)...')
        logger.debug('Call Successful: %s', 'get_specific_job_from_progress_no_lock: call successful', extra=d)
        logger.debug('Assign Variable: %s', 'get_specific_job_from_progress_no_lock: attempt to get the job from the hash',extra=d)
        job = self.r.hget('progress', key)
        logger.debug('Variable Success %s', 'get_specific_job_from_progress_no_lock: job was received from the hash', extra=d)

        if job is not None:
            logger.debug('Variable Assign: %s', 'get_specific_job_from_progress_no_lock: attempt to decode the job', extra=d)
            data = job.decode("utf-8")
            logger.debug('Variable Success: %s', 'get_specific_job_from_progress_no_lock: job was successfully decoded', extra=d)
            logger.debug("Returning: %s", 'get_specific_job_from_progress_no_lock: return the decoded job', extra=d)
            logger.info('Specific job found')
            return data
        logger.debug("Returning: %s", 'get_specific_job_from_progress_no_lock: returning nothing if the item wasnt found',extra=d)
        logger.info('Specific job not found')
        return '{"job_id":"null", "type":"none"}'

    def get_keys_from_progress(self, job_id):
        """
        Get the key of a job that is the "progress" queue
        :param job_id: The id of the job you want to get the key for
        :return: '' if the job does not exist, or the key if the job does exist
        """
        logger.info('Retrieving job key from progress queue (with lock)...')
        logger.debug('Call Successful: %s', 'get_keys_from_progress: call successful', extra=d)
        logger.debug('Locking: %s', 'get_keys_from_progress: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'get_keys_from_progress: lock retrieved successful', extra=d)
            logger.debug('Assign Variable: %s', 'get_keys_from_progress: get the list of keys from the progress hash', extra=d)
            key_list = self.r.hgetall('progress')

            logger.warning('Variable Success: %s', 'get_keys_from_progress: list of keys successfully received', extra=d)
            logger.warning('CLIENT_JOB_ID: %s', job_id, extra=d)
            for key in key_list:
                logger.warning('CURRENT_KEY: %s', key, extra=d)
                logger.warning('Assign Variable: %s', 'get_keys_from_progress: attempt to get the json using the key', extra=d)
                json_info = self.get_specific_job_from_progress_no_lock(key)
                logger.debug('Variable Success: %s', 'get_keys_from_progress: json was received using the key', extra=d)
                logger.debug('Assign Variable: %s', 'get_keys_from_progress: load the json into a string', extra=d)
                info = literal_eval(json_info)

                logger.debug('Variable Success: %s', 'get_keys_from_progress: successfully loaded the json', extra=d)
                if info["job_id"] == job_id:
                    logger.debug('Returning: %s', 'get_keys_from_progress: return the decoded key', extra=d)
                    logger.info('Key found')
                    return key.decode("utf-8")
            logger.debug("Returning: %s",'get_keys_from_progress: returning nothing if the item was not found',extra=d)
            logger.info('Key found')
            return -1

    def get_keys_from_progress_no_lock(self, job_id):
        """
        Get the key of a job that is the "progress" queue
        :param job_id: The id of the job you want to get the key for
        :return: '' if the job does not exist, or the key if the job does exist
        """
        logger.info('Retrieving job key from progress queue (no lock)...')
        logger.debug('Call Successful: %s', 'get_keys_from_progress_no_lock: call successful', extra=d)
        logger.debug('Assign Variable: %s', 'get_keys_from_progress_no_lock: get the list of keys from the progress hash',extra=d)
        key_list = self.r.hgetall('progress')
        logger.warning('Variable Success: %s', 'get_keys_from_progress_no_lock: list of keys successfully received', extra=d)
        logger.warning('CLIENT_JOB_ID: %s', 'get_keys_from_progress_no_lock: ' + str(job_id), extra=d)
        for key in key_list:
            logger.warning('CURRENT_KEY: %s', key, extra=d)
            logger.warning('Assign Variable: %s', 'get_keys_from_progress_no_lock: attempt to get the json using the key',extra=d)

            json_info = self.get_specific_job_from_progress_no_lock(key)
            logger.debug('Variable Success: %s', 'get_keys_from_progress_no_lock: json was received using the key', extra=d)
            logger.debug('Assign Variable: %s', 'get_keys_from_progress_no_lock: load the json into a string', extra=d)
            logger.debug('TestPrintedJson: %s', json_info, extra=d)
            logger.debug('JsonType: %s', type(json_info), extra=d)
            info = literal_eval(json_info)
            logger.debug('Variable Success: %s', 'get_keys_from_progress_no_lock: successfully loaded the json', extra=d)
            if info["job_id"] == job_id:
                logger.debug('Returning: %s', 'get_keys_from_progress_no_lock: return the decoded key', extra=d)
                logger.info('Key found')
                return key.decode("utf-8")
        logger.debug("Returning: %s", 'get_keys_from_progress_no_lock: returning nothing if the item was not found', extra=d)
        logger.info('Key not found')
        return -1

    def remove_job_from_progress(self, key):
        """
        Removes a job from the "progress" queue
        :param key: The key of the job that is to be removed
        :return:
        """
        logger.info('Removing job from progress queue...')
        logger.debug('Call Successful: %s', 'remove_job_from_progress: call successful', extra=d)
        logger.debug('Locking: %s', 'remove_job_from_progress: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'remove_job_from_progress: lock retrieved successful', extra=d)
            logger.debug('Queue Remove Attempt: %s', 'remove_job_from_progress: attempting to remove job from progress', extra=d)
            self.r.hdel('progress', key)
            logger.info('Job removed from progress queue')

    # Combined Functions
    def renew_job(self, job_id):
        """
        Takes an expired job from the "progress" queue and adds it back into the "queue" queue. It then deletes
        the expired job from the "progress" queue
        :param job_id: The id for the job in question
        :return:
        """
        logger.info('Renewing expired job...')
        logger.debug('Call Successful: %s', 'renew_job: call successful', extra=d)
        logger.debug('Locking: %s', 'renew_job: attempting to retrieve lock', extra=d)
        with self.lock:
            logger.debug('Locking: %s', 'renew_job: lock retrieved successful', extra=d)
            logger.debug('Assign Variable: %s', 'renew_job: attempt to get the key from the job_id', extra=d)
            key = self.get_keys_from_progress_no_lock(job_id)
            if float(key) > -1:
                logger.debug('Variable Success: %s', 'renew_job: received the key', extra=d)
                logger.debug('Assign Variable: %s', 'renew_job: attempt to get a job using the key', extra=d)
                job = self.get_specific_job_from_progress_no_lock(key)
                logger.debug('Variable Success: %s', 'renew_job: successfully got the job', extra=d)
                logger.debug('Queue Add Attempt: %s', 'renew_job: attempting to add job back to queue', extra=d)
                self.r.rpush("queue", job)
                logger.debug('Queue Add Success: %s', 'renew_job: added the job back to the queue', extra=d)
                logger.debug('Queue Remove Attempt: %s', 'renew_job: remove a job from progress', extra=d)
                self.r.hdel('progress', key)
                logger.info('Expired job added back to queue')


def reset_lock(database):
    """
    Used to reset the locks
    :param database: The database you are modifying
    :return:
    """
    logger.info('Reseting database locks')
    logger.debug('Call Successful: %s', 'reset_lock: call successful', extra=d)
    logger.debug('Locking: %s', 'reset_lock: all locks have been reset', extra=d)
    redis_lock.reset_all(database)
    logger.info('Database locks reset')


def set_lock(database):
    """
    Sets the lock for the database
    :param database: The database you are modifying
    :return: Locks the database
    """
    logger.info('Setting database locks')
    logger.debug('Call Successful: %s', 'set_lock: call successful', extra=d)
    logger.debug('Locking: %s', 'set_lock: lock has been sent', extra=d)
    logger.debug('Returning: %s', 'set_lock: return the set lock', extra=d)
    logger.info('Database locks set')
    return redis_lock.Lock(database, "lock72")


def get_curr_time():
    """
    Gets the current time
    :return: Returns the current time
    """
    logger.info('Retrieving current time')
    logger.debug('Call Successful: %s', 'get_curr_time: call successful', extra=d)
    logger.info('Current time retrieved')
    return float(time.time())
