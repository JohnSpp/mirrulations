import random
import json
import string
import logging
import os
import zipfile
import tempfile
import shutil
import mirrulations_core.documents_core as dc

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'docs_filter.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = { 'clientip': '192.168.0.1', 'user': 'FILTERS'}
logger = logging.getLogger('tcpserver')

"""
This program does the validation of data from the docs jobs and then creates doc jobs using that data
"""
version= 'v1.3'


# Validation Function
def workfile_length_checker(json_data):
    """
        Checks the file count and attachment count of each work file
        :param json_data: the json containing the work files
        :return: True if there are 1000 or less document ids and 1000 or less attachments per work file
                 False if either the ids or attachments are over 1000
        """
    logger.debug('Function Successful: % s',
                   'work_file_length_checker: work_file_length_checker successfully called from process_docs', extra=d)
    logger.info('Workfile length being checked...')

    file_count = 0
    attachment_count = 0
    for work_file in json_data["data"]:
        for line in work_file:
            file_count += 1
            attachment_count += line["count"]

        fc = file_count > 1000
        ac = attachment_count > 1000
        if fc or ac:

            logger.debug('Variable Failure: %s',
                           'workfile_length_checker: Something went wrong in work_file_length_checker', extra=d)
            if fc is False:
                logger.debug('Variable Failure: %s',
                               'workfile_length_checker: fc is False', extra=d)
            else:
                logger.debug('Variable Success: %s',
                               'work_file_length_checker: fc is True', extra=d)
            if ac is False:
                logger.debug('Variable Failure: %s',
                               'work_file_length_checker: ac is False', extra=d)
            else:
                logger.debug('Variable Success: %s',
                               'workfile_length_checker: ac is True', extra=d)

            logger.debug('Returning: %s',
                           'workfile_length_checker: returning False', extra=d)
            return False
        else:
            logger.debug('Variable Success: %s',
                           'workfile_length_checker: fc and ac are True', extra=d)
            file_count = 0
            attachment_count = 0
    logger.debug('Returning: %s',
                 'work_file_length_checker: returning True', extra=d)
    logger.warning('Workfile length check completed')
    return True


def check_document_exists(json_data):
    """
    Checks to see if a document was already downloaded or already in one of the queues.
    If the document has already been downloaded it will be removed from its workfile.
    If a workfile were to become empty it will be removed to prevent empty doc jobs from existing.
    :param json_data: the json containing the work files
    :return:
    """

    logger.warning('Function Successful: % s',
                   'workfile_length_checker: workfile_length_checker successfully called from process_docs', extra=d)

    home = os.getenv("HOME")
    path = home + "/regulations_data/"
    for workfile in json_data["data"]:
        count = 0
        for line in workfile:
            document = line["id"]
            alpha_doc_org,docket_id,document_id = dc.get_doc_attributes("doc." + document + ".json")
            full_path = path + alpha_doc_org + "/" + docket_id + "/" + document_id + "/" + "doc." + document + ".json"

            count, local_verdict = local_files_check(full_path, count)
            #redis_verdict = redis_files_check

            if local_verdict: #and redis_verdict:
                workfile.pop(count)

    json_data = remove_empty_lists(json_data)
    return json_data


def local_files_check(full_path, count):
    """
    Checks to see if the document exists.
    If the document does not, then the counter is increased
    If the document does exist, the workfile will remove the document
    :param full_path: The path to the document.json
    :param count: The current count of the workfile
    :return: Returns the count and True if the file does exist, else it will return False
    """
    if os.path.isfile(full_path):
        return count, True
    else:
        count += 1
        return count, False


def remove_empty_lists(json_data):
    """
    Removes any empty workfiles from the data field in the json data
    :param json_data: The json data being looked over
    :return: Returns the empty free json data
    """
    json_data["data"] = [workfile for workfile in json_data["data"] if workfile != []]
    return json_data


# Saving and Adding Functions
def add_document_job(redis_server, json_data):
    """
    Creates a job for each work file and then adds each job to the "queue"
    :param json_data: the json data containing all the work files
    :return:
    """
    logger.debug('Function Successful: % s',
                   'add_document_job: add_document_job successfully called from process_docs', extra=d)
    logger.warning('Adding document job to the queue...')

    for work_file in json_data["data"]:
        random_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))

        logger.debug('Calling Function: % s',
                       'add_document_job: add_document_job calling create_document_job', extra=d)
        job = create_document_job(work_file, random_id)
        logger.debug('Function Successful: % s',
                       'add_document_job: successfully add_document_job called create_document_job', extra=d)

        logger.debug('Calling Function: % s',
                       'add_document_job: add_document_job calling add_to_queue', extra=d)
        redis_server.add_to_queue(job)
        logger.debug('Function Successful: % s',
                       'add_document_job: successfully add_document_job called add_to_queue', extra=d)
        logger.warning('Document job successfully added to queue')


def create_document_job(work_file, job_id):
    """
    Creates a job for the server to provide to clients
    :param work_file: The list of ids for the clients to retrieve
    :param job_id: The id for the job
    :return: A json in the form of a dictionary
    """
    logger.debug('Function Successful: % s',
                   'create_document_job: create_document_job successfully called from add_document_job', extra=d)
    logger.warning('Creating document job...')

    dict = {"job_id": job_id, "type": "doc", "data": work_file, "version": version}
    logger.debug('Returning: %s',
                   'create_document_job: returning a json dictionary', extra=d)
    logger.info('Document job created...')
    return json.dumps(dict)


def save_client_log(client_id, compressed_file):
    """
    Save the client log to the home directory of the server
    :param client_id: the id of the client that did the job
    :param compressed_file: the compressed file containing the client log
    :return:
    """

    home=os.getenv("HOME")
    client_path = home + '/client-logs/' + str(client_id) + '/'
    logger.debug('Function Successful: % s',
                   'get_file_list: get_file_list successfully called from process_doc', extra=d)
    logger.info('Saving client log...')

    logger.debug('Calling Function: % s',
                   'get_file_list: get_file_list calling ZipFile', extra=d)
    files = zipfile.ZipFile(compressed_file, "r")
    logger.debug('Function Successful: % s',
                   'get_file_list: get_file_list successfully called ZipFile', extra=d)
    PATH = tempfile.mkdtemp()
    logger.debug('Calling Function: % s',
                   'get_file_list: get_file_list calling extractall', extra=d)
    PATHstr = str(PATH + "/")
    files.extractall(PATHstr)
    logger.debug('Function Successful: % s',
                   'get_file_list: get_file_list successfully called extractall', extra=d)

    # Create a list of all the files in the directory
    logger.debug('Calling Function: % s',
                   'get_file_list: get_file_list calling listdir', extra=d)
    file_list = os.listdir(PATHstr)
    logger.debug('Function Successful: % s',
                   'get_file_list: get_file_list successfully called listdir', extra=d)

    logger.debug('Loop: %s', 'get_file_list: loop through the files in the file list', extra=d)
    for file in file_list:
        if file.endswith(".log"):
            if not os.path.exists(client_path):
                os.makedirs(client_path)
                shutil.copy(PATHstr + file, client_path)
            else:
                shutil.copy(PATHstr + file, client_path)
    logger.debug('Loop Successful: %s', 'get_file_list: loop successful', extra=d)
    logger.info('Log successfully saved')


# Final Function
def process_docs(redis_server, json_data, compressed_file):
    """
    Main documents function, called by the server to compile list of document jobs and add them to the "queue" queue
    :param json_data: the json data for the jobs
    :param compressed_file: the zipfile containing the client's log
    :return:
    """
    logger.debug('Function Successful: % s',
                   'process_docs: process_docs successfully called from return_docs', extra=d)
    logger.info('Processing Jobs...')

    if redis_server.does_job_exist_in_progress(json_data["job_id"]) is False:

        logger.debug('Variable Failure: %s',
                     'process_docs: job_id does not exist in progress queue', extra=d)

    else:
        save_client_log(json_data['client_id'], compressed_file)
        logger.debug('Variable Success: %s',
                     'process_docs: job does exist in progress queue', extra=d)

        logger.debug('Calling Function: % s',
                     'process_docs: process_docs calling workfile_length_checker', extra=d)
        wklc = workfile_length_checker(json_data)
        logger.debug('Function Successful: % s',
                     'process_docs: process_docs successfully called workfile_length_checker', extra=d)

        job_type = json_data["type"] == "docs"

        if wklc and job_type:

            logger.debug('Calling Function: % s',
                         'process_docs: process_docs calling check_document_exists', extra=d)
            json_data = check_document_exists(json_data)

            logger.debug('Calling Function: % s',
                         'process_docs: process_docs calling add_document_job', extra=d)

            add_document_job(json_data)
            logger.debug('Function Successful: % s',
                         'process_docs: process_docs successfully called add_document_job', extra=d)

            logger.debug('Calling Function: % s',
                         'process_docs: process_docs calling get_keys_from_progress', extra=d)
            key = redis_server.get_keys_from_progress(json_data["job_id"])
            logger.debug('Function Successful: % s',
                         'process_docs: process_docs successfully called get_keys_from_progress', extra=d)

            logger.debug('Calling Function: % s',
                         'process_docs: process_docs calling remove_job_from_progress', extra=d)
            redis_server.remove_job_from_progress(key)
            logger.debug('Function Successful: % s',
                         'process_docs: process_docs successfully called remove_job_from_progress', extra=d)

        else:
            logger.debug('Variable Failure: %s',
                         'process_docs: Something went wrong in validation', extra=d)
            if wklc is False:

                logger.debug('Variable Failure: %s',
                             'process_docs: workfile_length_checker is False', extra=d)
            else:
                logger.debug('Variable Success: %s',
                             'process_docs: workfile_length_checker is True', extra=d)

            if job_type is False:
                logger.debug('Variable Failure: %s',
                             'process_docs: job_type is not docs', extra=d)
            else:
                logger.debug('Variable Success: %s',
                             'process_docs: job_type is docs', extra=d)

            logger.debug('Calling Function: % s',
                         'process_docs: process_docs calling renew_job', extra=d)
            redis_server.renew_job(json_data["job_id"])
            logger.debug('Function Successful: % s',
                           'process_docs: process_docs successfully called renew_job', extra=d)
            logger.info('Jobs successfully processed')
