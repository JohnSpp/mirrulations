import mirrulations.document_processor as doc
import mirrulations.documents_processor as docs
import mirrulations.api_call_management as man
import requests
import json
import os
import time
import logging
import shutil
import mirrulations.config as config
import tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path

# These variables are specific to the current implementation
version = "v1.3"
serverurl = "http://" + config.read_value("ip") + ":" + config.read_value("port")
home = os.getenv("HOME")
with open(home + '/.env/regulationskey.txt') as f:
    key = f.readline().strip()
    client_id = f.readline().strip()

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'client.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': client_id}
logger = logging.getLogger('tcpserver')
handler = RotatingFileHandler('client_log', maxBytes=20, backupCount=5)


client_health_url = "https://hc-ping.com/457a1034-83d4-4a62-8b69-c71060db3a08"


def get_work(client_id):
    """
    Calls the /get_work endpoint of the server to fetch work to process
    :param client_id: the id of the client calling /get_work
    :return: the result of making a call to get work
    """
    global d

    logger.debug('Call Successful: %s', 'get_work: call made successfully', extra=d)
    logger.debug('Assign Variable: %s', 'get_work: create the url for getting work', extra=d)

    logger.info('Attempting to retrieve work...')

    url = serverurl+"/get_work?client_id="+str(client_id)

    logger.debug('Variable Success: %s', 'get_work: url created successfully for get work', extra=d)
    logger.debug('Returning: %s', 'get_work: the respond from the api call to get_work', extra=d)
    logger.info('Work successfully retrieved')

    return man.api_call_manager(url)


def get_json_info(json_result):
    """
    Return job information from server json
    :param json_result: the json returned from
    :return:
    """
    global d

    logger.debug('Call Successful: %s', 'get_json_info: call made successfully', extra=d)
    logger.debug('Assign Variable: %s', 'get_json_info: get the job id from ', extra=d)

    logger.info('Collecting job information...')

    job_id = json_result["job_id"]

    logger.debug('Variable Success: %s', 'get_json_info: job_id retrieved from result json', extra=d)
    logger.debug('Assign Variable: %s', 'get_json_info: get the data from get work endpoint', extra=d)

    urls = json_result["data"]

    logger.debug('Variable Success: %s', 'get_json_info: data retrieved from result json', extra=d)
    logger.debug('Returning: %s', 'get_json_info: returning job id and data from get work', extra=d)

    logger.info('Job information retrieved')

    return job_id, urls


def return_docs(json_result, client_id):
    """
    Handles the documents processing necessary for a job
    Calls the /return_docs endpoint of the server to return data for the job it completed
    :param json_result: the json received from the /get_work endpoint
    :param client_id: the id of the client that is processing the documents job
    :return: result from calling /return_docs
    """
    global d

    logger.info('Returning documents data to server...')

    logger.debug('Call Successful: %s', 'return_docs: call made successfully', extra=d)
    logger.debug('Calling Function: %s','return_docs: call get_json_info for job id and urls',extra=d)

    job_id, urls = get_json_info(json_result)

    logger.debug('Function Successful: %s', 'return_docs: job_id and urls retrieved successfully', extra=d)
    logger.debug('Calling Function: %s','return_docs: call documents_processor',extra=d)

    json_info = docs.documents_processor(urls,job_id,client_id)

    logger.debug('Function Successful: %s', 'return_docs: successful call to documents processor', extra=d)

    logger.debug('Assign Variable: %s', 'return_docs: making the tempfile', extra=d)
    path = tempfile.TemporaryDirectory()
    logger.debug('Variable Success: %s', 'return_docs: tempfile successfully made', extra=d)
    logger.debug('Calling function: %s', 'return_docs: call add_client_log', extra=d)
    add_client_log_files(path.name, ".")
    logger.debug('Function Successful: %s', 'return_docs: add_client_log completed', extra=d)
    logger.debug('Attempt Archive: %s', 'return_docs: attempting to make the archive', extra=d)
    shutil.make_archive("result", "zip", path.name)
    logger.debug('Archive Success: %s', 'return_docs: archive successfully made', extra=d)
    logger.debug('Assign Variable: %s', 'return_docs: opening the zip file to send', extra=d)
    fileobj = open('result.zip', 'rb')
    logger.debug('Variable Success: %s', 'return_docs: zip file opened', extra=d)

    logger.debug('Calling Function: %s', 'return_docs: post to /return_docs endpoint', extra=d)
    r = requests.post(serverurl + "/return_docs", files={'file': fileobj}, data={'json':json.dumps(json_info)})
    logger.debug('Function Successful: %s', 'return_docs: successful call to /return_doc', extra=d)

    r.raise_for_status()

    logger.debug('Returning: %s', 'return_docs: returning information from the call to /return_docs', extra=d)
    logger.info('Documents data returned')

    return r


def return_doc(json_result, client_id):
    """
    Handles the document processing necessary for a job
    Calls the /return_doc endpoint of the server to return data for the job it completed
    :param json_result: the json received from the /get_work endpoint
    :param client_id: the id of the client that is processing the documents job
    :return: result from calling /return_doc
    """
    global d

    logger.info('Returning document data to server...')

    logger.debug('Call Successful: %s', 'return_doc: call made successfully', extra=d)
    logger.debug('Calling Function: %s','return_doc: call get_json_info for job id and urls',extra=d)

    job_id, doc_dicts = get_json_info(json_result)

    logger.debug('Function Successful: %s', 'return_doc: job_id and document ids retrieved successfully', extra=d)
    logger.debug('Assign Variable: %s', 'return_doc: attempting to get document ids from each json', extra=d)

    doc_ids = []
    for dic in doc_dicts:

        logger.debug('Assign Variable: %s', 'return_doc: attempting to get each document id from each json', extra=d)

        doc_ids.append(dic['id'])

        logger.debug('Variable Success: %s', 'return_doc: document id added to the list', extra=d)

    logger.debug('Variable Success: %s', 'return_doc: list of document ids was created', extra=d)

    logger.debug('Function Successful: %s', 'return_doc: result.zip created successfully', extra=d)
    logger.debug('Calling Function: %s', 'return_doc: call document_processor with the list of document ids', extra=d)

    path = doc.document_processor(doc_ids)

    add_client_log_files(path.name, ".")
    logger.debug('Function Successful: %s', 'return_doc: document_processor executed successfully', extra=d)

    logger.debug('File Create Attempt: %s', 'return_doc: attempting to create the zip file', extra=d)
    logger.info('Attempting to create doc file...')

    shutil.make_archive("result", "zip", path.name)
    logger.debug('File Creation Successful: %s', "return_doc: successfully created the zip file", extra=d)
    logger.debug('Assign Variable: %s', 'return_doc: opening the zip', extra=d)
    fileobj = open('result.zip', 'rb')
    logger.debug('Variable Success: %s', 'return_doc: zip opened', extra=d)
    logger.debug('Calling Function: %s', 'return_doc: post to /return_doc endpoint', extra=d)
    r = requests.post(serverurl+"/return_doc",
                      files={'file':('result.zip', fileobj)},
                      data={'json':json.dumps({"job_id" : job_id, "type" : "doc",
                                               "client_id": client_id, "version" : version })})

    logger.info('Doc file created')

    logger.debug('Function Successful: %s', 'return_doc: successful call to /return_doc', extra=d)
    logger.debug('Calling Function: %s','return_doc: Raise Exception for bad status code',extra=d)

    r.raise_for_status()

    logger.debug('Returning: %s', 'return_doc: returning information from the call to /return_docs', extra=d)

    logger.info('Document data returned')

    return r


def copy_file_safely(directory, filepath):
    """
    Safely copies a file to a directory; if the file isn't there to be copied, it won't be copied.
    :param directory: Directory to copy to
    :param filepath: File to copy
    """

    logger.info('Copying file...')
    if Path(filepath).exists():
        if Path(directory).exists():
            shutil.copy(filepath, directory)
            logger.debug('Call Successful: %s', 'copy_file_safely: File copied.', extra=d)
            logger.info('File copied')
        else:
            logger.debug('Exception: %s', 'copy_file_safely: Directory does not exist. Not copying.', extra=d)
            logger.warning('File not copied, directory does not exist')
    else:
        logger.debug('Exception: %s', 'copy_file_safely: No file exists. Not copying.', extra=d)
        logger.warning('File not copied, file does not exist')


def add_client_log_files(directory, log_directory):
    """
    Used to copy client log files into the temp directory to be sent to the server.
    :param directory: Directory to write files to
    :param log_directory: Directory to get files from
    :return:
    """
    logger.info('Copying log files...')

    logger.debug('Calling Function: %s', 'copy_file_safely: copying client.log to tempfile', extra=d)
    copy_file_safely(directory, log_directory + "/client.log")
    logger.debug('Calling Function: %s', 'copy_file_safely: copying document_processor.log to tempfile', extra=d)
    copy_file_safely(directory, log_directory + "/document_processor.log")
    logger.debug('Calling Function: %s', 'copy_file_safely: copying documents_processor.log to tempfile', extra=d)
    copy_file_safely(directory, log_directory + "/documents_processor.log")
    logger.debug('Calling Function: %s', 'copy_file_safely: copying api_call.log to tempfile', extra=d)
    copy_file_safely(directory, log_directory + "/api_call.log")
    logger.debug('Calling Function: %s', 'copy_file_safely: copying api_call_management.log to tempfile', extra=d)
    copy_file_safely(directory, log_directory + "/api_call_management.log")

    logger.info('Log files copied')

def do_work():
    """
    Working loop
    Get work - Determine type of work - Do work - Return work
    If there is no work in the server, sleep for an hour
    :return:
    """
    logger.debug('Call Successful: %s', 'do_work: called successfully', extra=d)
    logger.info('Beginning "do-work" process...')

    while True:

        logger.debug('Calling Function: %s', 'do_work: call to get_work function', extra=d)
        logger.info('Getting work...')
        try:
            work = get_work(client_id)
            logger.debug('Function Successful: %s', 'do_work: get_work call successful', extra=d)

            requests.get(client_health_url)

            logger.debug('Assign Variable: %s', 'do_work: decode the json variable from get_work', extra=d)
            work_json = json.loads(work.content.decode('utf-8'))
            logger.debug('Variable Success: %s', 'do_work: decode the json of work successfully', extra=d)

        except man.CallFailException:
            time.sleep(3600)

        if work_json["type"] == "doc":

            logger.debug('Calling Function: %s', 'do_work: call return_doc', extra=d)
            logger.info('Work is Doc job')

            r = return_doc(work_json, client_id)

            logger.debug('Function Successful: %s', 'do_work: return_doc call successful', extra=d)

            requests.get(client_health_url)

        elif work_json["type"] == "docs":

            logger.debug('Calling Function: %s', 'do_work: call return_docs', extra=d)
            logger.info('Work is Docs job')

            r = return_docs(work_json, client_id)

            logger.debug('Function Successful: %s', 'do_work: return_docs call successful', extra=d)

            requests.get(client_health_url)

        elif work_json["type"] == "none":

            logger.debug('Function Successful: %s', 'do_work: sleep due to no work', extra=d)
            logger.info('No work, sleeping...')

            time.sleep(3600)

            requests.get(client_health_url)

        else:
            logger.debug('Exception: %s', 'do_work: type specified in json object was not in - doc, docs, none')

            logger.error('Job type unexpected')


            requests.get(client_health_url + "/fail")
        logger.debug('Function Successful: %s', 'do_work: successful iteration in do work', extra=d)
        logger.info('Work completed')


if __name__ == '__main__':
    do_work()
