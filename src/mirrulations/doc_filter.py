import os
import os.path
import tempfile
import json
import shutil
import re
import zipfile
import logging
import mirrulations_core.documents_core as dc

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'doc_filter.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = { 'clientip': '192.168.0.1', 'user': 'FILTERS'}
logger = logging.getLogger('tcpserver')

"""
This program does the validation of data from the doc jobs and then saves that data locally
"""


# General Functions
def get_document_id(file_name):
    """
    Extract the document id from the file name
    :param file_name: name of the file that the id will be extracted from
    :return id: the string of the document id from the file name
    """
    logger.debug('Function Successful: % s',
                   'get_document_id: get_document_id successfully called from get_doc_attributes', extra=d)
    logger.info('Retrieving document ID...')

    doc,id,ending = file_name.split(".")

    logger.debug('Returning: %s',
                   'get_document_id: returning document_id', extra=d)
    logger.info('Document ID successfully retrieved')
    return id


def get_file_name(path):
    """
    Extracts the name of the file from the given path
    :param path: location of the file in which the name will be extracted from
    :return: file_name: The file name from the path
    """
    logger.debug('Function Successful: % s',
                   'get_file_name: get_file_name successfully called from local_save', extra=d)
    logger.info('Extracting file name...')

    split_path = path.split("/")
    file_name = split_path[len(split_path) - 1]

    logger.debug('Returning: %s',
                   'get_file_name: returning the file name', extra=d)
    logger.info('File name extracted')

    return file_name


# Validation Functions
def ending_is_number(document_id):
    """
    Ensure that the document id ends in a number
    :param document_id: the document id being checked
    :return: True if the number is a digit, else it will return False
    """
    logger.debug('Function Successful: % s',
                   'ending_is_number: ending_is_number successfully called from process_doc', extra=d)
    logger.info('Ensuring document ID ends in a number...')

    logger.debug('Calling Function: % s',
                   'ending_is_number: ending_is_number calling split', extra=d)
    list = re.split("-", document_id)
    logger.debug('Function Successful: % s',
                   'ending_is_number: ending_is_number successfully called split', extra=d)

    number = list[-1]

    logger.debug('Returning: %s',
                   'ending_is_number: returning the number', extra=d)
    logger.info('The ending of the document ID is, in fact, a number')

    return number.isdigit()


def id_matches(path, doc_id):
    """
    Ensures that the ids of the documents match correctly
    :param path: the file that is being looked at
    :param doc_id: the document id to check
    :return: True if the document_id equals the doc_id, else it will return False
    """
    logger.debug('Function Successful: % s',
                   'id_matches: id_matches successfully called from process_doc', extra=d)
    logger.info('Ensuring that the document IDs match...')

    logger.debug('Calling Function: % s',
                   'id_matches: id_matches calling open', extra=d)
    f = open(path, "r")
    logger.debug('Function Successful: % s',
                   'id_matches: id_matches successfully called open', extra=d)

    logger.debug('Calling Function: % s',
                   'id_matches: id_matches calling load', extra=d)
    j = json.load(f)
    logger.debug('Function Successful: % s',
                   'id_matches: id_matches successfully called load', extra=d)

    document_id = j["documentId"]["value"]

    result = document_id == doc_id
    if result is True:
        logger.debug('Returning: %s',
                       'id_matches: returning True', extra=d)
        logger.info('Document IDs match')
        return True
    else:
        logger.debug('Returning: %s',
                       'id_matches: returning False', extra=d)
        logger.warning('Document IDs do not match')
        return False


def beginning_is_letter(document_id):
    """
    Ensures that the beginning of the document id begins with a letter
    :param document_id: the document id being checked
    :return: True if the first character of the document_id is a letter, else it will return False
    """
    logger.debug('Function Successful: % s',
                   'beginning_is_letter: beginning_is_letter successfully called from process_doc', extra=d)
    logger.info('Ensuring that document ID begins with a letter...')
    letter = document_id[0]

    result = letter.isalpha()
    if result is True:
        logger.debug('Returning: %s',
                       'beginning_is_letter: returning True', extra=d)
        logger.info('Docuemnt ID begins with a letter')
        return True
    else:
        logger.debug('Returning: %s',
                       'beginning_is_letter: returning False', extra=d)
        logger.warning('Document ID does not begin with a letter')
        return False


# Saving Functions
def save_single_file_locally(cur_path, destination):
    """
    Save the file located at the current path to the destination location
    :param cur_path: location of the file to be saved
    :param destination: location that the file should be saved
    :return:
    """
    logger.debug('Function Successful: % s',
                   'save_single_file_locally: save_single_file_locally successfully called from process_doc', extra=d)

    logger.debug('Calling Function: % s',
                   'save_single_file_locally: save_single_file_locally calling get_file_name', extra=d)
    file_name = get_file_name(cur_path)
    logger.debug('Function Successful: % s',
                   'save_single_file_locally: save_single_file_locally successfully called get_file_name', extra=d)

    logger.debug('Calling Function: % s',
                   'save_single_file_locally: save_single_file_locally calling get_doc_attributes', extra=d)
    doc_id = get_document_id(file_name)
    org, docket_id, document_id = dc.get_doc_attributes(doc_id)
    logger.debug('Function Successful: % s',
                   'save_single_file_locally: save_single_file_locally successfully called get_doc_attributes', extra=d)

    destination_path = destination + org + "/" + docket_id + "/" + document_id + "/"

    logger.debug('Calling Function: % s',
                   'save_single_file_locally: save_single_file_locally calling create_new_dir', extra=d)
    create_new_dir(destination_path)
    logger.debug('Function Successful: % s',
                   'save_single_file_locally: save_single_file_locally successfully called create_new_dir', extra=d)

    logger.debug('Calling Function: % s',
                   'save_single_file_locally: save_single_file_locally calling copy', extra=d)
    shutil.copy(cur_path, destination_path + '/' + file_name)
    logger.debug('Function Successful: % s',
                   'save_single_file_locally: save_single_file_locally successfully called copy', extra=d)


def create_new_dir(path):
    """
    If the path does not exist, create the directory(s)
    :param path: the path to the directory to be created
    :return:
    """
    logger.debug('Function Successful: % s',
                   'create_new_dir: create_new_dir successfully called from save_single_file_locally', extra=d)

    if not os.path.exists(path):
        logger.debug('Calling Function: % s',
                       'create_new_dir: create_new_dir calling makedirs', extra=d)
        os.makedirs(path)
        logger.debug('Function Successful: % s',
                       'create_new_dir: create_new_dir successfully called makedirs', extra=d)


def get_file_list(compressed_file, PATHstr, client_id):
    """
    Get the list of files to be processed from a compressed file
    :param compressed_file: file containing file list to be uncompressed
    :param PATHstr: location of the file in string form
    :param client_id: the id of the client that did the job
    :return: The list of file names in the compressed file
    """

    home = os.getenv("HOME")
    client_path = home + '/client-logs/' + str(client_id) + '/'
    logger.debug('Function Successful: % s',
                   'get_file_list: get_file_list successfully called from process_doc', extra=d)

    logger.debug('Calling Function: % s',
                   'get_file_list: get_file_list calling ZipFile', extra=d)
    files = zipfile.ZipFile(compressed_file, "r")
    logger.debug('Function Successful: % s',
                   'get_file_list: get_file_list successfully called ZipFile', extra=d)

    logger.debug('Calling Function: % s',
                   'get_file_list: get_file_list calling extractall', extra=d)
    files.extractall(PATHstr)
    logger.debug('Function Successful: % s',
                   'get_file_list: get_file_list successfully called extractall', extra=d)

    # Create a list of all the files in the directory
    logger.debug('Calling Function: % s',
                   'get_file_list: get_file_list calling listdir', extra=d)
    file_list = os.listdir(PATHstr)
    logger.debug('Function Successful: % s',
                   'get_file_list: get_file_list successfully called listdir', extra=d)

    final_list = []
    logger.debug('Loop: %s', 'get_file_list: loop through the files', extra=d)
    for file in file_list:
        if file.startswith("doc."):
            final_list.append(file)
        elif file.endswith(".log"):
            if not os.path.exists(client_path):
                os.makedirs(client_path)
                shutil.copy(PATHstr + file, client_path)
            else:
                shutil.copy(PATHstr + file, client_path)
    logger.debug('Loop successful: %s', 'get_file_list: successfully looped through the files', extra=d)

    logger.debug('Returning: %s',
                   'get_file_list: returning list of files', extra=d)
    return final_list, PATHstr


# Final Function
def process_doc(redis_server, json_data, compressed_file):
    """
    Main document function, called by the server to check and save document files returned from the client
    :param json_data: json data of the job
    :param compressed_file: compressed file of document data
    :return:
    """
    logger.debug('FILTER JOB_ID: %s', 'process_doc: ' + str(json_data["job_id"]), extra=d)
    if redis_server.does_job_exist_in_progress(json_data["job_id"]) is False:
        logger.debug('Variable Failure: %s',
                       'process_doc: job_id does not exist in progress queue', extra=d)

    else:

        PATH = tempfile.mkdtemp()
        PATHstr = str(PATH + "/")

        # Unzip the zipfile and then remove the tar file and create a list of all the files in the directory
        file_list, path = get_file_list(compressed_file, PATHstr, json_data['client_id'])

        for file in file_list:
            ifRenew = check_single_document(file, json_data, path)
            if ifRenew is True:
                logger.debug('Calling Function: % s',
                             'process_doc: process_doc calling renew_job', extra=d)
                redis_server.renew_job(json_data)
                logger.debug('Function Successful: % s',
                             'process_doc: process_doc successfully called renew_job', extra=d)

        else:
            save_all_files_locally(file_list, path)
            logger.debug('Calling Function: % s',
                         'process_doc: process_doc calling remove_job_from_progress', extra=d)
            remove_job(redis_server, json_data)
            logger.debug('Function Successful: % s',
                         'process_doc: process_doc successfully called remove_job_from_progress', extra=d)


def check_single_document(file, json_data, path):
    """
    Checks to see if a document conforms to our naming conventions
    :param file:
    :param json_data:
    :param path:
    :return:
    """
    org, docket_id, document_id = dc.get_doc_attributes(file)
    ifFileStartsWithDoc = file.startswith("doc.")
    ifBeginWithDocLetter = beginning_is_letter(document_id)
    ifEndIsDocNum = ending_is_number(document_id)
    ifFileEndsWithJson = file.endswith(".json")
    job_type = json_data["type"] == "doc"
    ifDocumentsChecks = ifFileStartsWithDoc and ifBeginWithDocLetter and ifEndIsDocNum and job_type
    ifDocumentsChecksAndJson = ifDocumentsChecks and ifFileEndsWithJson

    if ifDocumentsChecksAndJson:
        if id_matches(path + file, document_id):
            logger.debug('Variable Success: %s',
                         'process_doc: ifFileStartsWithDoc, ifBeginWithDocLetter, ifEndIsDocNum, '
                         'and job_type are True', extra=d)
        else:
            logger.debug('Variable Failure: %s',
                         'process_doc: id_matches is False', extra=d)
            return True

    write_documents_checks_into_logger(ifBeginWithDocLetter, ifEndIsDocNum, ifFileStartsWithDoc, job_type)
    return ifDocumentsChecks


def remove_job(redis_server, json_data):
    """
    Removes a specified job from the progress queue
    :param json_data:
    :return:
    """
    key = redis_server.get_keys_from_progress(json_data["job_id"])
    redis_server.remove_job_from_progress(key)


def save_all_files_locally(file_list, path):
    """
    Saves all the files in a list locally
    :param file_list:
    :param path:
    :return:
    """
    for file in file_list:
        home = os.getenv("HOME")
        save_single_file_locally(path + file, home + "/regulations-data/")


def write_documents_checks_into_logger(ifBeginWithDocLetter, ifEndIsDocNum, ifFileStartsWithDoc, job_type):
    """
    Writes the results of a document's individual checks into the log
    :param ifBeginWithDocLetter:
    :param ifEndIsDocNum:
    :param ifFileStartsWithDoc:
    :param job_type:
    :return:
    """
    list = [ifBeginWithDocLetter, ifEndIsDocNum, ifFileStartsWithDoc, job_type]
    listNames = ["ifBeginWithDocLetter", "ifEndIsDocNum", "ifFileStartsWithDoc", "job_type"]

    for x in range(4):
        logger.debug('Variable Failure: %s',
                     'process_doc: ' + listNames[x] + " is " + str(list[x]), extra=d)
