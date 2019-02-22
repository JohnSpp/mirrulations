#!/usr/bin/env python
import os, os.path, tempfile, json, shutil, re, zipfile
import redis
import logging
from mirrulations.redis_manager import RedisManager

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(filename='doc_filter.log', filemode='w', format=FORMAT)
d = { 'clientip': '192.168.0.1', 'user': 'FILTERS'}
logger = logging.getLogger('tcpserver')

r = RedisManager(redis.Redis())

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


def get_doc_attributes(file_name):
    """
    Get the organization(s), the docket_id and the document_id from a file name
    :param file_name: name of the file to extract attributes of the document name
    :return: orgs: The organizations(s),
             docket_id: the docket_id,
             document_id: the document_id
    """

    logger.debug('Function Successful: % s',
                   'get_doc_attributes: get_doc_attributes successfully called from process_doc', extra=d)

    logger.debug('Calling Function: % s',
                   'get_doc_attributes: get_doc_attributes calling get_document_id', extra=d)
    document_id = get_document_id(file_name)
    logger.debug('Function Successful: % s',
                   'get_doc_attributes: get_doc_attributes successfully called get_document_id', extra=d)

    if "_" in document_id:
        logger.debug('Calling Function: % s',
                       'get_doc_attributes: get_doc_attributes calling split', extra=d)
        split_name = re.split("[-_]", document_id)
        logger.debug('Function Successful: % s',
                       'get_doc_attributes: get_doc_attributes successfully called split', extra=d)

        org = split_name[0] + "_" + split_name[1]
        docket_id = org + "_" + split_name[2]
        document_id = docket_id + "-" + split_name[3]
        logger.debug('Returning: %s',
                       'get_doc_attributes: returning the organization, docket_id, and document_id', extra=d)
        return org, docket_id, document_id

    else:
        logger.debug('Calling Function: % s',
                       'get_doc_attributes: get_doc_attributes calling split', extra=d)
        split_name = re.split("[-]", document_id)
        logger.debug('Function Successful: % s',
                       'get_doc_attributes: get_doc_attributes successfully called split', extra=d)
        length = len(split_name)
        count = 0
        for x in range(length):
            if split_name[x].isdigit():
                break
            else:
                count += 1

        org_list = split_name[:count]
        org_list.sort()

        logger.debug('Calling Function: % s',
                       'get_doc_attributes: get_doc_attributes calling add_hyphens', extra=d)
        org = add_hyphens(org_list)
        logger.debug('Function Successful: % s',
                       'get_doc_attributes: get_doc_attributes successfully called add_hyphens', extra=d)

        logger.debug('Calling Function: % s',
                       'get_doc_attributes: get_doc_attributes calling add_hyphens', extra=d)
        docket_id = add_hyphens(split_name[:len(split_name) - 1])
        logger.debug('Function Successful: % s',
                       'get_doc_attributes: get_doc_attributes successfully called add_hyphens', extra=d)

        logger.debug('Calling Function: % s',
                       'get_doc_attributes: get_doc_attributes calling add_hyphens', extra=d)
        document_id = add_hyphens(split_name[:len(split_name)])
        logger.debug('Function Successful: % s',
                       'get_doc_attributes: get_doc_attributes successfully called add_hyphens', extra=d)

        logger.debug('Returning: %s',
                       'get_doc_attributes: returning the organization, docket_id, and document_id', extra=d)
        return org,docket_id,document_id


def add_hyphens(list):
    """
    Adds hyphens between the list of strings passed
    :param list: the list to be hyphenated
    :return: A string of the list with hyphens in-between
    """
    logger.debug('Function Successful: % s',
                   'add_hyphens: add_hyphens successfully called from get_doc_attributes', extra=d)
    logger.info('Hyphenating strings...')

    hyphened_string = ""
    for x in range(len(list)):

        if x == 0:
            if len(list) == 1:
                hyphened_string = list[x]
            else:
                hyphened_string = list[x] + "-"

        elif x == len(list) - 1:
            hyphened_string = hyphened_string + list[x]

        else:
            hyphened_string = hyphened_string + list[x] + "-"

    logger.debug('Returning: %s',
                   'add_hyphens: returning the hyphened_string', extra=d)
    logger.info('Strings hyphenated')

    return hyphened_string


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
def local_save(cur_path, destination):
    """
    Save the file located at the current path to the destination location
    :param cur_path: location of the file to be saved
    :param destination: location that the file should be saved
    :return:
    """
    logger.debug('Function Successful: % s',
                   'local_save: local_save successfully called from process_doc', extra=d)
    logger.info('Saving file locally...')

    logger.debug('Calling Function: % s',
                   'local_save: local_save calling get_file_name', extra=d)
    file_name = get_file_name(cur_path)
    logger.debug('Function Successful: % s',
                   'local_save: local_save successfully called get_file_name', extra=d)

    logger.debug('Calling Function: % s',
                   'local_save: local_save calling get_doc_attributes', extra=d)
    org, docket_id, document_id = get_doc_attributes(file_name)
    logger.debug('Function Successful: % s',
                   'local_save: local_save successfully called get_doc_attributes', extra=d)

    destination_path = destination + org + "/" + docket_id + "/" + document_id + "/"

    logger.debug('Calling Function: % s',
                   'local_save: local_save calling create_new_dir', extra=d)
    create_new_dir(destination_path)
    logger.debug('Function Successful: % s',
                   'local_save: local_save successfully called create_new_dir', extra=d)

    logger.debug('Calling Function: % s',
                   'local_save: local_save calling copy', extra=d)
    shutil.copy(cur_path, destination_path + '/' + file_name)
    logger.debug('Function Successful: % s',
                   'local_save: local_save successfully called copy', extra=d)
    logger.info('File saved locally')


def create_new_dir(path):
    """
    If the path does not exist, create the directory(s)
    :param path: the path to the directory to be created
    :return:
    """
    logger.debug('Function Successful: % s',
                   'create_new_dir: create_new_dir successfully called from local_save', extra=d)

    if not os.path.exists(path):
        logger.debug('Calling Function: % s',
                       'create_new_dir: create_new_dir calling makedirs', extra=d)
        logger.info('Creating a new directory...')

        os.makedirs(path)
        logger.debug('Function Successful: % s',
                       'create_new_dir: create_new_dir successfully called makedirs', extra=d)
        logger.info('Directory created')


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
    logger.info('Retrieving files to be processed...')

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
    logger.info('Files retrieved and ready for processing')
    return final_list, PATHstr


# Final Function
def process_doc(json_data, compressed_file):
    """
    Main document function, called by the server to check and save document files returned from the client
    :param json_data: json data of the job
    :param compressed_file: compressed file of document data
    :return:
    """
    logger.debug('Function Successful: % s',
                   'process_doc: process_doc successfully called from return_doc', extra=d)
    logger.info('Processing Jobs...')

    logger.debug('FILTER JOB_ID: %s', 'process_doc: ' + json_data["job_id"], extra=d)
    if r.does_job_exist_in_progress(json_data["job_id"]) is False:
        logger.debug('Variable Failure: %s',
                       'process_doc: job_id does not exist in progress queue', extra=d)
        logger.warning('Job does not exist in progress queue')

    else:
        logger.debug('Calling Function: % s',
                       'process_doc: process_doc calling mkdtemp', extra=d)
        PATH = tempfile.mkdtemp()
        logger.debug('Function Successful: % s',
                       'process_doc: process_doc successfully called mkdtemp', extra=d)
        PATHstr = str(PATH + "/")

        # Unzip the zipfile and then remove the tar file and create a list of all the files in the directory
        logger.debug('Calling Function: % s',
                       'process_doc: process_doc calling mkdtemp', extra=d)
        file_list, path = get_file_list(compressed_file, PATHstr, json_data['client_id'])
        logger.debug('Function Successful: % s',
                       'process_doc: process_doc successfully called mkdtemp', extra=d)

        renew = False
        for file in file_list:
            logger.info('Still processing...')
            logger.debug('Calling Function: % s',
                           'process_doc: process_doc calling get_doc_attributes', extra=d)
            org, docket_id, document_id = get_doc_attributes(file)
            logger.debug('Function Successful: % s',
                           'process_doc: process_doc successfully called get_doc_attributes', extra=d)

            logger.debug('Calling Function: % s',
                           'process_doc: process_doc calling startswith', extra=d)
            fsw = file.startswith("doc.")
            logger.debug('Function Successful: % s',
                           'process_doc: process_doc successfully called startswith', extra=d)

            logger.debug('Calling Function: % s',
                           'process_doc: process_doc calling beginning_is_letter', extra=d)
            bil = beginning_is_letter(document_id)
            logger.debug('Function Successful: % s',
                           'process_doc: process_doc successfully called beginning_is_letter', extra=d)

            logger.debug('Calling Function: % s',
                           'process_doc: process_doc calling ending_is_number', extra=d)
            ein = ending_is_number(document_id)
            logger.debug('Function Successful: % s',
                           'process_doc: process_doc successfully called ending_is_number', extra=d)

            logger.debug('Calling Function: % s',
                           'process_doc: process_doc calling endswith', extra=d)
            few = file.endswith(".json")
            logger.debug('Function Successful: % s',
                           'process_doc: process_doc successfully called endswith', extra=d)

            job_type = json_data["type"] == "doc"

            if fsw and bil and ein and few and job_type:
                logger.debug('Calling Function: % s',
                               'process_doc: process_doc calling id_matches', extra=d)
                if id_matches(path + file, document_id):
                    logger.debug('Variable Success: %s',
                                   'process_doc: fsw, bil, ein, and job_type are True', extra=d)
                    logger.debug('Function Successful: % s',
                                   'process_doc: process_doc successfully called id_matches', extra=d)
                else:
                    logger.debug('Variable Success: %s',
                                   'process_doc: fsw, bil, ein, and job_type are True', extra=d)
                    logger.debug('Function Successful: % s',
                                   'process_doc: process_doc successfully called id_matches', extra=d)
                    logger.debug('Variable Failure: %s',
                                   'process_doc: id_matches is False', extra=d)
                    renew = True
                    break

            elif fsw and bil and ein and job_type:
                logger.debug('Variable Success: %s',
                               'process_doc: fsw, bil, ein, and job_type are True', extra=d)
            else:
                if fsw is False:
                    logger.debug('Variable Failure: %s',
                                   'process_doc: fsw is False', extra=d)
                else:
                    logger.debug('Variable Success: %s',
                                   'process_doc: fsw is True', extra=d)
                if bil is False:
                    logger.debug('Variable Failure: %s',
                                   'process_doc: bil is False', extra=d)
                else:
                    logger.debug('Variable Success: %s',
                                   'process_doc: bil is True', extra=d)
                if ein is False:
                    logger.debug('Variable Failure: %s',
                                   'process_doc: ein is False', extra=d)
                else:
                    logger.debug('Variable Success: %s',
                                   'process_doc: ein is True', extra=d)
                if job_type is False:
                    logger.debug('Variable Failure: %s',
                                   'process_doc: job_type is not doc', extra=d)
                else:
                    logger.debug('Variable Success: %s',
                                   'process_doc: job_type is doc', extra=d)
                renew = True
                break

        if renew is True:
            logger.debug('Calling Function: % s',
                           'process_doc: process_doc calling renew_job', extra=d)
            r.renew_job(json_data)
            logger.debug('Function Successful: % s',
                           'process_doc: process_doc successfully called renew_job', extra=d)

        else:
            for file in file_list:
                logger.debug('Calling Function: % s',
                               'process_doc: process_doc calling local_save', extra=d)
                home = os.getenv("HOME")
                local_save(path + file, home + "/regulations-data/")
                logger.debug('Function Successful: % s',
                               'process_doc: process_doc successfully called local_save', extra=d)

            logger.debug('Calling Function: % s',
                           'process_doc: process_doc calling get_keys_from_progress', extra=d)
            key = r.get_keys_from_progress(json_data["job_id"])
            logger.debug('Function Successful: % s',
                           'process_doc: process_doc successfully called get_keys_from_progress', extra=d)

            logger.debug('Calling Function: % s',
                           'process_doc: process_doc calling remove_job_from_progress', extra=d)
            r.remove_job_from_progress(key)
            logger.debug('Function Successful: % s',
                           'process_doc: process_doc successfully called remove_job_from_progress', extra=d)

    logger.info('Processing complete')
