import json
import os
import re
import shutil
import tempfile
import zipfile

import mirrulations_core.documents_core as dc

from mirrulations_core import LOGGER

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
    return file_name.split('.')[1]


def get_file_name(path):
    """
    Extracts the name of the file from the given path
    :param path: location of the file in which the name will be extracted from
    :return: file_name: The file name from the path
    """
    split_path = path.split('/')
    file_name = split_path[len(split_path) - 1]
    return file_name


# Validation Functions
def ending_is_number(document_id):
    """
    Ensure that the document id ends in a number
    :param document_id: the document id being checked
    :return: True if the number is a digit, else it will return False
    """
    number = re.split('-', document_id)[-1]
    return number.isdigit()


def id_matches(path, doc_id):
    """
    Ensures that the ids of the documents match correctly
    :param path: the file that is being looked at
    :param doc_id: the document id to check
    :return: True if the document_id equals the doc_id, else it will return False
    """
    f = open(path, 'r')
    j = json.load(f)
    document_id = j['documentId']['value']
    result = document_id == doc_id

    if result is True:
        return True
    else:
        LOGGER.warning('Document IDs do not match')
        return False


def beginning_is_letter(document_id):
    """
    Ensures that the beginning of the document id begins with a letter
    :param document_id: the document id being checked
    :return: True if the first character of the document_id is a letter, else it will return False
    """
    letter = document_id[0]

    result = letter.isalpha()
    if result is True:
        return True
    else:
        LOGGER.warning('Document ID does not begin with a letter')
        return False


# Saving Functions
def save_single_file_locally(cur_path, destination):
    """
    Save the file located at the current path to the destination location
    :param cur_path: location of the file to be saved
    :param destination: location that the file should be saved
    :return:
    """
    file_name = get_file_name(cur_path)
    doc_id = get_document_id(file_name)
    org, docket_id, document_id = dc.get_doc_attributes(doc_id)
    destination_path = destination + org + '/' + docket_id + '/' + document_id + '/'
    create_new_dir(destination_path)
    shutil.copy(cur_path, destination_path + '/' + file_name)


def create_new_dir(path):
    """
    If the path does not exist, create the directory(s)
    :param path: the path to the directory to be created
    :return:
    """
    if not os.path.exists(path):
        os.makedirs(path)


def get_file_list(compressed_file, path_string, client_id):
    """
    Get the list of files to be processed from a compressed file
    :param compressed_file: file containing file list to be uncompressed
    :param path_string: location of the file in string form
    :param client_id: the id of the client that did the job
    :return: The list of file names in the compressed file
    """
    home = os.getenv('HOME')
    client_path = home + '/client-logs/' + str(client_id) + '/'
    files = zipfile.ZipFile(compressed_file, 'r')
    files.extractall(path_string)
    # Create a list of all the files in the directory
    file_list = os.listdir(path_string)

    final_list = []
    for file in file_list:
        if file.startswith('doc.'):
            final_list.append(file)
        elif file.endswith('.log'):
            if not os.path.exists(client_path):
                os.makedirs(client_path)
                shutil.copy(path_string + file, client_path)
            else:
                shutil.copy(path_string + file, client_path)

    return final_list, path_string


# Final Function
def process_doc(redis_server, json_data, compressed_file):
    """
    Main document function, called by the server to check and save document files returned from the client
    :param json_data: json data of the job
    :param compressed_file: compressed file of document data
    :return:
    """
    if redis_server.does_job_exist_in_progress(json_data['job_id']):
        path = tempfile.mkdtemp()
        path_string = str(path + '/')

        # Unzip the zipfile and then remove the tar file and create a list of all the files in the directory
        file_list, path = get_file_list(compressed_file, path_string, json_data['client_id'])

        for file in file_list:
            if_renew = check_single_document(file, json_data)
            if if_renew is True:
                redis_server.renew_job(json_data)
        else:
            save_all_files_locally(file_list, path)
            remove_job(redis_server, json_data)


def check_single_document(file, json_data):
    """
    Checks to see if a document conforms to our naming conventions
    :param file:
    :param json_data:
    :return:
    """
    org, docket_id, document_id = dc.get_doc_attributes(file)
    if_file_starts_with_doc = file.startswith('doc.')
    if_begin_with_doc_letter = beginning_is_letter(document_id)
    if_end_is_doc_num = ending_is_number(document_id)
    job_type = json_data['type'] == 'doc'

    return if_file_starts_with_doc and if_begin_with_doc_letter and if_end_is_doc_num and job_type


def remove_job(redis_server, json_data):
    """
    Removes a specified job from the progress queue
    :param json_data:
    :return:
    """
    key = redis_server.get_keys_from_progress(json_data['job_id'])
    redis_server.remove_job_from_progress(key)


def save_all_files_locally(file_list, path):
    """
    Saves all the files in a list locally
    :param file_list:
    :param path:
    :return:
    """
    for file in file_list:
        home = os.getenv('HOME')
        save_single_file_locally(path + file, home + '/regulations-data/')
