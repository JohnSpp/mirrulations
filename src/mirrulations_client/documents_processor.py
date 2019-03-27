import json

from mirrulations_core import LOGGER, VERSION

WORK_FILES = []


def documents_processor(api_manager, docs_info_list, job_id, client_id):
    """
    Call each url in the list, process the results of the calls and then form a json file to send back the results
    :param urls: list of urls that have to be called
    :param job_id: the id of the job that is being worked on currently
    :param client_id: id of the client calling this function
    :return result: the json to be returned to the server after each call is processed
    """

    global WORK_FILES
    WORK_FILES = []

    for docs_info in docs_info_list:

        try:
            result = api_manager.make_documents_call(page_offset=docs_info[0], results_per_page=docs_info[1])
            process_results(result)
        except api_manager.CallFailException:
            LOGGER.error('Error - URL processing failed')

    result = json.loads(json.dumps({'job_id' : job_id,
                                    'type': 'docs',
                                    'data' : WORK_FILES,
                                    'client_id' : client_id,
                                    'version' : VERSION}))
    return result


def process_results(result):
    """
    Loads the json from the results of the api call
    Gets the list of documents from the json
    Creates a new json that contains the documents returned from each api call
    :param result: Result of the api call
    :return: returns True if the processing completed successfully
    """

    docs_json = json.loads(result.text)

    try:
        doc_list = docs_json['documents']
        make_docs(doc_list)
    except TypeError:
        LOGGER.error('Error - bad JSON')

    return True


def make_docs(doc_list):
    """
    Given a list of document jsons that contain the id and the attachment count
    Add the ids to lists that will contain calls that in total have no more than 1000 predicted API calls
    :param doc_list: list of document ids and attachment counts as a dictionary
    :return: the global workfiles variable that contains all of the work in list
    """
    global WORK_FILES
    size = 0
    work_list = []

    for doc in doc_list:
        doc_id = doc['documentId']
        calls = doc['attachmentCount'] + 1
        if size + calls > 1000:
            WORK_FILES.append(work_list)
            work_list = []
            size = 0
        size += calls
        document = {'id': doc_id, 'count': calls}
        work_list.append(document)

    if size != 0:
        WORK_FILES.append(work_list)

    return WORK_FILES


class BadJsonException(Exception):
    """
    Raised if the json is not correctly formatted or is empty
    """
    pass