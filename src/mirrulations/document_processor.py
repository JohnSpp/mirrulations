import tempfile
from mirrulations.documents_processor import *
import mirrulations_core.config as config

base_url = 'https://api.data.gov/regulations/v3/document?documentId='

key = config.read_value('key')
client_id = config.read_value('client_id')

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'document_processor.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': client_id}
logger = logging.getLogger('tcpserver')


def document_processor(doc_ids):
    """
    This process takes all of the document ids given to it and saves all of the data for the documents in a temporary directory.
    :param doc_ids: list of document ids that have to be collected.
    :return: temporary directory that data was written to.
    """
    logger.debug('Call Successful: %s', 'document_processor: processing document ID list', extra=d)
    logger.info('Writing documents to temporary directory...')
    dirpath = tempfile.TemporaryDirectory()
    for doc_id in doc_ids:
        logger.debug('Call Successful: %s', 'document_processor: processing document: ' + doc_id, extra=d)
        try:
            result = api_call_manager(add_api_key(make_doc_url(doc_id)))
            total = get_extra_documents(result, dirpath.name, doc_id)
        except CallFailException:
            logger.debug('CallFailException: %s', 'document_processor: error with doc_id ' + doc_id, extra=d)
            logger.error('Doc ID error')
    logger.info('Documents written to temporary directory')
    return dirpath


def make_doc_url(documentId):
    """
    Given a documentId as a string append it to the end of the api call
    :param documentId: the string of a documentId
    :return: the url that will be called with a documentId through regulations.gov API
    """
    return base_url + documentId


def save_document(dirpath, doc_json, documentId):
    """
    Saves the json of the document call
    :param dirpath: path to the directory where the json will be saved
    :param doc_json: the json received from the api call
    :param documentId: the string of a documentId
    :return:
    """
    logger.debug('Call Successful: %s', 'document_processor: saving document with docID: ' + documentId, extra=d)
    logger.info('Saving JSON from document call...')
    location = dirpath + "/doc." + documentId + ".json"
    with open(location , "w+") as f:
        json.dump(doc_json, f)
        logger.info('JSON saved')


def download_document(dirpath, documentId, result, type):
    """
    Saves the other file formats of the document call
    :param dirpath: path to the directory where the download will be saved
    :param documentId: the string of a documentId
    :param result: the result received from the api call
    :param type: the type of file that will be saved
    :return:
    """

    logger.debug('Call Successful: %s', 'document_processor: downloading document with docID: ' + documentId, extra=d)
    logger.info('Saving additional file formats...')

    # These are special cases where the api representation is different from the user's interpretation
    if(type == "excel12book"):
        type = "xlsx"
    if(type == "msw12"):
        type = "doc"
    with open(dirpath + "/doc." + documentId + "." + type, 'wb') as f:
        for chunk in result.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    logger.info('Additional formats saved')


def get_extra_documents(result, dirpath, documentId):
    """
    Download the json of the result from the original api call
    Determine if the document has additional file formats that need to be downloaded
    Determines if the document has attachments that need to be downloaded
    :param result: the result of the api call
    :param dirpath: path to the directory where the download will be saved
    :param documentId: the string of a documentId
    :return: the total number of requests required to download all of them
    """

    logger.debug('Call Successful: %s', 'document_processor: getting extra documents for docID: ' + documentId, extra=d)
    logger.info('Checking for attachments to document...')

    doc_json = json.loads(result.text)
    save_document(dirpath, doc_json, documentId)
    total_requests = 0
    total_requests += download_doc_formats(dirpath, doc_json, documentId)
    total_requests += download_attachments(dirpath, doc_json, documentId)

    logger.info('Found {} additional documents'.format(total_requests))
    return total_requests


def download_doc_formats(dirpath, doc_json, documentId):
    """
    Download the other formats for the document
    :param dirpath: path to the directory where the download will be saved
    :param doc_json: the json from a single document api call
    :param documentId: the string of a documentId
    :return: the total number of requests used to download the extra formats
    """

    logger.debug('Call Successful: %s', 'document_processor: downloading doc formats for docID: ' + documentId, extra=d)
    logger.info('Downloading additional formats...')

    total_requests = 0
    try:
        extra_formats = doc_json["fileFormats"]
        total_requests += len(extra_formats)
        for extra_doc in extra_formats:
            result = api_call_manager(add_api_key(str(extra_doc)))
            here = extra_doc.index("contentType") + 12
            type = extra_doc[here:]
            download_document(dirpath, documentId, result, type)
    except KeyError:
        pass
    except CallFailException:
        logger.debug('CallFailException: %s', 'download_doc_formats: Exception trying to download attachment for '
                     + documentId, extra=d)
        logger.error('Error - Call failed')
        pass
    logger.info('Additional formats downloaded')
    return total_requests


def download_attachments(dirpath, doc_json, documentId):
    """
    Download the other attachments for the document
    :param dirpath: path to the directory where the download will be saved
    :param doc_json: the json from a single document api call
    :param documentId: the string of a documentId
    :return: the total number of requests used to download the extra attachments
    """

    logger.debug('Call Successful: %s', 'document_processor: downloading attachments for docID: ' + documentId, extra=d)
    logger.info('Downloading attachments...')

    total_requests = 0
    try:
        extra_attachments = doc_json["attachments"]
        total_requests += len(extra_attachments)
        for attachment in extra_attachments:
            attachment_formats = attachment["fileFormats"]
            for a_format in attachment_formats:
                here = str(a_format).index("contentType") + 12
                type = str(a_format)[here:]
                result = api_call_manager(add_api_key(str(a_format)))
                download_document(dirpath, documentId, result, type)
    except KeyError:
        pass
    except CallFailException:
        logger.debug('CallFailException: %s', 'download_attachments: error trying to download attachment for '
                     + documentId, extra=d)
        logger.error('Error - Call failed')
        pass
    logger.info('Attachments downloaded')
    return total_requests
