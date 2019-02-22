from flask import Flask, request, Response
import redis
import json
from mirrulations.docs_filter import process_docs
from mirrulations.doc_filter import process_doc
from mirrulations.redis_manager import RedisManager
import logging
import io
import mirrulations.config as config


FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'endpoints_log.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': 'FLASK'}
logger = logging.getLogger('tcpserver')

app = Flask(__name__)


r = RedisManager(redis.Redis())

version = 'v1.3'


@app.route('/')
def default():
    """
    Default endpoint
    :return: Returns empty json
    """
    logger.debug('Successful API Call: %s', 'default: default endpoint', extra=d)
    return json.dumps({})


@app.route('/get_work')
def get_work():
    """
    Endpoint the user will use to get work from the queue
    client_id will be one of the parameters given for logging purposes
    :return: Returns the json containing the job_id, the type of work to be done, the work that nees to be done, and
    the version number
    """
    logging.warning("Successful API Call: %s", 'get_work: get_work', extra=d)
    logger.info('Calling API to get work...')
    if len(request.args) != 1:
        logger.debug('Exception: %s', 'get_work: Get Exception for incorrect number of parameters', extra=d)
        logger.error('Error - number of parameters incorrect')
        return 'Parameter Missing', 400
    logger.debug('Assign Variable: %s', 'get_work: attempting to get client_id', extra=d)
    client_id = request.args.get('client_id')
    logger.debug('Variable Success: %s', 'get_work: successfully retrieved the client id', extra=d)
    if client_id is None:
        logging.warning("Exception: %s", 'get_work: BadParameterException, client id was none', extra=d)
        logger.error('Error - no client ID')
        return 'Bad Parameter', 400
    logger.debug('Assign Variable: %s', 'get_work: attempting to get json_info from get_work - Calling get_work', extra=d)
    json_info = r.get_work()
    logger.debug('Variable Success: %s', 'get_work: successfully retrieved the json_info', extra=d)
    logger.debug('Returning: %s', 'get_work: returning json_info to client from get_work', extra=d)
    logger.info('Work retrieved')
    return json.dumps(json_info)


@app.route('/return_docs', methods=['POST'])
def return_docs():
    """
    The endpoint the client calls to return the document ids received from the regulations docs calls
    :return: Returns a string saying successful so the client knows the call was successful
    """
    logger.debug('Successful API Call: %s', 'return_docs: return docs', extra=d)
    logger.info('Attempting to return docs to server...')
    try:
        logger.debug('Assign Variable: %s', 'return_docs: attempting to get json_info from the request', extra=d)
        json_info = request.form['json']
        logger.debug('Variable Success: %s', 'return_docs: successfully retreived json_info', extra=d)
        logger.debug('Assign Variable: %s', 'return_doc: getting the files from the file request field', extra=d)
        files = request.files['file'].read()
        logger.debug('Variable Success: %s', 'return_doc: files successfully retrieved from the return doc post',
                       extra=d)
    except:
        logger.debug('Exception: %s', 'return_docs: BadParameterException for return docs', extra=d)
        logger.error('Error - bad parameter')
        return 'Bad Parameter', 400
    if json_info is None:
        logger.debug('Exception: %s', 'return_docs: PostException for return docs', extra=d)
        logger.error('Error - could not post docs')
        return 'Bad Parameter', 400
    logger.debug('Calling Function: %s', 'return_docs: return_docs calling process_docs', extra=d)
    files = io.BytesIO(files)
    process_docs(json.loads(json_info), files)
    logger.debug('Function Successful: %s', 'return_docs: process_docs successfully called from return_docs', extra=d)
    logger.debug('Returning: %s', 'return_docs: returning success from return_docs', extra=d)
    logger.info('Docs returned to server')
    return 'Successful!'


@app.route('/return_doc', methods=['POST'])
def return_doc():
    """
    The endpoint the client calls to return documents they received from the individual regulations doc calls
    :return: Returns a string saying successful so the client knows the call was successful
    """
    logger.debug('Successful API Call: %s', 'return_doc: return_doc call successful', extra=d)
    logger.info('Attempting to return doc to server...')

    try:
        logger.debug('Assign Variable: %s', 'return_doc: getting the files from the file request field', extra=d)
        files = request.files['file'].read()
        logger.debug('Variable Success: %s', 'return_doc: files successfully retrieved from the return doc post', extra=d)
        logger.debug('Assign Variable: %s', 'return_doc: get the json_info from the post request', extra=d)
        json_info= request.form['json']
        logger.debug('Variable Success: %s', 'return_doc: json retrieved from the doc post call', extra=d)
    except:
        logger.debug('Exception: %s', 'return_doc: BadParameterException for return_doc', extra=d)
        logger.error('Error - bad parameter')
        return 'Bad Parameter', 400
    files = io.BytesIO(files)
    logger.debug('Exception: %s', 'return_doc: BadParameterException for return_doc', extra=d)
    logger.debug('Calling Function: %s', 'return_doc: call process_docs with the json and files posted to return_doc endpoint', extra=d)
    process_doc(json.loads(json_info), files)
    logger.debug('Function Successful: %s', 'return_doc: success from return_doc', extra=d)
    logger.debug('Returning: %s', 'return_doc: returning success from return_doc', extra=d)
    logger.info('Doc returned to server')
    return 'Successful!'


def generate_json(work_list):
    """
    Given a list of values, the list will be converted into json format
    :param work_list: The list of values that will be converted into json
    :return: Returns the json formatted list
    """
    logger.info('Converting into JSON...')
    logger.debug('Call Successful: %s', 'generate_json: generate_json called successfully', extra=d)
    logger.debug('Assign Variable: %s', 'generate_json: assign job_id from the work_list', extra=d)
    job_id = work_list[0]
    logger.debug('Variable Success: %s', 'generate_json: jod_id assigned', extra=d)
    logger.debug('Assign Variable: %s', 'generate_json: assign type from the work_list', extra=d)
    type = work_list[1]
    logger.debug('Variable Success: %s', 'generate_json: type assigned', extra=d)
    logger.debug('Assign Variable: %s', 'generate_json: assign data from the work_list', extra=d)
    data = work_list[2]
    logger.debug('Variable Success: %s', 'generate_json: data assigned', extra=d)
    logger.debug('Assign Variable: %s', 'generate_json: assign converted_json from the combination of job_id, type, and data', extra=d)
    converted_json = {
        "job_id": job_id,
        "type": type,
        "data": data,
        "version": version
    }
    logger.debug('Variable Success: %s', 'generate_json: converted_json created', extra=d)
    logger.debug("Returning: %s", 'generate_json: returning converted_json', extra=d)
    logger.info('JSON conversion successful')
    return json.dumps(converted_json)


if __name__ == '__main__':
    app.run('0.0.0.0', '8080')
