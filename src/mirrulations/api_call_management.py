import time
import logging
from mirrulations.api_call import *


FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(filename='api_call_management.log', filemode='w', format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': 'CLIENT'}
logger = logging.getLogger('tcpserver')


def api_call_manager(url):
    """
    If there were no errors in making an API call, get the result
    If a Temporary error occurred, sleep for 5 minutes and try again. Do this 50 times, and if it continues to fail, raise a CallFailException
    If a Permanent error occurs, raise a CallFailException
    If the user's ApiCount is zero, sleep for one hour to refresh the calls
    :param url: the url that will be used to make the API call
    :return: returns the resulting information of the documents
    """

    logger.debug('Call Successful: %s', 'api_call_mangement: starting API Call Manager', extra=d)
    logger.info('Managing API call...')

    pause = 0
    while pause < 51:
        try:
            result = call(url)
            return result
        except TemporaryException:
            logger.debug('Exception: %s',
                         'api_call_mangement: Caught TemporaryException, waiting 5 minutes. Current pause: ' +
                         str(pause), extra=d)
            logger.error('Error: waiting 5 minutes...')
            time.sleep(300)
            pause += 1
        except PermanentException:
            logger.debug('Exception: %s', 'api_call_mangement: Caught PermanentException', extra=d)
            logger.error('Error with the API call')
            break
        except ApiCountZeroException:
            logger.debug('Exception: %s', 'api_call_mangement: Caught ApiCountZeroException. Waiting 1 hour.', extra=d)
            logger.error('Error: ran out of API calls')
            time.sleep(3600)
    logger.debug('Exception: %s', 'api_call_mangement: CallFailException for return docs', extra=d)
    logger.debug('Incomplete: %s', url, extra=d)
    logger.error('API call failed...')
    raise CallFailException


class CallFailException(Exception):
    """
    Raise an exception is there is an error making the API call
    """
    def __init__(self):
        logger.debug('EXCEPTION: %s', 'CallFailException: There seems to be an error with your API call', extra=d)
        logger.warning('API call failed...')
