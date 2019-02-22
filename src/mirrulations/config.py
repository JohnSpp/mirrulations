import json
import logging

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
log_file = 'client.log'
logging.basicConfig(filename=log_file, filemode='w', format=FORMAT)
d = {'clientip': '192.168.0.1', 'user': 'CONFIG'}
logger = logging.getLogger('tcpserver')


def read_value(value):
    """
    Reads a file from the configuration JSON file.
    :param value: Value to be read from the JSON
    :return: Value read from the JSON
    """
    logger.debug('Calling Function: %s', 'read_value: Reading a value from the configuration file', extra=d)
    logger.info('Reading config file...')
    try:
        logger.debug("Assign Variable: %s", 'read_value: loading json from config', extra=d)
        contents = json.loads(open("./config.json","r").read())
        logger.debug("Variable Success: %s", 'read_value: found json from config', extra=d)
        logger.info('Config file read successful...')
    except:
        logger.debug('Exception: %s', 'read_value: Error opening/loading JSON', extra=d)
        logger.error('Error reading JSON...')

    try:
        result = contents[value]
    except KeyError:
        logger.debug('Exception: %s', 'config: Caught KeyError, no value present for: ' + str(value) +
                     '. Returning None.', extra=d)
        logger.error('Key error')
        return None
    return result
