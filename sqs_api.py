import boto.sqs
import sys

from backup_logger import logger

def open_queue(region, queueName, accessKey, sharedKey):
    try:
        logger.debug('Connecting to SQS')

        conn = boto.sqs.connect_to_region(region, aws_access_key_id=accessKey, aws_secret_access_key=sharedKey)

        return conn.get_queue(queueName)
    except boto.exception.SQSError as e:
        logger.error('unable to connect to SQS : HTTP %d - %s' % (e.status, e.message))
        sys.exit(2)