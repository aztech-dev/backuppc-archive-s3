import boto
import hashlib
import os

from backup_logger import logger
from boto.s3.connection import S3Connection
from boto.s3.key import Key


class VerifyError(Exception):
    pass

def open_store(accesskey, sharedkey, host):
    conn = S3Connection(accesskey, sharedkey, is_secure=True)
    mybucketname = (accesskey + '-bkup-' + host).lower()
    try:
        bucket = conn.get_bucket(mybucketname)
    except boto.exception.S3ResponseError:
        logger.info('open_s3: creating new bucket %s' % mybucketname)
        bucket = conn.create_bucket(mybucketname)
    bucket.set_acl('private')
    return bucket

def handle_progress(transmitted, pending):
    logger.debug("send_file: %i of %i bytes transmitted (%.2f%%)", transmitted, pending, (transmitted/float(pending))*100)

def send_file(bucket, filename):
    basefilename = os.path.basename(filename)
    k = Key(bucket)
    k.key = basefilename

    if k.exists():
        if verify_file(bucket, filename):
            logger.warning("send_file: %s already exists and is identical, not overwriting", basefilename)
            return k
        logger.warning("send_file: %s already exists on S3, overwriting", basefilename)

    k.set_contents_from_filename(filename, cb=handle_progress, reduced_redundancy=True)

    logger.debug("send_file: %s sent, verifying fidelity", filename)
    if not verify_file(bucket, filename):
        raise VerifyError("verify failed")
    return k

def verify_file(bucket, filename):
    "Returns True if the file size and, number md5sum match, False otherwise"
    basefilename = os.path.basename(filename)
    key = bucket.get_key(basefilename)
    stat = os.stat(filename)
    if key:
        if key.size == stat[6]:
            fp = open(filename)
            local_md5 = hashlib.md5(fp.read())
            fp.close()
            logger.debug('verify_file: %s: local md5 "%s", etag %s', filename, local_md5.hexdigest(), key.etag)
            if '"%s"' % local_md5.hexdigest() == key.etag:
                return True
    return False