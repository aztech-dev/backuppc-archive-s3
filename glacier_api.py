import boto.exception
import boto
import os
import peewee
import secrets
import sys

from backup_logger import logger
from boto.glacier.exceptions import UnexpectedHTTPResponseError
from peewee import *

db = MySQLDatabase(secrets.mysql_db, host=secrets.mysql_host, port=int(secrets.mysql_port), user=secrets.mysql_user, passwd=secrets.mysql_pass)

class GlacierReference(peewee.Model):
    host = peewee.CharField()
    backupNumber = peewee.IntegerField()
    archiveName = peewee.CharField()
    shareName = peewee.CharField()
    glacierId = peewee.TextField()

    class Meta:
        database = db

def create_tables():
    db.connect()
    db.create_tables([GlacierReference], True)

def open_store(accesskey, sharedKey, host):
    logger.debug('Connecting with creds %s : %s to Glacier' % (accesskey, sharedKey))
    try:
        conn = boto.connect_glacier(aws_access_key_id=accesskey,
                                    aws_secret_access_key=sharedKey)
    except UnexpectedHTTPResponseError as e:
        logger.error('unable to connect to Glacier : HTTP %d - %s' %s (e.status, e.message))
        sys.exit(2)

    myvaultname = ('backup')
    try:
        vault = conn.get_vault(myvaultname)
    except UnexpectedHTTPResponseError:
        try:
            logger.info('open_glacier: creating new vault %s' % myvaultname)
            vault = conn.create_vault(myvaultname)
        except UnexpectedHTTPResponseError as e:
            logger.error('unable to create vault %s : HTTP %d - %s' % (myvaultname, e.status, e.message))
            sys.exit(3)

    return vault

def send(vault, filename, number):
    basefilename = os.path.basename(filename)
    id = vault.upload_archive(filename)

    logger.debug("send_file: %s sent, received id %s", filename, id)
    logger.debug('send_file: storing relationship in db')

    create_tables()
    ref = GlacierReference(host='192.168.50.1', backupNumber=number, archiveName=filename, glacierId=id)
    ref.save()

    return id