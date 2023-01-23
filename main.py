import logging
import os
import re
import pyheif
import argparse
from PIL import Image
from io import BytesIO
from minio import Minio
from minio.commonconfig import Tags
from concurrent.futures import ThreadPoolExecutor


def create_queue(client):
    '''
    returns a list of actionable objects
    '''
    work_queue = []
    objects = client.list_objects(bucket, recursive=True)
    for o in objects:
        if re.search('\.heic$', o.object_name, re.IGNORECASE):
            tags = client.get_object_tags(o.bucket_name, o.object_name)
            # for brevity, we dont bother to check value of tag 'lock'.
            if tags and 'lock' in tags:
                log.info('found locked object ' + o.object_name)
            else:
                tag_to_set = Tags.new_object_tags()
                tag_to_set['lock'] = 'true'
                client.set_object_tags(o.bucket_name, o.object_name, tag_to_set)
                work_queue.append(o)

    return work_queue


def unlock_all(client):
    '''
    unlock all objects in given bucket
    '''
    objects = client.list_objects(bucket, recursive=True)
    for o in objects:
        tags = client.get_object_tags(o.bucket_name, o.object_name)
        if tags and 'lock' in tags:
            log.info('unlocking ' + o.object_name)
            client.delete_object_tags(o.bucket_name, o.object_name)

def convert_heif(obj):
    '''
    download object, convert to png
    return BytesIO containing png
    '''
    try:
        r = client.get_object(obj.bucket_name, obj.object_name)
        heif = pyheif.read(r.data)
    finally:
        r.close()
        r.release_conn()

    log.info('converting ' + obj.object_name)
    image = Image.frombytes(mode=heif.mode, size=heif.size, data=heif.data)
    membuf = BytesIO()
    image.save(membuf, format="png")


def cpu_count():
    cpu = os.cpu_count()
    if not cpu:
        log.info('os.cpu_count() returned None. defaulting to 4')
        return 4
    else:
        return cpu / 2


if __name__ == '__main__':

    logging.basicConfig(format='%(funcName)s(): %(message)s')
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    endpoint = os.getenv('MINIO_ENDPOINT')
    access_key = os.getenv('ACCESS_KEY')
    secret_key = os.getenv('SECRET_KEY')
    bucket = os.getenv('BUCKET') or 'ingest'

    client = Minio(endpoint, access_key, secret_key, secure=False)

    parser = argparse.ArgumentParser()
    parser.add_argument('--unlock', action='store_true')
    args = parser.parse_args()

    if args.unlock:
        unlock_all(client)
        exit(0)

    work_queue = create_queue(client)
    log.info(str(len(work_queue)) + ' objects added to queue')

    threads_out = []
    with ThreadPoolExecutor(max_workers=cpu_count()) as work_pool:
        for o in work_queue:
            threads_out.append(work_pool.submit(convert_heif, o))

    for i in threads_out:
        if i.result():
            log.info(i.result())
