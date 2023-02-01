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


def is_heif(obj, client, log):
    '''
    determine if object is a heif to be converted
    '''
    if re.search('\.heic$', obj.object_name, re.IGNORECASE):
        return True
    else:
        return False


def unlock_all(client, bucket, log):
    '''
    unlock all objects in given bucket
    '''
    objects = client.list_objects(bucket, recursive=True)
    for o in objects:
        unlock_object(o, client)


def unlock_object(obj, client):
    tags = client.get_object_tags(obj.bucket_name, obj.object_name)
    if tags and 'lock' in tags:
        client.delete_object_tags(obj.bucket_name, obj.object_name)


def convert_heif(obj, client, log):
    '''
    download object, convert to jpg, upload it
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
    image.save(membuf, format="jpeg", quality=75)
    membuf.seek(0)

    to_replace = re.compile(re.escape('heic'), re.IGNORECASE)
    new_name = to_replace.sub('jpg', obj.object_name)

    result = client.put_object(
        obj.bucket_name, new_name, membuf, membuf.getbuffer().nbytes,
        'image/jpeg'
    )
    log.info("created {0} | etag: {1}".format(
        result.object_name, result.etag))

    client.remove_object(obj.bucket_name, obj.object_name)
    log.info('removed original {0}'.format(obj.object_name))


def add_content_type(obj, client, log):
    '''
    download object (ouch), and reupload with correct content-type tag.
    there doesn't appear to be a straighforward way to modify these tags remotely.
    '''
    try:
        r = client.get_object(obj.bucket_name, obj.object_name)
        membuf = BytesIO(r.data)
    finally:
        r.close()
        r.release_conn()

    membuf.seek(0)

    result = client.put_object(
        obj.bucket_name, obj.object_name, membuf, obj.size,
        'image/jpeg'
    )
    log.info("created {0} | etag: {1}".format(
        result.object_name, result.etag))


def cpu_count(log):
    cpu = os.cpu_count()
    if not cpu:
        log.info('os.cpu_count() returned None. defaulting to 4')
        return 4
    else:
        return cpu / 2


def lock_object(obj, client, log):
    # for brevity, we dont bother to check value of tag 'lock'.
    tag_to_set = Tags.new_object_tags()
    tag_to_set['lock'] = 'true'
    client.set_object_tags(obj.bucket_name, obj.object_name, tag_to_set)


def is_locked(obj, client, log):
    tags = client.get_object_tags(obj.bucket_name, obj.object_name)
    if tags and 'lock' in tags:
        return True
    else:
        return False


def is_jpg_missing_content_type(obj, client):
    if re.search('\.jpg$', obj.object_name, re.IGNORECASE):
        obj_stat = client.stat_object(obj.bucket_name, obj.object_name)
        if obj_stat.content_type != 'image/jpeg':
            return True
        else:
            return False


def job(obj, client, log):
    '''
    an individual parallel task for each object
    '''

    # if object is already locked, do nothing
    if is_locked(obj, client, log):
        return 'found locked object {}'.format(obj.object_name)
    else:
        lock_object(obj, client, log)

    changed = []

    if is_heif(obj, client, log):
        convert_heif(obj, client, log)
        changed.append(1)

    if is_jpg_missing_content_type(obj, client):
        log.info('missing content-type')
        add_content_type(obj, client, log)
        changed.append(1)

    unlock_object(obj, client)

    if len(changed) > 0:
        return '{} modified'.format(obj.object_name)


if __name__ == '__main__':
    def privileged_main():
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
        parser.add_argument('--noop', action='store_true')
        args = parser.parse_args()

        if args.unlock:
            unlock_all(client, bucket, log)
            exit(0)

        work_queue = list(client.list_objects(bucket, recursive=True))
        log.info(str(len(work_queue)) + ' objects found')

        if args.noop:
            for o in work_queue:
                log.info(o.object_name)
            exit(0)

        processes = []
        with ThreadPoolExecutor(max_workers=cpu_count(log)) as work_pool:
            for o in work_queue:
                mc = Minio(endpoint, access_key, secret_key, secure=False)
                processes.append(work_pool.submit(job, o, mc, log))

            # print traces if any
            for p in processes:
                if p.result():
                    log.info(p.result())

    privileged_main()
