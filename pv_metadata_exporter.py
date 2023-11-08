#!/usr/bin/env/python3

import threading
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY
import os
import subprocess
import json
import time
from threading import Lock
import logging

logging.basicConfig(
    filename='pv-metadata-exporter.log',
    filemode='w',
    level=logging.DEBUG)

logger = logging.getLogger()

class RBDData:
    def __init__(self):
        self.raw_json = {}
        self.collect_time = 0
        self.lock = Lock()

def issue_command(cmd: str):
    cmd = subprocess.run(
        cmd.split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if cmd.returncode != 0:
        logger.error(f'{cmd} failed : {cmd.returncode}')
        logger.error(f'{cmd}  stdout : {cmd.stdout}')
        logger.error(f'{cmd}  stderr : {cmd.stderr}')
    return cmd

def get_image_labels():
    get_pools_cmd = issue_command('ceph osd pool ls detail -f json')
    if get_pools_cmd.returncode != 0:
        return {}

    if get_pools_cmd.returncode == 0:
        pools = json.loads(get_pools_cmd.stdout)
        image_labels = {}
        for pool in pools:
            pool_name = pool['pool_name']
            if 'rbd' in pool['application_metadata'].keys():
                get_images_cmd = issue_command(f'rbd ls -p {pool_name} --format json')
                if get_images_cmd.returncode != 0:
                    continue

                images = json.loads(get_images_cmd.stdout)
                for img in images:
                    get_metadata_cmd = issue_command(f'rbd image-meta list {pool_name}/{img} --format json')
                    if get_metadata_cmd.returncode != 0 or get_metadata_cmd.stdout is None or get_metadata_cmd.stdout == '':
                        continue
                    metadata_labels = {}
                    try:
                        metadata_labels = json.loads(get_metadata_cmd.stdout)
                    except Exception as e:
                        logger.error(f'failed to parse json for {pool_name}/{img} : {get_metadata_cmd.stdout}')
                        logger.error(f'error : {e}')
                        continue
                    image_labels[img] = dict({'pool': pool_name, 'image': img} , **metadata_labels)

        return image_labels

def query_rbd(interval: int, rbd_data: RBDData):
    while True:
        st = time.time()
        logger.debug('aqcuiring rbd stats object lock')
        with rbd_data.lock:
            logger.debug('aquired lock on stats object')
            rbd_data.raw_json.clear()
            images_metadata = get_image_labels()

            if images_metadata is None:
                logger.error('unable to get image metadata')
            else:
                rbd_data.raw_json = images_metadata

        logger.debug('released rbd stats object lock')
        rbd_data.collect_time = time.time() - st
        logger.info(f'rbd command elapsed time : {rbd_data.collect_time}')
        logger.debug(f'sleeping for {interval}s')
        time.sleep(interval)

class RBDImageLabelsCollector:

    def __init__(self, rbd_data: RBDData):
        self.rbd_data = rbd_data
        self.metrics = {
            'ceph_rbd_image_labels': GaugeMetricFamily(
                'ceph_rbd_image_labels',
                'rbd image labels coming from metadata',
                labels=['pool', 'image', 'namespace', 'pv', 'pvc']
            ),
            'ceph_rbd_image_labels_collect_seconds': GaugeMetricFamily(
                'ceph_rbd_image_labels_collect_seconds',
                'time taken to gather rbd image labels from ceph (secs)'
            ),
        }

    def clear(self):
        for metric in self.metrics:
            self.metrics[metric].samples = []

    def collect(self):
        logger.debug('collect - aquiring lock on rbd object')
        with self.rbd_data.lock:
            logger.debug('collect - lock aquired')
            self.clear()
            self.metrics['ceph_rbd_image_labels_collect_seconds'].add_metric([],
                self.rbd_data.collect_time)  # noqa: E128

            # Generate metrics for each image
            # Note: Remove print's for production
            print("---------------- Collecting data -----------------")
            for image, labels in self.rbd_data.raw_json.items():
                print(labels)
                self.metrics['ceph_rbd_image_labels'].add_metric([
                    labels['pool'],
                    image,
                    labels['namespace'],
                    labels['PV'],
                    labels['PVC']], 1)

            for metric_name in self.metrics:
                yield self.metrics[metric_name]
        logger.debug('collect - released lock')


def main():
    listening_port = 8089
    interval = 30 #seconds
    rbd_data = RBDData()
    rbd_query = threading.Thread(target=query_rbd, args=(interval, rbd_data))
    rbd_query.daemon = True
    rbd_query.start()

    REGISTRY.register(RBDImageLabelsCollector(rbd_data))

    start_http_server(listening_port)

    logger.info("exporter started")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    logger.info('exporter shutdown')


if __name__ == '__main__':
    main()

