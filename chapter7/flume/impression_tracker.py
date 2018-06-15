#!/home/admin/anaconda3/envs/TF/bin/ python3.5
# -*- coding: utf-8 -*-  
'''
Created on 2018年6月15日

@author: Zhukun Luo
Jiangxi university of finance and economics
'''
import json
import logging
import os
import random
import string
import time


def random_ip():
    return ".".join(map(str, (random.randint(0, 255) for _ in range(4))))

def generate_customers(num):
    customers = []
    customers.append({'cid': 'anonymous'})
    for i in range(num):
        cid = str(random.randint(10000, 99999))
        customers.append({'ip': random_ip(), 'cid': cid})
    return customers

def generate_skus(num):
    skus = []
    for i in range(num):
        sku = '%s%d-%d' % (random.choice(string.ascii_uppercase), random.randint(1000, 9999), random.randint(0, 9))
        skus.append(sku)
    return skus

def milliseconds_since_epoch():
    return int(time.time() * 1000)


if __name__ == "__main__":

    # Setup choices
    customers = generate_customers(10)
    skus = generate_skus(30)
    actions = ('view', 'click', 'add_cart', 'remove_cart', 'purchase',)

    log_dir = '/tmp/impressions'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Setup logger
    logger = logging.getLogger("impression_tracking")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler("/tmp/impressions/impressions.log")
    fh.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    logger.addHandler(fh)
    logger.addHandler(ch)

    num_entries = 500

    for i in range(num_entries):
        customer = random.choice(customers)
        event = {
            'timestamp': milliseconds_since_epoch(),
            'ip': random_ip() if customer['cid'] == 'anonymous' else customer['ip'],
            'cid': customer['cid'],
            'sku': random.choice(skus),
            'action': random.choice(actions)
        }
        logger.info(json.dumps(event))