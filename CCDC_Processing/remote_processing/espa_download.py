import shutil
import os
import time
import subprocess

import requests

from CCDC_Processing.api_connect import api_instance


host = 'http://edclpdsftp.cr.usgs.gov/orders/{}/{}'


def retrieve_order(output_path, order_id, config_path=None):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    with api_instance(config_path) as api:
        prod_status = api.item_status(order_id)['orderid'][order_id]

    for item in prod_status:
        filename = item['product_dload_url'].split('/')[-1]
        url = host.format(order_id, filename)

        t = time.time()
        subprocess.call(['wget',
                         '-c'
                         '-P',
                         output_path,
                         url])
        print os.path.getsize(os.path.join(output_path, filename)) / 1024 / (time.time() - t) / 1024
        #
        # t = time.time()
        # resp = requests.get(host.format(order_id, filename), stream=True)
        # if resp.status_code == 200:
        #     with open(os.path.join(output_path, filename), 'wb') as f:
        #         for chunk in resp.iter_content(chunk_size=2048):
        #             f.write(chunk)
        #
        # print os.path.getsize(os.path.join(output_path, filename)) / (time.time() - t) / 1024
