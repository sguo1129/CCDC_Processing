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
        outfile = os.path.join(output_path, filename)
        url = host.format(order_id, filename)
        time.sleep(1)

        if os.path.exists(outfile):
            resp = requests.head(url)

            if os.path.getsize(outfile) == resp.headers['content-length']:
                continue

        t = time.time()
        # subprocess.call('wget -c -P {} {}'.format(output_path, url), shell=True)

        resp = requests.get(url, stream=True)
        if resp.status_code == 200:
            with open(outfile, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=2048):
                    f.write(chunk)

        if os.path.exists(os.path.join(output_path, filename)):
            print os.path.getsize(os.path.join(output_path, filename)) / 1024 / (time.time() - t) / 1024