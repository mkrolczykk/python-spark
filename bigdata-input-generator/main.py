import argparse
import csv
import gzip

import logging
import os
import pathlib
import shutil
import sys
import traceback
from faker import Faker
from mimesis.random import Random
from mimesis.schema import Field, Schema
from datetime import datetime, timedelta
import random
import json


class DataGenerator:

    def __init__(self, root_output_path):
        self.root_output_path = root_output_path
        self.random = random
        self.fake = Faker()
        self.random_mimesis = Random()
        self.random.seed(0)
        self.random_mimesis.seed(0)
        self._ = Field('en', seed=0)
        Faker.seed(0)

    def generate_purchases(self, size=10000):
        fake = self.fake
        _ = self._
        end_date = datetime.fromisoformat('2021-01-01')
        start_date = end_date - timedelta(days = 100)
        purchases = (
            lambda: {
                'purchaseId': fake.uuid4(),
                'purchaseTime': fake.date_time_between(start_date=start_date, end_date=end_date).strftime(
                    "%Y-%m-%d %H:%M:%S"),
                'billingCost': _('price')[1:],
                'isConfirmed': _('boolean')

            }
        )
        schema = Schema(schema=purchases)
        return schema.create(iterations=size)

    def generate_mobile_app_clickstream(self, purchases_list: list):
        random = self.random
        random_mimesis = self.random_mimesis
        fake = self.fake
        # Generate sample data for generates purchases
        events_fullset = ['app_open', 'search_product', 'view_product_details', 'purchase', 'app_close']
        # TODO ADD events without purchases
        events_without_purchase = ['app_open', 'search_product', 'view_product_details', 'purchase', 'app_close']
        channels = ['Google Ads', 'Yandex Ads', 'Facebook Ads', 'Twitter Ads', 'VK Ads']
        campaign_ids = random_mimesis.randints(5, 100, 999)
        res = []

        for purchase in purchases_list:
            # print(purchase['purchaseId'] + ' | ' + purchase['purchaseTime'])
            purchase_date = datetime.strptime(purchase['purchaseTime'], "%Y-%m-%d %H:%M:%S")
            user_id = fake.uuid4()
            app_open_date = purchase_date - timedelta(minutes=random.randint(10, 25),
                                                               seconds=random.randint(1, 59))
            search_date = app_open_date + timedelta(minutes=random.randint(5, 8),
                                                             seconds=random.randint(1, 59))
            view_date = search_date + timedelta(minutes=random.randint(1, 3),
                                                         seconds=random.randint(1, 59))
            app_close_date = purchase_date + timedelta(minutes=random.randint(1, 5))

            for type in events_fullset:
                mobile_event = {
                    'userId': user_id,
                    'eventId': fake.uuid4(),
                    'eventType': type
                }
                if type == 'app_open':
                    mobile_event['eventTime'] = app_open_date.strftime("%Y-%m-%d %H:%M:%S")
                    attributes = {'campaign_id': random.choice(campaign_ids),
                                  'channel_id': random.choice(channels)}
                    mobile_event['attributes'] = attributes
                elif type == 'search_product':
                    mobile_event['eventTime'] = search_date.strftime("%Y-%m-%d %H:%M:%S")
                elif type == 'view_product_details':
                    mobile_event['eventTime'] = view_date.strftime("%Y-%m-%d %H:%M:%S")
                elif type == 'purchase':
                    mobile_event['eventTime'] = purchase_date.strftime("%Y-%m-%d %H:%M:%S")
                    attributes = {'purchase_id': purchase['purchaseId']}
                    mobile_event['attributes'] = attributes
                elif type == 'app_close':
                    mobile_event['eventTime'] = app_close_date.strftime("%Y-%m-%d %H:%M:%S")
                res.append(mobile_event)
        return res

def write_data(dataset, dataset_name, num, args):
    dataset_name_csv = f'{dataset_name}_{num}.csv.gz'
    with gzip.open(os.path.join(args.output_path, dataset_name, dataset_name_csv), 'wt') as csvfile:
        writer = csv.DictWriter(csvfile, dataset[0].keys())
        writer.writeheader()
        writer.writerows(dataset)

def main():
    parser = argparse.ArgumentParser(prog='dlz_ingestion_service')

    parser.add_argument('--output_path', type=str, required=False, default='capstone-dataset/')
    # parser.add_argument('--size_lines_per_file', type=int, required=False, default=100)

    try:
        args = parser.parse_args()
        logging.info(args)
        ingestion_starter = DataGenerator(args.output_path)

        recreate_local_folder(os.path.join(args.output_path, 'user_purchases'))
        recreate_local_folder(os.path.join(args.output_path, 'mobile_app_clickstream'))
        for num in range(0, 50):
            purchases = ingestion_starter.generate_purchases(size=10000)
            clickstream = ingestion_starter.generate_mobile_app_clickstream(purchases)
            write_data(purchases, 'user_purchases', num, args)
            write_data(clickstream, 'mobile_app_clickstream', num, args)

    except Exception:
        logging.error(traceback.format_exc())
        sys.exit(99)


def recreate_local_folder(local_path):
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    pathlib.Path(local_path).mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    main()
