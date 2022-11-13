from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random
from random import randint


def generate_rand():
	return '192.1.1.0'


fake=Faker()





logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

p=Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)

def main():
    for i in range(1):
        data={
           'device_id': fake.random_int(min=20000, max=100000),
           'device_name':fake.last_name()+"-Computer",
           'device_ip':fake.ipv4_private(),
           'platform': random.choice(['Windows', 'MacOS', 'Ubuntu']),
           'login_date': str(fake.date_time_this_month())    
           }
        m=json.dumps(data)
        p.poll(1)
        p.produce('myLogs', m.encode('utf-8'),callback=receipt)
        p.flush()
        time.sleep(3)
main()
