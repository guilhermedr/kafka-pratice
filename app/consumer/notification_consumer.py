import json
import os
import sys 
sys.path.append(f"{os.getenv('GIT_REPO_KAFKA_PRATICE')}/app/common_functions/")

from time import sleep
from common_functions import KafkaConnectConsumer, getKafkaBootstrapServer

BOOTSTRAP_SERVERS = getKafkaBootstrapServer()

if __name__ == '__main__':
    parsed_topic_name = 'parsed_recipes'
    calories_threshold = 400

    consumer = KafkaConnectConsumer(parsed_topic_name, \
                                    auto_offset_reset='earliest', \
                                    bootstrap_servers=BOOTSTRAP_SERVERS)

    for msg in consumer:
        record = json.loads(msg.value)
        calories = float(record['calories'])
        title = record['title']

        if calories > calories_threshold:
            print('Alert: {} calories count is {}'.format(title,calories))
        sleep(3)

    if consumer is not None:
        consumer.close()