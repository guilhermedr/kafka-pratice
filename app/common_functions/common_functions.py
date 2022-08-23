import os
from kafka import KafkaProducer, KafkaConsumer

def getKafkaBootstrapServer():
    return os.getenv('LOCAL_KAFKA_BOOTSTRAP_SERVER')

def KafkaConnectConsumer(topic_name,auto_offset_reset,bootstrap_servers,consumer_timeout_ms=1000):
    """
    Connect with KafkaConsumer into a kafka topic
    Arguments:
        topic_name: define the topic name to connect
        auto_offset_reset: define the offset to start do consume the topic
        boostrap_servers: define the server url to connect
        consumer_timeout_ms: define the timeout for the connection trying default: 1000
    Returns:
        The return is an object KafkaConsumer
    """
    _consumer = None
    try:        
        _consumer = KafkaConsumer(topic_name, \
                                  auto_offset_reset=auto_offset_reset, \
                                  bootstrap_servers=bootstrap_servers, \
                                  consumer_timeout_ms=consumer_timeout_ms)
    except Exception as ex:
        print('Exception while connecting KafkaConsumer')
        print(str(ex))
    finally:
        return _consumer

def KafkaConnectProducer(bootstrap_servers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    except Exception as ex:
        print('Exception while connecting KafkaProducer')
        print(str(ex))
    finally:
        return _producer

def KafkaPublishMessage(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('>>> Message published successfully!')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))