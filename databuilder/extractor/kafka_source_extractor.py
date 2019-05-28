from datetime import datetime, timedelta
import logging

from confluent_kafka import Consumer, KafkaException, KafkaError

from databuilder.callback.call_back import Callback
from databuilder.extractor.base_extractor import Extractor


LOGGER = logging.getLogger(__name__)


class KafkaSourceExtractor(Extractor, Callback):
    """
    Kafka source extractor. The extractor itself is single consumer(single-threaded)
    which could consume all the partitions given a topic or a subset of partitions.

    It uses the "micro-batch" concept to ingest data from a given Kafka topic and
    persist into downstream sink.
    Once the publisher commit successfully, it will trigger the extractor's callback to commit the
    consumer offset.
    """
    # The dict of Kafka consumer config
    CONSUMER_CONFIG = 'consumer_config'
    # The consumer group id. Ideally each Kafka extractor should only associate with one consumer group.
    CONSUMER_GROUP_ID = 'group.id'
    # We don't deserde the key of the message.
    # CONSUMER_VALUE_DESERDE = 'value.deserializer'
    # Each Kafka extractor should only consume one single topic. We could extend to consume more topic if needed.
    TOPIC_NAME_LIST = 'topic_name_list'

    # Time out config. It will abort from reading the Kafka topic after timeout is reached. Unit is seconds
    CONSUMER_TOTAL_TIMEOUT = 'consumer_total_timeout'

    # The timeout for consumer polling messages. Default to 1 sec
    CONSUMER_POLL_TIMEOUT = 'consumer_poll_timeout'

    RAW_VALUE_TRANSFORMER = 'raw_value_transformer'

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.conf = conf
        # pyhocon doesn't allow dot to be in value dict,group.id -> group_id
        self.consumer_config = conf.get_config(KafkaSourceExtractor.CONSUMER_CONFIG).\
            as_plain_ordered_dict()

        # convert group_id back to group.id
        # noted: underscore is never used in Kafka consumer config
        self.consumer_config = {k.replace('_', '.'): v for k, v in self.consumer_config.items()}

        self.topic_names = conf.get_list(KafkaSourceExtractor.TOPIC_NAME_LIST)  # type: list

        if not self.topic_names:
            raise Exception('Kafka topic needs to be provided by the user.')

        self.consumer_total_timeout = conf.get_int(KafkaSourceExtractor.CONSUMER_TOTAL_TIMEOUT,
                                                   default=10)

        self.consumer_poll_timeout = conf.get_int(KafkaSourceExtractor.CONSUMER_POLL_TIMEOUT,
                                                  default=1)

        # Transform the protoBuf message with a transformer
        self.val_transformer = conf.get(KafkaSourceExtractor.RAW_VALUE_TRANSFORMER)
        if self.val_transformer is None:
            raise Exception('A message transformer should be provided.')

        # Consumer init
        try:
            self.consumer = Consumer(self.consumer_config)
            # TODO: to support only consume a subset of partitions.
            self.consumer.subscribe(self.topic_names)
        except Exception:
            raise RuntimeError('Consumer could not start correctly!')

    def extract(self):
        # type: () -> Any
        """
        :return: Provides a record or None if no more to extract
        """
        records = self.consume()
        for record in records:
            try:
                transform_record = self.val_transformer(record)
                yield transform_record
            except Exception as e:
                # Has issues tranform / deserde the record. drop the record
                LOGGER.exception(e)

    def on_success(self):
        # Type: () -> None
        """
        Commit the offset once get the success callback and close the consumer.

        :return:
        """
        # set enable.auto.commit to False to avoid auto commit offset
        if self.consumer:
            self.consumer.commit(asynchronous=False)
            self.consume.close()

    def on_failure(self):
        # Type: () -> None
        if self.consumer:
            self.consumer.close()

    def consume(self):
        # Type: () -> Any
        """
        Consume messages from a give list of topic

        :return:
        """
        records = []
        start = datetime.now()
        try:
            while True:
                msg = self.consumer.poll(timeout=self.consumer_poll_timeout)
                end = datetime.now()

                # The consumer exceeds consume timeout
                if (end - start) > timedelta(seconds=self.consumer_total_timeout):
                    # Exceed the consume timeout
                    break

                if msg is None:
                    continue

                if msg.error():
                    # Hit the EOF of partition
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                else:
                    records.append(msg.value())

        except Exception as e:
            LOGGER.exception(e)
        finally:
            return records

    def get_scope(self):
        # type: () -> str
        return 'extractor.kafka_source_extractor'
