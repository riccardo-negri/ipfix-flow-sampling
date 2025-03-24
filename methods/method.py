from __future__ import annotations  # noqa: D100

import os
import random
import time
import json
from abc import ABC, abstractmethod
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    topic_record_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from logger import logger

HIGHEST_SAMPLING_RATE = 2**32

class Method(ABC):
    """A base class for flow sampling methods that consumes from a queue and produces to a Kafka topic."""

    def __init__(
        self,
        *,
        writer_id: str,
        workers: int = 1,
    ) -> None:
        """Initialize the Method with input queue, Kafka producer configurations, output topic, writer id."""
        self.writer_id: str = writer_id
        self.workers = workers
        self.local_tot_counter = 0
        self.local_sampled_counter = 0
        self.estimated_sampling_rate_mapping = {}

    @abstractmethod
    def _consume_and_produce(self) -> None:
        pass

    def _consume_batch(self) -> list[dict[str, Any]]:
        """Consume a batch of messages from the input queue and deserialize using Avro."""
        while True:
            messages = self.consumer.consume(num_messages=1000, timeout=1.0)
            if messages is None:
                continue
            to_return = []
            for msg in messages:
                if msg is None:
                    continue  # no message available, keep polling
                if msg.error():
                    if msg.error().code() == KafkaError.PARTITION_EOF:
                        # end of partition, ignore and continue
                        self.logger.info(
                            "End of partition reached %s, offset %s",
                            msg.partition(),
                            msg.offset(),
                        )
                    else:
                        # handle other errors
                        self.logger.error("Consumer error: %s", msg.error())
                        raise KafkaException(msg.error())
                else:
                    self.logger.debug("Received message")
                    try:
                        deserialized_message = self.avro_deserializer(
                            msg.value(),
                            SerializationContext(msg.topic(), MessageField.VALUE),
                        )
                        to_return.append(deserialized_message)

                        # update the estimated sampling rate
                        curr_router = deserialized_message["peer_ip_src"]
                        if curr_router not in self.estimated_sampling_rate_mapping:
                            self.estimated_sampling_rate_mapping[curr_router] = HIGHEST_SAMPLING_RATE
                        else:
                            self.estimated_sampling_rate_mapping[curr_router] = min(
                                self.estimated_sampling_rate_mapping[curr_router],
                                deserialized_message["packets"],
                            )
                    except Exception as e:
                        self.logger.error(f"Error deserializing message: {e}")
            return to_return

    def _produce(self, message: dict[str, Any]) -> None:
        """Produce a message to the output topic using Avro serialization."""
        try:
            message["writer_id"] = self.writer_id
            self._produce_with_retry(json.dumps(message).encode())
            self.local_sampled_counter += 1
            if self.local_sampled_counter % 1000 == 0:
                with self.shared_sampled_counter.get_lock():
                    self.shared_sampled_counter.value += self.local_sampled_counter
                    self.local_sampled_counter = 0
        except Exception:
            self.logger.exception("Error processing message")

    def _produce_with_retry(self, msg_bytes: bytes, max_retries: int = 15) -> bool:
        """Produce a message with retry mechanism."""
        retries = 0
        while retries < max_retries:
            try:
                self.producer.produce(self.output_topic, value=msg_bytes)
                self.logger.debug("Produced message")
                return True  # noqa: TRY300
            except BufferError:  # noqa: PERF203
                self.logger.warning("Producer queue is full, waiting for space")
                self.producer.poll(1)
                time.sleep(2**retries)  # Exponential backoff
                retries += 1
            except Exception:
                self.logger.exception("Error producing message")
                break
        self.logger.error("Failed to produce message after multiple retries")
        return False


    def start(self, tot_counter: int, sampled_counter: int) -> None:
        """Start the method in a new thread and return the thread object."""
        schema_registry_conf = {
            "url": os.getenv("SCHEMA_REGISTRY_URL"),
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        self.avro_deserializer = AvroDeserializer(schema_registry_client)

        consumer_config = {
            "bootstrap.servers": os.getenv("CONSUMER_BOOTSTRAP_SERVERS"),
            "security.protocol": os.getenv("CONSUMER_SECURITY_PROTOCOL"),
            "auto.offset.reset": os.getenv("CONSUMER_AUTO_OFFSET_RESET"),
        }
        if consumer_config["security.protocol"].upper() == "SASL_SSL":
            consumer_config["sasl.mechanisms"] = os.getenv("CONSUMER_SASL_MECHANISMS")
            consumer_config["sasl.username"] = os.getenv("CONSUMER_SASL_USERNAME")
            consumer_config["sasl.password"] = os.getenv("CONSUMER_SASL_PASSWORD")
        elif consumer_config["security.protocol"].upper() == "SSL":
            consumer_config["ssl.certificate.location"] = os.getenv("CONSUMER_SSL_CERTIFICATE_LOCATION")
            consumer_config["ssl.key.location"] = os.getenv("CONSUMER_SSL_KEY_LOCATION")
            consumer_config["ssl.ca.location"] = os.getenv("CONSUMER_SSL_CA_LOCATION")
        consumer_id = f"{os.getenv('CONSUMER_ID')}"
        consumer_config["group.id"] = consumer_id + "-" + self.writer_id
        self.consumer: Consumer = Consumer(consumer_config)
        self.input_topic: str = os.getenv("INPUT_TOPIC")
        self.consumer.subscribe([self.input_topic])

        producer_config = {
            "bootstrap.servers": os.getenv("PRODUCER_BOOTSTRAP_SERVERS"),
            "security.protocol": os.getenv("PRODUCER_SECURITY_PROTOCOL"),
            "batch.num.messages": 100000,  # max 10000 messages per batch
            "queue.buffering.max.kbytes": 100000,  # max buffered messages before sending
            "linger.ms": 100,  # max 100ms delay before sending
            "compression.codec": "snappy",  # compress messages
        }
        if producer_config["security.protocol"].upper() == "SASL_SSL":
            producer_config["sasl.mechanisms"] = os.getenv("CONSUMER_SASL_MECHANISMS")
            producer_config["sasl.username"] = os.getenv("CONSUMER_SASL_USERNAME")
            producer_config["sasl.password"] = os.getenv("CONSUMER_SASL_PASSWORD")
        elif producer_config["security.protocol"].upper() == "SSL":
            producer_config["ssl.certificate.location"] = os.getenv("CONSUMER_SSL_CERTIFICATE_LOCATION")
            producer_config["ssl.key.location"] = os.getenv("CONSUMER_SSL_KEY_LOCATION")
            producer_config["ssl.ca.location"] = os.getenv("CONSUMER_SSL_CA_LOCATION")
        self.output_topic: str = os.getenv("OUTPUT_TOPIC", "default_output_topic")
        self.producer = Producer(producer_config)

        self.logger = logger.getChild(self.writer_id)
        self.logger.info("Started consuming and producing messages.")
        self.shared_tot_counter = tot_counter
        self.shared_sampled_counter = sampled_counter

        self._consume_and_produce()


class StatelessMethod(Method):
    """Implementation of random flow sampling."""

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        """Initialize the RandomFlowSampling method."""
        super().__init__(**kwargs)

    @abstractmethod
    def _get_probability(self, message: dict[str, Any]) -> float:
        pass

    @abstractmethod
    def _renormalize(self, message: dict[str, Any], curr_prob: float) -> dict[str, Any]:
        pass

    def _consume_and_produce(self) -> None:
        self.logger.info("Started method consume and produce ")

        try:
            while True:
                messages = self._consume_batch()
                if messages is None:
                    continue
                for msg in messages:
                    self.local_tot_counter += 1
                    if self.local_tot_counter % 1000 == 0:
                        with self.shared_tot_counter.get_lock():
                            self.shared_tot_counter.value += self.local_tot_counter
                            self.local_tot_counter = 0

                    # successfully received a message, process it using the method

                    p = self._get_probability(msg)
                    curr_prob: float = min(1, p)
                    if curr_prob == 1:
                        self._produce(msg)
                    elif random.random() < curr_prob:  # noqa: S311
                        msg_dict = self._renormalize(msg, curr_prob)
                        self._produce(msg_dict)

        except KeyboardInterrupt:
            self.logger.info("Method interrupted by user.")
        except Exception:
            self.logger.exception("Error in method.")
        finally:
            # close the producer
            self.producer.flush()
            self.logger.info("Producer flushed, closing method.")

    def _has_syn_flag(self, message: dict[str, Any]) -> int:
        flags: list[str] = message["tcp_flags"]
        return flags.count("SYN") > 0

    def _is_status_dropped(self, message: dict[str, Any]) -> bool:
        return "DROPPED" in message["fwd_status"]
