"""Implementation of adaptive sampling methods for flow data."""

import json
import math
import secrets
import time
from collections import defaultdict, deque
from multiprocessing import Queue
from typing import Any

from methods.method import Method


class AdaptiveSampling(Method):
    """Flow sampling method with an adaptive probability to maximize the information gain of certain features.

    If a certain feature (combination of flow attributes) is seen less than count_threshold, the sampling probability
    is fixed and corresponds to sampling_probability. If the feature is seen more than count_threshold times,
    the sampling probability is decreased logarithmically with the count of the feature.

    The core idea is to maximize the information gain of certain features by sampling less frequently flows with
    common features.
    """

    def __init__(
        self,
        *,
        feature_keys: list[str],
        sampling_probability: float,
        count_threshold: int,
        batch_size: int,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Initialize the AdaptiveSampling method."""
        super().__init__(**kwargs)
        self.sampling_probability = sampling_probability
        self.count_threshold = count_threshold
        self.feature_keys = feature_keys
        self.feature_counts = defaultdict(lambda: defaultdict(int))
        self.batch_size = batch_size
        self.window = deque(maxlen=batch_size)
        self.router_identifier = "peer_ip_src"

    def _get_feature_key(self, flow: dict[str, Any]) -> str:
        # concatenate the features to produce the hash key
        return "_".join(str(flow[key]) for key in self.feature_keys)

    def _compute_sampling_probability(self, flow: dict[str, Any]) -> float:
        # compute the count of flows with the same feature values
        peer_ip_src = flow["peer_ip_src"]
        feature_key = self._get_feature_key(flow)
        count = self.feature_counts[peer_ip_src][feature_key]

        if count <= self.count_threshold:
            return self.sampling_probability
        return self.sampling_probability * (
            math.log(self.count_threshold) / math.log(count)
        )

    def _renormalize_and_produce_message(self, message: dict[str, Any]) -> None:
        sampling_prob = self._compute_sampling_probability(message)
        if secrets.randbelow(100) / 100.0 <= sampling_prob:
            # renormalize the "packets" and "bytes" fields
            message["packets"] = int(int(message["packets"]) / sampling_prob)
            message["bytes"] = int(int(message["bytes"]) / sampling_prob)

            self._produce(message)


class SlidingAdaptiveSampling(AdaptiveSampling):
    """Adaptive sampling method with a sliding window approach.

    The sliding window approach keeps track of the last N flows and updates the feature counts accordingly.
    It is able to adapt to changing traffic patterns and it samples flows immediately after they are seen.

    The main issue about this method is that sampling probabilities keep changing over time, which can lead to
    wrong renormalization of the flow data.
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        feature_keys: list[str],
        sampling_probability: float,
        count_threshold: int,
        batch_size: int,
        input_queue: Queue,
        writer_id: str,
    ) -> None:
        """Initialize the SlidingAdaptiveSampling method."""
        super().__init__(
            feature_keys=feature_keys,
            sampling_probability=sampling_probability,
            count_threshold=count_threshold,
            batch_size=batch_size,
            input_queue=input_queue,
            writer_id=writer_id,
        )

    def _consume_and_produce(self) -> None:
        self.logger.info("Started method consume and produce ")
        try:
            while True:
                # get a message from the input queue
                if not self.input_queue.empty():
                    msg = self.input_queue.get(timeout=1)
                    with self.tot_counter.get_lock():
                        self.tot_counter.value += 1

                    # successfully received a message, process it using the method
                    msg_dict: dict[str, Any] = json.loads(msg.decode("utf-8"))

                    self._update_statistics(msg_dict)
                    self._renormalize_and_produce_message(msg_dict)
                else:
                    continue  # no message available, keep polling
        except KeyboardInterrupt:
            self.logger.info("Method interrupted by user.")
        except Exception:
            self.logger.exception("Uncatched exception")
        finally:
            self.logger.info("Method finished.")

    def _update_statistics(self, flow: dict[str, Any]) -> None:
        # update the sliding window and feature counts
        if len(self.window) == self.window.maxlen:
            oldest_flow = self.window.popleft()
            router_id = oldest_flow[self.router_identifier]
            feature_key = self._get_feature_key(oldest_flow)
            self.feature_counts[router_id][feature_key] -= 1
            if self.feature_counts[router_id][feature_key] == 0:
                del self.feature_counts[router_id][feature_key]

        self.window.append(flow)
        router_id = flow[self.router_identifier]
        feature_key = self._get_feature_key(flow)
        self.feature_counts[router_id][feature_key] += 1


class FixedAdaptiveSampling(AdaptiveSampling):
    """Adaptive sampling method with a fixed window approach.

    The fixed window approach collects a fixed number of packets before computing the statistics and processing them.

    This method should be more stable. Flows should be renormalized more accurately, as the sampling probabilities
    are computed over a fixed number of packets and do not change for these packets.
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        feature_keys: list[str],
        sampling_probability: float,
        count_threshold: int,
        batch_size: int,
        input_queue: Queue,
        writer_id: str,
    ) -> None:
        """Initialize the FixedAdaptiveSampling method."""
        super().__init__(
            feature_keys=feature_keys,
            sampling_probability=sampling_probability,
            count_threshold=count_threshold,
            batch_size=batch_size,
            input_queue=input_queue,
            writer_id=writer_id,
        )

    def _consume_and_produce(self) -> None:
        self.logger.info("Started method consume and produce ")

        try:
            while True:
                # collect a fixed number of packets
                while len(self.window) < self.batch_size:
                    if not self.input_queue.empty():
                        msg = self.input_queue.get(timeout=1)
                        with self.tot_counter.get_lock():
                            self.tot_counter.value += 1

                        # successfully received a message, process it using the method
                        msg_dict: dict[str, Any] = json.loads(msg.decode("utf-8"))
                        self.window.append(msg_dict)
                    else:
                        continue  # no message available, keep polling

                # compute statistics over the collected packets
                self._compute_statistics()

                # process all collected packets
                for msg_dict in self.window:
                    self._renormalize_and_produce_message(msg_dict)

                # clear the window for the next batch
                self.window.clear()

        except KeyboardInterrupt:
            self.logger.info("Method interrupted by user.")
        except Exception:
            self.logger.exception("Uncatched exception")
        finally:
            self.logger.info("Method finished.")

    def _compute_statistics(self) -> None:
        # compute the feature counts for the collected packets
        self.feature_counts.clear()
        for flow in self.window:
            router_id = flow[self.router_identifier]
            feature_key = self._get_feature_key(flow)
            self.feature_counts[router_id][feature_key] += 1
