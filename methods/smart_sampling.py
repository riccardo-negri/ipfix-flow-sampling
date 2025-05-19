from __future__ import annotations  # noqa: D100

import math
import warnings
from typing import Any

from pydantic import BaseModel, Field

from .method import StatelessMethod


class ThresholdValue(BaseModel):
    threshold: int = Field(..., gt=0)  # must be a positive integer > 0
    weight: float  # must be a percentage


class ThresholdValues(BaseModel):
    values: list[str]
    weight: float

class MfThresholdValue(BaseModel):
    bytes: int
    packets: int
    weight: float


class Features(BaseModel):
    bytes: ThresholdValue | None = None
    packets: ThresholdValue | None = None
    mf_bytes_packets: MfThresholdValue | None = None
    syn_flag: ThresholdValue | None = None
    dropped_status: ThresholdValue | None = None
    src_ports: ThresholdValues | None = None
    dst_ports: ThresholdValues | None = None
    random_prob: float | None = None
    renormilize_probs: bool = False


class MultifactorSmartSampling(StatelessMethod):
    # this implementation should be very general and allows to use any combination of factors

    def __init__(self, *, features: Features, **kwargs) -> None:
        """Initialize the MultifactorSmartSampling method."""
        warnings.warn(
            "MultifactorSmartSampling is deprecated and will be removed in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)
        self.features: Features = features
        self.to_renormalize = ["bytes", "packets"]

    def _get_probability(self, message: dict[str, Any]) -> float:
        total_probability: float = 0.0

        for feature_key, feature_value in self.features.dict(exclude_none=True).items():
            if feature_key in ["bytes", "packets"]:
                total_probability += (
                    int(message[feature_key]) / feature_value["threshold"]
                ) * feature_value["weight"]
            elif feature_key == "syn_flag":
                total_probability += (
                    self._count_syn(message) / feature_value["threshold"]
                ) * feature_value["weight"]
            elif feature_key in ["src_ports", "dst_ports"]:
                port: str = message[feature_key]
                if port in feature_value["values"]:
                    total_probability += feature_value["weight"]
            elif feature_key == "random_prob":
                total_probability += feature_value
            elif feature_key == "dropped_status" and self._is_status_dropped(message):
                total_probability += feature_value["weight"]

        return min(1, total_probability)

    def _renormalize(self, message: dict[str, Any], curr_prob: float) -> dict[str, Any]:
        for key in self.to_renormalize:
            if key in self.features:
                message[key] = self.features[key]["threshold"]
            else:
                message[key] = int(int(message[key]) / curr_prob)
        return message


class MultimodalParallelSampling(StatelessMethod):
    # this implementation should be very general and allows to use any combination of factors

    def __init__(self, *, features: Features, **kwargs) -> None:
        """Initialize the MultimodalParallelSampling method."""
        super().__init__(**kwargs)
        self.features: Features = features
        self.to_renormalize = ["bytes", "packets"]
        self._adj_factor_cache = {}

    def _get_probability(self, message: dict[str, Any]) -> float:
        probabilities: list[float] = []
        for feature_key, feature_value in self.features.dict(exclude_none=True).items():
            if feature_key in ["bytes", "packets"]:
                curr = (
                    int(message[feature_key]) / feature_value["threshold"]
                ) * feature_value["weight"]
                probabilities.append(curr)
            if feature_key == "mf_bytes_packets":
                curr = (
                    int(message["bytes"]) / feature_value["bytes"]
                    + int(message["packets"]) / feature_value["packets"]
                ) * feature_value["weight"]
                probabilities.append(curr)
            elif feature_key == "syn_flag" and self._has_syn_flag(message):
                probabilities.append(
                    feature_value["weight"]
                    / self._get_probability_adj_factor(message["peer_ip_src"])
                )
            elif feature_key == "src_ports":
                port: str = str(message["port_src"])
                if port in feature_value["values"]:
                    probabilities.append(
                        feature_value["weight"]
                        / self._get_probability_adj_factor(message["peer_ip_src"])
                    )
            elif feature_key == "dst_ports":
                port: str = str(message["port_dst"])
                if port in feature_value["values"]:
                    probabilities.append(
                        feature_value["weight"]
                        / self._get_probability_adj_factor(message["peer_ip_src"])
                    )
            elif feature_key == "random_prob":
                probabilities.append(
                    feature_value
                    / self._get_probability_adj_factor(message["peer_ip_src"])
                )
            elif feature_key == "dropped_status" and self._is_status_dropped(message):
                probabilities.append(feature_value["weight"])

        # calculate overall probability p1 + p2 - p1*p2
        p = self._get_combined_probability(probabilities)
        assert 0 <= p <= 1
        return p

    def _get_combined_probability(self, probabilietes: list[float]) -> float:
        # P(A or B or C or ... or N) = 1 - [(1 - P(A)) * (1 - P(B)) * (1 - P(C)) * ... * (1 - P(N))]
        not_happening = 1
        for prob in probabilietes:
            prob = min(prob, 1)
            not_happening = not_happening * (1 - prob)
        return 1 - not_happening

    def _get_probability_adj_factor(self, peer_ip_src: str) -> float:
        if self.features.renormilize_probs:
            sampling_rate = self.estimated_sampling_rate_mapping.get(peer_ip_src)
            if sampling_rate is not None:
                if sampling_rate in self._adj_factor_cache:
                    return self._adj_factor_cache[sampling_rate]
                if 1 <= sampling_rate < 2048:
                    result = 367.179 * math.pow(sampling_rate, -0.778)
                    self._adj_factor_cache[sampling_rate] = result
                    return result
                if sampling_rate >= 2048:
                    return 1.0
            else:
                self.logger.warning(
                    "Peer %s not found in estimated sampling rate mapping.",
                    peer_ip_src,
                )
        return 1.0

    def _renormalize(self, message: dict[str, Any], curr_prob: float) -> dict[str, Any]:
        for key in self.to_renormalize:
            if key in self.features:
                message[key] = self.features[key]["threshold"]
            else:
                message[key] = int(int(message[key]) / curr_prob)
        return message
