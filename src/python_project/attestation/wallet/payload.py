from __future__ import annotations

from python_project.messaging.payload import Payload
from typing import List, Tuple, Union


class RequestAttestationPayload(Payload):
    """
    Request an attestation based on some meta data.
    """

    format_list = ["raw"]

    def __init__(self, metadata: bytes) -> None:
        super(RequestAttestationPayload, self).__init__()
        self.metadata = metadata

    def to_pack_list(self) -> List[Tuple[str, bytes]]:
        data = [("raw", self.metadata)]
        return data

    @classmethod
    def from_unpack_list(cls, metadata: bytes) -> RequestAttestationPayload:
        return cls(metadata)


class VerifyAttestationRequestPayload(Payload):
    """
    Request an attestation by hash (published with metadata somewhere).
    """

    format_list = ["20s"]

    def __init__(self, hash: bytes) -> None:
        super(VerifyAttestationRequestPayload, self).__init__()
        self.hash = hash

    def to_pack_list(self) -> List[Tuple[str, bytes]]:
        data = [("20s", self.hash)]
        return data

    @classmethod
    def from_unpack_list(cls, hash: bytes) -> VerifyAttestationRequestPayload:
        return cls(hash)


class AttestationChunkPayload(Payload):
    """
    A chunk of Attestation.
    """

    format_list = ["20s", "H", "raw"]

    def __init__(self, hash: bytes, sequence_number: int, data: bytes) -> None:
        super(AttestationChunkPayload, self).__init__()
        self.hash = hash
        self.sequence_number = sequence_number
        self.data = data

    def to_pack_list(self) -> List[Union[Tuple[str, bytes], Tuple[str, int]]]:
        data = [("20s", self.hash), ("H", self.sequence_number), ("raw", self.data)]

        return data

    @classmethod
    def from_unpack_list(
        cls, hash: bytes, sequence_number: int, data: bytes
    ) -> AttestationChunkPayload:
        return cls(hash, sequence_number, data)


class ChallengePayload(Payload):
    """
    A challenge for an Attestee by a Verifier
    """

    format_list = ["20s", "raw"]

    def __init__(self, attestation_hash: bytes, challenge: bytes) -> None:
        self.attestation_hash = attestation_hash
        self.challenge = challenge

    def to_pack_list(self) -> List[Tuple[str, bytes]]:
        data = [("20s", self.attestation_hash), ("raw", self.challenge)]
        return data

    @classmethod
    def from_unpack_list(
        cls, attestation_hash: bytes, challenge: bytes
    ) -> ChallengePayload:
        return cls(attestation_hash, challenge)


class ChallengeResponsePayload(Payload):
    """
    A challenge response from an Attestee to a Verifier
    """

    format_list = ["20s", "raw"]

    def __init__(self, challenge_hash: bytes, response: bytes) -> None:
        self.challenge_hash = challenge_hash
        self.response = response

    def to_pack_list(self) -> List[Tuple[str, bytes]]:
        data = [("20s", self.challenge_hash), ("raw", self.response)]
        return data

    @classmethod
    def from_unpack_list(
        cls, challenge_hash: bytes, response: bytes
    ) -> ChallengeResponsePayload:
        return cls(challenge_hash, response)
