from __future__ import annotations

import binascii
import json
import os

from python_project.attestation.wallet.irmaexact.gabi.attributes import (
    make_attribute_list,
)
from python_project.attestation.wallet.irmaexact.gabi.keys import (
    DefaultSystemParameters,
)
from python_project.attestation.wallet.irmaexact.gabi.proofs import createChallenge
from python_project.attestation.wallet.irmaexact.wrappers import (
    challenge_response,
    serialize_proof_d,
    unserialize_proof_d,
)
from python_project.attestation.wallet.primitives.structs import ipack, iunpack
from python_project.attestation.identity_formats import (
    Attestation,
    FORMATS,
    IdentityAlgorithm,
)
from python_project.attestation.wallet.irmaexact.gabi.proofs import ProofD
from typing import Dict, List, Optional, Tuple, Type, Union


class IRMAAttestation(Attestation):
    def __init__(self, sign_date: int, proofd: ProofD, z: Optional[int] = None) -> None:
        self.sign_date = sign_date
        self.proofd = proofd
        self.z = z

    def serialize(self) -> bytes:
        return ipack(self.sign_date) + serialize_proof_d(self.proofd)

    def serialize_private(self, PK: None) -> bytes:
        return ipack(self.z) + ipack(self.sign_date) + serialize_proof_d(self.proofd)

    @classmethod
    def unserialize(cls, s: bytes, id_format: str) -> IRMAAttestation:
        sign_date, rem = iunpack(s)
        return IRMAAttestation(sign_date, unserialize_proof_d(rem))

    @classmethod
    def unserialize_private(cls, SK: None, s: bytes, id_format: str) -> IRMAAttestation:
        z, rem = iunpack(s)
        sign_date, rem = iunpack(rem)
        return IRMAAttestation(sign_date, unserialize_proof_d(rem), z)


class KeyStub(object):
    def public_key(self):
        return self

    def serialize(self):
        return b""

    @classmethod
    def unserialize(cls, s):
        return KeyStub()


class IRMAExactAlgorithm(IdentityAlgorithm):
    def __init__(self, id_format: str) -> None:
        super(IRMAExactAlgorithm, self).__init__(id_format)

        # Check algorithm match
        if FORMATS[id_format]["algorithm"] != "irmaexact":
            raise RuntimeError("Identity format linked to wrong algorithm")

        self.issuer_pk = FORMATS[self.id_format]["issuer_pk"]
        self.attribute_order = FORMATS[self.id_format]["order"]
        self.validity = FORMATS[self.id_format]["validity"]

        self.base_meta = {
            u"credential": FORMATS[self.id_format]["credential"],
            u"keyCounter": FORMATS[self.id_format]["keyCounter"],
            u"validity": FORMATS[self.id_format]["validity"],
        }

        self.system_parameters = DefaultSystemParameters[1024]
        self.challenge_count = 8

    def generate_secret_key(self):
        return KeyStub()

    def load_secret_key(self, serialized):
        return KeyStub()

    def load_public_key(self, serialized):
        return KeyStub()

    def get_attestation_class(self) -> Type[IRMAAttestation]:
        return IRMAAttestation

    def attest(self, PK, value):
        raise NotImplementedError("Only import_blob is supported (now) for IRMA.")

    def certainty(
        self,
        value: str,
        aggregate: Dict[Union[str, bytes], Union[IRMAAttestation, bytes]],
    ) -> float:
        value_json = {u"attributes": json.loads(value)}
        value_json.update(self.base_meta)
        attestation = aggregate["attestation"]
        attr_ints, sign_date = make_attribute_list(
            value_json, self.attribute_order, (self.validity, attestation.sign_date)
        )
        reconstructed_attr_map = {}
        for i in range(len(attr_ints)):
            reconstructed_attr_map[i + 1] = attr_ints[i]

        verified = 0.0
        failure = False
        for k, v in aggregate.items():
            if k != "attestation" and v:
                challenge_verif, _ = iunpack(k)
                p = attestation.proofd.Copy()
                p.ADisclosed = reconstructed_attr_map
                Ap, Zp = p.ChallengeContribution(self.issuer_pk)
                p.C, _ = iunpack(v)
                reconstructed_challenge = createChallenge(
                    challenge_verif, challenge_verif, [Ap, Zp], False
                )
                if p.VerifyWithChallenge(self.issuer_pk, reconstructed_challenge):
                    verified += 1.0
                else:
                    failure = True

        return 0.0 if failure else (verified / self.challenge_count)

    def create_challenges(self, PK: None, attestation: IRMAAttestation) -> List[bytes]:
        return [
            ipack(int(binascii.hexlify(os.urandom(32)), 16) % self.issuer_pk.N)
            for _ in range(self.challenge_count)
        ]

    def create_challenge_response(
        self, SK: None, attestation: IRMAAttestation, challenge: bytes
    ) -> bytes:
        return challenge_response(attestation.proofd, attestation.z, challenge)

    def create_certainty_aggregate(
        self, attestation: IRMAAttestation
    ) -> Dict[str, IRMAAttestation]:
        return {"attestation": attestation}

    def create_honesty_challenge(self, PK, value):
        raise NotImplementedError()

    def process_honesty_challenge(self, value, response):
        raise NotImplementedError()

    def process_challenge_response(
        self,
        aggregate: Union[
            Dict[str, IRMAAttestation],
            Dict[Union[str, bytes], Union[IRMAAttestation, bytes]],
        ],
        challenge: bytes,
        response: bytes,
    ) -> None:
        aggregate[challenge] = response

    def import_blob(self, blob: str) -> Tuple[bytes, None]:
        blob_json = json.loads(blob)

        sign_date = blob_json["sign_date"]
        proofd = unserialize_proof_d(binascii.unhexlify(blob_json["proofd"]))
        z = blob_json["z"]

        inst = self.get_attestation_class()(sign_date, proofd, z)

        return inst.serialize_private(None), None
