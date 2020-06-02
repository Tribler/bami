"""
Copyright (c) 2016, Maarten Everts
All rights reserved.

This source code has been ported from https://github.com/privacybydesign/gabi
The authors of this file are not -in any way- affiliated with the original authors or organizations.
"""
from __future__ import annotations

from typing import Dict, List

from python_project.attestation.wallet.irmaexact import secure_randint
from python_project.attestation.wallet.irmaexact.gabi.keys import CLSignature, PublicKey
from python_project.attestation.wallet.irmaexact.gabi.proofs import (
    ProofD,
    ProofPCommitment,
)
from python_project.attestation.wallet.irmaexact.gabi.proofs import hashCommit
from python_project.attestation.wallet.primitives.attestation import sha256_as_int
from python_project.attestation.wallet.primitives.value import FP2Value


class Credential(object):
    def __init__(
        self, Pk: PublicKey, Attributes: List[int], Signature: CLSignature
    ) -> None:
        self.Signature = Signature
        self.Pk = Pk
        self.Attributes = Attributes

    def CreateDisclosureProof(
        self, disclosedAttributes: List[int], context: int, nonce1: int
    ) -> ProofD:
        undisclosedAttributes = getUndisclosedAttributes(
            disclosedAttributes, len(self.Attributes)
        )

        randSig = self.Signature.Randomize(self.Pk)

        eCommit = secure_randint(self.Pk.Params.LeCommit)
        vCommit = secure_randint(self.Pk.Params.LvCommit)

        aCommits = {}

        for v in undisclosedAttributes:
            aCommits[v] = secure_randint(self.Pk.Params.LmCommit)

        Ae = FP2Value(self.Pk.N, randSig.A).intpow(eCommit).a
        Sv = FP2Value(self.Pk.N, self.Pk.S).intpow(vCommit).a
        Z = (Ae * Sv) % self.Pk.N

        for v in undisclosedAttributes:
            Z = (
                Z * FP2Value(self.Pk.N, self.Pk.R[v]).intpow(aCommits[v]).a
            ) % self.Pk.N

        c = hashCommit([context, randSig.A, Z, nonce1], False)

        ePrime = randSig.E - (1 << (self.Pk.Params.Le - 1))
        eResponse = c * ePrime + eCommit
        vResponse = c * randSig.V + vCommit

        aResponses = {}
        for v in undisclosedAttributes:
            exp = self.Attributes[v]
            if exp.bit_length() > self.Pk.Params.Lm:
                exp = sha256_as_int(str(exp))
            t = c * exp
            aResponses[v] = t + aCommits[v]

        aDisclosed = {}
        for v in disclosedAttributes:
            aDisclosed[v] = self.Attributes[v]

        return ProofD(c, randSig.A, eResponse, vResponse, aResponses, aDisclosed)

    def CreateDisclosureProofBuilder(
        self, disclosedAttributes: List[int]
    ) -> DisclosureProofBuilder:
        return DisclosureProofBuilder(
            self.Signature.Randomize(self.Pk),
            secure_randint(self.Pk.Params.LeCommit),
            secure_randint(self.Pk.Params.LvCommit),
            {
                v: secure_randint(self.Pk.Params.LmCommit)
                for v in getUndisclosedAttributes(
                    disclosedAttributes, len(self.Attributes)
                )
            },
            1,
            disclosedAttributes,
            getUndisclosedAttributes(disclosedAttributes, len(self.Attributes)),
            self.Pk,
            self.Attributes,
        )


class DisclosureProofBuilder(object):
    def __init__(
        self,
        randomizedSignature: CLSignature,
        eCommit: int,
        vCommit: int,
        attrRandomizers: Dict[int, int],
        z: int,
        disclosedAttributes: List[int],
        undisclosedAttributes: List[int],
        pk: PublicKey,
        attributes: List[int],
    ) -> None:
        self.randomizedSignature = randomizedSignature
        self.eCommit = eCommit
        self.vCommit = vCommit
        self.attrRandomizers = attrRandomizers
        self.z = z
        self.disclosedAttributes = disclosedAttributes
        self.undisclosedAttributes = undisclosedAttributes
        self.pk = pk
        self.attributes = attributes

    def MergeProofPCommitment(self, commitment: ProofPCommitment) -> None:
        self.z = (self.z * commitment.Pcommit) % self.pk.N

    def PublicKey(self):
        return self.pk

    def Commit(self, skRandomizer: int) -> List[int]:
        self.attrRandomizers[0] = skRandomizer

        Ae = FP2Value(self.pk.N, self.randomizedSignature.A).intpow(self.eCommit).a
        Sv = FP2Value(self.pk.N, self.pk.S).intpow(self.vCommit).a
        self.z = (self.z * Ae * Sv) % self.pk.N

        for v in self.undisclosedAttributes:
            self.z = (
                self.z
                * FP2Value(self.pk.N, self.pk.R[v]).intpow(self.attrRandomizers[v]).a
            ) % self.pk.N

        return [self.randomizedSignature.A, self.z]

    def CreateProof(self, challenge: int) -> ProofD:
        ePrime = self.randomizedSignature.E - (1 << (self.pk.Params.Le - 1))
        eResponse = challenge * ePrime + self.eCommit
        vResponse = challenge * self.randomizedSignature.V + self.vCommit

        aResponses = {}
        for v in self.undisclosedAttributes:
            exp = self.attributes[v]
            if exp.bit_length() > self.pk.Params.Lm:
                exp = sha256_as_int(str(exp))
            aResponses[v] = challenge * exp + self.attrRandomizers[v]

        aDisclosed = {v: self.attributes[v] for v in self.disclosedAttributes}

        return ProofD(
            challenge,
            self.randomizedSignature.A,
            eResponse,
            vResponse,
            aResponses,
            aDisclosed,
        )

    def TimestampRequestContributions(self):
        disclosed = [0] * len(self.attributes)
        for i in self.disclosedAttributes:
            disclosed[i] = self.attributes[i]
        return self.randomizedSignature.A, disclosed


def getUndisclosedAttributes(
    disclosedAttributes: List[int], numAttributes: int
) -> List[int]:
    return list(set(range(numAttributes)) - set(disclosedAttributes))
