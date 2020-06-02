from .gabi.proofs import ProofD, createChallenge
from ..primitives.structs import ipack, iunpack
from python_project.attestation.wallet.irmaexact.gabi.proofs import ProofD


def serialize_proof_d(proof_d: ProofD) -> bytes:
    return (
        ipack(proof_d.C)
        + ipack(proof_d.A)
        + ipack(proof_d.EResponse)
        + ipack(proof_d.VResponse)
        + ipack(proof_d.AResponses[0])
    )


def unserialize_proof_d(s: bytes) -> ProofD:
    C, rem = iunpack(s)
    A, rem = iunpack(rem)
    EResponse, rem = iunpack(rem)
    VResponse, rem = iunpack(rem)
    fa_response, rem = iunpack(rem)
    return ProofD(C, A, EResponse, VResponse, {0: fa_response}, {})


def challenge_response(my_proof_d: ProofD, Z: int, challenge: bytes) -> bytes:
    challenge_verif, _ = iunpack(challenge)
    return ipack(
        createChallenge(challenge_verif, challenge_verif, [my_proof_d.A, Z], False)
    )
