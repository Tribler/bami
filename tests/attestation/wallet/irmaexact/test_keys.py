"""
Copyright (c) 2016, Maarten Everts
All rights reserved.

This source code has been ported from https://github.com/privacybydesign/gabi
The authors of this file are not -in any way- affiliated with the original authors or organizations.
"""
import unittest

import asynctest

from python_project.attestation.wallet.irmaexact.gabi.keys import (
    DefaultSystemParameters,
    GenerateKeyPair,
    SignMessageBlock,
)


class TestKeys(asynctest.TestCase):
    @unittest.SkipTest  # Too slow and unused.
    def test_generate_and_sign(self):
        """
        Generate a new key and sign a message, see if the signature verifies.
        """
        privkey, pubkey = GenerateKeyPair(DefaultSystemParameters[1024], 1, 0, 0)
        sig = SignMessageBlock(privkey, pubkey, [1])

        self.assertTrue(sig.Verify(pubkey, [1]))
        self.assertTrue(sig.Randomize(pubkey).Verify(pubkey, [1]))
