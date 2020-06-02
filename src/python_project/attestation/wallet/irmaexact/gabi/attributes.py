"""
Copyright (c), Privacy By Design Foundation
All rights reserved.

This source code has been ported from https://github.com/privacybydesign/irmago
The authors of this file are not -in any way- affiliated with the original authors or organizations.
"""
import binascii
import calendar
import datetime
import hashlib
import time

from .....util import int2byte
from typing import Dict, List, Optional, Tuple, Union

ExpiryFactor = 60 * 60 * 24 * 7
metadataLength = 1 + 3 + 2 + 2 + 16


class metadataField(object):
    def __init__(self, length: int, offset: int) -> None:
        self.length = length
        self.offset = offset


versionField = metadataField(1, 0)
signingDateField = metadataField(3, 1)
validityField = metadataField(2, 4)
keyCounterField = metadataField(2, 6)
credentialID = metadataField(16, 8)


def int_to_str(n: int) -> bytes:
    hexInt = hex(n).lstrip("0x").rstrip("L")
    if (len(hexInt) % 2) == 1:
        hexInt = "0" + hexInt
    return binascii.unhexlify(hexInt)


def shortToByte(x: int) -> bytes:
    return int_to_str(x)[-2:]


class MetadataAttribute(object):
    def __init__(self, version: bytes) -> None:
        self.Int = 0
        self.pk = None
        self.Conf = None

        self.setField(versionField, version)
        self.setSigningDate()
        self.setKeyCounter(0)
        self.setExpiryDate()

    def Bytes(self) -> bytes:
        bytez = int_to_str(self.Int)
        if len(bytez) < metadataLength:
            bytez += b"\x00" * (metadataLength - len(bytez))
        return bytez

    def setField(self, field: metadataField, value: bytes) -> None:
        bytez_array = [int2byte(c) if isinstance(c, int) else c for c in self.Bytes()]
        startindex = field.length - len(value)
        for i in range(field.length):
            if i < startindex:
                bytez_array[i + field.offset] = b"\x00"
            else:
                bytez_array[i + field.offset] = value[
                    i - startindex : i - startindex + 1
                ]

        self.Int = int(binascii.hexlify(b"".join(bytez_array)), 16)

    def field(self, field: metadataField) -> bytes:
        return self.Bytes()[field.offset : field.offset + field.length]

    def setSigningDate(self, timestamp: Optional[int] = None) -> None:
        if timestamp:
            self.setField(signingDateField, shortToByte(timestamp))
        else:
            self.setField(
                signingDateField, shortToByte(int(time.time() / ExpiryFactor))
            )

    def setKeyCounter(self, i: int) -> None:
        self.setField(keyCounterField, shortToByte(i))

    def SigningDate(self) -> int:
        bytez_array = [
            int2byte(c) if isinstance(c, int) else c
            for c in self.field(signingDateField)
        ]
        bytez_array = bytez_array[1:]
        timestamp = int(binascii.hexlify(b"".join(bytez_array)), 16) * ExpiryFactor
        return timestamp

    def setValidityDuration(self, weeks: int) -> None:
        self.setField(validityField, shortToByte(weeks))

    def setExpiryDate(self) -> None:
        expiry = datetime.datetime.now()
        month = expiry.month - 1 + 6
        year = expiry.year + month // 12
        month = month % 12 + 1
        day = min(expiry.day, calendar.monthrange(year, month)[1])
        expiry = time.mktime(datetime.date(year, month, day).timetuple())
        signing = self.SigningDate()
        self.setValidityDuration(int((expiry - signing) / ExpiryFactor))

    def setExpiryDateFromTimestamp(self, expiry: int) -> None:
        signing = self.SigningDate()
        self.setValidityDuration(int((expiry - signing) / ExpiryFactor))

    def setCredentialTypeIdentifier(self, the_id: bytes) -> None:
        bytez = hashlib.sha256(the_id).digest()
        self.setField(credentialID, bytez[:16])


def make_attribute_list(
    cr: Dict[str, Union[Dict[str, str], str, int]],
    attribute_order: Optional[List[str]] = None,
    validity_signing: Optional[Tuple[int, int]] = None,
) -> Tuple[List[int], int]:
    """
    Args:
        cr: {
            u'attributes': { ... "name": "value" ... },
            u'credential': u'pbdf.nijmegen.address',
            u'keyCounter': 0,
            u'validity': 1570123936
        }
        attribute_order: the order in which to handle the keys
        validity_signing:
    """
    meta = MetadataAttribute(b"\x03")
    meta.setKeyCounter(cr[u"keyCounter"])
    meta.setCredentialTypeIdentifier(cr[u"credential"].encode("utf-8"))
    if validity_signing:
        meta.setValidityDuration(validity_signing[0])
        meta.setSigningDate(validity_signing[1])
    else:
        meta.setSigningDate()
        meta.setExpiryDateFromTimestamp(cr[u"validity"])
    signing_date = int(binascii.hexlify(meta.field(signingDateField)), 16)

    attrs = [meta.Int]
    attr_map = cr[u"attributes"]
    attribute_order = attribute_order or attr_map.keys()

    for k in attribute_order:
        if attr_map.get(k, None) is None:
            attrs.append(0)
        elif not attr_map[k]:
            attrs.append(1)
        else:
            encoded = attr_map[k].encode("utf-8")
            v = int(
                binascii.hexlify(
                    b"".join(int2byte(c) if isinstance(c, int) else c for c in encoded)
                ),
                16,
            )
            v <<= 1
            v += 1
            attrs.append(v)

    return attrs, signing_date
