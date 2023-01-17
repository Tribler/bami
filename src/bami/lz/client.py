from binascii import unhexlify

from ipv8.community import Community


class LZClient(Community):
    community_id = unhexlify("6c6564676572207a65726f206973206772656174")

