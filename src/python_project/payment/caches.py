from python_project.backbone.block import PlexusBlock
from python_project.backbone.caches import BlockSignCache
from python_project.backbone.datastore.utils import decode_raw


class PaymentSignCache(BlockSignCache):
    def confirm_block(self, b: PlexusBlock) -> None:
        self.community.confirm(b, {"value": decode_raw(b.transaction).get("value")})
