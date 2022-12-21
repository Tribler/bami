from abc import abstractmethod

from bami.lz.database import PeerTxDB


class BaseMixin:

    @property
    @abstractmethod
    def settings(self):
        pass

    @property
    @abstractmethod
    def peer_db(self) -> PeerTxDB:
        pass

    @property
    @abstractmethod
    def my_peer_id(self) -> bytes:
        pass

    @abstractmethod
    def get_peers(self):
        pass

    @abstractmethod
    def register_task(self, *args, **kwargs):
        pass

    @abstractmethod
    def ez_send(self, *args, **kwargs) -> None:
        pass

