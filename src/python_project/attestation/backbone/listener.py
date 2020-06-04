import abc

from .block import PlexusBlock


class BlockListener(metaclass=abc.ABCMeta):
    """
    This class defines a listener for TrustChain blocks with a specific type.
    """

    BLOCK_CLASS = PlexusBlock

    @abc.abstractmethod
    def should_sign(self, block):
        """
        Method to indicate whether this listener wants a specific block signed or not.
        """
        pass

    @abc.abstractmethod
    def received_block(self, block):
        """
        This method is called when a listener receives a block that matches the BLOCK_CLASS.
        :return:
        """
        pass
