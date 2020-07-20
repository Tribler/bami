from python_project.backbone.utils import Links


class BaseLinkFilter(object):
    """Class to filter out invalid links"""

    def filter(self, all_links: Links) -> Links:
        raise NotImplementedError


class AllLinkFilter(BaseLinkFilter):
    def filter(self, all_links: Links) -> Links:
        return all_links


DefaultLinkFilter = AllLinkFilter
