# src/python-project/wikipedia.py
from dataclasses import dataclass

import click
import desert
import marshmallow as mm
import requests


@dataclass
class Page:
    """Page resource.

      Attributes:
          title: The title of the Wikipedia page.
          extract: A plain text summary.
    """

    title: str
    extract: str


schema = desert.schema(Page, meta={"unknown": mm.EXCLUDE})
API_URL = "https://{language}.wikipedia.org/api/rest_v1/page/random/summary"


def random_page(language: str = "en") -> Page:
    """Return a random page.

    Performs a GET request to the /page/random/summary endpoint.

    Args:
        language: The Wikipedia language edition. By default, the English
            Wikipedia is used ("en").

    Returns:
        A page resource.

    Raises:
        ClickException: The HTTP request failed or the HTTP response
                contained an invalid body.

    Example:
        >>> from bami import wikipedia
        >>> page = wikipedia.random_page(language="en")
        >>> bool(page.title)
        True
    """
    url = API_URL.format(language=language)

    try:
        with requests.get(url) as response:
            response.raise_for_status()
            data = response.json()
            return schema.load(data)
    except (requests.RequestException, mm.ValidationError) as e:
        message = str(e)
        raise click.ClickException(message)
