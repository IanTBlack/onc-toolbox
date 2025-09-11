"""
Functions for token retrieval methods so that users don't accidentally
commit their tokens to public repositories.
"""

from netrc import netrc
from os import PathLike
import re


def scrub_token(query_url: str) -> str:
    """
    Replace a token in a query URL with the string 'REDACTED' so that users don't
    accidentally commit their tokens to public repositories if ONC Info/Warnings are
    too verbose.

    :param query_url: An Oceans 3.0 API URL with a token query parameter.
    :return: A scrubbed url.
    """
    token_regex = r'(&token=[a-f0-9-]{36})'
    token_qp = re.findall(token_regex, query_url)[0]
    redacted_url = query_url.replace(token_qp, '&token=REDACTED')
    return redacted_url


def get_onc_token_from_netrc(netrc_path: PathLike | None = None,
                             machine: str = 'data.oceannetworks.ca') -> str:
    """
    Retrieve an Oceans 3.0 API token from a .netrc file.

    :param netrc_path: Path to a .netrc file. If None, the user directory is assumed.
    :param machine: The machine lookup name in the .netrc file. Default is
                    'data.oceannetworks.ca'.
    :return: An Oceans 3.0 API token.
    """
    if netrc_path is None:
        _, __, onc_token = netrc().authenticators(machine)
    else:
        _, __, onc_token = netrc(netrc_path).authenticators(machine)
    return onc_token
