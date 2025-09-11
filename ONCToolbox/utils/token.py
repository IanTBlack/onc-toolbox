"""
Functions for various token retrieval methods so that users don't accidentally
commit their tokens to public repositories.
"""

from netrc import netrc
from os import PathLike


def get_onc_token_from_netrc(netrc_path: PathLike | None = None,
                             machine: str = 'data.oceannetworks.ca') -> str:
    if netrc_path is None:
        _, __, onc_token = netrc().authenticators(machine)
    else:
        _, __, onc_token = netrc(netrc_path).authenticators(machine)
    return onc_token

