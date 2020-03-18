"""
Pushshift.io API Wrapper (for reddit.com public comment/submission search)

https://github.com/dmarx/psaw
"""

from .PushshiftAPI import PushshiftAPI, PushshiftAPIMinimal

__version__ = '0.0.12'

# Copying logging pattern from https://github.com/urllib3/urllib3/
# Set default logging handler to avoid "No handler found" warnings.
import logging
from logging import NullHandler

logging.getLogger(__name__).addHandler(NullHandler())