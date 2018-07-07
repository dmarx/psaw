from unittest import mock
from requests import Response


class MockResponse:
    def __init__(self, status_code=200, text='{"status": "ok"}'):
        self._response = Response()
        self._response.status_code = status_code
        self.raise_for_status = mock.MagicMock(
            side_effect=self._response.raise_for_status
        )

        self.text = text

    @property
    def status_code(self):
        return self._response.status_code
