from typing import Any

from mock_io_app.resources.base import CallParams, FakeIOResource


class FakeHttpClient(FakeIOResource):
    base_url: str = "https://api.mock.invalid"

    async def get(self, path: str, params: CallParams) -> dict[str, Any]:
        await self._simulate("http", "get", params, self._attrs("GET", path))
        return {"status": 200, "path": path, "index": params.index}

    async def post(self, path: str, params: CallParams) -> dict[str, Any]:
        await self._simulate("http", "post", params, self._attrs("POST", path))
        return {"status": 201, "path": path, "index": params.index}

    def _attrs(self, method: str, path: str) -> dict[str, str]:
        return {"http.request.method": method, "url.full": f"{self.base_url}{path}"}
