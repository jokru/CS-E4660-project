from pydantic import PrivateAttr

from mock_io_app.resources.base import CallParams, FakeIOResource


class FakeGCS(FakeIOResource):
    bucket: str = "mock-bucket"
    # Only sizes are kept so large scenarios don't hold payloads in memory.
    _blob_sizes: dict[str, int] = PrivateAttr(default_factory=dict)

    async def upload(self, key: str, params: CallParams) -> str:
        await self._simulate("gcs", "upload", params, self._attrs(key))
        self._blob_sizes[key] = params.payload_bytes
        return f"gs://{self.bucket}/{key}"

    async def download(self, key: str, params: CallParams) -> bytes:
        await self._simulate("gcs", "download", params, self._attrs(key))
        return bytes(self._blob_sizes.get(key, params.payload_bytes))

    async def list_blobs(self, prefix: str, params: CallParams) -> list[str]:
        await self._simulate("gcs", "list", params, self._attrs(prefix))
        return [key for key in self._blob_sizes if key.startswith(prefix)]

    def _attrs(self, key: str) -> dict[str, str]:
        return {"gcs.bucket": self.bucket, "gcs.object": key}
