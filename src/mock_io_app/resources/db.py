from typing import Any

from pydantic import PrivateAttr

from mock_io_app.resources.base import CallParams, FakeIOResource

_QUERY_ROW_LIMIT = 100


class FakeDatabase(FakeIOResource):
    db_name: str = "mock_db"
    _rows: list[dict[str, Any]] = PrivateAttr(default_factory=list)

    async def query(self, params: CallParams) -> list[dict[str, Any]]:
        await self._simulate(
            "db",
            "query",
            params,
            {"db.system.name": "fake", "db.namespace": self.db_name, "db.operation.name": "SELECT"},
        )
        return self._rows[-_QUERY_ROW_LIMIT:]

    async def insert(self, params: CallParams) -> int:
        await self._simulate(
            "db",
            "insert",
            params,
            {"db.system.name": "fake", "db.namespace": self.db_name, "db.operation.name": "INSERT"},
        )
        row_id = len(self._rows)
        self._rows.append({"id": row_id, "payload_bytes": params.payload_bytes})
        return row_id
