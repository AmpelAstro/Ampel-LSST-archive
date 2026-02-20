import httpx
import pytest
from fastapi import status


@pytest.mark.asyncio
async def test_query(integration_client: httpx.AsyncClient):
    response = await integration_client.post(
        "/display/alerts/query",
        json={"condition": "true", "include": ["diaSourceId"]},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 98


@pytest.mark.asyncio
async def test_refs(integration_client: httpx.AsyncClient, alert_table_branch):
    response = await integration_client.get(
        "/refs",
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2
    assert any(ref["name"] == "main" for ref in data)
    assert any(ref["name"] == alert_table_branch for ref in data)
