import pytest


@pytest.fixture
def anyio_backend():
    """Run async tests with asyncio only."""
    return "asyncio"
