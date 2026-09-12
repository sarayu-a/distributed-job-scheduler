from fastapi.testclient import TestClient

from scheduler.main import app


client = TestClient(app)


def test_home():
    response = client.get("/")

    assert response.status_code == 200
    assert "Distributed Job Scheduler" in response.json()["message"]


def test_health():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_jobs():
    response = client.get("/jobs")

    assert response.status_code == 200


def test_workers():
    response = client.get("/workers")

    assert response.status_code == 200


def test_metrics():
    response = client.get("/metrics")

    assert response.status_code == 200