

def test_fetch_city_weather_mock(monkeypatch):
    # Test to make sure the API builds correctly and handles responses
    from ingestion import fetch_weather as fw

    # Mock of requests.get
    calls = {}

    def fake_get(url, params=None, timeout=30):
        calls["url"] = url
        calls["params"] = params

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "daily": {
                        "time": ["2025-01-10"],
                        "temperature_2m_max": [30.0],
                        "temperature_2m_min": [20.0],
                        "precipitation_sum": [0.0],
                    }
                }

        return Resp()

    monkeypatch.setattr(fw.requests, "get", fake_get)

    # Recordar que no le pasa los params directamente,
    # los captura del llamado real a la función mockeada,
    # porque lo que testeamos es que se construya bien el request.

    out = fw.fetch_city_weather("BUE", -34.61, -58.38, "2025-01-01", "2025-01-31")
    # Checks expected parameters
    assert calls["params"]["latitude"] == -34.61
    assert calls["params"]["longitude"] == -58.38
    assert calls["params"]["start_date"] == "2025-01-01"
    assert calls["params"]["end_date"] == "2025-01-31"

    # Verifyes added fields by the function
    assert out["_city_code"] == "BUE"
    assert out["_start_date"] == "2025-01-01"
    assert out["_end_date"] == "2025-01-31"

    # Minimal sanity check del payload
    assert "daily" in out and "time" in out["daily"]
