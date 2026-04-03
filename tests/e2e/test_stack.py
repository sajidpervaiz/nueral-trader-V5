import time
import requests

BRIDGE_BASE = "http://bridge-api:8080"
UI_BASE = "http://ui-static"
CLICKHOUSE_BASE = "http://clickhouse:8123"


def wait(url: str, timeout: float = 120.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"timeout waiting for {url}")


def main():
    wait(f"{BRIDGE_BASE}/health")
    wait(f"{UI_BASE}/")

    request_id = f"e2e-{int(time.time())}"
    submit = requests.post(
        f"{BRIDGE_BASE}/api/tool-selection",
        json={"request_id": request_id},
        timeout=10,
    )
    submit.raise_for_status()

    latest = requests.get(f"{BRIDGE_BASE}/api/tool-selection/latest", timeout=10)
    latest.raise_for_status()
    latest_json = latest.json()
    assert latest_json is not None, "latest event missing"
    assert latest_json["request_id"] == request_id, latest_json

    query = (
        "SELECT count() AS c FROM neural_trader.tool_selection_events "
        f"WHERE request_id = '{request_id}'"
    )
    ch = requests.get(
        f"{CLICKHOUSE_BASE}/?query={query}",
        auth=("nt_app", "nt_app_password"),
        timeout=10,
    )
    ch.raise_for_status()
    count = int(ch.text.strip())
    assert count >= 1, f"expected clickhouse record, got {count}"

    ui_html = requests.get(f"{UI_BASE}/", timeout=10)
    ui_html.raise_for_status()
    assert "NUERAL-TRADER-5 UI" in ui_html.text

    print("E2E PASS")


if __name__ == "__main__":
    main()
