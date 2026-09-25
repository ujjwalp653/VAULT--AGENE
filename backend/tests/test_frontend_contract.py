from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APP = (ROOT / "frontend" / "app.js").read_text(encoding="utf-8")
CSS = (ROOT / "frontend" / "styles.css").read_text(encoding="utf-8")


def test_navigation_targets_and_actions_exist():
    for target in [
        "dashboardSection", "objectsSection", "nodesSection", "replicationSection",
        "repairsSection", "analyticsSection", "chaosSection"
    ]:
        assert target in APP
    for action in [
        "kill", "partition", "revive", "heal", "corrupt", "inconsistent", "repair", "chaos"
    ]:
        assert f'data-action="{action}"' in APP or f"action === '{action}'" in APP
    assert "/api/rebalance" in APP
    assert "/api/policies" in APP


def test_contrast_and_status_styles_exist():
    assert "--red: #e64b33" in CSS.lower()
    assert ".status.inconsistent" in CSS
    assert ".nav button.active" in CSS
    assert ".btn:disabled" in CSS
