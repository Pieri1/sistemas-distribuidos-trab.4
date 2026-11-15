import sys

from cluster import client


class FakeResult:
    DESCRIPTOR = type(
        "Descriptor",
        (),
        {
            "fields": [
                type("Field", (), {"name": "success"}),
                type("Field", (), {"name": "leader"}),
            ]
        },
    )

    success = True
    leader = "node2"


class FakeService:
    def ClientWrite(self, request):
        return FakeResult()


def test_write_uses_cached_leader_and_updates_cache(tmp_path, monkeypatch, capsys):
    cache = tmp_path / "leader"
    cache.write_text("node1", encoding="utf-8")
    selected = []

    def fake_stub(address):
        selected.append(address)
        return FakeService()

    monkeypatch.setattr(client, "stub", fake_stub)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "client",
            "--address",
            "localhost:50051",
            "--nodes",
            "node1=localhost:50051,node2=localhost:50052",
            "--leader-cache",
            str(cache),
            "write",
            "key",
            "value",
        ],
    )

    client.main()

    assert selected == ["localhost:50051"]
    assert cache.read_text(encoding="utf-8") == "node2"
    assert '"success": true' in capsys.readouterr().out
