from vault.services.storage import sha256_bytes


def test_sha256():
    assert sha256_bytes(b"vault") == "e6f0a1fbb43c89196dcfcbef85908f19ab4c5f7cc4f4c452284697757683d7ef"
