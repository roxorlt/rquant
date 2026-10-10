"""Original price bytes and explicit shared extensions remain frozen."""

from hashlib import sha256
from pathlib import Path

import pytest

BASELINE_COMMIT = "b139c238580a5c8e63a0943d57e4a649b919d5cd"
_FROZEN_SOURCES = {
    "src/rquant/signal_contracts.py": (
        "7345d806de6d0236bb7deef633c1640ff6aefcb63dc2c92fdf29512f7b880508"
    ),
    "src/rquant/delivery_contracts.py": (
        "256d79b39fe5912ababc05118882d02a00a9087c15cd085339088006f54e3d07"
    ),
    "src/rquant/price_alert_rule_store.py": (
        "ed562820f524262337c2ed1382d076e6aa430b427ee530dff6a93af630382f9a"
    ),
    "src/rquant/page_control.py": (
        "03f3ffb55de69a116cbb5f78a2d1f22132ad11748d5488c5c074bf7fff3f8522"
    ),
    "src/rquant/price_alert_admission.py": (
        "cd72a6fbc2d90528ccf987ac6389d6800495e373e3325283c9f393b88bb3ed25"
    ),
    "src/rquant/runtime_routing_policy.py": (
        "975e0e8772ae45783d6f64ff2f93601e796fa304ad890c137b7a29f207001cd0"
    ),
    "src/rquant/runtime_serving_snapshot.py": (
        "70b41c3c2d6510fdaa40b81b00473891805af93660fa24aee30e7205983d6dfc"
    ),
    "src/rquant/runtime_serving_authority.py": (
        "908e6c518713044ba91b80ecd8f2c25287eb3b94cbb6a2deb92f0720cc830dce"
    ),
    "src/rquant/web/price_alert_commands.py": (
        "93661a1ba5eecc64b8a90885ee6b32825233ed7d1f34746fb4cc010d5313de3a"
    ),
    "src/rquant/web/price_alert_read.py": (
        "2d5df1a251e01520593219c2552c5c8de5e844733aaea3f671ae8de4cdbdf6df"
    ),
    "src/rquant/web/models/price_alert_rules.py": (
        "72ad328c5ff7d7b44b3de9194e5003a4bcefadb83bbdc90711f79d4aee490caa"
    ),
    "web/src/pages/monitor/priceAlertRuleCommandSession.ts": (
        "e25311e60676479471a3fc6c11d11138478113c28a8b99c15cf9eac1777dd923"
    ),
    "src/rquant/monitor.py": "9d418f41d827dcd2e4dc6289c05eace56532ec3517c88d4dc300a514d741a4b0",
    "src/rquant/surge_watch.py": "bb7fc4b99a5632ebbb12772b8f0007601666e53bacc88a226cbfaa2fcecbfc22",
    "src/rquant/pulse_watch.py": "66e885ced4ee3d2febbe49b7dc181b193853ac8b07802f0060df8a0a6d8174dc",
    "src/rquant/notify/client.py": (
        "7b1f2f06feb0490468abd01a8251f1603a280bacc1fc59523cf028519357bb83"
    ),
    "src/rquant/paper_signal_worker.py": (
        "33e8ee9e757cc1d9cfb8b6db2a7c1ef89b8681e75c90ad9af65088cb1b6ccfd3"
    ),
    "src/rquant/paper_broker.py": (
        "562b0a4eb2210aba2fac6d008adb45521eba6b8f23ba97b87306c7751633f37e"
    ),
    "src/rquant/serving_publisher.py": (
        "c1c1cefe429ee9450e7eb5abb9007d83a68dc31d7d05dc6e932d15d5730aa8a5"
    ),
    "src/rquant/serving_contracts.py": (
        "0387b18c6b2540a71e86bf28cf14a63b6b5cf2a2504095a556916be2c188cd60"
    ),
}

# These three owned seams gained additive condition paths. Keep the original lock.
_AUTHORIZED_CONDITION_EXTENSIONS = {
    "src/rquant/price_alert_rule_store.py": (
        "ed562820f524262337c2ed1382d076e6aa430b427ee530dff6a93af630382f9a",
        "b2deaf089353c9df47a4f26e76ceef1d351b9854cefb0ccb9d376c44f846622b",
    ),
    "src/rquant/page_control.py": (
        "03f3ffb55de69a116cbb5f78a2d1f22132ad11748d5488c5c074bf7fff3f8522",
        "9ed5e7d15d33a13c57095c95c91939a96d6f1213e3938bf9c349e42652477ccb",
    ),
    "src/rquant/price_alert_admission.py": (
        "cd72a6fbc2d90528ccf987ac6389d6800495e373e3325283c9f393b88bb3ed25",
        "ce482684ed9fd841286c0be81061cab16cdf023c6adc28ab40199d5cc038f8d7",
    ),
}

# Preserve the accepted extension lock before recording the exact repair of submit replay.
_AUTHORIZED_M4_REPAIRS = {
    "src/rquant/page_control.py": (
        "9ed5e7d15d33a13c57095c95c91939a96d6f1213e3938bf9c349e42652477ccb",
        "b0fbc62a7723efaecdca42d0d0fdf9956ddd7c65d29f8c87953234d38c5c2b4d",
    ),
}


# Preserve prior locks while extending the accepted React shared seams.
# Source/review provenance: ci-full-wave02-contracts-fix-01/EV.json in goal verification.
_AUTHORIZED_REACT_EXTENSIONS = {
    "src/rquant/delivery_contracts.py": (
        "256d79b39fe5912ababc05118882d02a00a9087c15cd085339088006f54e3d07",
        "2056aacb38444d7ae6aacf14c8d31d11ef398ad80269842a187972021b90ccd1",
    ),
    "src/rquant/monitor.py": (
        "9d418f41d827dcd2e4dc6289c05eace56532ec3517c88d4dc300a514d741a4b0",
        "db73b5fbecafe9ceca018c29d64b4a6469d5a44b4e97dbaa4c54de22790ca23a",
    ),
    "src/rquant/notify/client.py": (
        "7b1f2f06feb0490468abd01a8251f1603a280bacc1fc59523cf028519357bb83",
        "b7c3f8298f079402167438d70c5c1e1d12855afa4a2c059729d67926303d6c24",
    ),
    "src/rquant/page_control.py": (
        "b0fbc62a7723efaecdca42d0d0fdf9956ddd7c65d29f8c87953234d38c5c2b4d",
        "5948ebfe9243fed7a7d0551a54c501662b552c25f6b70b85667bc2042a11aa43",
    ),
    "src/rquant/paper_broker.py": (
        "562b0a4eb2210aba2fac6d008adb45521eba6b8f23ba97b87306c7751633f37e",
        "4f2a01cbb58374a96f7a3559d463b18a7310d5c051cf0527dab8b11f67cafa97",
    ),
    "src/rquant/paper_signal_worker.py": (
        "33e8ee9e757cc1d9cfb8b6db2a7c1ef89b8681e75c90ad9af65088cb1b6ccfd3",
        "e4e7727910886a137fddfa07250c4aacd8ca0e69a38270277447fbf8133c0eaa",
    ),
    "src/rquant/pulse_watch.py": (
        "66e885ced4ee3d2febbe49b7dc181b193853ac8b07802f0060df8a0a6d8174dc",
        "775f8904d1606170f6163bd834f1caf9794521696e16479a8dde4a4f6f1ff831",
    ),
    "src/rquant/runtime_serving_snapshot.py": (
        "70b41c3c2d6510fdaa40b81b00473891805af93660fa24aee30e7205983d6dfc",
        "bd4e9f0c487a53c685938839ec7bb37c95d1199dc5f9d5edd693094bc8bb1612",
    ),
    "src/rquant/surge_watch.py": (
        "bb7fc4b99a5632ebbb12772b8f0007601666e53bacc88a226cbfaa2fcecbfc22",
        "630f340279e07255b10ec832f393faea441bfbafb8ccf20b324b852577ff20f9",
    ),
}


@pytest.mark.parametrize(("relative", "expected"), tuple(_FROZEN_SOURCES.items()))
def test_original_contracts_and_authorized_shared_extensions_remain_frozen(
    relative: str, expected: str
) -> None:
    extension = _AUTHORIZED_CONDITION_EXTENSIONS.get(relative)
    if extension is not None:
        before, after = extension
        assert before == expected
        expected = after
    repair = _AUTHORIZED_M4_REPAIRS.get(relative)
    if repair is not None:
        before, after = repair
        assert before == expected
        expected = after
    react_extension = _AUTHORIZED_REACT_EXTENSIONS.get(relative)
    if react_extension is not None:
        before, after = react_extension
        assert before == expected
        expected = after
    root = Path(__file__).resolve().parents[2]
    assert sha256((root / relative).read_bytes()).hexdigest() == expected
