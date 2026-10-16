from __future__ import annotations

import importlib.util

import pytest


def symbols_module():
    assert importlib.util.find_spec('rquant.l2.symbols') is not None, 'shared symbol normalization is missing'
    from rquant.l2 import symbols
    return symbols


def test_symbols_are_strict_preserve_leading_zero_and_first_seen_order():
    symbols = symbols_module()
    assert symbols.MAX_SYMBOLS == 100
    assert symbols.normalize_symbols(['000001', '600000', '000001', '300001']) == ('000001', '600000', '300001')
    assert [symbols.market_for_code(c) for c in ('600000', '000001', '300001')] == [17, 33, 33]


@pytest.mark.parametrize('value', [123456, True, ' 000001', '000001.SZ', '６０００００', '900001', '200001', '12345', None])
def test_unsupported_symbol_inputs_are_not_coerced(value):
    with pytest.raises(ValueError):
        symbols_module().market_for_code(value)


def test_symbol_list_is_bounded_and_not_a_string():
    symbols = symbols_module()
    for value in ('600000', [f'600{i:03}' for i in range(101)], [123456]):
        with pytest.raises(ValueError):
            symbols.normalize_symbols(value)


def test_binding_accepts_new_routed_symbols_and_keeps_market_and_side_checks():
    from rquant.l2.models import Binding
    assert Binding(page_id=10137, kind='vendor4013', code='600000', market=17).code == '600000'
    assert Binding(page_id=10138, kind='vendor4027', code='000001', market=33, side='buy').code == '000001'
    for values in ({'code': '600000', 'market': 33}, {'code': '200001', 'market': 33}, {'code': 600000, 'market': 17}):
        with pytest.raises(ValueError):
            Binding(page_id=1, kind='vendor4013', **values)
