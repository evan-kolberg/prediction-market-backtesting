# Tests for Polymarket execution-client helpers added to handle
# CLOB-side decimal-precision rejections and to short-circuit infinite
# rejection loops.

from decimal import Decimal

from prediction_market_extensions.adapters.polymarket.execution import (
    _ORDER_REJECT_CIRCUIT_THRESHOLD,
    _quantize_usdc_amount,
)


class TestQuantizeUsdcAmount:
    def test_already_two_decimals_is_unchanged(self) -> None:
        assert _quantize_usdc_amount(5.43) == 5.43

    def test_strips_float_dust(self) -> None:
        # The raw float 5.43 + tiny epsilon should still snap to 5.43.
        amount = float(Decimal("5.43") + Decimal("0.0000000001"))
        result = _quantize_usdc_amount(amount)
        assert result == 5.43

    def test_rounds_down_third_decimal(self) -> None:
        # Round-down is intentional — never spend more than the strategy sized.
        assert _quantize_usdc_amount(5.439) == 5.43
        assert _quantize_usdc_amount(5.999) == 5.99

    def test_handles_kelly_sized_long_decimal(self) -> None:
        assert _quantize_usdc_amount(5.4321987654) == 5.43

    def test_zero_stays_zero(self) -> None:
        assert _quantize_usdc_amount(0.0) == 0.0

    def test_sub_cent_amounts_floor_to_zero(self) -> None:
        # Amounts smaller than 1 cent round down to zero — caller must handle.
        assert _quantize_usdc_amount(0.009) == 0.0
        assert _quantize_usdc_amount(0.001) == 0.0

    def test_large_amount_preserved(self) -> None:
        assert _quantize_usdc_amount(1234.567890) == 1234.56

    def test_no_more_than_two_decimals_in_output(self) -> None:
        # The whole point: the output, viewed as a Decimal-via-string, must
        # have <= 2 fractional digits — that's what the CLOB enforces.
        for raw in [5.43, 5.4321, 5.999, 0.01, 1234.5678, 7.005]:
            quantized = _quantize_usdc_amount(raw)
            frac = Decimal(str(quantized)).as_tuple().exponent
            assert frac >= -2, f"{raw} -> {quantized} has too many decimals"


class TestCircuitBreakerThresholdConstant:
    def test_threshold_is_conservative(self) -> None:
        # Sanity: threshold should be small enough to stop spam quickly but
        # large enough to tolerate transient API hiccups. 1 would be hostile,
        # 10 would let a tilted strategy burn 10x quota before tripping.
        assert 2 <= _ORDER_REJECT_CIRCUIT_THRESHOLD <= 5
