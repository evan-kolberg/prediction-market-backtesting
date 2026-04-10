from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from copy import deepcopy
from typing import Any

from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.config import StrategyFactory as NautilusStrategyFactory
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


type StrategyConfigSpec = Mapping[str, Any]

_PRIMARY_INSTRUMENT_SENTINELS = {None, "__PRIMARY_INSTRUMENT__", "__PRIMARY_INSTRUMENTS__"}


def _normalized_config(
    *, raw_config: Mapping[str, Any], instrument_id: InstrumentId
) -> dict[str, Any]:
    config = deepcopy(dict(raw_config))
    instrument_ids = config.get("instrument_ids")
    if instrument_ids is None or instrument_ids == "__PRIMARY_INSTRUMENTS__":
        config["instrument_ids"] = [instrument_id]

    if "instrument_id" not in config and "instrument_ids" not in config:
        config["instrument_id"] = instrument_id
    elif config.get("instrument_id") in _PRIMARY_INSTRUMENT_SENTINELS:
        config["instrument_id"] = instrument_id

    return config


def build_importable_strategy_configs(
    *, strategy_configs: Sequence[StrategyConfigSpec], instrument_id: InstrumentId
) -> list[ImportableStrategyConfig]:
    importable_configs: list[ImportableStrategyConfig] = []
    for spec in strategy_configs:
        strategy_path = str(spec["strategy_path"])
        config_path = str(spec["config_path"])
        raw_config = spec.get("config", {})
        if not isinstance(raw_config, Mapping):
            raise TypeError("strategy config payload must be a mapping")

        importable_configs.append(
            ImportableStrategyConfig(
                strategy_path=strategy_path,
                config_path=config_path,
                config=_normalized_config(raw_config=raw_config, instrument_id=instrument_id),
            )
        )

    return importable_configs


def build_strategies_from_configs(
    *, strategy_configs: Sequence[StrategyConfigSpec], instrument_id: InstrumentId
) -> list[Strategy]:
    return [
        NautilusStrategyFactory.create(importable_config)
        for importable_config in build_importable_strategy_configs(
            strategy_configs=strategy_configs, instrument_id=instrument_id
        )
    ]
