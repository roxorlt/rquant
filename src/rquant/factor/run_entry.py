"""Explicit local file configuration and one worker/private listener entry.

This command neither loads dotenv nor prepares a live source or initializes a
factor registry/ledger. Their captured ready identities are required inputs.
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel

from rquant.factor.auction_source import FactorAuctionLakeInput, prepare_factor_auction_source
from rquant.factor.daily_feature_source import (
    FactorAuctionPrepareRequest,
    FactorDailyFeaturePrepareRequest,
    FactorDailyFeatureSource,
    FactorMarketTemperaturePrepareRequest,
    FactorMinuteFeaturePrepareRequest,
    FactorStockFeaturePrepareRequest,
    prepare_factor_daily_feature_source,
)
from rquant.factor.industry_source import FactorIndustrySource
from rquant.factor.market_cap_source import FactorMarketCapSource
from rquant.factor.market_temperature_source import prepare_factor_market_temperature_source
from rquant.factor.member_archive import _read_file
from rquant.factor.minute_feature_source import prepare_factor_minute_feature_source
from rquant.factor.neutralization_context import (
    bind_factor_neutralization_context,
    open_factor_neutralization_context,
)
from rquant.factor.result_artifact import _open_private_root, _root_path
from rquant.factor.run_configuration import (
    FactorRunConfiguration,
    FactorRunFileReference,
    open_factor_run_configuration,
    run_configured_factor_worker,
    save_factor_daily_feature_source,
    save_factor_neutralization_context,
    save_factor_prepared_source,
    save_factor_run_configuration,
)
from rquant.factor.run_request import RUN_IMMUTABLE
from rquant.factor.source_prepare import FactorPreparedStreamSource
from rquant.factor.stock_feature_source import prepare_factor_stock_feature_source
from rquant.factor.technical_history_source import (
    FactorTechnicalHistoryPrepareRequest,
    prepare_factor_technical_history_source,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads


class FactorNeutralizationSealReceipt(BaseModel):
    model_config = RUN_IMMUTABLE
    context_reference: FactorRunFileReference
    configuration_reference: FactorRunFileReference | None = None


class FactorDailyFeatureSealReceipt(BaseModel):
    model_config = RUN_IMMUTABLE
    source_reference: FactorRunFileReference
    configuration_reference: FactorRunFileReference | None = None


def _input(path: Path, model: type[BaseModel], limit: int) -> BaseModel:
    path = _root_path(path)
    descriptor = _open_private_root(path.parent)
    try:
        data, _ = _read_file(descriptor, path.name, limit)
        strict_canonical_json_loads(data)
        return model.model_validate_json(data)
    finally:
        os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="使用显式冻结配置的离线因子入口")
    parser.add_argument(
        "action",
        choices=(
            "save-source",
            "save-configuration",
            "seal-context",
            "seal-daily-features",
            "seal-technical-history",
            "seal-stock-features",
            "seal-minute-features",
            "seal-market-temperature",
            "seal-auction",
            "worker",
            "serve",
        ),
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--reference", help="保存入口返回的完整配置引用 JSON")
    parser.add_argument("--control-root", type=Path)
    parser.add_argument("--socket", type=Path)
    parser.add_argument("--web-uid", type=int)
    parser.add_argument("--shared-gid", type=int)
    parser.add_argument("--prepared-source", type=Path)
    parser.add_argument("--base-daily-source", type=Path)
    parser.add_argument("--industry-source", type=Path)
    parser.add_argument("--market-cap-source", type=Path)
    parser.add_argument("--lake-root", type=Path)
    parser.add_argument(
        "--auction-lake-input", type=Path, help="具名竞价分区、catalog和marker的冻结清单"
    )
    parser.add_argument("--max-input-rows", type=int)
    parser.add_argument("--max-code-observations", type=int, default=50_000)
    parser.add_argument("--max-output-cells", type=int, default=32_000_000)
    parser.add_argument("--max-code-rows", type=int, default=300_000)
    args = parser.parse_args(argv)
    if args.max_input_rows is None:
        args.max_input_rows = 64_000_000 if args.action == "seal-minute-features" else 16_000_000
    if args.action in (
        "seal-daily-features",
        "seal-technical-history",
        "seal-stock-features",
        "seal-minute-features",
        "seal-market-temperature",
        "seal-auction",
    ):
        if args.prepared_source is None or args.lake_root is None:
            parser.error("必须提供实际行情准备包和私有数据根")
        prepared = _input(args.prepared_source, FactorPreparedStreamSource, 16 * 1024 * 1024)
        if args.action == "seal-auction":
            base = (
                None
                if args.base_daily_source is None
                else _input(args.base_daily_source, FactorDailyFeatureSource, 16 * 1024 * 1024)
            )
            lake = (
                None
                if args.auction_lake_input is None
                else _input(args.auction_lake_input, FactorAuctionLakeInput, 4 * 1024 * 1024)
            )
            source = prepare_factor_auction_source(
                FactorAuctionPrepareRequest(
                    prepared_source=prepared,
                    base_daily_source=base,
                    lake_input=lake,
                    max_input_rows=args.max_input_rows,
                    max_output_cells=args.max_output_cells,
                ),
                lake_root=args.lake_root,
            )
        elif args.action == "seal-market-temperature":
            base = (
                None
                if args.base_daily_source is None
                else _input(args.base_daily_source, FactorDailyFeatureSource, 16 * 1024 * 1024)
            )
            source = prepare_factor_market_temperature_source(
                FactorMarketTemperaturePrepareRequest(
                    prepared_source=prepared, base_daily_source=base
                ),
                lake_root=args.lake_root,
            )
        elif args.action == "seal-minute-features":
            base = (
                None
                if args.base_daily_source is None
                else _input(args.base_daily_source, FactorDailyFeatureSource, 16 * 1024 * 1024)
            )
            source = prepare_factor_minute_feature_source(
                FactorMinuteFeaturePrepareRequest(
                    prepared_source=prepared,
                    base_daily_source=base,
                    max_input_rows=args.max_input_rows,
                    max_code_rows=args.max_code_rows,
                    max_output_cells=args.max_output_cells,
                ),
                lake_root=args.lake_root,
            )
        elif args.action == "seal-stock-features":
            base = (
                None
                if args.base_daily_source is None
                else _input(args.base_daily_source, FactorDailyFeatureSource, 16 * 1024 * 1024)
            )
            source = prepare_factor_stock_feature_source(
                FactorStockFeaturePrepareRequest(
                    prepared_source=prepared,
                    base_daily_source=base,
                    max_input_rows=args.max_input_rows,
                    max_code_observations=args.max_code_observations,
                    max_output_cells=args.max_output_cells,
                ),
                lake_root=args.lake_root,
            )
        elif args.action == "seal-technical-history":
            source = prepare_factor_technical_history_source(
                FactorTechnicalHistoryPrepareRequest(
                    prepared_source=prepared,
                    max_input_rows=args.max_input_rows,
                    max_code_observations=args.max_code_observations,
                    max_output_cells=args.max_output_cells,
                ),
                lake_root=args.lake_root,
            )
        else:
            source = prepare_factor_daily_feature_source(
                FactorDailyFeaturePrepareRequest(prepared_source=prepared), lake_root=args.lake_root
            )
        configuration = None
        if args.reference is not None:
            reference = FactorRunFileReference.model_validate_json(args.reference)
            with open_factor_run_configuration(args.root, reference) as loaded:
                source.require_prepared(loaded.source)
                if loaded.configuration.lake_root != args.lake_root:
                    raise ValueError("配置与日线事实私有数据根不同")
                configuration = loaded.configuration
        source_reference = save_factor_daily_feature_source(args.root, source)
        configuration_reference = None
        if configuration is not None:
            configuration = FactorRunConfiguration.model_validate(
                {**configuration.model_dump(), "daily_feature_source": source_reference}
            )
            configuration_reference = save_factor_run_configuration(args.root, configuration)
        print(
            canonical_json_bytes(
                FactorDailyFeatureSealReceipt(
                    source_reference=source_reference,
                    configuration_reference=configuration_reference,
                ).model_dump(mode="json")
            ).decode()
        )
        return 0
    if args.action == "seal-context":
        if (
            args.prepared_source is None
            or args.lake_root is None
            or (args.industry_source is None and args.market_cap_source is None)
        ):
            parser.error("必须提供行情准备包、私有数据根及实际上下文来源")
        prepared = _input(args.prepared_source, FactorPreparedStreamSource, 16 * 1024 * 1024)
        industry = (
            None
            if args.industry_source is None
            else _input(args.industry_source, FactorIndustrySource, 16 * 1024 * 1024)
        )
        cap = (
            None
            if args.market_cap_source is None
            else _input(args.market_cap_source, FactorMarketCapSource, 16 * 1024 * 1024)
        )
        context = bind_factor_neutralization_context(prepared, industry=industry, market_cap=cap)
        with open_factor_neutralization_context(context, lake_root=args.lake_root):
            pass
        configuration = None
        if args.reference is not None:
            reference = FactorRunFileReference.model_validate_json(args.reference)
            with open_factor_run_configuration(args.root, reference) as loaded:
                context.require_prepared(loaded.source)
                if loaded.configuration.lake_root != args.lake_root:
                    raise ValueError("配置与上下文私有数据根不同")
                configuration = loaded.configuration
        context_reference = save_factor_neutralization_context(args.root, context)
        configuration_reference = None
        if configuration is not None:
            configuration = FactorRunConfiguration.model_validate(
                {**configuration.model_dump(), "neutralization_context": context_reference}
            )
            configuration_reference = save_factor_run_configuration(args.root, configuration)
        receipt = FactorNeutralizationSealReceipt(
            context_reference=context_reference, configuration_reference=configuration_reference
        )
        print(canonical_json_bytes(receipt.model_dump(mode="json")).decode())
        return 0
    if args.action in ("save-source", "save-configuration"):
        if args.input is None:
            parser.error("保存必须提供实际输入文件")
        if args.action == "save-source":
            source = _input(args.input, FactorPreparedStreamSource, 16 * 1024 * 1024)
            result = save_factor_prepared_source(args.root, source)
        else:
            configuration = _input(args.input, FactorRunConfiguration, 256 * 1024)
            result = save_factor_run_configuration(args.root, configuration)
            with open_factor_run_configuration(args.root, result) as loaded:
                from rquant.factor.registry import FactorDefinitionRegistry

                config = loaded.configuration
                FactorDefinitionRegistry(Path(config.registry_identity.path)).get_head(
                    "configuration_probe",
                    expected_identity=config.registry_identity,
                )
                loaded.open_ledger(clock=lambda: datetime.now(UTC))
        print(canonical_json_bytes(result.model_dump(mode="json")).decode())
        return 0
    if args.reference is None:
        parser.error("必须提供保存入口返回的完整配置引用")
    reference = FactorRunFileReference.model_validate_json(args.reference)
    if args.action == "worker":
        result = run_configured_factor_worker(args.root, reference)
        print(result.model_dump_json())
        return 0
    if args.control_root is None or args.socket is None:
        parser.error("私有监听必须提供显式控制根及 socket")
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor_run_admission import FactorRunAdmission, build_factor_run_admission_server
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    root = _root_path(args.control_root)
    descriptor = _open_private_root(root)
    os.close(descriptor)
    backend = FactorRunPageControlBackend(args.root, reference)
    outbox = PageControlOutbox(root / "page-control.sqlite")
    consumer = PageControlConsumer(
        outbox=outbox, data_dir=root, log_dir=root, factor_run_backend=backend
    )
    admission = FactorRunAdmission(
        PageControlService(outbox=outbox, consumer=consumer),
        run_users=frozenset(backend.run_users),
        enabled=backend.enabled,
    )
    server = build_factor_run_admission_server(
        admission, socket_path=args.socket, trusted_web_uid=args.web_uid, shared_gid=args.shared_gid
    )
    if server is None:
        raise PermissionError("运行入口尚未开启")
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
