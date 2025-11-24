from __future__ import annotations

import argparse

from crypto_lab.lab_core.pipeline.pipeline_v0 import load_and_qc
from crypto_lab.lab_core.aggregator.aggregator_v0 import aggregate_ohlcv_v0
from crypto_lab.lab_core.qc.dashboard_v0 import pipeline_qc_dashboard
from crypto_lab.lab_core.logging.logging_v1 import setup_logging, get_logger


def run_cli():
    parser = argparse.ArgumentParser(
        description="Crypto Lab end-to-end pipeline (v0): loader → aggregator → QC dashboard."
    )

    parser.add_argument("--exchange", type=str, required=True, help="Биржа, например binance.")
    parser.add_argument("--symbol", type=str, required=True, help="Торговая пара, например BTC/USDT.")
    parser.add_argument("--timeframe", type=str, required=True, help="Таймфрейм, например 1m, 5m, 1h.")
    parser.add_argument("--limit", type=int, default=1000, help="Количество свечей на загрузку.")
    parser.add_argument("--since", type=int, default=None, help="Timestamp в ms для начала загрузки.")

    parser.add_argument("--save", action="store_true", help="Сохранять ли сырой parquet из pipeline.")
    parser.add_argument(
        "--root-dir",
        type=str,
        default="data/pipeline",
        help="Корень для сохранения данных pipeline и чтения aggregator.",
    )
    parser.add_argument(
        "--qc-dir",
        type=str,
        default="data/qc_reports",
        help="Куда сохранять QC dashboard отчёты.",
    )

    parser.add_argument(
        "--produce-qc-dashboard",
        action="store_true",
        help="Создавать Markdown отчёт QC Dashboard.",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Уровень логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL (по умолчанию INFO).",
    )

    args = parser.parse_args()

    # -------------------- ЛОГИРОВАНИЕ --------------------
    setup_logging(level=args.log_level)
    logger = get_logger("cli")

    logger.info(
        "cli_start",
        extra={
            "event": "cli_start",
            "exchange": args.exchange,
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "limit": args.limit,
            "since": args.since,
            "save": args.save,
            "root_dir": args.root_dir,
            "qc_dir": args.qc_dir,
            "produce_qc_dashboard": args.produce_qc_dashboard,
            "log_level": args.log_level,
        },
    )

    # ---------------------------------------------------
    # 1. PIPELINE V0
    # ---------------------------------------------------
    logger.info(
        "pipeline_start",
        extra={
            "event": "pipeline_start",
            "exchange": args.exchange,
            "symbol": args.symbol,
            "timeframe": args.timeframe,
        },
    )

    pipeline_result = load_and_qc(
        exchange_name=args.exchange,
        symbol=args.symbol,
        timeframe=args.timeframe,
        limit=args.limit,
        since=args.since,
        save=args.save,
        output_dir=args.root_dir,
    )

    df_pipeline = pipeline_result["df"]
    qc_pipeline = pipeline_result["qc"]
    saved_raw_path = pipeline_result["saved_path"]

    logger.info(
        "pipeline_done",
        extra={
            "event": "pipeline_done",
            "rows": int(len(df_pipeline)),
            "qc_summary": qc_pipeline.get("summary"),
            "saved_raw_path": str(saved_raw_path) if saved_raw_path else None,
        },
    )

    # ---------------------------------------------------
    # 2. AGGREGATOR V0
    # ---------------------------------------------------
    logger.info(
        "aggregator_start",
        extra={
            "event": "aggregator_start",
            "exchange": args.exchange,
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "root_dir": args.root_dir,
        },
    )

    agg_result = aggregate_ohlcv_v0(
        exchange=args.exchange,
        symbol=args.symbol,
        timeframe=args.timeframe,
        root_dir=args.root_dir,
        drop_dirty=True,   # v0: удаляем грязные строки
        recheck_qc=True,   # v0: повторная QC после очистки
    )

    df_final = agg_result["df"]
    qc_before = agg_result["qc_before_clean"]
    qc_after = agg_result["qc_after_clean"]
    files_read = agg_result["files_read"]

    logger.info(
        "aggregator_done",
        extra={
            "event": "aggregator_done",
            "rows": int(len(df_final)),
            "files_read": len(files_read),
            "qc_before_summary": qc_before.get("summary") if qc_before else None,
            "qc_after_summary": qc_after.get("summary") if qc_after else None,
        },
    )

    # ---------------------------------------------------
    # 3. QC DASHBOARD
    # ---------------------------------------------------
    if args.produce_qc_dashboard:
        logger.info(
            "qc_dashboard_start",
            extra={
                "event": "qc_dashboard_start",
                "symbol": args.symbol,
                "timeframe": args.timeframe,
                "qc_dir": args.qc_dir,
            },
        )

        qc_for_dashboard = qc_after or qc_before
        if qc_for_dashboard is None:
            logger.warning(
                "qc_dashboard_skipped_no_qc",
                extra={
                    "event": "qc_dashboard_skipped_no_qc",
                    "reason": "qc_before_clean and qc_after_clean are None",
                },
            )
        else:
            out_path = pipeline_qc_dashboard(
                df=df_final,
                qc=qc_for_dashboard,
                symbol=args.symbol,
                timeframe=args.timeframe,
                output_dir=args.qc_dir,
            )
            logger.info(
                "qc_dashboard_done",
                extra={
                    "event": "qc_dashboard_done",
                    "report_path": str(out_path),
                },
            )

    logger.info(
        "cli_finished",
        extra={
            "event": "cli_finished",
            "exchange": args.exchange,
            "symbol": args.symbol,
            "timeframe": args.timeframe,
        },
    )
