#!/usr/bin/env python3
"""PDF 추출 파이프라인 CLI 엔트리포인트.

사용 예:
    python run_pipeline.py produce          # PDF → Kafka 발행
    python run_pipeline.py consume          # Kafka → CSV 적재
    python run_pipeline.py all              # 발행 후 적재 순차 실행
"""

from __future__ import annotations

import argparse
import logging
import sys

from pipeline.config import PipelineConfig
from pipeline.consumer import run_consumer
from pipeline.producer import run_producer


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PDF → Kafka → CSV 파이프라인")
    parser.add_argument(
        "command",
        choices=("produce", "consume", "all"),
        help="produce: PDF 발행 / consume: CSV 적재 / all: 둘 다",
    )
    args = parser.parse_args(argv)

    _configure_logging()
    config = PipelineConfig()

    if args.command in ("produce", "all"):
        run_producer(config)
    if args.command in ("consume", "all"):
        run_consumer(config)

    return 0


if __name__ == "__main__":
    sys.exit(main())
