"""디렉토리의 PDF들을 추출해 Kafka 토픽으로 발행하는 Producer (confluent-kafka)."""

from __future__ import annotations

import glob
import json
import logging
import os
from typing import Any, Iterator

from confluent_kafka import KafkaException, Producer

from .config import PipelineConfig
from .extractor import extract_pdf

logger = logging.getLogger(__name__)


def _iter_pdf_paths(pdf_dir: str) -> Iterator[str]:
    """디렉토리 내 .pdf 파일 경로를 정렬된 순서로 순회한다."""
    pattern = os.path.join(pdf_dir, "**", "*.pdf")
    yield from sorted(glob.glob(pattern, recursive=True))


def build_producer(config: PipelineConfig) -> Producer:
    """confluent-kafka Producer를 생성한다.

    bootstrap.servers는 쉼표 구분 문자열을 받는다.
    acks=all + 재시도로 유실을 최소화한다.
    """
    return Producer(
        {
            "bootstrap.servers": ",".join(config.kafka.bootstrap_servers),
            "acks": "all",
            "retries": 3,
            "enable.idempotence": True,  # 중복 발행 방지(정확히 한 번 발행)
        }
    )


def _delivery_report(err: Any, msg: Any) -> None:
    """비동기 발행 결과 콜백. 실패 건만 로깅한다."""
    if err is not None:
        logger.error("발행 실패 (key=%s): %s", msg.key(), err)


def run_producer(config: PipelineConfig | None = None) -> int:
    """PDF들을 추출해 Kafka로 발행한다. 발행 시도 건수를 반환한다."""
    config = config or PipelineConfig()
    paths = list(_iter_pdf_paths(config.pdf_dir))

    if not paths:
        logger.warning("발행할 PDF가 없습니다: %s", config.pdf_dir)
        return 0

    producer = build_producer(config)
    sent = 0
    try:
        for path in paths:
            try:
                record = extract_pdf(path)
            except (FileNotFoundError, ValueError) as exc:
                # 한 파일 실패가 전체 파이프라인을 멈추지 않도록 격리한다.
                logger.error("추출 건너뜀: %s", exc)
                continue

            # doc_id를 메시지 key로 사용 → 동일 문서는 같은 파티션으로 라우팅
            producer.produce(
                topic=config.kafka.topic,
                key=record["doc_id"].encode("utf-8"),
                value=json.dumps(record, ensure_ascii=False).encode("utf-8"),
                callback=_delivery_report,
            )
            # 누적된 delivery 콜백을 주기적으로 처리(내부 큐 비우기)
            producer.poll(0)
            sent += 1
            logger.info("발행: %s (doc_id=%s)", record["source_file"], record["doc_id"])

        # 버퍼에 남은 메시지 전송 완료까지 대기
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            logger.error("미전송 메시지 %d건 (flush 타임아웃)", remaining)
    except KafkaException as exc:
        logger.error("Kafka 발행 오류: %s", exc)
        raise

    logger.info("발행 완료: %d건", sent)
    return sent
