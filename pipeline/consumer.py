"""Kafka 토픽을 구독해 레코드를 CSV로 적재하는 Consumer (confluent-kafka)."""

from __future__ import annotations

import json
import logging
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from .config import PipelineConfig
from .enrich import build_provider
from .obsidian_sink import ObsidianSink
from .sink import CsvSink

logger = logging.getLogger(__name__)

# poll() 1회 대기 시간(초). 이 간격으로 idle 종료 여부를 확인한다.
_POLL_INTERVAL_SEC = 1.0


def _build_sink_handler(config: PipelineConfig):
    """설정에 맞는 (싱크 컨텍스트매니저, 레코드 처리 함수)를 만든다.

    obsidian 싱크는 적재 전에 LLM 보강 단계를 거친다.
    """
    if config.sink == "obsidian":
        provider = build_provider(config.llm)

        def handle(record: dict, sink: ObsidianSink) -> None:
            text = record.get("full_text") or record.get("text_preview", "")
            enrichment = provider.enrich(text, record.get("title", ""))
            sink.write(record, enrichment)

        return ObsidianSink(config.vault), handle

    def handle_csv(record: dict, sink: CsvSink) -> None:
        sink.write(record)

    return CsvSink(config.output_csv, encoding=config.csv_encoding), handle_csv


def build_consumer(config: PipelineConfig) -> Consumer:
    """confluent-kafka Consumer를 생성한다.

    수동 커밋(enable.auto.commit=False) + earliest로 그룹 첫 실행 시 처음부터 소비.
    """
    return Consumer(
        {
            "bootstrap.servers": ",".join(config.kafka.bootstrap_servers),
            "group.id": config.kafka.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # CSV 적재 성공 후 수동 커밋
        }
    )


def run_consumer(config: PipelineConfig | None = None) -> int:
    """토픽을 소비해 선택된 싱크(csv|obsidian)로 적재한다. 적재 건수를 반환한다.

    consumer_timeout_ms 동안 새 메시지가 없으면 루프가 종료된다(배치성 실행).
    """
    config = config or PipelineConfig()
    idle_timeout_sec = config.kafka.consumer_timeout_ms / 1000.0
    sink_cm, handle = _build_sink_handler(config)
    logger.info("싱크: %s", config.sink)

    consumer = build_consumer(config)
    consumer.subscribe([config.kafka.topic])
    consumed = 0
    last_message_at = time.monotonic()

    try:
        with sink_cm as sink:
            while True:
                msg = consumer.poll(timeout=_POLL_INTERVAL_SEC)

                if msg is None:
                    # 메시지 없음 — idle 타임아웃 초과 시 종료
                    if time.monotonic() - last_message_at >= idle_timeout_sec:
                        logger.info("%.0f초간 신규 메시지 없음 — 종료", idle_timeout_sec)
                        break
                    continue

                if msg.error():
                    # 파티션 끝(EOF)은 정상 신호이므로 무시한다.
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                last_message_at = time.monotonic()
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    handle(record, sink)
                except Exception as exc:
                    # 적재 실패 시 오프셋을 커밋하지 않아 재처리가 가능하다.
                    logger.error("적재 실패 (offset=%s): %s", msg.offset(), exc)
                    continue

                consumed += 1
                logger.info(
                    "적재: %s (doc_id=%s)",
                    record.get("source_file"),
                    record.get("doc_id"),
                )
                # 성공 건만 오프셋 커밋 — at-least-once 보장.
                consumer.commit(message=msg, asynchronous=False)
    except KafkaException as exc:
        logger.error("Kafka 소비 오류: %s", exc)
        raise
    finally:
        consumer.close()

    logger.info("소비 완료: %d건", consumed)
    return consumed
