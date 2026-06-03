"""파이프라인 전역 설정.

환경 변수로 모든 값을 오버라이드할 수 있어 운영/개발 환경 전환이 쉽다.
시크릿(브로커 인증 등)은 코드에 하드코딩하지 말고 환경 변수로 주입한다.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka 브로커 및 토픽 설정 (불변)."""

    bootstrap_servers: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
        )
    )
    topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "pdf-documents"))
    consumer_group: str = field(
        default_factory=lambda: _env("KAFKA_CONSUMER_GROUP", "pdf-csv-writer")
    )
    # 컨슈머가 일정 시간 동안 새 메시지가 없으면 종료할 때 사용 (ms)
    consumer_timeout_ms: int = field(
        default_factory=lambda: int(_env("KAFKA_CONSUMER_TIMEOUT_MS", "10000"))
    )


@dataclass(frozen=True)
class PipelineConfig:
    """디렉토리 및 출력 경로 설정 (불변)."""

    pdf_dir: str = field(default_factory=lambda: _env("PDF_DIR", "sample_pdfs"))
    output_csv: str = field(
        default_factory=lambda: _env("OUTPUT_CSV", "output/extracted.csv")
    )
    kafka: KafkaConfig = field(default_factory=KafkaConfig)


# CSV 컬럼 순서 — DB 스키마와 1:1 매핑되도록 한 곳에서 관리한다.
CSV_FIELDS: tuple[str, ...] = (
    "doc_id",
    "source_file",
    "page_count",
    "char_count",
    "title",
    "extracted_at",
    "text_preview",
)

# text_preview에 담을 본문 미리보기 최대 길이
PREVIEW_MAX_CHARS = 500
