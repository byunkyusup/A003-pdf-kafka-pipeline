"""CSV 적재 + (주석 처리된) DB 저장 싱크.

Kafka 메시지 payload(dict)를 받아 CSV 한 행으로 기록한다.
DB 저장 로직은 요구사항에 따라 주석으로만 남겨두고 실제로는 실행하지 않는다.
"""

from __future__ import annotations

import csv
import logging
import os
from typing import Any

from .config import CSV_FIELDS

logger = logging.getLogger(__name__)


class CsvSink:
    """레코드를 CSV 파일에 append 하는 싱크.

    컨텍스트 매니저로 사용하면 파일 핸들과 헤더를 안전하게 관리한다.
    """

    def __init__(self, output_csv: str, encoding: str = "utf-8-sig") -> None:
        # encoding 기본 utf-8-sig: BOM을 붙여 Excel에서 한글이 깨지지 않게 한다.
        self._path = output_csv
        self._encoding = encoding
        self._file = None
        self._writer: csv.DictWriter | None = None
        self._written = 0

    def __enter__(self) -> "CsvSink":
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        # 파일이 없을 때만 헤더를 쓴다(append 안전).
        write_header = not os.path.exists(self._path) or os.path.getsize(self._path) == 0
        self._file = open(self._path, "a", newline="", encoding=self._encoding)
        # CSV에 없는 키(full_text 등)는 무시한다.
        self._writer = csv.DictWriter(
            self._file, fieldnames=CSV_FIELDS, extrasaction="ignore"
        )
        if write_header:
            self._writer.writeheader()
        return self

    def write(self, record: dict[str, Any]) -> None:
        if self._writer is None:
            raise RuntimeError("CsvSink는 with 블록 안에서 사용해야 합니다.")
        # CSV_FIELDS에 정의된 컬럼만 추려서 기록 — 스키마 일관성 보장.
        row = {key: record.get(key, "") for key in CSV_FIELDS}
        self._writer.writerow(row)
        self._written += 1

        # ----------------------------------------------------------------
        # DB 저장 (요구사항에 따라 주석 처리)
        # ----------------------------------------------------------------
        # save_to_db(record)
        # ----------------------------------------------------------------

    def __exit__(self, *exc: object) -> None:
        if self._file is not None:
            self._file.close()
        logger.info("CSV 적재 완료: %d행 → %s", self._written, self._path)


# ====================================================================
# DB 저장 스텁 — 실제 인프라 연결 시 주석 해제 후 구현.
# ====================================================================
# import psycopg2  # 또는 sqlalchemy 등
#
# def save_to_db(record: dict[str, Any]) -> None:
#     """추출 레코드를 관계형 DB에 upsert 한다.
#
#     doc_id를 PK로 사용해 동일 문서 재처리 시 중복을 방지한다.
#     """
#     conn = psycopg2.connect(os.environ["DATABASE_URL"])
#     try:
#         with conn, conn.cursor() as cur:
#             cur.execute(
#                 """
#                 INSERT INTO documents
#                     (doc_id, source_file, page_count, char_count,
#                      title, extracted_at, full_text)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s)
#                 ON CONFLICT (doc_id) DO UPDATE SET
#                     char_count = EXCLUDED.char_count,
#                     extracted_at = EXCLUDED.extracted_at,
#                     full_text = EXCLUDED.full_text;
#                 """,
#                 (
#                     record["doc_id"],
#                     record["source_file"],
#                     record["page_count"],
#                     record["char_count"],
#                     record["title"],
#                     record["extracted_at"],
#                     record.get("full_text", ""),
#                 ),
#             )
#     finally:
#         conn.close()
