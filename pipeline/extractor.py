"""PDF 문서에서 구조화된 레코드를 추출한다.

추출 결과는 dict(JSON 직렬화 가능)로 반환되어 Kafka 메시지 payload로 그대로 쓰인다.
부수효과 없이 입력 파일 1개 → 레코드 1개로 매핑하는 순수 함수에 가깝다.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Any

import pdfplumber

from .config import PREVIEW_MAX_CHARS


def _doc_id(path: str, text: str) -> str:
    """파일 경로 + 내용 해시로 멱등한 문서 ID를 만든다.

    같은 파일을 다시 처리해도 동일 ID가 나와 중복 적재를 식별할 수 있다.
    """
    digest = hashlib.sha256()
    digest.update(os.path.basename(path).encode("utf-8"))
    digest.update(text.encode("utf-8"))
    return digest.hexdigest()[:16]


def _guess_title(text: str, fallback: str) -> str:
    """본문 첫 비어있지 않은 줄을 제목으로 추정한다."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:200]
    return fallback


def extract_pdf(path: str) -> dict[str, Any]:
    """단일 PDF를 추출해 직렬화 가능한 레코드 dict를 반환한다.

    Raises:
        FileNotFoundError: 경로가 존재하지 않을 때.
        ValueError: PDF를 열거나 파싱하지 못할 때.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"PDF를 찾을 수 없습니다: {path}")

    try:
        with pdfplumber.open(path) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
            page_count = len(pdf.pages)
    except Exception as exc:  # pdfplumber는 다양한 예외를 던질 수 있다
        raise ValueError(f"PDF 파싱 실패 ({path}): {exc}") from exc

    full_text = "\n".join(pages).strip()
    filename = os.path.basename(path)

    # 새 dict를 만들어 반환 — 입력을 변형하지 않는다(불변 패턴).
    return {
        "doc_id": _doc_id(path, full_text),
        "source_file": filename,
        "page_count": page_count,
        "char_count": len(full_text),
        "title": _guess_title(full_text, fallback=filename),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "text_preview": full_text[:PREVIEW_MAX_CHARS],
        # 전체 본문은 미리보기와 분리해 보관(CSV에는 미포함, DB 적재 시 사용 가능)
        "full_text": full_text,
    }
