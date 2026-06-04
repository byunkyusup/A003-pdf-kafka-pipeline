"""LLM 기반 문서 보강 — 요약·핵심포인트·태그·키워드 추출.

provider 추상화로 Ollama(기본)와 mock(외부 의존성 없는 검증용)을 교체할 수 있다.
Ollama 호출은 stdlib(urllib)만 사용해 추가 런타임 의존성이 없다.
"""

from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Protocol

from .config import LLM_INPUT_MAX_CHARS, LlmConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EnrichmentResult:
    """LLM 보강 결과 (불변)."""

    summary: str
    key_points: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    keywords: tuple[str, ...] = ()


class LlmProvider(Protocol):
    """문서 텍스트를 받아 보강 결과를 반환하는 provider 인터페이스."""

    def enrich(self, text: str, title: str) -> EnrichmentResult: ...


# LLM에 보낼 지시문 — JSON 스키마를 강제해 파싱을 안정화한다.
_PROMPT_TEMPLATE = """당신은 지식 관리 도우미입니다. 아래 문서를 분석해 JSON으로만 답하세요.

스키마:
{{
  "summary": "2~3문장 한국어 요약",
  "key_points": ["핵심 포인트 3~5개"],
  "tags": ["주제 태그 3~6개, 공백 대신 하이픈"],
  "keywords": ["핵심 키워드 5개 내외"]
}}

문서 제목: {title}
문서 본문:
{body}
"""


def _normalize_tag(raw: str) -> str:
    """태그를 위키링크/파일명 안전 형태로 정규화한다(공백→하이픈, 특수문자 제거)."""
    cleaned = re.sub(r"[^\w가-힣\- ]", "", raw).strip().lower()
    # 공백→하이픈으로 바꾸고 앞뒤·중복 하이픈을 정리한다(예: "- q1 2026" → "q1-2026").
    slug = re.sub(r"\s+", "-", cleaned)
    return re.sub(r"-{2,}", "-", slug).strip("-")


def _coerce_result(payload: dict) -> EnrichmentResult:
    """LLM이 돌려준 dict를 EnrichmentResult로 방어적으로 변환한다."""

    def _str_list(value: object) -> tuple[str, ...]:
        if not isinstance(value, list):
            return ()
        return tuple(str(v).strip() for v in value if str(v).strip())

    tags = tuple(_normalize_tag(t) for t in _str_list(payload.get("tags")) if _normalize_tag(t))
    return EnrichmentResult(
        summary=str(payload.get("summary", "")).strip(),
        key_points=_str_list(payload.get("key_points")),
        tags=tags,
        keywords=_str_list(payload.get("keywords")),
    )


class OllamaProvider:
    """로컬 Ollama 서버로 보강을 수행하는 provider."""

    def __init__(self, config: LlmConfig) -> None:
        self._url = config.ollama_url.rstrip("/") + "/api/generate"
        self._model = config.model
        self._timeout = config.timeout_sec

    def enrich(self, text: str, title: str) -> EnrichmentResult:
        prompt = _PROMPT_TEMPLATE.format(title=title, body=text[:LLM_INPUT_MAX_CHARS])
        body = json.dumps(
            {"model": self._model, "prompt": prompt, "stream": False, "format": "json"}
        ).encode("utf-8")
        req = urllib.request.Request(
            self._url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:  # noqa: S310 (로컬 신뢰 URL)
                outer = json.loads(resp.read().decode("utf-8"))
            # format=json이면 response 문자열 자체가 JSON 본문이다.
            inner = json.loads(outer.get("response", "{}"))
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError(f"Ollama 호출 실패 ({self._url}): {exc}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"Ollama 응답 JSON 파싱 실패: {exc}") from exc
        return _coerce_result(inner)


class MockProvider:
    """외부 의존성 없이 동작하는 결정적 provider (테스트·시연용).

    실제 LLM 없이 본문에서 규칙적으로 요약/태그를 추출한다.
    """

    def enrich(self, text: str, title: str) -> EnrichmentResult:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        summary = " ".join(lines[:2])[:200] if lines else title
        key_points = tuple(lines[:4])
        # 본문에서 빈도 높은 단어를 키워드로 (간단 휴리스틱)
        words = re.findall(r"[\w가-힣]{2,}", text.lower())
        freq: dict[str, int] = {}
        for w in words:
            freq[w] = freq.get(w, 0) + 1
        top = sorted(freq, key=lambda w: freq[w], reverse=True)[:5]
        tags = tuple(_normalize_tag(w) for w in top[:3] if _normalize_tag(w))
        return EnrichmentResult(
            summary=summary, key_points=key_points, tags=tags, keywords=tuple(top)
        )


def build_provider(config: LlmConfig) -> LlmProvider:
    """설정에 따라 LLM provider를 생성한다."""
    provider = config.provider.lower()
    if provider == "mock":
        return MockProvider()
    if provider == "ollama":
        return OllamaProvider(config)
    raise ValueError(f"알 수 없는 LLM provider: {config.provider} (ollama|mock)")
