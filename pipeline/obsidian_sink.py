"""보강 레코드를 Obsidian vault의 마크다운 노트로 적재하는 싱크.

각 문서 → 노트 1개(YAML frontmatter + 요약/핵심포인트 + [[위키링크]] 태그 + 원문 미리보기).
실행 종료 시 태그별 MOC(Map of Content) 노트를 생성해 Obsidian 그래프뷰에서
문서들이 태그를 중심으로 연결된 지식망으로 보이게 한다.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

from .config import PREVIEW_MAX_CHARS, VaultConfig
from .enrich import EnrichmentResult

logger = logging.getLogger(__name__)


def _slugify(value: str, fallback: str) -> str:
    """파일명/위키링크에 안전한 slug를 만든다(한글 보존, 특수문자 제거)."""
    cleaned = re.sub(r"[^\w가-힣\- ]", "", value).strip()
    slug = re.sub(r"\s+", "-", cleaned)
    return slug or fallback


def _yaml_list(items: tuple[str, ...]) -> str:
    """YAML 인라인 리스트로 직렬화한다."""
    return "[" + ", ".join(items) + "]"


def _render_note(record: dict[str, Any], enrichment: EnrichmentResult) -> str:
    """레코드+보강결과를 마크다운 노트 문자열로 렌더링한다."""
    title = record.get("title") or record.get("source_file", "untitled")
    preview = (record.get("full_text") or record.get("text_preview", ""))[:PREVIEW_MAX_CHARS]

    frontmatter = [
        "---",
        f"title: {title}",
        f"source_file: {record.get('source_file', '')}",
        f"doc_id: {record.get('doc_id', '')}",
        f"created: {record.get('extracted_at', '')}",
        f"page_count: {record.get('page_count', 0)}",
        f"tags: {_yaml_list(enrichment.tags)}",
        f"keywords: {_yaml_list(enrichment.keywords)}",
        "---",
    ]

    body = [f"# {title}", "", "## 요약", enrichment.summary or "_(요약 없음)_", ""]

    if enrichment.key_points:
        body.append("## 핵심 포인트")
        body.extend(f"- {kp}" for kp in enrichment.key_points)
        body.append("")

    if enrichment.tags:
        body.append("## 태그")
        # 태그를 위키링크로 연결 → 태그 MOC 노트와 그래프상 연결된다.
        body.append(" ".join(f"[[{tag}]]" for tag in enrichment.tags))
        body.append("")

    body.extend(["## 원문 미리보기", "```text", preview, "```", ""])

    return "\n".join(frontmatter) + "\n\n" + "\n".join(body)


class ObsidianSink:
    """Obsidian vault에 노트를 기록하고, 종료 시 태그별 MOC를 생성하는 싱크."""

    def __init__(self, vault: VaultConfig) -> None:
        self._notes_dir = os.path.join(vault.vault_dir, vault.notes_subdir)
        self._moc_dir = os.path.join(vault.vault_dir, vault.moc_subdir)
        # 태그 → [(note_slug, title)] 누적. MOC 생성에 사용.
        self._tag_index: dict[str, list[tuple[str, str]]] = {}
        self._written = 0

    def __enter__(self) -> "ObsidianSink":
        os.makedirs(self._notes_dir, exist_ok=True)
        os.makedirs(self._moc_dir, exist_ok=True)
        return self

    def write(self, record: dict[str, Any], enrichment: EnrichmentResult) -> None:
        """단일 문서를 노트로 기록한다."""
        title = record.get("title") or record.get("source_file", "untitled")
        slug = _slugify(title, fallback=record.get("doc_id", "note"))
        path = os.path.join(self._notes_dir, f"{slug}.md")

        with open(path, "w", encoding="utf-8") as f:
            f.write(_render_note(record, enrichment))

        # 새 dict 변형 없이 태그 인덱스만 누적(MOC 생성용).
        for tag in enrichment.tags:
            self._tag_index.setdefault(tag, []).append((slug, title))
        self._written += 1

    def _write_mocs(self) -> None:
        """태그별 MOC 노트를 생성한다. 각 MOC는 해당 태그 노트들을 위키링크로 모은다."""
        for tag, notes in sorted(self._tag_index.items()):
            path = os.path.join(self._moc_dir, f"{tag}.md")
            lines = [f"# {tag} MOC", "", f"`{tag}` 태그가 달린 문서 모음.", ""]
            lines.extend(f"- [[{slug}|{title}]]" for slug, title in notes)
            lines.append("")
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))

    def __exit__(self, *exc: object) -> None:
        # 예외 발생 시에도 지금까지 누적된 MOC는 기록한다.
        self._write_mocs()
        logger.info(
            "Obsidian 적재 완료: 노트 %d개, 태그 MOC %d개 → %s",
            self._written,
            len(self._tag_index),
            self._notes_dir,
        )
