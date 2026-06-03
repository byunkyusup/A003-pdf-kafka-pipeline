#!/usr/bin/env python3
"""한글 샘플 PDF 생성기 (Pretendard 폰트 사용).

코어 폰트(Helvetica)는 latin-1만 지원하므로 한글을 쓰려면 유니코드 폰트를
임베드해야 한다. fpdf2 기준 **TTF**를 써야 pdfminer가 텍스트를 추출할 수 있다
(OTF/CFF는 임베드는 되지만 추출이 실패하는 경우가 있음).

사용:
    python scripts/make_korean_sample.py
환경 변수:
    FONT_PATH  사용할 TTF 경로 (기본: fonts/Pretendard-Regular.ttf)
"""

from __future__ import annotations

import os

from fpdf import FPDF
from fpdf.enums import XPos, YPos

FONT_PATH = os.environ.get("FONT_PATH", "fonts/Pretendard-Regular.ttf")
OUTPUT = os.environ.get("OUTPUT", "sample_pdfs/report_kr.pdf")

LINES = (
    "2026년 1분기 운영 보고서",
    "총 처리 문서: 1,284건",
    "평균 추출 정확도: 97.3%",
    "Kafka 파이프라인 안정성 검증 완료.",
)


def main() -> None:
    if not os.path.isfile(FONT_PATH):
        raise FileNotFoundError(f"폰트를 찾을 수 없습니다: {FONT_PATH}")

    pdf = FPDF()
    pdf.add_page()
    pdf.add_font("Pretendard", "", FONT_PATH)  # 유니코드 TTF 등록
    pdf.set_font("Pretendard", size=14)
    for line in LINES:
        pdf.cell(0, 10, line, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)
    pdf.output(OUTPUT)
    print(f"생성: {OUTPUT}")


if __name__ == "__main__":
    main()
