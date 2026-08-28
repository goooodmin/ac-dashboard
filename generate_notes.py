"""
generate_notes.py — 읽기·의미·그래서 문구 자동 생성

auto_update.py가 DATA를 다 만든 뒤에 호출한다. 갱신된 수치를 Claude에 넘겨
7개 섹션 x 4개 기간 = 28개 문구를 새로 쓰게 하고 index.html의 NOTES를 교체한다.

설계 원칙 — 문구 생성 실패가 데이터 갱신을 막으면 안 된다.
API 키가 없거나 호출이 실패하면 기존 NOTES를 그대로 두고 경고만 남긴다.
대시보드에는 문구 기준일이 노출되고 14일이 지나면 낡았다는 안내가 뜨므로,
조용히 틀린 문구가 남는 상황은 피할 수 있다.

    ANTHROPIC_API_KEY   필수. 없으면 생성을 건너뛴다.
    NOTES_MODEL         선택. 기본 claude-opus-5.
"""
import json
import os
import re
import sys
from datetime import datetime

SECTIONS = ["02", "03", "04", "05", "07", "09", "10"]
PERIODS = ["week", "month", "quarter", "year"]

SECTION_TITLES = {
    "02": "인기 콘텐츠 TOP 10 (뷰 x 체류)",
    "03": "유입 소스별 분해",
    "04": "신규 vs 재방문 (국문/영문 사이트)",
    "05": "국가별 방문 · 체류시간",
    "07": "인기 핀 TOP 10 (Pinterest)",
    "09": "인기 게시물 TOP 10 (Instagram)",
    "10": "팔로워 증감 추이 (Instagram)",
}

PERIOD_KO = {"week": "주간", "month": "월간", "quarter": "분기", "year": "연간"}

SYSTEM = """당신은 아모레퍼시픽 크리에이티브 센터의 디자인 아카이브 사이트
(design.amorepacific.com) 대시보드에 들어갈 해석 문구를 쓴다.

읽는 사람은 이 사이트를 운영하는 디자인 팀이다. 지표 전문가가 아니라
콘텐츠를 만드는 사람들이므로, 숫자를 보여주는 데 그치지 말고 그래서 무엇을
해야 하는지까지 짚어야 한다.

각 섹션마다 세 단계로 쓴다.

  읽기   지금 데이터가 무엇을 보여주는가. 구체적인 숫자를 인용한다.
         비교 대상(1위 대 2위, 이번 기간 대 지난 기간)을 함께 넣어
         숫자가 큰지 작은지 알 수 있게 한다.
  의미   그 숫자가 왜 그런지, 무엇을 뜻하는지. 데이터에서 바로 읽히지
         않는 해석을 더한다. 추측이면 추측이라고 밝힌다.
  그래서 이번 기간에 할 수 있는 구체적인 행동. "모니터링한다",
         "개선한다" 같은 막연한 말은 쓰지 않는다.

문체와 규칙:
- 한국어 평서체. '~다'로 끝낸다. 존댓말을 쓰지 않는다.
- 각 단계는 1~2문장. 읽기는 3문장까지 허용한다.
- 데이터에 없는 사실을 지어내지 않는다. 원인을 단정할 근거가 없으면
  "~로 보인다", "확인이 필요하다"로 쓴다.
- 과장하지 않는다. 좋은 신호와 나쁜 신호를 있는 그대로 쓴다.
- 같은 섹션이라도 기간마다 관점이 달라야 한다. 주간은 이번 주에 벌어진
  변화, 연간은 구조와 추세를 본다. 네 기간에 같은 문장을 반복하지 않는다.
- 이전 문구를 참고 자료로 받는다. 같은 상황이 이어지면 "3주째",
  "지난주에 이어" 처럼 연속성을 드러내되, 문장을 그대로 복사하지 않는다.

데이터를 읽을 때 알아둘 것:
- 봇·스팸 트래픽은 이미 걸러진 수치다. 국가×유입소스 단위로 행동 지표를
  보고 자동 판별해 제외한다. 급증한 해외 유입을 봇으로 의심할 필요는 없다.
- Pinterest는 API가 최대 90일까지만 조회를 허용한다. 분기·연간 구간이
  비어 있으면 데이터 공백이지 성과 부진이 아니다.
- Instagram 팔로워 추이는 API가 최근 30일까지만 준다. 분기·연간에서도
  최근 구간만 표시된다.
- 연간은 달력상 1월~12월이 아니라 최근 365일이다.
- 재방문 비율은 기간이 길수록 낮게 나온다. 기간 간 직접 비교는 하지 않는다."""


def _block_schema():
    return {
        "type": "object",
        "properties": {
            "read": {"type": "string", "description": "읽기 — 구체적 수치와 비교"},
            "mean": {"type": "string", "description": "의미 — 해석"},
            "so":   {"type": "string", "description": "그래서 — 구체적 행동"},
        },
        "required": ["read", "mean", "so"],
        "additionalProperties": False,
    }


def _schema():
    period = {
        "type": "object",
        "properties": {s: _block_schema() for s in SECTIONS},
        "required": SECTIONS,
        "additionalProperties": False,
    }
    return {
        "type": "object",
        "properties": {p: period for p in PERIODS},
        "required": PERIODS,
        "additionalProperties": False,
    }


def build_brief(data: dict) -> str:
    """notes_brief.py의 브리핑을 그대로 재사용한다."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from notes_brief import brief
    parts = []
    for p in PERIODS:
        if p in data:
            parts.append(brief(data, p))
    return "\n".join(parts)


def _extract_notes(html: str):
    """기존 NOTES 블록을 (시작, 끝, dict)로 돌려준다. 없으면 None."""
    marker = "const NOTES = {"
    if marker not in html:
        return None
    a = html.index(marker)
    b = html.index("\n};", a) + len("\n};")
    try:
        obj = json.loads(html[a + len("const NOTES = "):b].rstrip().rstrip(";"))
    except Exception:
        obj = None
    return a, b, obj


def generate(data: dict, previous: dict | None = None) -> dict:
    """Claude에 데이터를 넘겨 28개 문구를 받는다."""
    import anthropic

    model = os.getenv("NOTES_MODEL", "claude-opus-5")
    client = anthropic.Anthropic()

    sections = "\n".join(f"  {k} — {v}" for k, v in SECTION_TITLES.items())
    prev_txt = ""
    if previous:
        trimmed = {p: previous.get(p, {}) for p in PERIODS if p in previous}
        prev_txt = ("\n\n# 이전 문구 (기준일 "
                    + str(previous.get("updated", "?"))
                    + ")\n연속성 참고용이다. 상황이 이어지면 그 점을 드러내되 문장을 복사하지 않는다.\n"
                    + json.dumps(trimmed, ensure_ascii=False, indent=1))

    user = f"""아래는 오늘 갱신된 대시보드 데이터다. 기간별로 펼쳐 놓았다.

# 문구를 쓸 섹션
{sections}

# 데이터
{build_brief(data)}{prev_txt}

네 기간(week, month, quarter, year) 각각에 대해 위 7개 섹션의
읽기·의미·그래서를 써라. 총 28개 블록이다."""

    with client.messages.stream(
        model=model,
        max_tokens=32000,
        system=SYSTEM,
        thinking={"type": "adaptive"},
        output_config={
            "effort": "high",
            "format": {"type": "json_schema", "schema": _schema()},
        },
        messages=[{"role": "user", "content": user}],
    ) as stream:
        msg = stream.get_final_message()

    if msg.stop_reason == "refusal":
        raise RuntimeError(f"모델이 응답을 거부했다 ({msg.stop_details})")

    text = next((b.text for b in msg.content if b.type == "text"), None)
    if not text:
        raise RuntimeError("응답에 텍스트 블록이 없다")

    notes = json.loads(text)
    for p in PERIODS:
        missing = [s for s in SECTIONS if s not in notes.get(p, {})]
        if missing:
            raise RuntimeError(f"{p} 기간에 {missing} 섹션이 빠졌다")

    u = msg.usage
    print(f"  토큰 입력 {u.input_tokens:,} · 출력 {u.output_tokens:,}")
    return notes


def refresh_notes(html: str, data: dict) -> str:
    """index.html의 NOTES 블록을 새로 생성한 문구로 교체한다.

    실패하면 원본 html을 그대로 돌려준다 — 데이터 갱신은 계속되어야 한다.
    """
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("  ⚠️  ANTHROPIC_API_KEY 없음 — 문구 생성 건너뜀 (기존 문구 유지)")
        return html

    found = _extract_notes(html)
    if not found:
        print("  ⚠️  index.html에 NOTES 블록이 없음 — 건너뜀")
        return html
    a, b, previous = found

    try:
        notes = generate(data, previous)
    except Exception as ex:
        print(f"  ❌ 문구 생성 실패 — 기존 문구를 유지한다: {type(ex).__name__}: {ex}")
        return html

    notes = {"updated": datetime.now().strftime("%Y-%m-%d"), **notes}
    block = "const NOTES = " + json.dumps(notes, ensure_ascii=False, indent=2) + ";"
    print(f"  ✅ 문구 생성 완료 ({len(SECTIONS) * len(PERIODS)}개 블록)")
    return html[:a] + block + html[b:]


if __name__ == "__main__":
    # 단독 실행 — 로컬 index.html의 DATA를 읽어 NOTES만 갱신한다.
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    src = open(path, encoding="utf-8").read()
    i = src.index("const DATA = {")
    j = src.index("\n};", i)
    DATA = json.loads(src[i + len("const DATA = "):j + 2].rstrip().rstrip(";"))
    out = refresh_notes(src, DATA)
    if out != src:
        open(path, "w", encoding="utf-8", newline="\r\n").write(out)
        print("index.html 갱신 완료")
    else:
        print("변경 없음")
