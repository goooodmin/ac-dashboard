"""
notes_brief.py — 대시보드 문구(NOTES) 작성용 데이터 브리핑

라이브 대시보드의 DATA를 읽어 기간별로 펼쳐 출력한다.
읽기·의미·그래서 문구를 쓸 때 숫자를 일일이 뒤지지 않기 위한 도구.

    python notes_brief.py            # 라이브 사이트 기준
    python notes_brief.py --local    # 로컬 index.html 기준
"""
import json, re, sys, urllib.request
from pathlib import Path

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

URL = "https://goooodmin.github.io/ac-dashboard/"
SECTIONS = {
    "02": "인기 콘텐츠 TOP 10",
    "03": "유입 소스별 분해",
    "04": "신규 vs 재방문",
    "05": "국가별 방문 · 체류시간",
    "07": "인기 핀 TOP 10 (Pinterest)",
    "09": "인기 게시물 TOP 10 (Instagram)",
    "10": "팔로워 증감 추이 (Instagram)",
}


def load(local=False):
    if local:
        html = Path(__file__).with_name("index.html").read_text(encoding="utf-8")
    else:
        html = urllib.request.urlopen(URL).read().decode("utf-8")
    i = html.index("const DATA = {")
    j = html.index("\n};", i)
    data = json.loads(html[i + len("const DATA = "):j + 2].rstrip().rstrip(";"))
    notes = None
    if "const NOTES = {" in html:
        a = html.index("const NOTES = {")
        b = html.index("\n};", a)
        notes = json.loads(html[a + len("const NOTES = "):b + 2].rstrip().rstrip(";"))
    return data, notes


def pct(v):
    return f"{v:+.1f}%" if isinstance(v, (int, float)) else "—"


def brief(d, period):
    p = d[period]
    out = [f"\n{'='*78}", f"■ {period.upper()}  ({p.get('range','')})  {p.get('tag','')}", "=" * 78]

    out.append("\n[01] 웹 KPI")
    for k in p.get("kpi", []):
        out.append(f"   {k['label']:<12} {k['value']:>10}{k.get('unit','')}"
                   f"   전기간 {pct(k.get('prev'))}   전년 {pct(k.get('yoy'))}")

    tc = p.get("topContent", [])
    out.append(f"\n[02] {SECTIONS['02']} — 상위 6")
    for c in tc[:6]:
        out.append(f"   {c['rank']:>2}. {c['title'][:44]:<44} 뷰 {c['views']:>6,}  체류 {c['dur']}")
    if tc:
        top_view = max(tc, key=lambda x: x["views"])
        def secs(t):
            m, s = t.split(":"); return int(m) * 60 + int(s)
        top_dur = max(tc, key=lambda x: secs(x["dur"]))
        out.append(f"   → 뷰 1위: {top_view['title'][:30]} / 체류 1위: {top_dur['title'][:30]}"
                   f" {'(동일)' if top_view is top_dur else '(불일치)'}")

    src = p.get("sources", [])
    out.append(f"\n[03] {SECTIONS['03']} — 상위 6")
    tot_s = sum(x["sessions"] for x in src) or 1
    for c in src[:6]:
        out.append(f"   {c['source']:<20} {c['sessions']:>7,}세션 "
                   f"({c['sessions']/tot_s*100:>4.1f}%)  체류 {c['dur']}")

    nr = p.get("newRet", {})
    out.append(f"\n[04] {SECTIONS['04']}")
    sm = nr.get("summary", {})
    for key, lab in (("kr", "국문"), ("en", "영문")):
        v = sm.get(key)
        if v:
            out.append(f"   {lab}  총 {v['total']:>6,}  신규 {v['newN']:>6,}({v['newPct']}%)"
                       f"  재방문 {v['retN']:>5,}({v['retPct']}%)")
    if sm.get("kr") and sm.get("en"):
        gap = sm["en"]["newPct"] - sm["kr"]["newPct"]
        out.append(f"   → 영문 신규비중이 국문보다 {gap:+.1f}%p")
    labs = nr.get("labels") or []
    if labs:
        out.append(f"   구간: {' · '.join(str(x) for x in labs)}")
        for key, lab in (("krNew", "국문신규"), ("krReturn", "국문재방문"),
                         ("enNew", "영문신규"), ("enReturn", "영문재방문")):
            v = nr.get(key) or []
            if v:
                out.append(f"   {lab:<10} {' '.join(f'{x:>5,}' for x in v)}")

    ct = p.get("countries", [])
    out.append(f"\n[05] {SECTIONS['05']} — 상위 8")
    for c in ct[:8]:
        out.append(f"   {c['country']:<22} {c['visits']:>7,}  {c['pct']:>5}%  체류 {c['dur']}")

    pin = p.get("pin", {})
    out.append(f"\n[06/07] Pinterest")
    for k in pin.get("kpi", []):
        out.append(f"   {k['label']:<12} {k['value']:>10}{k.get('unit','')}   전기간 {pct(k.get('prev'))}")
    for t in (pin.get("top") or [])[:4]:
        bits = "  ".join(f"{kk} {vv:,}" if isinstance(vv, int) else f"{kk} {vv}"
                         for kk, vv in t.items() if kk not in ("rank", "title", "url", "img"))
        out.append(f"   {t.get('rank','')}. {str(t.get('title',''))[:38]:<38} {bits}")

    ig = p.get("ig", {})
    out.append(f"\n[08/09/10] Instagram")
    if not ig:
        out.append("   (데이터 없음)")
    else:
        for k in ig.get("kpi", []):
            out.append(f"   {k['label']:<12} {k['value']:>10}{k.get('unit','')}   전기간 {pct(k.get('prev'))}")
        for n, t in enumerate((ig.get("top") or [])[:5], 1):
            bits = "  ".join(f"{kk} {vv:,}" if isinstance(vv, int) else f"{kk} {vv}"
                             for kk, vv in t.items() if kk not in ("rank", "title", "url", "img", "caption"))
            out.append(f"   {n}. {str(t.get('caption', t.get('title','')))[:36]:<36} {bits}")
        g = ig.get("growth")
        if isinstance(g, dict):
            vals = g.get("values") or g.get("data") or []
            labs2 = g.get("labels") or []
            nums = [x for x in vals if isinstance(x, (int, float))]
            if nums:
                out.append(f"   팔로워 증감: {' '.join(f'{x:+,}' for x in nums)}")
                if labs2:
                    out.append(f"   구간      : {' '.join(str(x) for x in labs2)}")
                out.append(f"   → 합계 {sum(nums):+,}  최대 {max(nums):+,}  최소 {min(nums):+,}")
        elif isinstance(g, list) and g:
            nums = [x for x in g if isinstance(x, (int, float))]
            if nums:
                out.append(f"   팔로워 증감: {' '.join(f'{x:+,}' for x in nums)}  합계 {sum(nums):+,}")
    return "\n".join(out)


def main():
    local = "--local" in sys.argv
    data, notes = load(local)
    print(f"출처: {'로컬 index.html' if local else URL}")
    if notes:
        print(f"기존 NOTES 기준일: {notes.get('updated', '(없음)')}")
    else:
        print("기존 NOTES: 없음 (최초 작성)")
    for period in ("week", "month", "quarter", "year"):
        if period in data:
            print(brief(data, period))


if __name__ == "__main__":
    main()
