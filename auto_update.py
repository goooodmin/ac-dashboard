"""
auto_update.py  –  AC 대시보드 전체 자동 업데이트
GA4 (4기간) · Pinterest (4기간) · Instagram (4기간) → index.html DATA 갱신 → GitHub 배포
"""
import sys, os, json, re
from pathlib import Path
from datetime import datetime, timedelta

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# .env 로드 (GitHub Actions에서는 os.environ이 이미 채워져 있으므로 override=False가 안전)
from dotenv import load_dotenv
load_dotenv()

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from main import init_ga4_client, upload_to_github, collect_instagram_data, PROPERTY_ID, PINTEREST_TOKEN

from google.analytics.data_v1beta.types import (
    DateRange, Metric, Dimension, RunReportRequest,
    FilterExpression, FilterExpressionList, Filter,
)
import urllib.request


# ── 유틸 ──────────────────────────────────────────────────────
def fmt_num(n):
    return f"{n:,}" if isinstance(n, int) else str(n)

def fmt_dur(secs):
    s = max(0, int(float(secs)))
    return f"{s // 60}:{s % 60:02d}"

def pct_diff(new_val, old_val):
    """% 변화율. old가 0이면 None."""
    if not old_val:
        return None
    return round((new_val - old_val) / old_val * 100, 1)

def ds(dt):
    return dt.strftime("%Y-%m-%d")


# ── 기간 레이블 ───────────────────────────────────────────────
MONTHS_EN = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
MONTHS_KO = ["","1월","2월","3월","4월","5월","6월","7월","8월","9월","10월","11월","12월"]
WEEKDAY_KO = ["월","화","수","목","금","토","일"]
PERIOD_KO  = {7: "주간", 30: "월간", 90: "분기", 365: "연간"}
PERIOD_SUB = {7: "주간 누적", 30: "월간 누적", 90: "분기 누적", 365: "연간 누적"}

def range_label(days: int) -> str:
    now = datetime.now()
    if days == 7:
        start = now - timedelta(days=7)
        m1 = start.strftime("%b").upper()
        m2 = now.strftime("%b").upper()
        return f"{m1} {start.day}–{now.day}" if m1 == m2 else f"{m1} {start.day}–{m2} {now.day}"
    if days == 30:
        return now.strftime("%b %Y").upper()
    if days == 90:
        q = (now.month - 1) // 3 + 1
        qs = (q - 1) * 3 + 1
        return f"Q{q} {now.year} · {MONTHS_EN[qs].upper()}–{MONTHS_EN[min(qs+2,12)].upper()}"
    return str(now.year)

def period_tag(days: int) -> str:
    now = datetime.now()
    if days == 7:
        start = now - timedelta(days=7)
        m1 = start.strftime("%b").upper()
        m2 = now.strftime("%b").upper()
        suffix = f"{m1} {start.day}–{now.day}" if m1 == m2 else f"{m1} {start.day}–{m2} {now.day}"
        return f"WEEK · {suffix}"
    if days == 30:
        return f"MONTH · {now.strftime('%b %Y').upper()}"
    if days == 90:
        q = (now.month - 1) // 3 + 1
        return f"QUARTER · Q{q} {now.year}"
    return f"YEAR · {now.year}"


# ── GA4 헬퍼 ──────────────────────────────────────────────────
def ga4_run(client, start, end, metrics, dims=[], filt=None, limit=25):
    req = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start, end_date=end)],
        metrics=[Metric(name=m) for m in metrics],
        dimensions=[Dimension(name=d) for d in dims],
        limit=limit,
    )
    if filt:
        req.dimension_filter = filt
    return client.run_report(req)

def path_filter(prefix):
    return FilterExpression(filter=Filter(
        field_name="pagePath",
        string_filter=Filter.StringFilter(
            match_type=Filter.StringFilter.MatchType.BEGINS_WITH,
            value=prefix,
        )
    ))

# ── 봇 트래픽 자동 탐지 · 제외 ─────────────────────────────────
# 2026-08 GA4 실측으로 두 종류의 오염을 확인했다. 둘 다 (direct)로 들어온다.
#   1) 자동 수집형 — 체류 0.5초, PV/세션 1.0. 페이지를 실제로 1회 받아가는 크롤러.
#   2) 유령세션형 — PV/세션 0.0~0.1. 사이트에 접속조차 하지 않고 측정 ID로
#      가짜 세션만 쏘는 GA4 측정 프로토콜 스팸.
#
# 국가를 고정 목록으로 박아두면 안 된다. 스팸이 국가를 바꿔가며 들어오고
# (Venezuela·Russia·Morocco가 주 단위로 새로 등장), 반대로 Vietnam·India처럼
# 정상 유입이 많은 나라 안에 봇이 섞여 들어오기도 한다.
# 그래서 국가×소스 조합의 행동 지표로 매 실행마다 새로 판별한다.
BOT_MIN_SESSIONS = 30     # 표본이 이보다 적으면 판정하지 않는다 (과잉 차단 방지)
BOT_MIN_PV_RATIO = 0.30   # 세션당 페이지뷰. 사람은 최소 1페이지는 연다
BOT_MIN_DURATION = 5.0    # 초. 사람이 페이지를 인지하는 최소 시간


def detect_bot_segments(client, start, end):
    """국가×소스 조합 중 봇 시그니처를 보이는 것을 찾아낸다."""
    r = ga4_run(client, start, end,
                ["sessions", "screenPageViews", "averageSessionDuration"],
                ["country", "sessionSource"], limit=500)
    bad = []
    for row in r.rows:
        sess = int(float(row.metric_values[0].value))
        if sess < BOT_MIN_SESSIONS:
            continue
        pv  = int(float(row.metric_values[1].value))
        dur = float(row.metric_values[2].value)
        if pv / sess < BOT_MIN_PV_RATIO or dur < BOT_MIN_DURATION:
            bad.append((row.dimension_values[0].value,
                        row.dimension_values[1].value, sess))
    return bad


def _exclude_pairs(segments):
    """(국가, 소스) 조합들을 제외하는 FilterExpression."""
    if not segments:
        return None
    pairs = [
        FilterExpression(and_group=FilterExpressionList(expressions=[
            FilterExpression(filter=Filter(
                field_name="country",
                string_filter=Filter.StringFilter(value=c))),
            FilterExpression(filter=Filter(
                field_name="sessionSource",
                string_filter=Filter.StringFilter(value=s))),
        ]))
        for c, s, _ in segments
    ]
    return FilterExpression(not_expression=FilterExpression(
        or_group=FilterExpressionList(expressions=pairs)))


_SEG_CACHE: dict = {}
_BOT_CACHE: dict = {}

def _segments(client, start, end):
    key = (start, end)
    if key not in _SEG_CACHE:
        _SEG_CACHE[key] = detect_bot_segments(client, start, end)
    return _SEG_CACHE[key]


def bot_filter(client, start, end):
    """해당 기간의 봇 제외 필터. 기간마다 새로 판별하고 캐시한다."""
    key = (start, end)
    if key not in _BOT_CACHE:
        seg = list(_segments(client, start, end))

        # 최근 30일 판정을 합집합으로 더한다.
        # 분기·연간처럼 긴 구간은 과거 정상 트래픽에 평균이 희석돼,
        # 최근 급증한 봇이 임계값을 통과해 버린다.
        # (연간 기준 싱가포르는 PV/세션 1.0·체류 26초로 판정을 빠져나갔다.)
        now = datetime.now()
        seg += _segments(client, ds(now - timedelta(days=30)), ds(now))

        merged, seen = [], set()
        for c, s, n in seg:
            if (c, s) not in seen:
                seen.add((c, s))
                merged.append((c, s, n))

        if merged:
            tot = sum(n for _, _, n in merged)
            top = ", ".join(f"{c}/{s}" for c, s, _ in
                            sorted(merged, key=lambda x: -x[2])[:3])
            print(f"  🤖 {start}~{end}: {len(merged)}개 구간 제외 "
                  f"(최대 {tot:,}세션 · {top} …)")
        _BOT_CACHE[key] = _exclude_pairs(merged)
    return _BOT_CACHE[key]


def clean(client, start, end, extra=None):
    """봇 제외 필터. extra가 있으면 AND로 결합한다."""
    bf = bot_filter(client, start, end)
    if bf is None:
        return extra
    if extra is None:
        return bf
    return FilterExpression(
        and_group=FilterExpressionList(expressions=[bf, extra]))


SKIP_PATHS = [
    "/work/page/", "/en/work/page/", "/en/work/",
    "/about", "/en/about", "/ci", "/en/ci",
    "/now", "/en/now", "/en/", "/work/2024/aritaum/template",
]

SOURCE_LABEL = {
    "google": "Google 검색", "(direct)": "Direct",
    "pinterest.com": "Pinterest", "naver": "Naver 검색",
    "bing": "Bing 검색", "daum": "Daum 검색",
    "facebook.com": "Facebook", "instagram.com": "Instagram",
    "youtube.com": "YouTube", "t.co": "X(트위터)",
}

SOURCE_MEANING = {
    "google":           "목적성 검색에서 온 독자, 가장 깊이 머뭄.",
    "Google 검색":      "목적성 검색에서 온 독자, 가장 깊이 머뭄.",
    "(direct)":         "주소를 아는 팬과 업계 지인의 재방문.",
    "Direct":           "주소를 아는 팬과 업계 지인의 재방문.",
    "pinterest.com":    "이미지 탐색으로 유입 · 의외로 긴 체류.",
    "Pinterest":        "이미지 탐색으로 유입 · 의외로 긴 체류.",
    "naver":            "국내 검색 유입 · 수는 적지만 깊이 읽힘.",
    "Naver 검색":       "국내 검색 유입 · 수는 적지만 깊이 읽힘.",
    "m.search.naver":   "네이버 모바일 검색 · 짧은 체류 후 이탈.",
    "chatgpt.com":      "AI 검색 추천 유입 · 관찰할 신호.",
    "kr.pinterest.com": "Pinterest 국내 도메인 · 탐색성 체류.",
    "bing":             "해외 Bing 검색 · 주로 영문 사이트 유입.",
    "Bing 검색":        "해외 Bing 검색 · 주로 영문 사이트 유입.",
    "gemini.google":    "AI 검색 추천 · 체류가 가장 김.",
    "instagram.com":    "SNS 연계 유입 · 규모 작지만 반응 뜨거움.",
    "Instagram":        "SNS 연계 유입 · 규모 작지만 반응 뜨거움.",
    "facebook.com":     "Facebook 유입.",
    "youtube.com":      "영상 콘텐츠 유입.",
}

FLAGS = {
    "South Korea":"🇰🇷","United States":"🇺🇸","Japan":"🇯🇵","China":"🇨🇳",
    "France":"🇫🇷","United Kingdom":"🇬🇧","Singapore":"🇸🇬","Taiwan":"🇹🇼",
    "Vietnam":"🇻🇳","Germany":"🇩🇪","Australia":"🇦🇺","Canada":"🇨🇦",
    "Thailand":"🇹🇭","Hong Kong":"🇭🇰","Indonesia":"🇮🇩",
}


# ── GA4 기간별 수집 ───────────────────────────────────────────
def collect_ga4_period(client, days: int) -> dict:
    now = datetime.now()
    s   = ds(now - timedelta(days=days));   e  = ds(now)
    ps  = ds(now - timedelta(days=days*2)); pe = ds(now - timedelta(days=days+1))
    ys  = ds(now - timedelta(days=days+365)); ye = ds(now - timedelta(days=366))

    pl_ko  = PERIOD_KO[days]
    pl_sub = PERIOD_SUB[days]

    # ── KPI ──
    def kpi_vals(st, en):
        r = ga4_run(client, st, en, ["activeUsers","averageSessionDuration","newUsers"],
                    filt=clean(client, st, en))
        if not r.rows:
            return 0, 0.0, 0
        v = r.rows[0].metric_values
        return int(v[0].value), float(v[1].value), int(v[2].value)

    users,   avg_s,   new_u   = kpi_vals(s,  e)
    p_users, p_avg_s, p_new_u = kpi_vals(ps, pe)
    y_users, y_avg_s, y_new_u = kpi_vals(ys, ye) if days <= 30 else (0, 0.0, 0)

    ret_u     = users   - new_u
    p_ret_u   = p_users - p_new_u
    y_ret_u   = y_users - y_new_u
    new_pct   = round(new_u   / users   * 100, 1) if users   else 0
    ret_pct   = round(ret_u   / users   * 100, 1) if users   else 0
    p_ret_pct = round(p_ret_u / p_users * 100, 1) if p_users else 0
    y_ret_pct = round(y_ret_u / y_users * 100, 1) if y_users else 0

    kpi = [
        {"label": "총 방문자", "value": fmt_num(users), "sub": pl_sub,
         "prev": pct_diff(users, p_users),
         "yoy":  pct_diff(users, y_users) if days <= 30 and y_users else None,
         "note": f"{pl_ko} 총 방문자 (신규 {new_pct}%)."},
        {"label": "평균 체류시간", "value": fmt_dur(avg_s), "sub": "분 : 초",
         "prev": pct_diff(avg_s, p_avg_s),
         "yoy":  pct_diff(avg_s, y_avg_s) if days <= 30 and y_avg_s else None,
         "note": f"{pl_ko} 평균 세션 체류시간."},
        {"label": "신규 방문자", "value": fmt_num(new_u), "sub": f"전체의 {new_pct}%",
         "prev": pct_diff(new_u, p_new_u),
         "yoy":  pct_diff(new_u, y_new_u) if days <= 30 and y_new_u else None,
         "note": f"{pl_ko} 신규 도달 방문자."},
        {"label": "재방문 비율", "value": str(ret_pct), "unit": "%", "sub": "재방문 비중",
         "prev": round(ret_pct - p_ret_pct, 1),
         "yoy":  round(ret_pct - y_ret_pct, 1) if days <= 30 and y_users else None,
         "note": "전체 방문 중 재방문 비중.", "deltaUnit": "pp"},
    ]

    # ── Top Content ──
    tc = ga4_run(client, s, e,
                 ["screenPageViews", "averageSessionDuration"],
                 ["pagePath", "pageTitle"], filt=clean(client, s, e), limit=50)
    top_content = []
    for row in tc.rows:
        path  = row.dimension_values[0].value
        title = row.dimension_values[1].value
        parts = [p for p in path.split("/") if p]
        if len(parts) <= 1:
            continue
        if any(path.startswith(sk) for sk in SKIP_PATHS):
            continue
        views = int(row.metric_values[0].value)
        dur   = float(row.metric_values[1].value)
        t = title.split(" - ")[1] if " - " in title else title
        t = re.sub(r'\s*\([^)]*\)', '', t).strip()
        if re.match(r'^[a-zA-Z0-9\s\-\./]+$', t) and len(parts) >= 3:
            t = f"{parts[1]} · {parts[2]}"
        top_content.append({
            "rank":  len(top_content) + 1,
            "title": t,
            "views": views,
            "dur":   fmt_dur(dur),
            "url":   "https://design.amorepacific.com" + path,
        })
        if len(top_content) >= 10:
            break

    # ── Sources ──
    sr = ga4_run(client, s, e,
                 ["sessions", "averageSessionDuration"],
                 ["sessionSource"], filt=clean(client, s, e), limit=15)
    sources = []
    for row in sr.rows:
        raw   = row.dimension_values[0].value
        label = SOURCE_LABEL.get(raw, raw)
        sources.append({
            "source":   label,
            "sessions": int(row.metric_values[0].value),
            "dur":      fmt_dur(float(row.metric_values[1].value)),
            "meaning":  SOURCE_MEANING.get(raw, SOURCE_MEANING.get(label, "기타 유입 채널.")),
        })
        if len(sources) >= 10:
            break

    # ── Countries ──
    cr = ga4_run(client, s, e,
                 ["activeUsers", "averageSessionDuration"],
                 ["country"], filt=clean(client, s, e), limit=10)
    total_c = sum(int(r.metric_values[0].value) for r in cr.rows) or 1
    countries = []
    for row in cr.rows:
        c = row.dimension_values[0].value
        u = int(row.metric_values[0].value)
        countries.append({
            "country": f"{FLAGS.get(c, '🌐')} {c}",
            "visits":  u,
            "pct":     round(u / total_c * 100, 1),
            "dur":     fmt_dur(float(row.metric_values[1].value)),
        })

    # ── NewRet 시계열 ──
    if days == 7:
        dim_name = "date"
        def make_labels(keys):
            return [WEEKDAY_KO[datetime.strptime(k, "%Y%m%d").weekday()] for k in keys]
        def make_dates(keys):
            # 축 라벨은 요일뿐이라 실제 날짜가 사라진다. 툴팁용으로 따로 남긴다.
            out = []
            for k in keys:
                dt = datetime.strptime(k, "%Y%m%d")
                out.append(f"{dt.month}월 {dt.day}일 ({WEEKDAY_KO[dt.weekday()]})")
            return out
    elif days == 30:
        dim_name = "week"
        def make_labels(keys):
            return [f"W{i+1}" for i in range(len(keys))]
        def make_dates(keys):
            # GA4 week 차원은 ISO 주차 번호만 준다. 실제 기간으로 되돌린다.
            yr = datetime.now().year
            out = []
            for k in keys:
                try:
                    mon = datetime.fromisocalendar(yr, int(k), 1)
                except ValueError:
                    out.append(f"{yr}년 {int(k)}주차"); continue
                sun = mon + timedelta(days=6)
                out.append(f"{mon.month}.{mon.day}~{sun.month}.{sun.day}")
            return out
    elif days == 90:
        dim_name = "month"
        def make_labels(keys):
            return [MONTHS_KO[int(k)] for k in keys]
        def make_dates(keys):
            return [f"{datetime.now().year}년 {MONTHS_KO[int(k)]}" for k in keys]
    else:
        dim_name = "month"
        def make_labels(keys):
            return [MONTHS_EN[int(k)] for k in keys]
        def make_dates(keys):
            return [f"{datetime.now().year}년 {MONTHS_KO[int(k)]}" for k in keys]

    def newret_series(prefix):
        r = ga4_run(client, s, e, ["activeUsers"],
                    [dim_name, "newVsReturning"],
                    filt=clean(client, s, e, path_filter(prefix)), limit=200)
        d: dict = {}
        for row in r.rows:
            k  = row.dimension_values[0].value
            nv = row.dimension_values[1].value
            u  = int(row.metric_values[0].value)
            if k not in d:
                d[k] = {"new": 0, "ret": 0}
            if nv == "new":
                d[k]["new"] += u
            else:
                d[k]["ret"] += u
        return d

    kr_d = newret_series("/")
    en_d = newret_series("/en/")
    keys = sorted(set(list(kr_d) + list(en_d)))

    kr_new = [kr_d.get(k, {}).get("new", 0) for k in keys]
    kr_ret = [kr_d.get(k, {}).get("ret", 0) for k in keys]
    en_new = [en_d.get(k, {}).get("new", 0) for k in keys]
    en_ret = [en_d.get(k, {}).get("ret", 0) for k in keys]

    kr_t = sum(kr_new) + sum(kr_ret) or 1
    en_t = sum(en_new) + sum(en_ret) or 1
    newRet = {
        "labels":   make_labels(keys) if keys else [],
        "dates":    make_dates(keys)  if keys else [],
        "krNew":    kr_new, "krReturn": kr_ret,
        "enNew":    en_new, "enReturn": en_ret,
        "summary": {
            "kr": {"total": kr_t,
                   "newPct": round(sum(kr_new)/kr_t*100, 1),
                   "retPct": round(sum(kr_ret)/kr_t*100, 1),
                   "newN": sum(kr_new), "retN": sum(kr_ret)},
            "en": {"total": en_t,
                   "newPct": round(sum(en_new)/en_t*100, 1),
                   "retPct": round(sum(en_ret)/en_t*100, 1),
                   "newN": sum(en_new), "retN": sum(en_ret)},
        }
    }

    lede = (f"{pl_ko} 방문자 {fmt_num(users)}명 · "
            f"평균 체류 {fmt_dur(avg_s)}. "
            f"방문 수와 체류 깊이를 함께 읽는다.")

    print(f"  ✅ GA4 {pl_ko}: 방문 {users:,}명 · 체류 {fmt_dur(avg_s)}")
    return {
        "range":      range_label(days),
        "tag":        period_tag(days),
        "lede":       lede,
        "kpi":        kpi,
        "topContent": top_content,
        "sources":    sources,
        "newRet":     newRet,
        "countries":  countries,
    }


# ── Pinterest 기간별 수집 ─────────────────────────────────────
def collect_pinterest_period(days: int):
    if not PINTEREST_TOKEN:
        return None

    now = datetime.now()
    s   = ds(now - timedelta(days=days));    e  = ds(now)
    ps  = ds(now - timedelta(days=days*2));  pe = ds(now - timedelta(days=days+1))
    pl_ko  = PERIOD_KO[days]
    pl_sub = PERIOD_SUB[days]

    def api_get(url):
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {PINTEREST_TOKEN}")
        with urllib.request.urlopen(req) as res:
            return json.loads(res.read().decode("utf-8"))

    def get_summary(st, en):
        url = (f"https://api.pinterest.com/v5/user_account/analytics"
               f"?start_date={st}&end_date={en}"
               f"&metric_types=IMPRESSION,OUTBOUND_CLICK,SAVE")
        m = api_get(url).get("all", {}).get("summary_metrics", {})
        imp   = int(m.get("IMPRESSION", 0))
        click = int(m.get("OUTBOUND_CLICK", 0))
        save  = int(m.get("SAVE", 0))
        ctr   = round(click / imp * 100, 2) if imp else 0.0
        return imp, click, save, ctr

    try:
        imp,   click,   save,   ctr   = get_summary(s, e)
        p_imp, p_click, p_save, p_ctr = get_summary(ps, pe)
    except Exception as ex:
        print(f"  ⚠️  Pinterest {pl_ko} summary 실패: {ex}")
        return None

    # 분기·연간은 직전 비교 의미가 낮으므로 null 처리 (기존 데이터 패턴 유지)
    has_delta = days <= 30
    no_delta  = {"prevNote": "누적 중", "yoyNote": "누적 중"}

    kpi = [
        {"label": "노출", "value": fmt_num(imp), "sub": pl_sub,
         "prev": pct_diff(imp, p_imp) if has_delta else None, "yoy": None,
         "note": f"{pl_ko} 전체 핀 노출.",
         **({}  if has_delta else no_delta)},
        {"label": "사이트 클릭", "value": fmt_num(click), "sub": "Pin → Site",
         "prev": pct_diff(click, p_click) if has_delta else None, "yoy": None,
         "note": "핀에서 사이트로 유입된 클릭.",
         **({} if has_delta else no_delta)},
        {"label": "저장", "value": fmt_num(save), "sub": "Save 액션",
         "prev": pct_diff(save, p_save) if has_delta else None, "yoy": None,
         "note": "핀 저장 수 · 콘텐츠 매력도 지표.",
         **({} if has_delta else no_delta)},
        {"label": "CTR", "value": str(ctr), "unit": "%", "sub": "클릭률",
         "prev": round(ctr - p_ctr, 2) if has_delta else None, "yoy": None,
         "note": "노출 대비 클릭률.", "deltaUnit": "pp",
         **({} if has_delta else no_delta)},
    ]

    # Top 10 핀
    top = []
    try:
        pins_url = (f"https://api.pinterest.com/v5/user_account/analytics/top_pins"
                    f"?start_date={s}&end_date={e}"
                    f"&sort_by=IMPRESSION&num_of_pins=10")
        for i, pin in enumerate(api_get(pins_url).get("pins", [])[:10]):
            pin_id  = pin.get("pin_id", "")
            metrics = pin.get("metrics", {})
            title   = f"핀 #{pin_id[-6:]}"
            try:
                detail = api_get(f"https://api.pinterest.com/v5/pins/{pin_id}")
                title  = (detail.get("title") or
                          detail.get("description", "")[:40] or title)
            except Exception:
                pass
            top.append({
                "rank":  i + 1,
                "title": str(title)[:40],
                "imp":   int(metrics.get("IMPRESSION", 0)),
                "save":  int(metrics.get("SAVE", 0)),
                "url":   f"https://www.pinterest.com/pin/{pin_id}/",
            })
    except Exception as ex:
        print(f"  ⚠️  Pinterest {pl_ko} top pins 실패: {ex}")

    print(f"  ✅ Pinterest {pl_ko}: 노출 {imp:,} · 저장 {save:,}")
    return {"kpi": kpi, "top": top}


# ── Instagram 구조 변환 ───────────────────────────────────────
def format_ig(ig_all: dict, period_key: str, pl_ko: str) -> dict:
    d         = ig_all.get(period_key, {})
    followers = ig_all.get("followers", 0)
    eng_rate  = ig_all.get("eng_rate", 0)
    today_str = datetime.now().strftime("%y.%m.%d")

    kpi = [
        {"label": "팔로워", "value": fmt_num(followers),
         "sub": f"{today_str} 기준",
         "note": "@amorepacific_creatives 팔로워 현황."},
        {"label": "도달 (Reach)", "value": fmt_num(d.get("reach", 0)),
         "sub": f"{pl_ko} 고유 도달",
         "note": "기간 내 1회 이상 노출된 고유 계정 수."},
        {"label": "총 상호작용", "value": fmt_num(d.get("total_interactions", 0)),
         "sub": f"{pl_ko} 합산",
         "note": "좋아요 + 댓글 + 저장 + 공유 합산."},
        {"label": "참여율", "value": str(eng_rate), "unit": "%",
         "sub": "최근 20개 게시물",
         "note": "(좋아요 + 댓글) ÷ 팔로워 × 100"},
    ]
    top = [
        {"title":    p.get("caption", "")[:40].rstrip(),
         "url":      p.get("url", ""),
         "likes":    p.get("likes", 0),
         "saves":    p.get("saves", 0),
         "comments": p.get("comments", 0)}
        for p in d.get("top10", [])
    ]
    return {"kpi": kpi, "top": top, "growth": d.get("growth")}


# ── index.html DATA 블록 교체 ─────────────────────────────────
def find_data_bounds(html: str):
    """'const DATA = {' ~ 대응 '};' 범위 반환"""
    marker = "const DATA = {"
    start  = html.index(marker)
    brace_start = start + len(marker) - 1   # '{' 위치
    depth = 0
    for i, ch in enumerate(html[brace_start:], brace_start):
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                end = i + 1
                if end < len(html) and html[end] == ';':
                    end += 1
                return start, end
    raise ValueError("const DATA 블록의 끝을 찾을 수 없습니다.")

def update_index_html(data: dict) -> str:
    html_path = BASE_DIR / "index.html"
    html = html_path.read_text(encoding="utf-8")

    # DATA 블록 통째로 교체
    data_js = "const DATA = " + json.dumps(data, ensure_ascii=False, indent=2) + ";"
    s_idx, e_idx = find_data_bounds(html)
    html = html[:s_idx] + data_js + html[e_idx:]

    # 날짜 헤더 갱신
    today     = datetime.now()
    date_lbl  = today.strftime("%y.%m.%d")
    wk_num    = int(today.strftime("%W"))
    day_short = today.strftime("%a").upper()

    html = re.sub(r'<strong>\d{2}\.\d{2}\.\d{2}</strong>',
                  f'<strong>{date_lbl}</strong>', html)
    html = re.sub(r'WK \d+ · \w+',
                  f'WK {wk_num} · {day_short}', html)

    html_path.write_text(html, encoding="utf-8")
    print(f"  ✅ index.html 갱신 완료 ({len(html):,} bytes)")
    return html


# ── 메인 ──────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 56)
    print("  AC 대시보드 전체 자동 업데이트")
    print(f"  {datetime.now().strftime('%Y.%m.%d %H:%M')}")
    print("=" * 56)

    # 1. GA4
    print("\n[1/3] GA4 데이터 수집 (4기간)...")
    client = init_ga4_client()
    ga4 = {}
    for key, days in [("week", 7), ("month", 30), ("quarter", 90), ("year", 365)]:
        try:
            ga4[key] = collect_ga4_period(client, days)
        except Exception as ex:
            print(f"  ❌ GA4 {key} 실패: {ex}")
            ga4[key] = {}

    # 2. Pinterest
    print("\n[2/3] Pinterest 데이터 수집 (4기간)...")
    pin = {}
    for key, days in [("week", 7), ("month", 30), ("quarter", 90), ("year", 365)]:
        try:
            pin[key] = collect_pinterest_period(days) or {"kpi": [], "top": []}
        except Exception as ex:
            print(f"  ❌ Pinterest {key} 실패: {ex}")
            pin[key] = {"kpi": [], "top": []}

    # 3. Instagram
    print("\n[3/3] Instagram 데이터 수집 (4기간)...")
    ig = {}
    try:
        ig = collect_instagram_data()
        if not ig.get("available"):
            print(f"  ⚠️  Instagram 실패: {ig.get('error', 'unknown')}")
            ig = {}
    except Exception as ex:
        print(f"  ❌ Instagram 예외: {ex}")

    # DATA 조립
    print("\n데이터 조립 중...")
    PERIODS = [("week", 7, "주간"), ("month", 30, "월간"),
               ("quarter", 90, "분기"), ("year", 365, "연간")]
    DATA = {}
    for key, days, ko in PERIODS:
        DATA[key] = {
            **ga4.get(key, {}),
            "pin": pin.get(key, {"kpi": [], "top": []}),
            "ig":  format_ig(ig, key, ko) if ig.get("available") else {},
        }

    # 수집 결과 점검 — 세 소스가 전부 비면 빈 대시보드를 덮어쓰게 되므로 중단한다.
    healthy = []
    if any(ga4.get(k) for k, _, _ in PERIODS):
        healthy.append("GA4")
    if any(pin.get(k, {}).get("kpi") for k, _, _ in PERIODS):
        healthy.append("Pinterest")
    if ig.get("available"):
        healthy.append("Instagram")

    print(f"\n수집 성공: {', '.join(healthy) if healthy else '없음'} ({len(healthy)}/3)")
    if not healthy:
        print("  ❌ 세 소스 모두 실패 — 빈 데이터 배포를 막기 위해 중단합니다.")
        sys.exit(1)

    # index.html 갱신
    print("\nindex.html 갱신 중...")
    html = update_index_html(DATA)

    # 해석 문구 자동 생성 — 실패해도 데이터 갱신은 계속된다.
    print("")
    print("읽기·의미·그래서 문구 생성 중...")
    try:
        from generate_notes import refresh_notes
        new_html = refresh_notes(html, DATA)
        if new_html != html:
            html = new_html
            (BASE_DIR / "index.html").write_text(html, encoding="utf-8")
    except Exception as ex:
        print(f"  ❌ 문구 생성 모듈 오류 — 기존 문구 유지: {type(ex).__name__}: {ex}")

    # GitHub 배포
    print("\nGitHub 배포 중...")
    date_str = datetime.now().strftime("%Y.%m.%d")
    ok = upload_to_github(html, "index.html",
                          f"dashboard: auto-update {date_str}")
    if ok:
        print("  ✅ 배포 완료!")
        print("  🔗 https://goooodmin.github.io/ac-dashboard/")
    else:
        print("  ❌ GitHub 배포 실패 (로컬 파일은 갱신됨)")

    print("\n" + "=" * 56)
    print("  완료!")
    print("=" * 56)

    # 배포 실패 시 워크플로우도 실패시킨다.
    # 예전에는 여기서 조용히 끝나서, Actions는 초록불인데 대시보드만 멈춰 있었다.
    if not ok:
        sys.exit(1)
