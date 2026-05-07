"""
AC 사이트 주간 대시보드 자동화
design.amorepacific.com 운영 현황 수집 → HTML 생성 → GitHub 업로드
"""

import os
import sys
import re
import json
import base64
import http.client
import ssl
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

# Windows 터미널 UTF-8 출력 설정
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from dotenv import load_dotenv
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Metric, Dimension, RunReportRequest,
    FilterExpression, Filter
)

# ── 환경변수 로드 ─────────────────────────────────────────────
load_dotenv()

PROPERTY_ID         = os.getenv("GA4_PROPERTY_ID", "395669678")
SERVICE_ACCOUNT_JSON = os.getenv("GA4_SERVICE_ACCOUNT_JSON", "service_account.json")
PINTEREST_TOKEN     = os.getenv("PINTEREST_TOKEN")
GITHUB_TOKEN        = os.getenv("GITHUB_TOKEN")
GITHUB_USERNAME     = os.getenv("GITHUB_USERNAME", "goooodmin")
GITHUB_REPO         = os.getenv("GITHUB_REPO", "ac-dashboard")

BASE_DIR     = Path(__file__).parent
DATA_DIR     = BASE_DIR / "data"
OUTPUT_DIR   = BASE_DIR / "output"
HISTORY_FILE = DATA_DIR / "ac_dashboard_history.json"

DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


# ── GA4 클라이언트 초기화 ────────────────────────────────────
def init_ga4_client():
    json_path = BASE_DIR / SERVICE_ACCOUNT_JSON
    credentials = service_account.Credentials.from_service_account_file(
        str(json_path),
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)


# ── GA4 리포트 실행 ──────────────────────────────────────────
def run_report(client, metrics=[], dimensions=[], filters=None, limit=10):
    req = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
        metrics=[Metric(name=m) for m in metrics],
        dimensions=[Dimension(name=d) for d in dimensions],
        limit=limit,
    )
    if filters:
        req.dimension_filter = filters
    return client.run_report(req)


# ── GA4 데이터 수집 ──────────────────────────────────────────
def collect_ga4_data(client):
    print("📊 GA4 데이터 수집 중...")

    # KPI
    kpi = run_report(client, metrics=["activeUsers", "sessions", "averageSessionDuration", "newUsers"])
    kv = kpi.rows[0].metric_values
    total_users = int(kv[0].value)
    avg_dur = float(kv[2].value)
    new_users = int(kv[3].value)
    ret_users = total_users - new_users
    avg_min, avg_sec = int(avg_dur // 60), int(avg_dur % 60)
    ret_pct = round(ret_users / total_users * 100, 1)
    new_pct = round(new_users / total_users * 100, 1)

    # TOP 프로젝트 (필터링)
    top = run_report(
        client,
        metrics=["screenPageViews", "averageSessionDuration"],
        dimensions=["pagePath", "pageTitle"],
        limit=50
    )

    SKIP = [
        "/work/page/", "/en/work/page/", "/en/work/",
        "/about", "/en/about", "/ci", "/en/ci",
        "/now", "/en/now", "/en/",
        "/work/2024/aritaum/template"
    ]

    content_pages = []
    for row in top.rows:
        path = row.dimension_values[0].value
        title = row.dimension_values[1].value
        parts = [p for p in path.split("/") if p]
        if len(parts) <= 1:
            continue
        if any(path.startswith(s) for s in SKIP):
            continue
        views = int(row.metric_values[0].value)
        dur = float(row.metric_values[1].value)
        m, s = int(dur // 60), int(dur % 60)
        t = title.split(" - ")[1] if " - " in title else title
        t = re.sub(r'\s*\([^)]*\)', '', t).strip()
        if re.match(r'^[a-zA-Z0-9\s\-]+$', t):
            t = f"{parts[1]} · {parts[2]}" if len(parts) >= 3 else path
        content_pages.append({
            "path": path, "title": t,
            "views": views, "dur": f"{m}분{s}초"
        })
        if len(content_pages) >= 10:
            break

    # 유입 소스 TOP 10
    src = run_report(
        client,
        metrics=["sessions", "averageSessionDuration"],
        dimensions=["sessionSource"],
        limit=10
    )

    SOURCE_LABELS = {
        "google": "Google 검색", "pinterest.com": "Pinterest",
        "instagram.com": "Instagram", "(direct)": "Direct (직접 입력)",
        "naver": "Naver 검색", "facebook.com": "Facebook",
        "t.co": "Twitter/X", "youtube.com": "YouTube",
        "bing": "Bing 검색", "daum": "Daum 검색",
    }
    SOURCE_MEANING = {
        "google": "검색해서 들어온 목적성 유입",
        "pinterest.com": "Pinterest에서 이미지 보고 유입",
        "instagram.com": "AC 공식 채널 팔로워 유입",
        "(direct)": "팬·업계 지인의 직접 방문",
        "naver": "국내 일반 검색 유입",
        "facebook.com": "Facebook 유입",
        "youtube.com": "영상 콘텐츠 유입",
    }

    sources = []
    for row in src.rows:
        dur = float(row.metric_values[1].value)
        m, s = int(dur // 60), int(dur % 60)
        raw = row.dimension_values[0].value
        sources.append({
            "channel": SOURCE_LABELS.get(raw, raw),
            "raw": raw,
            "sessions": int(row.metric_values[0].value),
            "dur": f"{m}분{s}초",
            "meaning": SOURCE_MEANING.get(raw, "기타 유입")
        })

    # 국가별 TOP 10
    FLAGS = {
        "South Korea": "🇰🇷", "United States": "🇺🇸", "Japan": "🇯🇵",
        "China": "🇨🇳", "France": "🇫🇷", "United Kingdom": "🇬🇧",
        "Singapore": "🇸🇬", "Taiwan": "🇹🇼", "Vietnam": "🇻🇳",
        "Germany": "🇩🇪", "Australia": "🇦🇺", "Canada": "🇨🇦",
        "Thailand": "🇹🇭", "Hong Kong": "🇭🇰", "Indonesia": "🇮🇩"
    }

    ctry = run_report(
        client,
        metrics=["activeUsers", "averageSessionDuration"],
        dimensions=["country"],
        limit=10
    )
    countries = []
    for row in ctry.rows:
        dur = float(row.metric_values[1].value)
        m, s = int(dur // 60), int(dur % 60)
        c = row.dimension_values[0].value
        countries.append({
            "country": c, "flag": FLAGS.get(c, "🌐"),
            "users": int(row.metric_values[0].value), "dur": f"{m}분{s}초"
        })

    # 신규/재방문 (국문 vs 영문)
    def get_nvr(path_prefix):
        f = FilterExpression(filter=Filter(
            field_name="pagePath",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.BEGINS_WITH,
                value=path_prefix
            )
        ))
        res = run_report(client, metrics=["activeUsers"],
                         dimensions=["newVsReturning"], filters=f, limit=5)
        nw, rt = 0, 0
        for row in res.rows:
            u = int(row.metric_values[0].value)
            if row.dimension_values[0].value == "new":
                nw = u
            else:
                rt = u
        total = nw + rt
        return {
            "new": nw, "ret": rt, "total": total,
            "new_pct": round(nw / total * 100, 1) if total > 0 else 0,
            "ret_pct": round(rt / total * 100, 1) if total > 0 else 0
        }

    ko = get_nvr("/")
    en = get_nvr("/en/")

    print(f"  ✅ 방문 {total_users:,}명 | 신규 {new_pct}% | 평균 {avg_min}분{avg_sec}초")
    return {
        "total_users": total_users, "avg_min": avg_min, "avg_sec": avg_sec,
        "new_users": new_users, "ret_pct": ret_pct, "new_pct": new_pct,
        "content_pages": content_pages, "sources": sources,
        "countries": countries, "ko": ko, "en": en,
    }


# ── Pinterest 데이터 수집 ────────────────────────────────────
def collect_pinterest_data():
    if not PINTEREST_TOKEN:
        print("  ⚠️  PINTEREST_TOKEN 없음 — 건너뜀")
        return {"available": False}

    print("📌 Pinterest 데이터 수집 중...")
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        def api_get(url):
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {PINTEREST_TOKEN}")
            with urllib.request.urlopen(req) as res:
                return json.loads(res.read().decode("utf-8"))

        # 계정 전체 지표
        summary_url = (
            f"https://api.pinterest.com/v5/user_account/analytics"
            f"?start_date={start_date}&end_date={end_date}"
            f"&metric_types=IMPRESSION,OUTBOUND_CLICK,SAVE,PIN_CLICK"
        )
        data = api_get(summary_url)
        summary = data.get("all", {}).get("summary_metrics", {})
        impressions = int(summary.get("IMPRESSION", 0))
        clicks = int(summary.get("OUTBOUND_CLICK", 0))
        saves = int(summary.get("SAVE", 0))
        pin_clicks = int(summary.get("PIN_CLICK", 0))
        ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0

        # TOP 5 핀
        pins_url = (
            f"https://api.pinterest.com/v5/user_account/analytics/top_pins"
            f"?start_date={start_date}&end_date={end_date}"
            f"&sort_by=IMPRESSION&num_of_pins=10"
        )
        pins_data = api_get(pins_url)

        top_pins = []
        for pin in pins_data.get("pins", [])[:10]:
            pin_id = pin.get("pin_id", "")
            metrics = pin.get("metrics", {})
            title = "제목 없음"
            link = ""
            try:
                detail = api_get(f"https://api.pinterest.com/v5/pins/{pin_id}")
                title = (
                    detail.get("title")
                    or detail.get("description", "")[:40]
                    or f"핀 #{pin_id[-6:]}"
                )
                link = detail.get("link", "")
            except Exception:
                title = f"핀 #{pin_id[-6:]}"

            top_pins.append({
                "pin_id": pin_id,
                "title": title[:40],
                "link": link,
                "impressions": int(metrics.get("IMPRESSION", 0)),
                "saves": int(metrics.get("SAVE", 0)),
                "clicks": int(metrics.get("OUTBOUND_CLICK", 0)),
                "pin_clicks": int(metrics.get("PIN_CLICK", 0)),
            })

        print(f"  ✅ 노출 {impressions:,} | 저장 {saves:,} | CTR {ctr}%")
        return {
            "available": True,
            "impressions": impressions, "clicks": clicks,
            "saves": saves, "pin_clicks": pin_clicks,
            "ctr": ctr, "top_pins": top_pins,
        }
    except Exception as e:
        print(f"  ❌ Pinterest 오류: {e}")
        return {"available": False, "error": str(e)}


# ── Instagram 데이터 수집 ────────────────────────────────────
def collect_instagram_data():
    INSTAGRAM_TOKEN      = os.getenv("INSTAGRAM_TOKEN")
    INSTAGRAM_ACCOUNT_ID = os.getenv("INSTAGRAM_ACCOUNT_ID")

    if not INSTAGRAM_TOKEN or not INSTAGRAM_ACCOUNT_ID:
        print("  ⚠️  INSTAGRAM_TOKEN/ACCOUNT_ID 없음 — 건너뜀")
        return {"available": False}

    print("📸 Instagram 데이터 수집 중...")

    BASE_IG = "https://graph.facebook.com/v25.0"

    def ig_get(path, params=""):
        url = f"{BASE_IG}/{path}?access_token={INSTAGRAM_TOKEN}{params}"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"   HTTP {e.code}: {body[:120]}")
            return {}

    THIRTY_DAYS_AGO = int((datetime.now() - timedelta(days=31)).timestamp())

    def collect_chunk(since, until):
        """단일 청크(최대 28일) 인사이트 수집"""
        ins_reach = ig_get(
            f"{INSTAGRAM_ACCOUNT_ID}/insights",
            f"&metric=reach&period=day&since={since}&until={until}"
        )
        reach = 0
        for item in ins_reach.get("data", []):
            if item.get("name") == "reach":
                reach = sum(v.get("value", 0) for v in item.get("values", []))

        ins_total = ig_get(
            f"{INSTAGRAM_ACCOUNT_ID}/insights",
            f"&metric=total_interactions,accounts_engaged"
            f"&metric_type=total_value&period=day&since={since}&until={until}"
        )
        ti, ae = 0, 0
        for item in ins_total.get("data", []):
            name = item.get("name", "")
            tv   = item.get("total_value", {}).get("value", 0)
            vs   = sum(v.get("value", 0) for v in item.get("values", []))
            val  = tv or vs
            if name == "total_interactions": ti = val
            elif name == "accounts_engaged": ae = val

        # follower_count는 최근 30일 이내 청크만 수집
        growth_raw = []
        if since >= THIRTY_DAYS_AGO:
            fc_data = ig_get(
                f"{INSTAGRAM_ACCOUNT_ID}/insights",
                f"&metric=follower_count&period=day&since={since}&until={until}"
            )
            for item in fc_data.get("data", []):
                if item.get("name") == "follower_count":
                    for v in item.get("values", []):
                        growth_raw.append({
                            "date":  v.get("end_time", "")[:10],
                            "delta": v.get("value", 0),
                        })
        return reach, ti, ae, growth_raw

    def collect_period(days):
        """기간별 인사이트 수집 — 30일 초과 시 청크 분할"""
        end_dt   = datetime.now()
        start_dt = end_dt - timedelta(days=days)
        chunk_days = 28  # 안전 마진

        reach_total = 0
        ti_total    = 0
        ae_total    = 0
        growth_all  = []

        cursor = start_dt
        while cursor < end_dt:
            chunk_end = min(cursor + timedelta(days=chunk_days), end_dt)
            s = int(cursor.timestamp())
            u = int(chunk_end.timestamp())
            r, ti, ae, gr = collect_chunk(s, u)
            reach_total += r
            ti_total    += ti
            ae_total    += ae
            growth_all.extend(gr)
            cursor = chunk_end

        # 중복 날짜 제거 (날짜 오름차순 정렬)
        seen = set()
        growth_dedup = []
        for g in sorted(growth_all, key=lambda x: x["date"]):
            if g["date"] not in seen:
                seen.add(g["date"])
                growth_dedup.append(g)

        return reach_total, ti_total, ae_total, growth_dedup

    try:
        # 1. 기본 계정 정보
        info = ig_get(
            INSTAGRAM_ACCOUNT_ID,
            "&fields=username,followers_count,media_count"
        )
        followers = info.get("followers_count", 0)
        username  = info.get("username", "amorepacific_creatives")

        # 2. 4기간 인사이트 수집
        periods = {"week": 7, "month": 30, "quarter": 90, "year": 365}
        period_data = {}
        for pname, days in periods.items():
            print(f"   [{pname}] {days}일 수집 중...")
            r, ti, ae, graw = collect_period(days)
            period_data[pname] = {
                "reach": r, "total_interactions": ti,
                "accounts_engaged": ae, "growth_raw": graw,
            }

        # 3. TOP 미디어 (최근 50개 중 좋아요+댓글 상위)
        media = ig_get(
            f"{INSTAGRAM_ACCOUNT_ID}/media",
            "&fields=id,caption,like_count,comments_count,timestamp,permalink&limit=50"
        )
        posts = []
        for m in media.get("data", []):
            likes    = m.get("like_count", 0)
            comments = m.get("comments_count", 0)
            caption  = (m.get("caption") or "")[:60].replace("\n", " ").strip()
            posts.append({
                "id": m.get("id"), "caption": caption,
                "likes": likes, "comments": comments,
                "eng": likes + comments,
                "date": m.get("timestamp", "")[:10],
                "url": m.get("permalink", ""),
            })
        posts.sort(key=lambda x: x["eng"], reverse=True)

        # 기간별 TOP 5 (해당 기간 내 게시물 필터링)
        def top5_for(days):
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            filtered = [p for p in posts if p["date"] >= cutoff] or posts
            top = filtered[:10]
            # 저장 수 추가
            for p in top:
                if "saves" not in p:
                    sd = ig_get(f"{p['id']}/insights", "&metric=saved")
                    for item in sd.get("data", []):
                        if item.get("name") == "saved":
                            vals = item.get("values", [])
                            p["saves"] = vals[-1].get("value", 0) if vals else 0
                    if "saves" not in p:
                        p["saves"] = 0
            return top

        # 4. 참여율 계산
        avg_eng  = sum(p["eng"] for p in posts[:20]) / min(len(posts), 20) if posts else 0
        eng_rate = round(avg_eng / followers * 100, 2) if followers > 0 else 0

        # 5. 성장 차트 데이터 구성 (cumulative 팔로워 수)
        def build_growth_chart(growth_raw, current_followers):
            if not growth_raw:
                return None
            total_delta = sum(g["delta"] for g in growth_raw)
            base = current_followers - total_delta
            labels, values = [], []
            cumul = base
            for g in growth_raw:
                cumul += g["delta"]
                # 날짜 포맷: MM.DD
                labels.append(g["date"][5:].replace("-", "."))
                values.append(cumul)
            return {"labels": labels, "values": values}

        print(f"  ✅ @{username} | 팔로워 {followers:,} | "
              f"주간 도달 {period_data['week']['reach']:,} | "
              f"월간 상호작용 {period_data['month']['total_interactions']:,}")

        return {
            "available": True,
            "username":  username,
            "followers": followers,
            "eng_rate":  eng_rate,
            "week": {
                "reach":              period_data["week"]["reach"],
                "total_interactions": period_data["week"]["total_interactions"],
                "accounts_engaged":   period_data["week"]["accounts_engaged"],
                "top10":  top5_for(7),
                "growth": build_growth_chart(period_data["week"]["growth_raw"], followers),
            },
            "month": {
                "reach":              period_data["month"]["reach"],
                "total_interactions": period_data["month"]["total_interactions"],
                "accounts_engaged":   period_data["month"]["accounts_engaged"],
                "top10":  top5_for(30),
                "growth": build_growth_chart(period_data["month"]["growth_raw"], followers),
            },
            "quarter": {
                "reach":              period_data["quarter"]["reach"],
                "total_interactions": period_data["quarter"]["total_interactions"],
                "accounts_engaged":   period_data["quarter"]["accounts_engaged"],
                "top10":  top5_for(90),
                "growth": build_growth_chart(period_data["quarter"]["growth_raw"], followers),
            },
            "year": {
                "reach":              period_data["year"]["reach"],
                "total_interactions": period_data["year"]["total_interactions"],
                "accounts_engaged":   period_data["year"]["accounts_engaged"],
                "top10":  top5_for(365),
                "growth": build_growth_chart(period_data["year"]["growth_raw"], followers),
            },
        }

    except Exception as e:
        import traceback
        print(f"  ❌ Instagram 오류: {e}")
        traceback.print_exc()
        return {"available": False, "error": str(e)}


# ── HTML 생성 ────────────────────────────────────────────────
COMMON_STYLE = """
body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#fafafa;}
.card{background:#fff;border:0.5px solid #ddd;border-radius:12px;padding:1.25rem;margin-bottom:14px;}
.kpi{background:#f5f5f5;border-radius:8px;padding:1rem;}
.kpi-label{font-size:12px;color:#888;}
.kpi-value{font-size:26px;font-weight:500;margin-top:4px;}
.kpi-sub{font-size:12px;color:#888;margin-top:2px;}
.card-title{font-size:14px;font-weight:500;margin-bottom:4px;}
.card-sub{font-size:12px;color:#888;margin-bottom:14px;}
.insight{margin-top:14px;padding:10px 12px;background:#f5f5f5;border-radius:8px;font-size:12px;line-height:1.7;}
.il{color:#888;}
.nvr-bar{display:flex;gap:2px;height:26px;border-radius:8px;overflow:hidden;margin-bottom:6px;}
.nvr-seg{display:flex;align-items:center;justify-content:center;color:white;font-size:11px;font-weight:500;}
table{width:100%;font-size:13px;border-collapse:collapse;}
th{font-weight:400;color:#888;font-size:12px;padding:6px 0;text-align:left;}
.footer{font-size:11px;color:#aaa;text-align:center;margin-top:0.5rem;}
@media print{body{background:white;padding:0;}.card{break-inside:avoid;}}
"""


def build_dashboard(d, layout="vertical"):
    max_views = d["content_pages"][0]["views"] if d["content_pages"] else 1
    colors = ["#534AB7"] * 3 + ["#7F77DD"] * 3 + ["#AFA9EC"] * 3 + ["#C8C4F0"]
    base_url = "https://design.amorepacific.com"
    ko, en = d["ko"], d["en"]
    p = d.get("pinterest", {})

    # TOP 프로젝트
    pages_html = ""
    for i, pg in enumerate(d["content_pages"]):
        pct = round(pg["views"] / max_views * 100)
        pages_html += f"""
        <div style="margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:2px;gap:8px;">
            <a href="{base_url}{pg['path']}" target="_blank"
               style="color:#333;text-decoration:none;border-bottom:1px solid #ddd;">{i+1}. {pg['title']}</a>
            <span style="color:#888;font-size:12px;white-space:nowrap;">{pg['views']}뷰 · {pg['dur']}</span>
          </div>
          <div style="height:6px;background:#eee;border-radius:3px;overflow:hidden;margin-top:4px;">
            <div style="width:{pct}%;height:100%;background:{colors[i]};"></div>
          </div>
        </div>"""

    # 유입 소스
    sources_html = "".join([f"""
        <tr style="border-top:0.5px solid #eee;">
          <td style="padding:8px 0;">{s['channel']}</td>
          <td style="text-align:right;">{s['sessions']:,}</td>
          <td style="text-align:right;">{s['dur']}</td>
          <td style="padding-left:16px;color:#888;font-size:12px;">{s['meaning']}</td>
        </tr>""" for s in d["sources"]])

    # 국가별
    countries_html = "".join([f"""
        <tr style="border-top:0.5px solid #eee;">
          <td style="padding:7px 0;font-size:12px;">{c['flag']} {c['country']}</td>
          <td style="text-align:right;font-size:12px;">{c['users']:,}</td>
          <td style="text-align:right;color:#888;font-size:12px;">{round(c['users']/d['total_users']*100,1)}%</td>
          <td style="text-align:right;color:#1D9E75;font-size:12px;">{c['dur']}</td>
        </tr>""" for c in d["countries"]])

    kpi_block = f"""
    <div class="kpi-grid">
      <div class="kpi"><div class="kpi-label">주간 방문</div>
        <div class="kpi-value">{d['total_users']:,}</div></div>
      <div class="kpi"><div class="kpi-label">평균 체류시간</div>
        <div class="kpi-value">{d['avg_min']}분 {d['avg_sec']}초</div></div>
      <div class="kpi"><div class="kpi-label">신규 방문자</div>
        <div class="kpi-value">{d['new_users']:,}</div>
        <div class="kpi-sub">전체의 {d['new_pct']}%</div></div>
      <div class="kpi"><div class="kpi-label">재방문 비율</div>
        <div class="kpi-value">{d['ret_pct']}%</div></div>
    </div>"""

    top_block = f"""
    <div class="card">
      <div class="card-title">이번 주 TOP 프로젝트</div>
      <div class="card-sub">실 콘텐츠 페이지 기준 · 클릭하면 해당 페이지로 이동</div>
      {pages_html}
      <div class="insight">
        <span class="il">지표 읽기 · </span>뷰와 체류가 함께 높은 콘텐츠가 진짜 읽히는 콘텐츠.<br>
        <span class="il">왜 의미 · </span>뷰 많아도 체류 짧으면 유입 후 이탈. 뷰×체류 함께 봐야 함.<br>
        <span class="il">그래서 · </span>뷰×체류 기준으로 콘텐츠 품질 이원화해 운영 전략 분리 가능.
      </div>
    </div>"""

    source_block = f"""
    <div class="card">
      <div class="card-title">유입 소스별 분해</div>
      <div class="card-sub">어디서 왔는지 × 얼마나 깊이 보는지</div>
      <table><thead><tr>
        <th style="width:28%;">소스</th>
        <th style="text-align:right;width:15%;">세션</th>
        <th style="text-align:right;width:15%;">체류</th>
        <th style="padding-left:16px;">데이터의 의미</th>
      </tr></thead><tbody>{sources_html}</tbody></table>
      <div class="insight">
        <span class="il">지표 읽기 · </span>Google 검색이 유입 1위. Pinterest는 노출 대비 사이트 유입 전환 낮음.<br>
        <span class="il">왜 의미 · </span>Pinterest는 저장·탐색 채널. Direct·Referral이 깊은 독자를 데려옴.<br>
        <span class="il">그래서 · </span>Pinterest는 인지 확대용으로 유지, 깊은 독자는 Referral 기획에 투자.
      </div>
    </div>"""

    country_block = f"""
    <div class="card" style="margin-bottom:0;">
      <div class="card-title">국가별 방문 · 체류시간</div>
      <div class="card-sub">누가, 얼마나 깊이 보는가</div>
      <table><thead><tr>
        <th>국가</th><th style="text-align:right;">방문</th>
        <th style="text-align:right;">비중</th><th style="text-align:right;">체류</th>
      </tr></thead><tbody>{countries_html}</tbody></table>
      <div class="insight">
        <span class="il">지표 읽기 · </span>한국이 압도적 1위. 해외는 중국·미국·싱가포르 순.<br>
        <span class="il">왜 의미 · </span>해외 독자가 적어도 체류시간이 길어 깊이 읽는 패턴.<br>
        <span class="il">그래서 · </span>영문 콘텐츠 강화 시 글로벌 체류 품질 유지 가능성 높음.
      </div>
    </div>"""

    nvr_block = f"""
    <div class="card" style="margin-bottom:0;">
      <div class="card-title">신규 vs 재방문</div>
      <div class="card-sub">국문(/) vs 영문(/en/)</div>
      <div style="margin-bottom:14px;">
        <div style="display:flex;justify-content:space-between;font-size:12px;color:#888;margin-bottom:6px;">
          <span style="color:#111;font-weight:500;">국문 사이트</span><span>총 {ko['total']:,}명</span>
        </div>
        <div class="nvr-bar">
          <div class="nvr-seg" style="flex:{ko['new_pct']};background:#378ADD;">{ko['new_pct']}%</div>
          <div class="nvr-seg" style="flex:{ko['ret_pct']};background:#185FA5;">{ko['ret_pct']}%</div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:11px;color:#888;">
          <span>신규 {ko['new']:,}명</span><span>재방문 {ko['ret']:,}명</span>
        </div>
      </div>
      <div>
        <div style="display:flex;justify-content:space-between;font-size:12px;color:#888;margin-bottom:6px;">
          <span style="color:#111;font-weight:500;">영문 사이트</span><span>총 {en['total']:,}명</span>
        </div>
        <div class="nvr-bar">
          <div class="nvr-seg" style="flex:{en['new_pct']};background:#1D9E75;">{en['new_pct']}%</div>
          <div class="nvr-seg" style="flex:{en['ret_pct']};background:#0F6E56;">{en['ret_pct']}%</div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:11px;color:#888;">
          <span>신규 {en['new']:,}명</span><span>재방문 {en['ret']:,}명</span>
        </div>
      </div>
      <div class="insight">
        <span class="il">지표 읽기 · </span>영문 신규 {en['new_pct']}%, 국문 신규 {ko['new_pct']}%.<br>
        <span class="il">왜 의미 · </span>영문은 새로운 해외 독자의 첫 발견 채널.<br>
        <span class="il">그래서 · </span>영문 콘텐츠 확충 시 글로벌 독자 확보 효과 기대.
      </div>
    </div>"""

    # Pinterest 블록
    if p.get("available"):
        saves_per_click = round(p['saves'] / p['clicks']) if p.get('clicks', 0) > 0 else 0
        top_pins_html = "".join([f"""
            <div style="display:flex;justify-content:space-between;font-size:13px;padding:8px 0;border-top:0.5px solid #eee;">
              <span style="color:#333;">{i}. {pin.get('title','제목 없음')}</span>
              <span style="color:#888;font-size:12px;white-space:nowrap;">노출 {pin.get('impressions',0):,} · 저장 {pin.get('saves',0):,}</span>
            </div>"""
            for i, pin in enumerate(p.get("top_pins", [])[:5], 1)
        ])
        pinterest_block = f"""
        <div class="card">
          <div class="card-title">Pinterest 인사이트</div>
          <div class="card-sub">지난 7일 · 계정 전체 기준</div>
          <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px;">
            <div class="kpi"><div class="kpi-label">노출</div>
              <div style="font-size:20px;font-weight:500;margin-top:4px;">{p['impressions']:,}</div></div>
            <div class="kpi"><div class="kpi-label">사이트 클릭</div>
              <div style="font-size:20px;font-weight:500;margin-top:4px;">{p['clicks']:,}</div></div>
            <div class="kpi"><div class="kpi-label">저장</div>
              <div style="font-size:20px;font-weight:500;margin-top:4px;">{p['saves']:,}</div></div>
            <div class="kpi"><div class="kpi-label">CTR</div>
              <div style="font-size:20px;font-weight:500;margin-top:4px;">{p['ctr']}%</div></div>
          </div>
          {f'<div style="font-size:13px;font-weight:500;margin-bottom:8px;">이번 주 TOP 핀</div>{top_pins_html}' if top_pins_html else ''}
          <div class="insight">
            <span class="il">지표 읽기 · </span>노출 {p['impressions']:,}회, 저장 {p['saves']:,}회, 사이트 클릭 {p['clicks']:,}회. CTR {p['ctr']}%.<br>
            <span class="il">왜 의미 · </span>저장이 클릭보다 {saves_per_click}배 많음. 콘텐츠는 저장하지만 사이트 방문으로 이어지지 않는 패턴.<br>
            <span class="il">그래서 · </span>핀 설명에 사이트 유입 유도 문구 강화, 링크 클릭 CTA 개선 필요.
          </div>
        </div>"""
    else:
        pinterest_block = ""

    header = f"""
    <div style="margin-bottom:1.5rem;">
      <div style="font-size:20px;font-weight:500;">AC 사이트 · 주간 리포트</div>
      <div style="font-size:12px;color:#888;margin-top:2px;">실데이터 · 지난 7일 · {d['date']} 기준 · design.amorepacific.com</div>
    </div>"""
    footer = f'<div class="footer">실데이터 연결 완료 · GA4 + Pinterest API · {d["date"]} 자동 생성</div>'

    if layout == "vertical":
        grid_css = ".kpi-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px;}"
        max_width = "720px"
        body = f"""
        {header}{kpi_block}{top_block}{source_block}
        <div style="display:grid;grid-template-columns:1.3fr 1fr;gap:12px;margin-bottom:14px;">
          {country_block}{nvr_block}
        </div>
        {pinterest_block}{footer}"""
    else:
        grid_css = ".kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:1.5rem;}"
        max_width = "1200px"
        body = f"""
        {header}{kpi_block}
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px;">
          {top_block}{source_block}
        </div>
        <div style="display:grid;grid-template-columns:1.3fr 1fr;gap:12px;margin-bottom:14px;">
          {country_block}{nvr_block}
        </div>
        {pinterest_block}{footer}"""

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>AC 사이트 · 주간 리포트</title>
    <style>
    {COMMON_STYLE}
    {grid_css}
    body{{max-width:{max_width};margin:0 auto;padding:2rem 1rem;}}
    </style></head><body>{body}</body></html>"""


# ── GitHub 업로드 ────────────────────────────────────────────
def minify_html(html):
    html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
    html = re.sub(r'>\s+<', '><', html)
    html = re.sub(r'\s{2,}', ' ', html)
    return html.replace('\n', '').replace('\r', '').strip()


def upload_to_github(content: str, file_path: str, commit_msg: str):
    if not GITHUB_TOKEN:
        print("  ⚠️  GITHUB_TOKEN 없음 — 업로드 건너뜀")
        return False

    ctx = ssl.create_default_context()
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
        "User-Agent": "ac-dashboard"
    }

    # 기존 SHA 조회
    conn = http.client.HTTPSConnection("api.github.com", context=ctx)
    conn.request(
        "GET",
        f"/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{file_path}",
        headers=headers
    )
    res = conn.getresponse()
    body = json.loads(res.read().decode("utf-8"))
    sha = body.get("sha")

    # 업로드
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
    payload = json.dumps({
        "message": commit_msg,
        "content": content_b64,
        **({"sha": sha} if sha else {})
    }).encode("utf-8")

    conn2 = http.client.HTTPSConnection("api.github.com", context=ctx)
    conn2.request(
        "PUT",
        f"/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{file_path}",
        body=payload,
        headers=headers
    )
    res2 = conn2.getresponse()
    result = json.loads(res2.read().decode("utf-8"))

    if res2.status in [200, 201]:
        return True
    else:
        print(f"  ❌ GitHub 오류 {res2.status}: {result.get('message', '')}")
        return False


# ── 히스토리 저장 ────────────────────────────────────────────
def save_history(week_data: dict):
    history = {}
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)

    week_key = week_data["week_key"]
    history[week_key] = week_data

    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    print(f"  ✅ 히스토리 저장 완료 (총 {len(history)}주치)")
    return history


# ── 메인 실행 ────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("  AC 사이트 주간 대시보드 생성 시작")
    print(f"  {datetime.now().strftime('%Y.%m.%d %H:%M')}")
    print("=" * 50)

    date_str = datetime.now().strftime("%Y.%m.%d")
    week_key = datetime.now().strftime("%Y-%m-%d")

    # 1. GA4 데이터 수집
    client = init_ga4_client()
    ga4_data = collect_ga4_data(client)

    # 2. Pinterest 데이터 수집
    pinterest_data = collect_pinterest_data()

    # 3. Instagram 데이터 수집
    instagram_data = collect_instagram_data()

    # 4. 이번 주 데이터 통합
    this_week = {
        "date": date_str,
        "week_key": week_key,
        **ga4_data,
        "pinterest":  pinterest_data,
        "instagram":  instagram_data,
    }

    # 5. 히스토리 저장
    print("💾 히스토리 저장 중...")
    save_history(this_week)

    # 6. HTML 생성
    print("🎨 HTML 생성 중...")
    html_vertical = build_dashboard(this_week, layout="vertical")
    html_horizontal = build_dashboard(this_week, layout="horizontal")

    vertical_path = OUTPUT_DIR / "report_vertical.html"
    horizontal_path = OUTPUT_DIR / "index.html"

    with open(vertical_path, "w", encoding="utf-8") as f:
        f.write(html_vertical)
    with open(horizontal_path, "w", encoding="utf-8") as f:
        f.write(html_horizontal)

    print(f"  ✅ 세로형 → {vertical_path}")
    print(f"  ✅ 가로형 → {horizontal_path}")

    # 7. GitHub 업로드
    print("🚀 GitHub 업로드 중...")
    commit_msg = f"Update dashboard {date_str}"

    ok1 = upload_to_github(minify_html(html_horizontal), "index.html", commit_msg)
    if ok1:
        print(f"  ✅ index.html 업로드 완료")
        print(f"  🔗 https://{GITHUB_USERNAME}.github.io/{GITHUB_REPO}/")

    # history.json도 GitHub에 업로드
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        history_content = f.read()
    ok2 = upload_to_github(history_content, "ac_dashboard_history.json", commit_msg)
    if ok2:
        print(f"  ✅ history.json 업로드 완료")

    print("=" * 50)
    print("  🎉 완료!")
    print("=" * 50)


if __name__ == "__main__":
    main()
