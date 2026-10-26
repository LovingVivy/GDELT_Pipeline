"""REST API cho dashboard: chỉ đọc tầng gold trong ClickHouse.

Mọi khoảng thời gian được neo vào cửa sổ dữ liệu mới nhất chứ không phải đồng hồ hệ thống,
vì GDELT luôn trễ so với thực tế và pipeline có thể vừa được bật lại.
Thời gian trả về là epoch giây (UTC).
"""
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query

from .db import row, rows

app = FastAPI(title="GDELT API", docs_url="/api/docs", openapi_url="/api/openapi.json")

Hours = Query(24, ge=1, le=24 * 14)


def period(hours: int) -> dict | None:
    """Kỳ đang xem [t0, t1) và kỳ liền trước [tp, t0) để so sánh; None nếu chưa có dữ liệu."""
    latest = row("SELECT max(window_start) AS latest, count() AS n FROM agg_events_15m")
    if not latest or not latest["n"]:
        return None
    t1 = latest["latest"] + timedelta(minutes=15)
    t0 = t1 - timedelta(hours=hours)
    return {"t0": t0, "t1": t1, "tp": t0 - timedelta(hours=hours)}


def epoch(value: datetime) -> int:
    return int(value.replace(tzinfo=timezone.utc).timestamp())


def scope(country: str | None, root: str | None) -> tuple[str, dict]:
    """Điều kiện lọc theo quốc gia / nhóm sự kiện, dùng chung cho nhiều endpoint."""
    clauses, params = [], {}
    if country:
        clauses.append("country_code = {country:String}")
        params["country"] = country
    if root:
        clauses.append("event_root_code = {root:String}")
        params["root"] = root
    return " AND ".join(clauses) or "1", params


@app.get("/api/health")
def health():
    p = period(1)
    return {"status": "ok", "ready": p is not None, "latest_ts": epoch(p["t1"]) if p else None}


@app.get("/api/overview")
def overview(hours: int = Hours):
    p = period(hours)
    if p is None:
        return {"ready": False}
    stats = row(
        """
        SELECT
            sumIf(n_events, window_start >= {t0:DateTime}) AS events,
            sumIf(n_events, window_start < {t0:DateTime}) AS events_prev,
            uniqExactIf(country_code, window_start >= {t0:DateTime} AND country_code != '') AS countries,
            sumIf(sum_tone, window_start >= {t0:DateTime}) / greatest(events, 1) AS avg_tone,
            sumIf(n_events, window_start >= {t0:DateTime} AND quad_class >= 3) / greatest(events, 1) AS conflict_share,
            sumIf(n_events, window_start < {t0:DateTime} AND quad_class >= 3) / greatest(events_prev, 1) AS conflict_share_prev,
            uniqExactIf(window_start, window_start < {t0:DateTime}) AS prev_windows
        FROM agg_events_15m FINAL
        WHERE window_start >= {tp:DateTime} AND window_start < {t1:DateTime}
        """,
        t0=p["t0"], t1=p["t1"], tp=p["tp"],
    )
    # n_articles của bảng tổng hợp không cộng được giữa các nhóm: đếm số bài trên bảng fact.
    articles = row(
        """
        SELECT uniqExact(source_url) AS articles FROM fct_events FINAL
        WHERE event_time >= {t0:DateTime} AND event_time < {t1:DateTime} AND source_url != ''
        """,
        t0=p["t0"], t1=p["t1"],
    )
    bursts = row(
        """
        SELECT uniqExactIf((dimension, key), window_start >= {t0:DateTime}) AS bursts,
               uniqExactIf((dimension, key), window_start < {t0:DateTime}) AS bursts_prev
        FROM bursts
        WHERE window_start >= {tp:DateTime} AND window_start < {t1:DateTime}
        """,
        t0=p["t0"], t1=p["t1"], tp=p["tp"],
    )
    return {
        "ready": True,
        "hours": hours,
        "latest_ts": epoch(p["t1"]),
        "from_ts": epoch(p["t0"]),
        # Chỉ so sánh với kỳ trước khi kỳ đó có dữ liệu ở ít nhất 90% số cửa sổ 15 phút;
        # kỳ trước thiếu dữ liệu (pipeline tắt, mới nạp) sẽ cho phần trăm thay đổi vô nghĩa.
        "has_prev": stats["prev_windows"] >= 0.9 * hours * 4,
        **stats,
        **articles,
        **bursts,
    }


@app.get("/api/timeseries")
def timeseries(hours: int = Hours, country: str | None = None, root: str | None = None):
    p = period(hours)
    if p is None:
        return {"bucket_minutes": 15, "points": []}
    bucket = 15 if hours <= 48 else 60
    cond, params = scope(country, root)
    data = rows(
        f"""
        SELECT toUnixTimestamp(toStartOfInterval(window_start, toIntervalMinute({{bucket:UInt32}}))) AS ts,
               sumIf(n_events, quad_class = 1 AND {cond}) AS verbal_coop,
               sumIf(n_events, quad_class = 2 AND {cond}) AS material_coop,
               sumIf(n_events, quad_class = 3 AND {cond}) AS verbal_conflict,
               sumIf(n_events, quad_class = 4 AND {cond}) AS material_conflict,
               sumIf(n_events, {cond}) AS total,
               sumIf(sum_tone, {cond}) / greatest(total, 1) AS avg_tone
        FROM agg_events_15m FINAL
        WHERE window_start >= {{t0:DateTime}} AND window_start < {{t1:DateTime}}
        GROUP BY ts ORDER BY ts
        """,
        bucket=bucket, t0=p["t0"], t1=p["t1"], **params,
    )
    return {"bucket_minutes": bucket, "points": data}


@app.get("/api/countries")
def countries(hours: int = Hours, root: str | None = None, limit: int = Query(12, ge=1, le=300)):
    p = period(hours)
    if p is None:
        return []
    cond, params = scope(None, root)
    return rows(
        f"""
        SELECT country_code AS code, any(country_name) AS name,
               sumIf(n_events, window_start >= {{t0:DateTime}}) AS events,
               sumIf(n_events, window_start < {{t0:DateTime}}) AS events_prev,
               sumIf(sum_tone, window_start >= {{t0:DateTime}}) / greatest(events, 1) AS avg_tone,
               sumIf(n_events, window_start >= {{t0:DateTime}} AND quad_class >= 3) / greatest(events, 1) AS conflict_share
        FROM agg_events_15m FINAL
        WHERE window_start >= {{tp:DateTime}} AND window_start < {{t1:DateTime}} AND country_code != '' AND {cond}
        GROUP BY country_code HAVING events > 0
        ORDER BY events DESC LIMIT {{limit:UInt32}}
        """,
        t0=p["t0"], t1=p["t1"], tp=p["tp"], limit=limit, **params,
    )


@app.get("/api/event-types")
def event_types(hours: int = Hours, country: str | None = None):
    p = period(hours)
    if p is None:
        return []
    cond, params = scope(country, None)
    return rows(
        f"""
        SELECT event_root_code AS code, any(quad_class) AS quad_class,
               sumIf(n_events, window_start >= {{t0:DateTime}}) AS events,
               sumIf(sum_tone, window_start >= {{t0:DateTime}}) / greatest(events, 1) AS avg_tone
        FROM agg_events_15m FINAL
        WHERE window_start >= {{t0:DateTime}} AND window_start < {{t1:DateTime}} AND {cond}
        GROUP BY event_root_code HAVING events > 0
        ORDER BY events DESC
        """,
        t0=p["t0"], t1=p["t1"], **params,
    )


@app.get("/api/bursts")
def bursts(hours: int = Hours, dimension: str | None = None, limit: int = Query(30, ge=1, le=200)):
    """Mỗi dòng là một đối tượng (dimension, key) có đột biến trong kỳ, mô tả tại cửa sổ đỉnh."""
    p = period(hours)
    if p is None:
        return []
    cond, params = ("b.dimension = {dimension:String}", {"dimension": dimension}) if dimension else ("1", {})
    return rows(
        f"""
        SELECT b.dimension AS dimension, b.key AS key, any(b.label) AS label,
               count() AS windows,
               max(b.zscore) AS zscore,
               argMax(b.n_articles, b.zscore) AS n_articles,
               argMax(b.n_sources, b.zscore) AS n_sources,
               argMax(b.baseline_mean, b.zscore) AS baseline_mean,
               toUnixTimestamp(argMax(b.window_start, b.zscore)) AS peak_ts,
               toUnixTimestamp(max(b.window_start)) AS last_ts
        FROM bursts AS b
        WHERE b.window_start >= {{t0:DateTime}} AND b.window_start < {{t1:DateTime}} AND {cond}
        GROUP BY b.dimension, b.key
        ORDER BY last_ts DESC, zscore DESC LIMIT {{limit:UInt32}}
        """,
        t0=p["t0"], t1=p["t1"], limit=limit, **params,
    )


def burst_scope(dimension: str, key: str) -> tuple[str, dict]:
    if dimension == "country":
        return scope(key, None)
    if dimension == "event_type":
        return scope(None, key)
    if dimension == "country_event":
        country, _, root = key.partition("|")
        return scope(country, root)
    raise HTTPException(400, "dimension phải là country, event_type hoặc country_event")


@app.get("/api/burst-series")
def burst_series(dimension: str, key: str, hours: int = Query(48, ge=1, le=24 * 14)):
    """Số bài báo theo cửa sổ 15 phút của một đối tượng, kèm các cửa sổ bị đánh dấu đột biến."""
    p = period(hours)
    if p is None:
        return {"points": [], "bursts": []}
    cond, params = burst_scope(dimension, key)
    points = rows(
        f"""
        SELECT toUnixTimestamp(toStartOfFifteenMinutes(event_time)) AS ts,
               uniqExactIf(source_url, source_url != '' AND {cond}) AS n
        FROM fct_events FINAL
        WHERE event_time >= {{t0:DateTime}} AND event_time < {{t1:DateTime}}
        GROUP BY ts ORDER BY ts
        """,
        t0=p["t0"], t1=p["t1"], **params,
    )
    marks = rows(
        """
        SELECT toUnixTimestamp(window_start) AS ts, n_articles, baseline_mean, baseline_std, zscore
        FROM bursts
        WHERE dimension = {dimension:String} AND key = {key:String}
          AND window_start >= {t0:DateTime} AND window_start < {t1:DateTime}
        ORDER BY ts
        """,
        dimension=dimension, key=key, t0=p["t0"], t1=p["t1"],
    )
    return {"points": points, "bursts": marks}


@app.get("/api/articles")
def articles(
    hours: int = Hours,
    country: str | None = None,
    root: str | None = None,
    ts: int | None = Query(None, description="chỉ lấy cửa sổ 15 phút bắt đầu tại mốc này (epoch giây)"),
    order: str = Query("top", pattern="^(top|recent)$"),
    limit: int = Query(15, ge=1, le=100),
):
    """Các bài báo đứng sau số liệu: gom sự kiện theo URL nguồn."""
    p = period(hours)
    if p is None:
        return []
    cond, params = scope(country, root)
    t0, t1 = p["t0"], p["t1"]
    if ts is not None:
        t0 = datetime.fromtimestamp(ts, timezone.utc).replace(tzinfo=None)
        t1 = t0 + timedelta(minutes=15)
    sort = "ts DESC, events DESC" if order == "recent" else "events DESC, ts DESC"
    return rows(
        f"""
        SELECT source_url AS url, any(source_domain) AS domain,
               count() AS events,
               topK(1)(event_root_code)[1] AS root,
               topK(1)(country_name)[1] AS country,
               topK(1)(geo_full_name)[1] AS place,
               avg(avg_tone) AS tone,
               toUnixTimestamp(max(event_time)) AS ts
        FROM fct_events FINAL
        WHERE event_time >= {{t0:DateTime}} AND event_time < {{t1:DateTime}} AND source_url != '' AND {cond}
        GROUP BY source_url
        ORDER BY {sort}
        LIMIT {{limit:UInt32}}
        """,
        t0=t0, t1=t1, limit=limit, **params,
    )


@app.get("/api/map")
def event_map(hours: int = Hours, country: str | None = None, root: str | None = None,
              limit: int = Query(600, ge=1, le=3000)):
    """Điểm nóng địa lý: gom sự kiện theo toạ độ nơi diễn ra (làm tròn 0,1 độ)."""
    p = period(hours)
    if p is None:
        return []
    cond, params = scope(country, root)
    return rows(
        f"""
        SELECT round(lat, 1) AS la, round(lon, 1) AS lo,
               topK(1)(geo_full_name)[1] AS place,
               topK(1)(country_code)[1] AS code, topK(1)(country_name)[1] AS country,
               count() AS events, avg(avg_tone) AS tone,
               countIf(quad_class >= 3) / count() AS conflict_share
        FROM fct_events FINAL
        WHERE event_time >= {{t0:DateTime}} AND event_time < {{t1:DateTime}}
          AND lat IS NOT NULL AND lon IS NOT NULL AND {cond}
        GROUP BY la, lo
        ORDER BY events DESC LIMIT {{limit:UInt32}}
        """,
        t0=p["t0"], t1=p["t1"], limit=limit, **params,
    )


# --------------------------------------------------------------------------- GKG (batch hàng tuần)

def latest_run() -> dict:
    return row(
        """
        SELECT run_id, period_start, period_end, n_docs, n_titles, n_topics, vocab_size, log_perplexity,
               toUnixTimestamp(created_at) AS created_ts
        FROM gkg_runs ORDER BY created_at DESC LIMIT 1
        """
    )


@app.get("/api/gkg/topics")
def gkg_topics():
    run = latest_run()
    if not run:
        return {"ready": False}
    key = {"run_id": run["run_id"]}

    topics: dict[int, dict] = {}
    for r in rows("SELECT topic_id, term, weight FROM gkg_topic_terms WHERE run_id = {run_id:String} ORDER BY topic_id, rank", **key):
        topic = topics.setdefault(r["topic_id"], {"topic_id": r["topic_id"], "terms": [], "daily": [], "docs": [], "n_docs": 0})
        topic["terms"].append({"term": r["term"], "weight": r["weight"]})
    for r in rows("SELECT topic_id, date, n_docs, avg_tone FROM gkg_topic_daily WHERE run_id = {run_id:String} ORDER BY topic_id, date", **key):
        if r["topic_id"] in topics:
            topics[r["topic_id"]]["daily"].append({"date": r["date"], "n_docs": r["n_docs"], "avg_tone": r["avg_tone"]})
            topics[r["topic_id"]]["n_docs"] += r["n_docs"]
    for r in rows("SELECT topic_id, title, url, source, prob FROM gkg_topic_docs WHERE run_id = {run_id:String} ORDER BY topic_id, rank", **key):
        if r["topic_id"] in topics:
            topics[r["topic_id"]]["docs"].append({k: r[k] for k in ("title", "url", "source", "prob")})

    return {"ready": True, "run": run, "topics": sorted(topics.values(), key=lambda t: -t["n_docs"])}


@app.get("/api/gkg/entities")
def gkg_entities(kind: str = Query("keyword", pattern="^(keyword|theme|person|org)$"), limit: int = Query(10, ge=1, le=25)):
    """Thực thể nổi bật của từng ngày trong kỳ, xếp theo TF-IDF."""
    run = latest_run()
    if not run:
        return {"ready": False}
    data = rows(
        """
        SELECT date, name, n_docs, tfidf, rank FROM gkg_entity_daily
        WHERE run_id = {run_id:String} AND kind = {kind:String} AND rank <= {limit:UInt32}
        ORDER BY date, rank
        """,
        run_id=run["run_id"], kind=kind, limit=limit,
    )
    days: dict = {}
    for r in data:
        days.setdefault(r["date"], []).append({k: r[k] for k in ("name", "n_docs", "tfidf", "rank")})
    return {"ready": True, "kind": kind, "days": [{"date": d, "items": items} for d, items in days.items()]}
