import { useEffect, useRef, useState } from "react";

import { usePoll } from "../api.js";
import { DIMENSIONS, QUAD_BY_ID, RANGES, ROOTS } from "../constants.js";
import {
  burstLabel,
  delta,
  dimensionLabel,
  fmtAgo,
  fmtCompact,
  fmtDayTime,
  fmtDec,
  fmtInt,
  fmtPct,
  fmtSigned,
  headline,
  severity,
} from "../format.js";
import { BarList, BurstChart, Legend, TimeSeriesChart } from "./charts.jsx";
import EventMap from "./EventMap.jsx";

function Card({ title, subtitle, loading, className = "", action, children }) {
  return (
    <section className={`card ${className}`}>
      <header className="card-head">
        <div>
          <h2>{title}</h2>
          {subtitle && <p>{subtitle}</p>}
        </div>
        {action}
      </header>
      <div className={loading ? "card-body stale" : "card-body"}>{children}</div>
    </section>
  );
}

function StatTile({ label, value, change, hint, help }) {
  return (
    <div className="tile" title={help}>
      <div className="tile-label">{label}</div>
      <div className="tile-value">{value}</div>
      <div className="tile-delta">{change ? `${change} so với kỳ trước` : hint}</div>
    </div>
  );
}

function SeverityBadge({ z }) {
  const sev = severity(z);
  return (
    <span className={`sev sev-${sev.level}`}>
      <svg viewBox="0 0 12 12" width="10" height="10" aria-hidden="true"><path d="M6 1 11 10H1z" fill="currentColor" /></svg>
      {sev.label}
    </span>
  );
}

function ArticleList({ rows, empty }) {
  if (!rows?.length) return <p className="empty">{empty}</p>;
  return (
    <ul className="article-list">
      {rows.map((a) => (
        <li key={a.url}>
          <a href={a.url} target="_blank" rel="noreferrer">{headline(a.url)}</a>
          <span className="domain">
            <span className="swatch" style={{ background: QUAD_BY_ID[quadOf(a.root)]?.color }} />
            {a.domain} · {a.place || a.country || "—"} · {ROOTS[a.root] ?? a.root} · {fmtInt(a.events)} sự kiện · {fmtDayTime(a.ts)}
          </span>
        </li>
      ))}
    </ul>
  );
}

// QuadClass suy ra từ nhóm sự kiện gốc theo CAMEO.
function quadOf(root) {
  const code = Number(root);
  if (code <= 5) return 1;
  if (code <= 8) return 2;
  if (code <= 13) return 3;
  return 4;
}

/** Tham số lọc bài báo tương ứng với một đối tượng đột biến. */
function articleScope(item) {
  if (item.dimension === "country") return { country: item.key };
  if (item.dimension === "event_type") return { root: item.key };
  const [country, root] = item.key.split("|");
  return { country, root };
}

/** Ô chọn quốc gia: gõ vài chữ để tìm trong danh sách các nước có sự kiện. */
function CountryPicker({ options, value, onChange }) {
  const [text, setText] = useState(value?.name ?? "");
  useEffect(() => setText(value?.name ?? ""), [value]);

  const handle = (event) => {
    const typed = event.target.value;
    setText(typed);
    if (!typed) return onChange(null);
    const hit = options.find((o) => o.name.toLowerCase() === typed.trim().toLowerCase());
    if (hit) onChange({ code: hit.code, name: hit.name });
  };

  return (
    <label className="field">
      <span>Quốc gia</span>
      <input list="country-options" value={text} onChange={handle} placeholder="Tất cả · gõ để tìm" />
      <datalist id="country-options">
        {options.map((o) => <option key={o.code} value={o.name} />)}
      </datalist>
    </label>
  );
}

function BurstDetail({ item, onClose }) {
  const series = usePoll("burst-series", { dimension: item.dimension, key: item.key, hours: 48 });
  const articles = usePoll("articles", { ...articleScope(item), hours: 48, ts: item.peak_ts, limit: 8 });

  return (
    <Card
      className="detail"
      title={<>Chi tiết đột biến: {burstLabel(item)} <SeverityBadge z={item.zscore} /></>}
      subtitle={`${dimensionLabel(item.dimension)} · đỉnh lúc ${fmtDayTime(item.peak_ts)}: ${fmtInt(item.n_articles)} bài từ ${item.n_sources} nguồn, trong khi bình thường chỉ khoảng ${fmtDec(item.baseline_mean)} bài (z = ${fmtDec(item.zscore)})`}
      loading={series.loading && !series.data}
      action={<button className="close-button" onClick={onClose} aria-label="Đóng chi tiết">Đóng ✕</button>}
    >
      <div className="detail-grid">
        <div>
          <h3>Số bài báo trong 48 giờ qua</h3>
          {series.data && <BurstChart points={series.data.points} bursts={series.data.bursts} />}
        </div>
        <div>
          <h3>Bài báo trong cửa sổ đỉnh</h3>
          <ArticleList rows={articles.data} empty="Không tìm thấy bài báo cho cửa sổ này." />
        </div>
      </div>
    </Card>
  );
}

export default function Realtime() {
  const [hours, setHours] = useState(24);
  const [country, setCountry] = useState(null); // { code, name }
  const [root, setRoot] = useState(null);
  const [burstDim, setBurstDim] = useState("");
  const [selected, setSelected] = useState(null);
  const detailRef = useRef(null);

  const scope = { hours, country: country?.code, root };
  const overview = usePoll("overview", { hours });
  const series = usePoll("timeseries", scope);
  const bursts = usePoll("bursts", { hours, dimension: burstDim, limit: 30 });
  const map = usePoll("map", scope, 60000);
  const countries = usePoll("countries", { hours, root, limit: 12 });
  const allCountries = usePoll("countries", { hours, limit: 300 }, 120000);
  const eventTypes = usePoll("event-types", { hours, country: country?.code });
  const articles = usePoll("articles", { ...scope, order: "recent", limit: 10 });

  // Bấm vào một đột biến thì cuộn tới phần chi tiết.
  useEffect(() => {
    if (selected) detailRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [selected]);

  const o = overview.data;
  if (o && !o.ready) {
    return (
      <div className="notice">
        <h2>Chưa có dữ liệu</h2>
        <p>Pipeline chưa đưa sự kiện nào vào ClickHouse. Kiểm tra DAG <code>gdelt_events_ingest</code> trên Airflow và hai job Spark streaming.</p>
      </div>
    );
  }

  const prev = (current, previous) => (o?.has_prev ? delta(current, previous) : null);
  const rangeLabel = RANGES.find((r) => r.hours === hours)?.label;
  const filterNote = [country?.name, root && ROOTS[root]].filter(Boolean).join(" · ");
  const pickCountry = (c) => setCountry((current) => (current?.code === c?.code ? null : c));

  return (
    <>
      <div className="toolbar">
        <div className="field">
          <span>Khoảng thời gian</span>
          <div className="segmented" role="group" aria-label="Khoảng thời gian">
            {RANGES.map((r) => (
              <button key={r.hours} className={r.hours === hours ? "active" : ""} onClick={() => setHours(r.hours)}>
                {r.label}
              </button>
            ))}
          </div>
        </div>
        <CountryPicker options={allCountries.data ?? []} value={country} onChange={setCountry} />
        <label className="field">
          <span>Loại sự kiện</span>
          <select value={root ?? ""} onChange={(e) => setRoot(e.target.value || null)}>
            <option value="">Tất cả 20 nhóm</option>
            {Object.entries(ROOTS).map(([code, name]) => (
              <option key={code} value={code}>{code} · {name}</option>
            ))}
          </select>
        </label>
        {(country || root) && (
          <button className="clear-button" onClick={() => { setCountry(null); setRoot(null); }}>Xoá bộ lọc</button>
        )}
        {o && <span className="freshness">Dữ liệu đến {fmtDayTime(o.latest_ts)} · tự làm mới mỗi 30 giây</span>}
      </div>

      <div className="tiles">
        <StatTile label="Sự kiện" value={fmtCompact(o?.events)} change={prev(o?.events, o?.events_prev)} hint={`trong ${rangeLabel} gần nhất`}
          help="Số hành động GDELT trích ra từ tin tức (ai làm gì với ai), toàn cầu" />
        <StatTile label="Bài báo" value={fmtCompact(o?.articles)} hint="số URL nguồn khác nhau"
          help="Số bài báo khác nhau đứng sau các sự kiện" />
        <StatTile label="Quốc gia có sự kiện" value={fmtInt(o?.countries)} hint="theo nơi diễn ra" />
        <StatTile label="Tỉ lệ xung đột" value={fmtPct(o?.conflict_share, 1)} change={prev(o?.conflict_share, o?.conflict_share_prev)} hint="QuadClass 3 và 4"
          help="Phần sự kiện thuộc nhóm xung đột lời nói hoặc xung đột vật chất" />
        <StatTile label="Tone trung bình" value={fmtSigned(o?.avg_tone)} hint="âm = tiêu cực, dương = tích cực"
          help="Giọng điệu trung bình của các bài báo, GDELT chấm từ khoảng -10 đến +10" />
        <StatTile label="Đối tượng có đột biến" value={fmtInt(o?.bursts)} change={prev(o?.bursts, o?.bursts_prev)} hint="z-score ≥ 4 so với 24 giờ trước"
          help="Số quốc gia / loại sự kiện có lúc được nhắc tới nhiều bất thường" />
      </div>

      <div className="grid">
        <Card
          className="span-2"
          title="Số sự kiện theo thời gian"
          subtitle={`${filterNote || "Toàn cầu"} · mỗi cột là một cửa sổ ${series.data?.bucket_minutes ?? 15} phút, chia theo mức hợp tác/xung đột`}
          loading={series.loading}
        >
          {series.data && <TimeSeriesChart points={series.data.points} bucketMinutes={series.data.bucket_minutes} longRange={hours > 24} />}
        </Card>

        <Card title="Đột biến được phát hiện" subtitle="Số bài báo vọt lên so với 24 giờ trước. Bấm để xem bài báo đứng sau." loading={bursts.loading}>
          <div className="segmented small" role="group" aria-label="Loại đối tượng">
            <button className={burstDim === "" ? "active" : ""} onClick={() => setBurstDim("")}>Tất cả</button>
            {Object.entries(DIMENSIONS).map(([id, label]) => (
              <button key={id} className={burstDim === id ? "active" : ""} onClick={() => setBurstDim(id)}>{label}</button>
            ))}
          </div>
          {bursts.data?.length ? (
            <ul className="burst-list">
              {bursts.data.map((b) => (
                <li key={`${b.dimension}:${b.key}`}>
                  <button
                    className={`burst${selected?.dimension === b.dimension && selected?.key === b.key ? " selected" : ""}`}
                    onClick={() => setSelected(b)}
                  >
                    <span className="burst-top">
                      <span className="burst-name">{burstLabel(b)}</span>
                      <SeverityBadge z={b.zscore} />
                    </span>
                    <span className="burst-meta">
                      <span className="tag">{dimensionLabel(b.dimension)}</span>
                      <strong>{fmtInt(b.n_articles)} bài</strong> từ {b.n_sources} nguồn · bình thường {fmtDec(b.baseline_mean)} · z {fmtDec(b.zscore)}
                    </span>
                    <span className="burst-time">
                      {fmtAgo(b.peak_ts)} · {fmtDayTime(b.peak_ts)}{b.windows > 1 && ` · kéo dài ${b.windows} cửa sổ`}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <p className="empty">Chưa phát hiện đột biến trong khoảng này. Bộ phát hiện cần ít nhất 6 giờ dữ liệu liên tục làm nền.</p>
          )}
          <div className="legend sev-legend">
            <span className="legend-caption">Mức độ:</span>
            <span className="sev sev-critical">Rất mạnh z ≥ 8</span>
            <span className="sev sev-serious">Mạnh z ≥ 6</span>
            <span className="sev sev-warning">Đáng chú ý z ≥ 4</span>
          </div>
        </Card>

        {selected && (
          <div className="span-all detail-anchor" ref={detailRef}>
            <BurstDetail key={`${selected.dimension}:${selected.key}`} item={selected} onClose={() => setSelected(null)} />
          </div>
        )}

        <Card
          className="span-2"
          title="Điểm nóng địa lý"
          subtitle={`${filterNote || "Toàn cầu"} · theo toạ độ nơi diễn ra sự kiện · bấm vào một điểm để lọc theo quốc gia`}
          loading={map.loading}
        >
          <EventMap points={map.data ?? []} onSelectCountry={pickCountry} />
        </Card>

        <Card title="Quốc gia nhiều sự kiện nhất" subtitle={root ? `Chỉ tính: ${ROOTS[root]} · bấm để lọc` : "Bấm vào một nước để lọc cả trang"} loading={countries.loading}>
          <BarList
            selected={country?.code}
            onSelect={(r) => pickCountry({ code: r.key, name: r.label })}
            rows={(countries.data ?? []).map((c) => ({
              key: c.code,
              label: c.name,
              value: c.events,
              note: `${fmtPct(c.conflict_share)} xung đột`,
              title: `${c.name}: ${fmtInt(c.events)} sự kiện, tone ${fmtSigned(c.avg_tone)}`,
            }))}
          />
        </Card>

        <Card title="Loại sự kiện (CAMEO)" subtitle={country ? `Tại ${country.name} · bấm để lọc` : "20 nhóm gốc · bấm để lọc"} loading={eventTypes.loading}>
          <Legend items={Object.values(QUAD_BY_ID)} />
          <BarList
            selected={root}
            onSelect={(r) => setRoot(root === r.key ? null : r.key)}
            rows={(eventTypes.data ?? []).map((t) => ({
              key: t.code,
              label: ROOTS[t.code] ?? t.code,
              value: t.events,
              color: QUAD_BY_ID[t.quad_class]?.color,
              title: `${ROOTS[t.code]}: ${fmtInt(t.events)} sự kiện, tone ${fmtSigned(t.avg_tone)}`,
            }))}
          />
        </Card>

        <Card className="span-2" title="Bài báo mới ghi nhận" subtitle={`${filterNote || "Toàn cầu"} · tiêu đề dựng lại từ URL`} loading={articles.loading}>
          <ArticleList rows={articles.data} empty="Chưa có bài báo." />
        </Card>
      </div>
    </>
  );
}
