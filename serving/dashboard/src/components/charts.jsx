import { useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { QUADS } from "../constants.js";
import { fmtCompact, fmtDayTime, fmtDec, fmtInt, fmtSigned, fmtTime } from "../format.js";

const AXIS_TICK = { fill: "var(--muted)", fontSize: 12 };
const BURST_COLOR = "#d03b3b";

function TooltipBox({ title, rows, footer }) {
  return (
    <div className="tooltip">
      <div className="tooltip-title">{title}</div>
      {rows.map((r) => (
        <div className="tooltip-row" key={r.name}>
          <span className="line-key" style={{ background: r.color }} />
          <span className="tooltip-name">{r.name}</span>
          <strong>{r.value}</strong>
        </div>
      ))}
      {footer && <div className="tooltip-footer">{footer}</div>}
    </div>
  );
}

export function Legend({ items, shape = "rect" }) {
  return (
    <div className="legend">
      {items.map((item) => (
        <span className="legend-item" key={item.name}>
          <span className={`legend-key ${shape}`} style={{ background: item.color }} />
          {item.name}
        </span>
      ))}
    </div>
  );
}

/** Cột chồng theo 4 lớp QuadClass; mỗi cột là một cửa sổ thời gian. */
export function TimeSeriesChart({ points, bucketMinutes, longRange }) {
  const [asTable, setAsTable] = useState(false);
  const tick = longRange ? fmtDayTime : fmtTime;

  const tooltip = ({ active, payload }) => {
    if (!active || !payload?.length) return null;
    const p = payload[0].payload;
    return (
      <TooltipBox
        title={`${fmtDayTime(p.ts)} · cửa sổ ${bucketMinutes} phút`}
        rows={[...QUADS].reverse().map((q) => ({ name: q.name, color: q.color, value: fmtInt(p[q.key]) }))}
        footer={`Tổng ${fmtInt(p.total)} sự kiện · tone ${fmtSigned(p.avg_tone)}`}
      />
    );
  };

  return (
    <>
      <div className="chart-head">
        <Legend items={QUADS} />
        <button className="link-button" onClick={() => setAsTable((v) => !v)}>
          {asTable ? "Xem biểu đồ" : "Xem bảng"}
        </button>
      </div>
      {asTable ? (
        <div className="table-scroll" style={{ maxHeight: 280 }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Cửa sổ</th>
                {QUADS.map((q) => <th key={q.key} className="num">{q.name}</th>)}
                <th className="num">Tổng</th>
                <th className="num">Tone</th>
              </tr>
            </thead>
            <tbody>
              {[...points].reverse().map((p) => (
                <tr key={p.ts}>
                  <td>{fmtDayTime(p.ts)}</td>
                  {QUADS.map((q) => <td key={q.key} className="num">{fmtInt(p[q.key])}</td>)}
                  <td className="num">{fmtInt(p.total)}</td>
                  <td className="num">{fmtSigned(p.avg_tone)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={points} margin={{ top: 8, right: 8, bottom: 0, left: 0 }} barCategoryGap={1}>
            <CartesianGrid vertical={false} stroke="var(--grid)" />
            <XAxis dataKey="ts" tickFormatter={tick} tickLine={false} axisLine={{ stroke: "var(--axis)" }} tick={AXIS_TICK} minTickGap={56} />
            <YAxis tickFormatter={fmtCompact} tickLine={false} axisLine={false} tick={AXIS_TICK} width={48} />
            <Tooltip content={tooltip} cursor={{ fill: "rgba(11, 11, 11, 0.06)" }} />
            {QUADS.map((q) => (
              <Bar key={q.key} dataKey={q.key} stackId="quad" fill={q.color} stroke="var(--surface)" strokeWidth={0.5} maxBarSize={24} isAnimationActive={false} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      )}
    </>
  );
}

/** Số bài báo mỗi 15 phút của một đối tượng, đánh dấu các cửa sổ bị phát hiện đột biến. */
export function BurstChart({ points, bursts }) {
  const tooltip = ({ active, payload }) => {
    if (!active || !payload?.length) return null;
    const p = payload[0].payload;
    const mark = bursts.find((b) => b.ts === p.ts);
    return (
      <TooltipBox
        title={`${fmtDayTime(p.ts)} · cửa sổ 15 phút`}
        rows={[{ name: "Bài báo", color: "var(--series-1)", value: fmtInt(p.n) }]}
        footer={mark ? `Đột biến: z = ${fmtDec(mark.zscore)} (nền ${fmtDec(mark.baseline_mean)} ± ${fmtDec(mark.baseline_std)})` : null}
      />
    );
  };
  const known = new Set(points.map((p) => p.ts));

  return (
    <>
      <div className="legend">
        <span className="legend-item"><span className="legend-key line" style={{ background: "var(--series-1)" }} />Bài báo mỗi 15 phút</span>
        <span className="legend-item"><span className="legend-key dot" style={{ background: BURST_COLOR }} />Cửa sổ đột biến</span>
      </div>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={points} margin={{ top: 12, right: 12, bottom: 0, left: 0 }}>
          <CartesianGrid vertical={false} stroke="var(--grid)" />
          <XAxis dataKey="ts" tickFormatter={fmtDayTime} tickLine={false} axisLine={{ stroke: "var(--axis)" }} tick={AXIS_TICK} minTickGap={72} />
          <YAxis tickFormatter={fmtCompact} tickLine={false} axisLine={false} tick={AXIS_TICK} width={40} allowDecimals={false} />
          <Tooltip content={tooltip} cursor={{ stroke: "var(--axis)" }} />
          <Line dataKey="n" stroke="var(--series-1)" strokeWidth={2} dot={false} activeDot={{ r: 4, stroke: "var(--surface)", strokeWidth: 2 }} isAnimationActive={false} />
          {bursts.filter((b) => known.has(b.ts)).map((b) => (
            <ReferenceDot key={b.ts} x={b.ts} y={b.n_articles} r={5} fill={BURST_COLOR} stroke="var(--surface)" strokeWidth={2} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}

/** Đường xu hướng nhỏ, không trục: chỉ để nhìn hình dạng. */
export function Sparkline({ values, width = 120, height = 32 }) {
  if (values.length < 2) return <svg width={width} height={height} />;
  const max = Math.max(...values, 1);
  const x = (i) => 4 + (i * (width - 8)) / (values.length - 1);
  const y = (v) => height - 4 - (v / max) * (height - 8);
  const path = values.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const last = values.length - 1;
  return (
    <svg width={width} height={height} role="img" aria-label="xu hướng theo ngày">
      <path d={path} fill="none" stroke="var(--series-1)" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={x(last)} cy={y(values[last])} r="4" fill="var(--series-1)" stroke="var(--surface)" strokeWidth="2" />
    </svg>
  );
}

/** Danh sách thanh ngang: nhãn - thanh - giá trị. Bấm vào dòng để lọc. */
export function BarList({ rows, selected, onSelect, empty = "Chưa có dữ liệu" }) {
  if (!rows.length) return <p className="empty">{empty}</p>;
  const max = Math.max(...rows.map((r) => r.value), 1);
  return (
    <ul className="bar-list">
      {rows.map((r) => (
        <li key={r.key}>
          <button
            className={`bar-row${selected === r.key ? " selected" : ""}`}
            onClick={onSelect ? () => onSelect(r) : undefined}
            disabled={!onSelect}
            title={r.title}
          >
            <span className="bar-label">{r.label}</span>
            <span className="bar-track">
              <span className="bar-fill" style={{ width: `${Math.max((r.value / max) * 100, 1)}%`, background: r.color ?? "var(--series-1)" }} />
            </span>
            <span className="bar-value">{r.display ?? fmtInt(r.value)}</span>
            {r.note !== undefined && <span className="bar-note">{r.note}</span>}
          </button>
        </li>
      ))}
    </ul>
  );
}
