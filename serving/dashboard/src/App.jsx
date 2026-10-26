import { useState } from "react";

import { usePoll } from "./api.js";
import Gkg from "./components/Gkg.jsx";
import Realtime from "./components/Realtime.jsx";
import { fmtAgo, fmtDayTime } from "./format.js";

const TABS = [
  { id: "realtime", label: "Thời gian thực", hint: "Events · mỗi 15 phút", view: Realtime },
  { id: "gkg", label: "Chủ đề tuần", hint: "GKG · TF-IDF + LDA", view: Gkg },
];

/** Đèn trạng thái: dữ liệu mới nhất cách đây bao lâu. GDELT vốn trễ 45–60 phút nên dưới 2 giờ là bình thường. */
function Freshness() {
  const { data } = usePoll("health", {}, 30000);
  if (!data) return null;
  if (!data.ready) return <span className="status status-off"><span className="status-dot" />Chưa có dữ liệu</span>;
  const lagHours = (Date.now() / 1000 - data.latest_ts) / 3600;
  const state = lagHours <= 2 ? "ok" : lagHours <= 6 ? "late" : "off";
  const text = { ok: "Đang cập nhật", late: "Dữ liệu đang trễ", off: "Pipeline có thể đã dừng" }[state];
  return (
    <span className={`status status-${state}`} title={`Cửa sổ dữ liệu mới nhất kết thúc lúc ${fmtDayTime(data.latest_ts)}`}>
      <span className="status-dot" />
      {text} · dữ liệu {fmtAgo(data.latest_ts)}
    </span>
  );
}

export default function App() {
  const [tab, setTab] = useState("realtime");
  const View = TABS.find((t) => t.id === tab).view;

  return (
    <div className="page">
      <header className="page-head">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">
            <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
              <circle cx="12" cy="12" r="9" />
              <path d="M3 12h18M12 3c2.8 3 2.8 15 0 18M12 3c-2.8 3-2.8 15 0 18" />
            </svg>
          </span>
          <div>
            <h1>GDELT Realtime</h1>
            <p>Phát hiện sự kiện và xu hướng tin tức toàn cầu</p>
          </div>
        </div>
        <Freshness />
      </header>

      <nav className="tabs" aria-label="Chế độ xem">
        {TABS.map((t) => (
          <button key={t.id} className={t.id === tab ? "active" : ""} onClick={() => setTab(t.id)}>
            <span>{t.label}</span>
            <small>{t.hint}</small>
          </button>
        ))}
      </nav>

      <main>
        <View />
      </main>

      <footer className="page-foot">
        Nguồn: GDELT 2.0 · Kafka → Spark Structured Streaming → S3 (bronze/silver/gold) → ClickHouse · điều phối bằng Airflow
      </footer>
    </div>
  );
}
