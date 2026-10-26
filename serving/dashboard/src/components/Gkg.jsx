import { useState } from "react";

import { usePoll } from "../api.js";
import { ENTITY_KINDS } from "../constants.js";
import { fmtCompact, fmtDate, fmtDayTime, fmtDec, fmtInt, fmtPct, fmtSigned } from "../format.js";
import { BarList, Sparkline } from "./charts.jsx";

function TopicCard({ topic, index, totalDocs }) {
  const [open, setOpen] = useState(false);
  const title = topic.terms.slice(0, 3).map((t) => t.term).join(" · ");
  const tone = topic.n_docs ? topic.daily.reduce((sum, d) => sum + d.avg_tone * d.n_docs, 0) / topic.n_docs : 0;
  const docs = open ? topic.docs : topic.docs.slice(0, 3);

  return (
    <section className="card topic">
      <header className="card-head">
        <div>
          <h2><span className="topic-index">#{index + 1}</span> {title}</h2>
          <p>{fmtInt(topic.n_docs)} bài ({fmtPct(topic.n_docs / Math.max(totalDocs, 1))}) · tone {fmtSigned(tone)}</p>
        </div>
      </header>
      <div className="topic-trend">
        <Sparkline values={topic.daily.map((d) => d.n_docs)} width={160} height={36} />
        <span>Số bài mỗi ngày</span>
      </div>
      <BarList rows={topic.terms.slice(0, 8).map((t) => ({ key: t.term, label: t.term, value: t.weight, display: fmtPct(t.weight, 1) }))} />
      <h3>Bài tiêu biểu</h3>
      <ul className="article-list">
        {docs.map((d) => (
          <li key={d.url}>
            <a href={d.url} target="_blank" rel="noreferrer">{d.title}</a>
            <span className="domain">{d.source} · xác suất {fmtPct(d.prob)}</span>
          </li>
        ))}
      </ul>
      {topic.docs.length > 3 && (
        <button className="link-button" onClick={() => setOpen((v) => !v)}>{open ? "Thu gọn" : `Xem thêm ${topic.docs.length - 3} bài`}</button>
      )}
    </section>
  );
}

export default function Gkg() {
  const [kind, setKind] = useState("keyword");
  const topics = usePoll("gkg/topics", {}, 60000);
  const entities = usePoll("gkg/entities", { kind, limit: 10 }, 60000);

  if (topics.data && !topics.data.ready) {
    return (
      <div className="notice">
        <h2>Chưa có kết quả phân tích GKG</h2>
        <p>
          Job này chạy theo lô hàng tuần. Để chạy ngay: mở Airflow tại{" "}
          <a href="http://localhost:8089" target="_blank" rel="noreferrer">localhost:8089</a>, chọn DAG{" "}
          <code>gdelt_gkg_manual</code> và bấm <strong>Trigger DAG w/ config</strong> (đặt <code>days = 1</code> nếu muốn thử nhanh).
        </p>
      </div>
    );
  }
  if (!topics.data) return <p className="empty">Đang tải…</p>;

  const { run, topics: list } = topics.data;
  const assigned = list.reduce((sum, t) => sum + t.n_docs, 0);

  return (
    <>
      <div className="filters">
        <span className="freshness">
          Kỳ phân tích {fmtDate(run.period_start)} → {fmtDate(run.period_end)} · chạy lúc {fmtDayTime(run.created_ts)}
        </span>
      </div>
      <div className="tiles">
        <div className="tile"><div className="tile-label">Bài báo trong kỳ</div><div className="tile-value">{fmtCompact(run.n_docs)}</div><div className="tile-delta">sau khi khử trùng URL</div></div>
        <div className="tile"><div className="tile-label">Số chủ đề LDA</div><div className="tile-value">{run.n_topics}</div><div className="tile-delta">tham số k của mô hình</div></div>
        <div className="tile"><div className="tile-label">Kích thước từ điển</div><div className="tile-value">{fmtInt(run.vocab_size)}</div><div className="tile-delta">sau lọc stop word, minDF/maxDF</div></div>
        <div className="tile"><div className="tile-label">Bài gán được chủ đề</div><div className="tile-value">{fmtPct(assigned / Math.max(run.n_titles, 1))}</div><div className="tile-delta">có một chủ đề trội rõ</div></div>
        <div className="tile"><div className="tile-label">Log-perplexity</div><div className="tile-value">{fmtDec(run.log_perplexity, 2)}</div><div className="tile-delta">thấp hơn = mô hình khớp hơn</div></div>
      </div>

      <h2 className="section-title">Chủ đề của kỳ (LDA trên tiêu đề bài báo)</h2>
      <div className="grid topics">
        {list.map((topic, index) => <TopicCard key={topic.topic_id} topic={topic} index={index} totalDocs={assigned} />)}
      </div>

      <h2 className="section-title">Nổi bật theo ngày (xếp hạng TF-IDF)</h2>
      <div className="filters">
        <div className="segmented" role="group" aria-label="Loại thực thể">
          {ENTITY_KINDS.map((k) => (
            <button key={k.id} className={k.id === kind ? "active" : ""} onClick={() => setKind(k.id)}>{k.label}</button>
          ))}
        </div>
      </div>
      <section className="card">
        <div className={entities.loading ? "card-body stale table-scroll" : "card-body table-scroll"}>
          <table className="data-table days">
            <thead>
              <tr>
                <th className="num">#</th>
                {(entities.data?.days ?? []).map((d) => <th key={d.date}>{fmtDate(d.date)}</th>)}
              </tr>
            </thead>
            <tbody>
              {Array.from({ length: 10 }, (_, i) => (
                <tr key={i}>
                  <td className="num">{i + 1}</td>
                  {(entities.data?.days ?? []).map((d) => (
                    <td key={d.date}>
                      {d.items[i] ? (
                        <>
                          <span className="entity">{d.items[i].name.replaceAll("_", " ").toLowerCase()}</span>
                          <span className="domain">{fmtInt(d.items[i].n_docs)} bài</span>
                        </>
                      ) : "—"}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}
