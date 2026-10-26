import { DIMENSIONS, ROOTS } from "./constants.js";

const number = new Intl.NumberFormat("vi-VN");
const compact = new Intl.NumberFormat("vi-VN", { notation: "compact", maximumFractionDigits: 1 });
const time = new Intl.DateTimeFormat("vi-VN", { hour: "2-digit", minute: "2-digit" });
const weekday = new Intl.DateTimeFormat("vi-VN", { weekday: "short", day: "2-digit", month: "2-digit" });
const pad = (n) => String(n).padStart(2, "0");

export const fmtInt = (v) => number.format(Math.round(v ?? 0));
export const fmtCompact = (v) => ((v ?? 0) >= 1e6 ? compact.format(v) : fmtInt(v));
export const fmtPct = (v, digits = 0) => `${((v ?? 0) * 100).toFixed(digits).replace(".", ",")}%`;
export const fmtDec = (v, digits = 1) => (v ?? 0).toFixed(digits).replace(".", ",");
export const fmtSigned = (v, digits = 2) =>
  `${v > 0 ? "+" : v < 0 ? "−" : ""}${Math.abs(v ?? 0).toFixed(digits).replace(".", ",")}`;

// Mọi mốc thời gian từ API là epoch giây (UTC); hiển thị theo giờ máy người xem.
export const fmtTime = (ts) => time.format(new Date(ts * 1000));
export const fmtDayTime = (ts) => {
  const d = new Date(ts * 1000);
  return `${time.format(d)} ${pad(d.getDate())}/${pad(d.getMonth() + 1)}`;
};
export const fmtDate = (iso) => weekday.format(new Date(`${iso}T00:00:00`));

/** "12 phút trước", "3 giờ trước", "2 ngày trước". */
export function fmtAgo(ts, now = Date.now() / 1000) {
  const minutes = Math.max(0, Math.round((now - ts) / 60));
  if (minutes < 1) return "vừa xong";
  if (minutes < 60) return `${minutes} phút trước`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours} giờ trước`;
  return `${Math.round(hours / 24)} ngày trước`;
}

/** Mức độ của một đột biến theo z-score; luôn đi kèm nhãn chữ, không chỉ dựa vào màu. */
export function severity(z) {
  if (z >= 8) return { level: "critical", label: "Rất mạnh" };
  if (z >= 6) return { level: "serious", label: "Mạnh" };
  return { level: "warning", label: "Đáng chú ý" };
}

/** Thay đổi tương đối so với kỳ trước, vd "▲ 12%"; null nếu không có kỳ trước. */
export function delta(current, previous) {
  if (!previous) return null;
  const change = (current - previous) / previous;
  const arrow = change > 0.005 ? "▲" : change < -0.005 ? "▼" : "■";
  return `${arrow} ${Math.abs(change * 100).toFixed(0)}%`;
}

/** Bảng Events không có tiêu đề bài báo; dựng lại một tiêu đề đọc được từ slug của URL. */
export function headline(url) {
  try {
    const { hostname, pathname } = new URL(url);
    const slug = pathname
      .split("/")
      .map((part) => decodeURIComponent(part).replace(/\.[a-z0-9]{2,5}$/i, ""))
      .filter((part) => part.split(/[-_]/).length >= 3)
      .sort((a, b) => b.length - a.length)[0];
    if (!slug) return hostname + pathname;
    const words = slug.replace(/[-_]+/g, " ").replace(/\s+\d{5,}$/, "").trim();
    return words.charAt(0).toUpperCase() + words.slice(1);
  } catch {
    return url;
  }
}

/** Tên hiển thị của một đột biến, dịch tên nhóm sự kiện sang tiếng Việt. */
export function burstLabel(burst) {
  if (burst.dimension === "event_type") return ROOTS[burst.key] ?? burst.label;
  if (burst.dimension === "country_event") {
    const [, root] = burst.key.split("|");
    return `${burst.label.split(" · ")[0]} · ${ROOTS[root] ?? root}`;
  }
  return burst.label;
}

export const dimensionLabel = (dimension) => DIMENSIONS[dimension] ?? dimension;
