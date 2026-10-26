// Bốn lớp QuadClass của CAMEO. Màu mã hoá cực tính: xanh = hợp tác, đỏ = xung đột;
// nhạt = lời nói, đậm = vật chất. Bộ màu đã qua kiểm tra phân biệt cho người mù màu.
export const QUADS = [
  { id: 2, key: "material_coop", name: "Hợp tác vật chất", color: "#1c5cab" },
  { id: 1, key: "verbal_coop", name: "Hợp tác lời nói", color: "#6da7ec" },
  { id: 3, key: "verbal_conflict", name: "Xung đột lời nói", color: "#ee8279" },
  { id: 4, key: "material_conflict", name: "Xung đột vật chất", color: "#b3261e" },
];
export const QUAD_BY_ID = Object.fromEntries(QUADS.map((q) => [q.id, q]));

// 20 nhóm sự kiện gốc của CAMEO (EventRootCode).
export const ROOTS = {
  "01": "Tuyên bố công khai",
  "02": "Kêu gọi",
  "03": "Bày tỏ ý định hợp tác",
  "04": "Tham vấn",
  "05": "Hợp tác ngoại giao",
  "06": "Hợp tác vật chất",
  "07": "Viện trợ",
  "08": "Nhượng bộ",
  "09": "Điều tra",
  "10": "Yêu sách",
  "11": "Phản đối",
  "12": "Bác bỏ",
  "13": "Đe doạ",
  "14": "Biểu tình",
  "15": "Phô trương lực lượng",
  "16": "Cắt giảm quan hệ",
  "17": "Cưỡng ép",
  "18": "Tấn công",
  "19": "Giao tranh",
  "20": "Bạo lực quy mô lớn",
};

export const DIMENSIONS = {
  country: "Quốc gia",
  event_type: "Loại sự kiện",
  country_event: "Quốc gia × loại",
};

export const RANGES = [
  { hours: 6, label: "6 giờ" },
  { hours: 24, label: "24 giờ" },
  { hours: 72, label: "3 ngày" },
  { hours: 168, label: "7 ngày" },
];

export const ENTITY_KINDS = [
  { id: "keyword", label: "Từ khoá tiêu đề" },
  { id: "theme", label: "Chủ đề GKG" },
  { id: "person", label: "Nhân vật" },
  { id: "org", label: "Tổ chức" },
];

// Thang phân kỳ cho tỉ lệ xung đột trên bản đồ: xanh (hợp tác) - xám trung tính - đỏ (xung đột).
export const CONFLICT_SCALE = [
  { max: 0.15, color: "#1c5cab", label: "< 15%" },
  { max: 0.3, color: "#6da7ec", label: "15–30%" },
  { max: 0.45, color: "#a9a7a0", label: "30–45%" },
  { max: 0.6, color: "#ee8279", label: "45–60%" },
  { max: Infinity, color: "#b3261e", label: "> 60%" },
];
