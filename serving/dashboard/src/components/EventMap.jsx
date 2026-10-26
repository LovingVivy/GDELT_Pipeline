import { CircleMarker, MapContainer, TileLayer, Tooltip } from "react-leaflet";

import { CONFLICT_SCALE } from "../constants.js";
import { fmtInt, fmtPct, fmtSigned } from "../format.js";

const colorFor = (share) => CONFLICT_SCALE.find((step) => share < step.max).color;

/**
 * Điểm nóng địa lý: mỗi vòng tròn là một địa điểm, diện tích theo số sự kiện, màu theo tỉ lệ xung đột.
 * Bấm vào một điểm để lọc cả trang theo quốc gia của điểm đó.
 */
export default function EventMap({ points, onSelectCountry }) {
  const max = Math.max(...points.map((p) => p.events), 1);
  // Vẽ điểm lớn trước để điểm nhỏ nằm trên và vẫn rê chuột tới được.
  const ordered = [...points].sort((a, b) => b.events - a.events);

  return (
    <>
      <MapContainer center={[22, 12]} zoom={2} minZoom={1} maxZoom={8} worldCopyJump className="map">
        <TileLayer
          url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        />
        {ordered.map((p) => (
          <CircleMarker
            key={`${p.la},${p.lo}`}
            center={[p.la, p.lo]}
            radius={3 + 19 * Math.sqrt(p.events / max)}
            pathOptions={{ color: "#fcfcfb", weight: 1.5, fillColor: colorFor(p.conflict_share), fillOpacity: 0.8 }}
            eventHandlers={{
              click: () => p.code && onSelectCountry?.({ code: p.code, name: p.country }),
            }}
          >
            <Tooltip direction="top" opacity={1}>
              <div className="map-tip">
                <strong>{p.place || p.country}</strong>
                <span>{fmtInt(p.events)} sự kiện</span>
                <span>Xung đột {fmtPct(p.conflict_share)} · tone {fmtSigned(p.tone)}</span>
                {p.code && <em>Bấm để lọc theo {p.country}</em>}
              </div>
            </Tooltip>
          </CircleMarker>
        ))}
      </MapContainer>
      <div className="legend map-legend">
        <span className="legend-caption">Tỉ lệ sự kiện xung đột:</span>
        {CONFLICT_SCALE.map((step) => (
          <span className="legend-item" key={step.label}>
            <span className="legend-key dot" style={{ background: step.color }} />
            {step.label}
          </span>
        ))}
        <span className="legend-caption">· Diện tích vòng tròn = số sự kiện</span>
      </div>
    </>
  );
}
