import { useEffect, useRef, useState } from "react";

export async function get(path, params = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined && value !== "") query.set(key, value);
  }
  const suffix = query.toString();
  const response = await fetch(`/api/${path}${suffix ? `?${suffix}` : ""}`);
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

/**
 * Gọi API rồi tự làm mới theo chu kỳ. Trong lúc tải lại vẫn giữ dữ liệu cũ
 * (loading = true) để biểu đồ không nhấp nháy.
 */
export function usePoll(path, params = {}, intervalMs = 30000) {
  const [state, setState] = useState({ data: null, loading: true, error: null });
  const key = `${path}?${JSON.stringify(params)}`;
  const latest = useRef(key);

  useEffect(() => {
    latest.current = key;
    if (path === null) return undefined;
    let timer;
    const load = async () => {
      setState((s) => ({ ...s, loading: true }));
      try {
        const data = await get(path, params);
        if (latest.current === key) setState({ data, loading: false, error: null });
      } catch (error) {
        if (latest.current === key) setState((s) => ({ ...s, loading: false, error }));
      }
      if (intervalMs) timer = setTimeout(load, intervalMs);
    };
    load();
    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, intervalMs]);

  return state;
}
