import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Khi chạy `npm run dev` trên máy, chuyển tiếp /api tới API đang mở cổng 8000.
const api = { "/api": process.env.API_TARGET || "http://localhost:8000" };

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, proxy: api },
  preview: { port: 4173, proxy: api },
});
