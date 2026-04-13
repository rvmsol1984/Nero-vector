import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// During local dev the Vite server runs on 5173 and proxies the API
// calls to the FastAPI backend on 3005. In production the FastAPI
// process serves the built bundle from /app/frontend_dist directly.
export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/api":    "http://localhost:3005",
      "/health": "http://localhost:3005",
      "/auth":   "http://localhost:3006",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
  },
});
