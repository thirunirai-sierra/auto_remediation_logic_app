import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  server: {
    port: 3000,
    proxy: {
      // Old dashboard & pipeline endpoints
      "/smart-monitoring": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
      "/autonomous": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
      "/dashboard": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
      "/aem": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
      "/incidents": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
      "/logs": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
      // New observability endpoints (already covered by /api)
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
      // User API (if needed)
      "/user-api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: {
          "ui5-core":    ["@ui5/webcomponents-react"],
          "recharts":    ["recharts"],
          "react-vendor": ["react", "react-dom", "react-router-dom"],
          "query":       ["@tanstack/react-query", "zustand"],
        },
      },
    },
    chunkSizeWarningLimit: 600,
  },
});