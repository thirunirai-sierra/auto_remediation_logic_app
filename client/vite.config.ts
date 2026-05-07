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
      "/user-api": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
      "/api": {
        target: "http://127.0.0.1:8000",   // Use 127.0.0.1
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