import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The built output lands in `conductor/web/dist`, which is committed into
// the Python package (see `[tool.setuptools.package-data]`). `npm run build`
// must be re-run whenever the frontend changes.
export default defineConfig({
  plugins: [react()],
  base: "/",
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    // Dev convenience: proxy /api to a locally running dashboard server.
    proxy: {
      "/api": "http://127.0.0.1:8080",
    },
  },
});
