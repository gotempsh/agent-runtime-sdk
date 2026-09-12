import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const siteDirectory = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  base: process.env.DOCS_BASE ?? "/",
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": resolve(siteDirectory, "src"),
    },
  },
  server: {
    fs: {
      allow: [resolve(siteDirectory, "..")],
    },
  },
});
