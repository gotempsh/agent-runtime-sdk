import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: { timeout: 8_000 },
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: process.env.CI ? [["line"], ["html", { open: "never" }]] : "line",
  use: {
    ...devices["Desktop Chrome"],
    baseURL: "http://127.0.0.1:7788",
    viewport: { width: 1440, height: 900 },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  webServer: {
    command: "bun run build && cargo run --manifest-path ../Cargo.toml",
    url: "http://127.0.0.1:7788/api/health",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
