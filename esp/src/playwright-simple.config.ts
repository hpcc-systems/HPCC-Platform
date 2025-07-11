import { defineConfig, devices } from "@playwright/test";

/**
 * Temporary config for testing converted selenium tests without HPCC server dependency
 */
export default defineConfig({
    testDir: "./tests",
    forbidOnly: false,
    retries: 0,
    workers: 1,
    timeout: 30_000,
    expect: {
        timeout: 10_000
    },
    reporter: "html",
    use: {
        baseURL: "http://127.0.0.1:3000", // Static file server
        trace: "on-first-retry",
        screenshot: "on-first-failure",
        video: "retain-on-failure",
        ignoreHTTPSErrors: true
    },

    projects: [
        {
            name: "chromium",
            use: { ...devices["Desktop Chrome"] }
        }
    ],

    /* Run local dev server for static files */
    webServer: {
        command: "npm run start",
        url: "http://127.0.0.1:3000",
        reuseExistingServer: true,
        ignoreHTTPSErrors: true,
        stdout: "pipe"
    }
});