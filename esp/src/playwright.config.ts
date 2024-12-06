import { defineConfig, devices } from "@playwright/test";

export const baseURL = process.env.CI ? "https://play.hpccsystems.com:18010" : "http://127.0.0.1:8080";

/**
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig({
    testDir: "./tests",
    fullyParallel: true,
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 0,
    workers: process.env.CI ? 4 : undefined,
    timeout: 60_000,
    expect: {
        timeout: 30_000
    },
    reporter: "html",
    use: {
        baseURL,
        trace: "on-first-retry",
        screenshot: "on-first-failure",
        ignoreHTTPSErrors: true
    },

    projects: [
        {
            name: "setup",
            testMatch: /global\.setup\.ts/,
            teardown: "teardown"
        },
        {
            name: "chromium",
            use: { ...devices["Desktop Chrome"] },
            dependencies: ["setup"]
        },
        {
            name: "firefox",
            use: { ...devices["Desktop Firefox"] },
            dependencies: ["setup"]
        },
        {
            name: "webkit",
            use: { ...devices["Desktop Safari"] },
            dependencies: ["setup"]
        },
        {
            name: "teardown",
            testMatch: /global\.teardown\.ts/
        }

    ],

    /* Run your local dev server before starting the tests */
    webServer: {
        command: "npm run start",
        url: baseURL,
        reuseExistingServer: true,
        ignoreHTTPSErrors: true,
    },
});
