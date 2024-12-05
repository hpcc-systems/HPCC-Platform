import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.CI ? "https://play.hpccsystems.com:18010" : "http://127.0.0.1:8080";

/**
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig({
    testDir: "./tests",
    fullyParallel: true,
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 0,
    workers: process.env.CI ? 4 : undefined,
    reporter: "html",
    use: {
        baseURL,
        trace: "on-first-retry",
        ignoreHTTPSErrors: true
    },

    projects: [
        {
            name: "chromium",
            use: { ...devices["Desktop Chrome"] },
        },

        {
            name: "firefox",
            use: { ...devices["Desktop Firefox"] },
        },

        {
            name: "webkit",
            use: { ...devices["Desktop Safari"] },
        },

    ],

    /* Run your local dev server before starting the tests */
    webServer: {
        command: "npm run start",
        url: baseURL,
        reuseExistingServer: true,
        ignoreHTTPSErrors: true,
    },
});
