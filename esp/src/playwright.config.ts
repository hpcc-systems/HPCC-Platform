import { defineConfig, devices } from "@playwright/test";
import { baseURL } from "./tests/global";

/**
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig({
    testDir: "./tests",
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 1,
    workers: process.env.CI ? 1 : "80%",
    timeout: process.env.CI ? 60_000 : 20_000,
    expect: {
        timeout: process.env.CI ? 30_000 : 10_000
    },
    reporter: "html",
    use: {
        baseURL: `${baseURL}/esp/files/`,
        trace: "on-first-retry",
        screenshot: "on-first-failure",
        video: process.env.CI ? undefined : "on-first-retry",
        ignoreHTTPSErrors: true
    },

    projects: [
        {
            name: "setup",
            testMatch: /global\.setup\.ts/,
            teardown: "teardown"
        },
        {
            name: "teardown",
            testMatch: /global\.teardown\.ts/
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
            dependencies: ["setup"],

        }
    ],

    /* Run your local dev server before starting the tests */
    webServer: {
        command: "npm run start",
        url: baseURL,
        reuseExistingServer: !process.env.CI,
        ignoreHTTPSErrors: true,
        stdout: "pipe"
    },
});
