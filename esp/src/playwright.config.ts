import { defineConfig, devices } from "@playwright/test";
import { baseURL, setBaseURL } from "./tests/global";

const isCIL = !!process.env.CIL;
const isCI = isCIL || !!process.env.CI;
const isFull = !!process.env.FULL;
if (isCI && !isCIL) {
    setBaseURL("http://127.0.0.1:8010");
} else {
    setBaseURL("http://127.0.0.1:8080");
}

console.log("target URL", baseURL);

/**
 * See https://playwright.dev/docs/test-configuration.
 */

export default defineConfig({
    testDir: "./tests",
    forbidOnly: isCI,
    retries: isCI ? 0 : 1,
    workers: isCI ? "100%" : "80%",
    timeout: isCI ? 30_000 : 20_000,
    expect: {
        timeout: isCI ? 10_000 : 10_000
    },
    reporter: isCI ? "line" : "html",
    use: {
        baseURL: `${baseURL}/esp/files/`,
        trace: "on-first-retry",
        screenshot: "only-on-failure",
        video: "off",
        ignoreHTTPSErrors: true,
        launchOptions: isCI ? {
            args: [
                "--disable-background-timer-throttling",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-sandbox"
            ]
        } : undefined
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
        ...(isFull ? [
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
        ] : [])
    ],

});
