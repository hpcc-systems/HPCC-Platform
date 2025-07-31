import { defineConfig, devices } from "@playwright/test";
import { baseURL, setBaseURL } from "./tests/global";

const isCI = !!process.env.CI;
const isFull = !!process.env.FULL;
if (isCI) {
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
    retries: isCI ? 2 : 1,
    workers: isCI ? "50%" : "80%",
    timeout: isCI ? 60_000 : 20_000,
    expect: {
        timeout: isCI ? 30_000 : 10_000
    },
    reporter: "html",
    use: {
        baseURL: `${baseURL}/esp/files/`,
        trace: "on-first-retry",
        screenshot: "on-first-failure",
        video: isCI ? undefined : "on-first-retry",
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
