import { test as setup } from "@playwright/test";
import { baseURL, userID, password, storageStatePath } from "./global";

setup("Authenticate", async ({ page }) => {
    if (!userID) {
        throw new Error(
            "AUTH is enabled but no credentials were provided.\n" +
            "Please set HPCC_USER and HPCC_PASSWORD environment variables in .env"
        );
    }

    await page.goto(`${baseURL}/esp/files/Login.html`);
    await page.waitForLoadState("networkidle");

    await page.locator("#username").fill(userID);
    await page.locator("#password").fill(password);
    await page.locator("#button").click();

    // Wait until the app navigates away from the login page
    await page.waitForURL(/(?!.*[#/]login).*/, { timeout: 15_000 });
    await page.waitForLoadState("networkidle");

    await page.context().storageState({ path: storageStatePath });
});
