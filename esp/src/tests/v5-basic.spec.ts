import { test, expect } from "@playwright/test";

test.describe("V5-Basic", () => {
    test.beforeEach(async ({ page }) => {
        await page.goto("index.html");
        await page.waitForLoadState("networkidle");
        await page.evaluate(() => {
            sessionStorage.setItem("ECLWatch:ModernMode-9.0", "false");
        });
    });

    test("Basic Frame", async ({ page }) => {
        await page.goto("stub.htm");
        await page.waitForLoadState("networkidle");
        await expect(page.locator("#stubStackController_stub_Main span").first()).toBeVisible();
        await expect(page.getByLabel("Advanced")).toBeVisible();
    });

    test("Activities", async ({ page }) => {
        await page.goto("stub.htm");
        await page.waitForLoadState("networkidle");
        await expect(page.locator("#stub_Main-DLStackController_stub_Main-DL_Activity_label")).toBeVisible();
        await expect(page.getByLabel("Auto Refresh")).toBeVisible();
        await expect(page.getByLabel("Maximize/Restore")).toBeVisible();
        await expect(page.locator("i")).toBeVisible();
        await expect(page.locator("svg").filter({ hasText: "%hthor" })).toBeVisible();
        await expect(page.getByRole("img", { name: "Priority" })).toBeVisible();
        await expect(page.getByText("Target/Wuid")).toBeVisible();
        await expect(page.getByText("Graph")).toBeVisible();
        await expect(page.getByText("State")).toBeVisible();
        await expect(page.getByText("Owner")).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
    });
});
