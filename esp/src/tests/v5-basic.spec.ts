import { test, expect } from "./fixtures";

test.describe("V5-Basic", () => {
    test.beforeEach(async ({ page }) => {
        // Set V5 mode before page scripts run to prevent V9 redirect race
        await page.addInitScript(() => {
            sessionStorage.setItem("ECLWatch:ModernMode-9.0", "false");
        });
    });

    test("Basic Frame", async ({ page }) => {
        await page.goto("stub.html");

        await expect(page.locator("#stubStackController_stub_Main span").first()).toBeVisible();
        await expect(page.getByLabel("Advanced")).toBeVisible();
    });

    test("Activities", async ({ page }) => {
        await page.goto("stub.html");

        // Check if environment is containerized
        const isContainer = await page.evaluate(() => {
            return (window as any).dojoConfig?.isContainer ?? false;
        });

        await expect(page.locator("#stub_Main-DLStackController_stub_Main-DL_Activity_label")).toBeVisible();
        await expect(page.getByLabel("Auto Refresh")).toBeVisible();
        await expect(page.getByLabel("Maximize/Restore")).toBeVisible();
        if (!isContainer) {
            await expect(page.locator("i.fa-database")).toBeVisible();
            await expect(page.locator("svg").filter({ hasText: "%hthor" })).toBeVisible();
        }
        await expect(page.getByRole("img", { name: "Priority" })).toBeVisible();
        await expect(page.getByText("Target/Wuid", { exact: true })).toBeVisible();
        await expect(page.getByText("Graph", { exact: true })).toBeVisible();
        await expect(page.getByText("State", { exact: true })).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();
        await expect(page.getByText("Job Name", { exact: true })).toBeVisible();
    });
});
