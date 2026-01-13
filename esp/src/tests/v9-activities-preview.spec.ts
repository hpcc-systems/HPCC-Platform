import { test, expect } from "./fixtures";

test.describe("V9 Activities", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
    });

    test("Activities page loads and displays core elements", async ({ page }) => {
        const refresh = page.getByRole("button", { name: "Refresh" });
        await expect(refresh).toBeVisible();
        await expect(page.getByText("Disk Usage")).toBeVisible();
        await expect(page.getByLabel("Activities").getByText("Activities")).toBeVisible();
        await expect(page.getByRole("group").filter({ hasText: "%hthor" })).toBeVisible();
        await expect(page.getByRole("group").filter({ hasText: "%hthor" }).getByLabel("Maximize")).toBeVisible();
        await expect(page.getByText("ECLCCserver")).toBeVisible();
        await expect(page.getByRole("group").filter({ hasText: "ECLCCserver" }).locator("path").first()).toBeVisible();
        await expect(page.getByLabel("ECLCCserver - myeclccserver")).toBeVisible();
        await expect(page.getByRole("group").filter({ hasText: "ECLCCserver" }).getByLabel("Open")).toBeVisible();
        await expect(page.getByRole("group").filter({ hasText: "ECLCCserver" }).getByLabel("Clear")).toBeVisible();
    });

    test("pause/unpause", async ({ page, browserName }) => {
        const refresh = page.getByRole("button", { name: "Refresh" });
        const card = page.getByRole("group").filter({ hasText: "RoxieServer" }).first();
        const icon = card.getByRole("img");
        await expect(icon).toBeVisible();
        if (browserName === "chromium") {
            await expect(icon).toHaveAttribute("aria-label", "Active");

            const pauseBtn = card.getByLabel("Pause");
            await expect(pauseBtn).toBeVisible();
            await pauseBtn.click();
            await refresh.click();
            await expect(icon).toHaveAttribute("aria-label", "Stopped", { timeout: 5000 });

            const resumeBtn = page.getByRole("button", { name: "Resume" });
            await expect(resumeBtn).toBeVisible();
            await resumeBtn.click();
            await refresh.click();
            await expect(icon).toHaveAttribute("aria-label", "Active", { timeout: 5000 });
        }
    });
});
