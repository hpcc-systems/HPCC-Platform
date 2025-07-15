import { test, expect } from "@playwright/test";

test.describe("V9 Activities - Basic Elements (Converted from Selenium)", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
        await expect(page.getByRole("heading", { name: "...loading..." })).not.toBeVisible({ timeout: 15000 });
    });

    test("Should display Job Name element (converted from Activities.java)", async ({ page }) => {
        await expect(page.getByText("Job Name")).toBeVisible();
    });

    test("Should display Owner element (converted from Activities.java)", async ({ page }) => {
        await expect(page.getByText("Owner")).toBeVisible();
    });

    test("Should display Target/Wuid element (converted from Activities.java)", async ({ page }) => {
        await expect(page.getByText("Target/Wuid")).toBeVisible();
    });

    test("Should display all required elements together (converted from Activities.java)", async ({ page }) => {
        const requiredElements = ["Job Name", "Owner", "Target/Wuid"];

        for (const elementText of requiredElements) {
            await expect(page.getByText(elementText)).toBeVisible();
        }
    });
});