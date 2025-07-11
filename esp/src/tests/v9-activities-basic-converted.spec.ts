import { test, expect } from "@playwright/test";

/**
 * This test converts the basic functionality from Activities.java selenium test
 * It checks for the essential page elements that the original Java test verified
 */
test.describe("V9 Activities - Basic Elements (Converted from Selenium)", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
    });

    test("Should display Job Name element (converted from Activities.java)", async ({ page }) => {
        // Original Java test: if (driver.getPageSource().contains("Job Name"))
        await expect(page.getByText("Job Name")).toBeVisible();
    });

    test("Should display Owner element (converted from Activities.java)", async ({ page }) => {
        // Original Java test: if (driver.getPageSource().contains("Owner"))
        await expect(page.getByText("Owner")).toBeVisible();
    });

    test("Should display Target/Wuid element (converted from Activities.java)", async ({ page }) => {
        // Original Java test: if (driver.getPageSource().contains("Target/Wuid"))
        await expect(page.getByText("Target/Wuid")).toBeVisible();
    });

    test("Should display all required elements together (converted from Activities.java)", async ({ page }) => {
        // This combines all the checks from the original Java test
        const requiredElements = ["Job Name", "Owner", "Target/Wuid"];
        
        for (const elementText of requiredElements) {
            await expect(page.getByText(elementText)).toBeVisible();
        }
    });
});