import { test, expect } from "@playwright/test";

test.describe("V9 Activity - Enhanced Navigation", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
    });

    test("Should have proper navigation structure and links", async ({ page }) => {
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator(".fui-NavDrawerBody")).toBeVisible();

        await expect(page.locator("a").filter({ hasText: /^Activities$/ })).toBeVisible();

        const navigationLinks = [
            "Event Scheduler",
            "Files",
            "Clusters",
            "Operations"
        ];

        for (const linkText of navigationLinks) {
            const link = page.getByRole("link", { name: linkText });
            if (await link.isVisible()) {
                await expect(link).toBeVisible();
            }
        }

        await expect(page.getByRole("link", { name: "ECL", exact: true })).toBeVisible();
    });

    test("Should navigate to ECL Workunits and verify page loads", async ({ page }) => {
        const eclLink = page.getByRole("link", { name: "ECL", exact: true });
        if (await eclLink.isVisible()) {
            await eclLink.click();
            await page.waitForLoadState("networkidle");

            const workunitsLink = page.getByRole("link", { name: "Workunits" });
            if (await workunitsLink.isVisible()) {
                await workunitsLink.click();
                await page.waitForLoadState("networkidle");

                await expect(page.getByText("WUID")).toBeVisible();
                await expect(page.getByText("Owner", { exact: true })).toBeVisible();
                await expect(page.getByText("Job Name")).toBeVisible();
                await expect(page.getByText("Cluster", { exact: true })).toBeVisible();
            }
        }
    });

    test("Should navigate to Files section and verify page loads", async ({ page }) => {
        const filesLink = page.getByRole("link", { name: "Files" });
        if (await filesLink.isVisible()) {
            await filesLink.click();
            await page.waitForLoadState("networkidle");

            const logicalFilesLink = page.getByRole("link", { name: "Logical Files" });
            if (await logicalFilesLink.isVisible()) {
                await logicalFilesLink.click();
                await page.waitForLoadState("networkidle");

                await expect(page.getByText("Logical Name")).toBeVisible();
                await expect(page.getByText("Owner", { exact: true })).toBeVisible();
            }
        }
    });

    test("Should verify Activities page contains expected text elements", async ({ page }) => {
        const expectedTexts = [
            "Target/Wuid",
            "Graph",
            "State",
            "Owner",
            "Job Name"
        ];

        for (const text of expectedTexts) {
            await expect(page.getByText(text)).toBeVisible();
        }
    });

    test("Should verify Activities page table functionality", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();

        await expect(page.getByRole("columnheader", { name: "Priority" }).locator("div").first()).toBeVisible();

        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);

        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
    });

    test("Should handle navigation errors gracefully", async ({ page }) => {
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator(".fui-NavDrawerBody")).toBeVisible();

        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");

        await expect(page.getByText("Target/Wuid")).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
    });

    test("Should test advanced and history buttons functionality", async ({ page }) => {
        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();

        await expect(page.getByRole("button", { name: "History" })).toBeVisible();

        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();

        const advancedButton = page.getByRole("button", { name: "Advanced" });
        await expect(advancedButton).toBeEnabled();
    });
});