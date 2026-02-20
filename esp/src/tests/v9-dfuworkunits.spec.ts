import { test, expect } from "@playwright/test";

test.describe("V9 DFU Workunits", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/dfuworkunits");
        await page.waitForLoadState("networkidle");
    });

    test("Should display the DFU Workunits page with all expected columns and controls", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();

        const expectedColumns = [
            "WUID",
            "Type",
            "Owner",
            "Job Name",
            "Cluster",
            "State",
            "% Complete",
            "Time Started",
            "Time Stopped",
            "Transfer Rate",
            "Transfer Rate (Avg)"
        ];

        for (const column of expectedColumns) {
            const header = page.getByRole("columnheader", { name: column, exact: true });
            await expect(header).toBeVisible();
        }

        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Should update the URL when sorting columns", async ({ page }) => {
        const rows = page.locator(".ms-DetailsRow");
        if (await rows.count() === 0) {
            test.skip(true, "No DFU workunits available to test sorting");
        }

        const wuidHeader = page.getByRole("columnheader", { name: "WUID" });
        if (await wuidHeader.count() === 0) {
            test.skip(true, "WUID column header not available for sorting");
        }

        await wuidHeader.first().click();
        await page.waitForLoadState("networkidle");

        expect(page.url()).toContain("sortBy=");
    });

    test("Should update the URL when changing pages", async ({ page }) => {
        const rows = page.locator(".ms-DetailsRow");
        if (await rows.count() === 0) {
            test.skip(true, "No DFU workunits available to test pagination");
        }

        const nextButton = page.locator(".ms-Pagination-container button[aria-label*='Next']");
        if (await nextButton.count() === 0) {
            test.skip(true, "Pagination controls not available");
        }

        if (await nextButton.first().isDisabled()) {
            test.skip(true, "Only one page of results available");
        }

        await nextButton.first().click();
        await page.waitForLoadState("networkidle");

        expect(page.url()).toContain("pageNum=");
    });

});
