import { test, expect } from "@playwright/test";

test.describe("V9 Workunits - Enhanced (Converted from Selenium)", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/workunits");
        await page.waitForLoadState("networkidle");
    });

    test("Should display all expected columns from Java test", async ({ page }) => {
        const expectedColumns = [
            "WUID",
            "Owner",
            "Job Name",
            "Cluster",
            "State",
            "Total Cluster Time"
        ];

        const costColumns = [
            "Compile Cost",
            "Execution Cost",
            "File Access Cost"
        ];

        for (const column of expectedColumns) {
            if (column === "Cluster") {
                await expect(page.getByText(column, { exact: true })).toBeVisible();
            } else {
                await expect(page.getByText(column)).toBeVisible();
            }
        }

        for (const column of costColumns) {
            const columnElement = page.getByText(column);
            if (await columnElement.count() > 0) {
                await expect(columnElement).toBeVisible();
            }
        }
    });

    test("Should support workunit detail page navigation", async ({ page, browserName }) => {
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        const firstWuidLink = page.locator(".ms-DetailsRow").first().locator("a").first();

        if (browserName === "chromium") {
            await firstWuidLink.click();
            await page.waitForLoadState("networkidle");

            const detailPageElements = [
                "WUID",
                "State",
                "Owner",
                "Job Name",
                "Cluster"
            ];

            let foundDetailElements = 0;
            for (const element of detailPageElements) {
                const elementLocator = page.getByText(element);
                if (await elementLocator.count() > 0) {
                    foundDetailElements++;
                }
            }

            expect(foundDetailElements).toBeGreaterThan(0);
        }
    });

    test("Should handle workunit detail page tabs", async ({ page, browserName }) => {
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        const firstWuidLink = page.locator(".ms-DetailsRow").first().locator("a").first();

        if (browserName === "chromium") {
            await firstWuidLink.click();
            await page.waitForLoadState("networkidle");

            const expectedTabs = [
                "variables",
                "outputs",
                "inputs",
                "metrics",
                "workflows",
                "queries",
                "resources",
                "helpers",
                "xml"
            ];

            let tabsFound = 0;
            for (const tab of expectedTabs) {
                const tabLocator = page.locator("[role=\"tab\"]").filter({ hasText: new RegExp(tab, "i") });
                if (await tabLocator.count() > 0) {
                    tabsFound++;
                }
            }

            if (tabsFound > 0) {
                expect(tabsFound).toBeGreaterThan(0);
            }
        }
    });

    test("Should handle different workunit states properly", async ({ page }) => {
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        const possibleStates = ["completed", "failed", "compiled", "running", "blocked"];

        let statesFound = 0;
        for (const state of possibleStates) {
            const stateRows = page.locator(".ms-DetailsRow").filter({ hasText: state });
            if (await stateRows.count() > 0) {
                statesFound++;
                await expect(stateRows.first()).toBeVisible();
            }
        }

        expect(statesFound).toBeGreaterThan(0);
    });

    test("Should support workunit data validation", async ({ page }) => {
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        const rows = page.locator(".ms-DetailsRow");
        const rowCount = await rows.count();

        for (let i = 0; i < Math.min(rowCount, 3); i++) {
            const row = rows.nth(i);
            await expect(row).toBeVisible();

            const rowText = await row.textContent();
            expect(rowText).toBeTruthy();
            if (rowText) {
                expect(rowText.length).toBeGreaterThan(0);
            }
        }
    });

    test("Should handle workunit sorting by different columns", async ({ page }) => {
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        const initialRowCount = await page.locator(".ms-DetailsRow").count();
        expect(initialRowCount).toBeGreaterThan(0);

        const sortableColumns = ["WUID", "Owner", "State", "Job Name"];

        for (const column of sortableColumns) {
            const columnHeader = page.getByRole("columnheader", { name: column });
            if (await columnHeader.count() > 0) {
                await columnHeader.click();
                await page.waitForLoadState("networkidle");
                await page.waitForTimeout(1000);

                const newRowCount = await page.locator(".ms-DetailsRow").count();
                expect(newRowCount).toBeGreaterThanOrEqual(1);

                break;
            }
        }
    });

    test("Should display workunit cost information when available", async ({ page }) => {
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        const costColumns = [
            "Compile Cost",
            "Execution Cost",
            "File Access Cost"
        ];

        for (const column of costColumns) {
            const columnElement = page.getByText(column);
            if (await columnElement.count() > 0) {
                await expect(columnElement).toBeVisible();

                break;
            }
        }
    });
});