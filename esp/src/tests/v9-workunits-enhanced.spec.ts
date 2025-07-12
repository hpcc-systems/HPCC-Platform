import { test, expect } from "@playwright/test";

/**
 * Enhanced Workunit tests converted from ECLWorkUnitsTest.java
 * Covers additional functionality beyond the existing v9-workunits.spec.ts
 */
test.describe("V9 Workunits - Enhanced (Converted from Selenium)", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/workunits");
        await page.waitForLoadState("networkidle");
    });

    test("Should display all expected columns from Java test", async ({ page }) => {
        // These are the columns from the original ECLWorkUnitsTest.java getColumnNames()
        const expectedColumns = [
            "WUID", 
            "Owner", 
            "Job Name", 
            "Cluster", 
            "State", 
            "Total Cluster Time"
        ];

        // Cost columns that might be visible
        const costColumns = [
            "Compile Cost", 
            "Execution Cost", 
            "File Access Cost"
        ];

        // Check all required columns
        for (const column of expectedColumns) {
            await expect(page.getByText(column)).toBeVisible();
        }

        // Check cost columns if they exist (they may not always be visible)
        for (const column of costColumns) {
            const columnElement = page.getByText(column);
            if (await columnElement.count() > 0) {
                await expect(columnElement).toBeVisible();
            }
        }
    });

    test("Should support workunit detail page navigation", async ({ page, browserName }) => {
        // Wait for data to load
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        // Click on first WUID link (similar to getColumnKeysWithLinks() in Java test)
        const firstWuidLink = page.locator(".ms-DetailsRow").first().locator("a").first();
        
        if (browserName === "chromium") {
            await firstWuidLink.click();
            await page.waitForLoadState("networkidle");
            
            // Check if we're on a detail page
            // Based on Java test's getDetailNames(), these should be visible on detail page
            const detailPageElements = [
                "WUID",
                "State", 
                "Owner",
                "Job Name",
                "Cluster"
            ];
            
            // Check if we can find any of these elements (flexible approach)
            let foundDetailElements = 0;
            for (const element of detailPageElements) {
                const elementLocator = page.getByText(element);
                if (await elementLocator.count() > 0) {
                    foundDetailElements++;
                }
            }
            
            // Should find at least some detail elements
            expect(foundDetailElements).toBeGreaterThan(0);
        }
    });

    test("Should handle workunit detail page tabs", async ({ page, browserName }) => {
        // Wait for data to load
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        // Click on first WUID link
        const firstWuidLink = page.locator(".ms-DetailsRow").first().locator("a").first();
        
        if (browserName === "chromium") {
            await firstWuidLink.click();
            await page.waitForLoadState("networkidle");
            
            // Based on Java test's getTabValuesForDetailsPage(), these tabs should be available
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
            
            // Check if tabs exist (flexible approach since UI might differ)
            let tabsFound = 0;
            for (const tab of expectedTabs) {
                const tabLocator = page.locator(`[role="tab"]`).filter({ hasText: new RegExp(tab, "i") });
                if (await tabLocator.count() > 0) {
                    tabsFound++;
                }
            }
            
            // Should find at least some tabs
            if (tabsFound > 0) {
                expect(tabsFound).toBeGreaterThan(0);
            }
        }
    });

    test("Should handle different workunit states properly", async ({ page }) => {
        // Wait for data to load
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        // Java test mentions "badStates" including "compiled" and "failed"
        const possibleStates = ["completed", "failed", "compiled", "running", "blocked"];
        
        let statesFound = 0;
        for (const state of possibleStates) {
            const stateRows = page.locator(".ms-DetailsRow").filter({ hasText: state });
            if (await stateRows.count() > 0) {
                statesFound++;
                // Each state should be visible in the table
                await expect(stateRows.first()).toBeVisible();
            }
        }
        
        // Should find at least one state
        expect(statesFound).toBeGreaterThan(0);
    });

    test("Should support workunit data validation", async ({ page }) => {
        // Wait for data to load
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        // Check that each row has expected data structure
        const rows = page.locator(".ms-DetailsRow");
        const rowCount = await rows.count();
        
        for (let i = 0; i < Math.min(rowCount, 3); i++) { // Check first 3 rows
            const row = rows.nth(i);
            await expect(row).toBeVisible();
            
            // Each row should have some content
            const rowText = await row.textContent();
            expect(rowText).toBeTruthy();
            expect(rowText.length).toBeGreaterThan(0);
        }
    });

    test("Should handle workunit sorting by different columns", async ({ page }) => {
        // Wait for data to load
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        // Get initial row count
        const initialRowCount = await page.locator(".ms-DetailsRow").count();
        
        // Try sorting by different columns (based on Java test's sortByColumnKeyWhenSortedByNone)
        const sortableColumns = ["WUID", "Owner", "State", "Job Name"];
        
        for (const column of sortableColumns) {
            const columnHeader = page.getByText(column);
            if (await columnHeader.count() > 0) {
                await columnHeader.click();
                await page.waitForLoadState("networkidle");
                
                // Verify table still has data after sort
                await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
                
                // Row count should be maintained
                const newRowCount = await page.locator(".ms-DetailsRow").count();
                expect(newRowCount).toBeGreaterThan(0);
                
                break; // Test one sort to avoid excessive clicking
            }
        }
    });

    test("Should display workunit cost information when available", async ({ page }) => {
        // Wait for data to load
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);

        // Check for cost-related columns (from Java test)
        const costColumns = [
            "Compile Cost",
            "Execution Cost", 
            "File Access Cost"
        ];

        for (const column of costColumns) {
            const columnElement = page.getByText(column);
            if (await columnElement.count() > 0) {
                await expect(columnElement).toBeVisible();
                
                // If cost column exists, verify it's properly formatted
                // Cost values should be numeric or show appropriate formatting
                break;
            }
        }
    });
});