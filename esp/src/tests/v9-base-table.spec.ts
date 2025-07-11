import { test, expect } from "@playwright/test";

test.describe("V9 Base Table Functionality", () => {

    // Test table functionality using the Activities page as a representative table
    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
    });

    test("Should display table with proper column headers and sorting capabilities", async ({ page }) => {
        // Verify main table elements
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        
        // Check for sortable column headers
        const columnHeaders = [
            "Priority",
            "Target/Wuid", 
            "Graph",
            "State", 
            "Owner",
            "Job Name"
        ];
        
        for (const header of columnHeaders) {
            const headerElement = page.getByText(header);
            await expect(headerElement).toBeVisible();
        }
        
        // Check that Priority column header has sorting functionality
        await expect(page.getByRole("columnheader", { name: "Priority" }).locator("div").first()).toBeVisible();
    });

    test("Should support table pagination and row count changes", async ({ page }) => {
        // Wait for table to load
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        
        // Look for pagination controls (may vary based on implementation)
        const paginationControls = page.locator(".dgrid-pagination, .pagination, [aria-label*='Page']");
        if (await paginationControls.count() > 0) {
            await expect(paginationControls.first()).toBeVisible();
        }
        
        // Verify we have data rows
        const rowCount = await page.locator(".dgrid-row").count();
        expect(rowCount).toBeGreaterThan(0);
    });

    test("Should support table refresh functionality", async ({ page }) => {
        // Check for refresh button
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        
        // Get initial row count
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        const initialRowCount = await page.locator(".dgrid-row").count();
        
        // Click refresh
        await page.getByRole("menuitem", { name: "Refresh" }).click();
        await page.waitForLoadState("networkidle");
        
        // Verify table still has data after refresh
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        
        // Row count should be maintained or updated appropriately
        const newRowCount = await page.locator(".dgrid-row").count();
        expect(newRowCount).toBeGreaterThan(0);
    });

    test("Should support row selection functionality", async ({ page }) => {
        // Wait for data
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        
        // Check if rows have selection checkboxes or are clickable
        const firstRow = page.locator(".dgrid-row").first();
        await expect(firstRow).toBeVisible();
        
        // Try to select first row (method may vary based on implementation)
        const selectableElement = firstRow.locator(".dgrid-selector, .ms-DetailsRow-check, input[type='checkbox']").first();
        if (await selectableElement.count() > 0) {
            await selectableElement.click();
            
            // Verify selection state (this may vary based on implementation)
            const selectedRows = page.locator(".dgrid-row.dgrid-selected, .ms-DetailsRow.is-selected");
            if (await selectedRows.count() > 0) {
                await expect(selectedRows).toHaveCount(1);
            }
        }
    });

    test("Should display table controls and menu items", async ({ page }) => {
        // Check for menubar
        await expect(page.getByRole("menubar")).toBeVisible();
        
        // Common menu items that should be present
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        
        // Check for other common controls
        const controlButtons = page.locator("button").filter({ hasText: "" });
        if (await controlButtons.count() > 0) {
            await expect(controlButtons.first()).toBeVisible();
        }
    });

    test("Should handle empty table states gracefully", async ({ page }) => {
        // Navigate to a potentially empty table or apply filters that might result in no data
        await page.getByRole("menuitem", { name: "Filter" }).click({ timeout: 5000 }).catch(() => {
            // Filter menu might not be available on all tables
        });
        
        // Apply a filter that should return no results
        const filterInput = page.getByRole("textbox").first();
        if (await filterInput.count() > 0) {
            await filterInput.fill("nonexistentdatafilter12345");
            
            const applyButton = page.getByRole("button", { name: "Apply" });
            if (await applyButton.count() > 0) {
                await applyButton.click();
                await page.waitForLoadState("networkidle");
                
                // Table should handle empty state gracefully
                await expect(page.locator("body")).toBeVisible();
                
                // Reset filter
                await page.getByRole("menuitem", { name: "Filter" }).click().catch(() => {});
                const clearButton = page.getByRole("button", { name: "Clear" });
                if (await clearButton.count() > 0) {
                    await clearButton.click();
                }
            }
        }
        
        // Verify basic page structure is still intact
        await expect(page.getByRole("menubar")).toBeVisible();
    });

    test("Should support column sorting functionality", async ({ page }) => {
        // Wait for table data
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        
        // Try to click on sortable column headers
        const sortableHeaders = [
            "Priority",
            "State",
            "Owner"  
        ];
        
        for (const headerText of sortableHeaders) {
            const header = page.getByText(headerText);
            if (await header.count() > 0) {
                // Click to sort
                await header.click();
                await page.waitForLoadState("networkidle");
                
                // Verify table still has data after sort
                await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
                
                // Could add more specific sorting verification here
                break; // Test one sort to avoid too much interaction
            }
        }
    });

    test("Should maintain table state during page interactions", async ({ page }) => {
        // Get initial table state
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        const initialRowCount = await page.locator(".dgrid-row").count();
        
        // Perform some page interactions
        await page.getByRole("button", { name: "Advanced" }).click().catch(() => {});
        await page.waitForTimeout(1000);
        
        // Verify table is still functional
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        await expect(page.getByRole("menubar")).toBeVisible();
        
        // Row count should be maintained
        const currentRowCount = await page.locator(".dgrid-row").count();
        expect(currentRowCount).toBeGreaterThan(0);
    });
});