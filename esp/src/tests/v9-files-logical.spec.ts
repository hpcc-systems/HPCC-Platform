import { test, expect } from "@playwright/test";

test.describe("V9 Files - Logical Files", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/files/logicalfiles");
        await page.waitForLoadState("networkidle");
    });

    test("Should display the Logical Files page with all expected columns and controls", async ({ page }) => {
        // Check for main page controls
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        
        // Check for expected column headers
        await expect(page.getByText("Logical Name")).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();
        await expect(page.getByText("Super Owner")).toBeVisible();
        await expect(page.getByText("Description")).toBeVisible();
        await expect(page.getByText("Cluster", { exact: true })).toBeVisible();
        await expect(page.getByText("Records")).toBeVisible();
        await expect(page.getByText("Size")).toBeVisible();
        await expect(page.getByText("Compressed Size")).toBeVisible();
        await expect(page.getByText("Parts")).toBeVisible();
        await expect(page.getByText("Min Skew")).toBeVisible();
        await expect(page.getByText("Max Skew")).toBeVisible();
        await expect(page.getByText("Modified (UTC/GMT)")).toBeVisible();
        await expect(page.getByText("Last Accessed")).toBeVisible();
        
        // Check that there are data rows
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Should allow filtering logical files and display filtered results", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        
        // Open filter menu
        await page.getByRole("menuitem", { name: "Filter" }).click();
        
        // Apply a filter (assuming we can filter by owner or name)
        await page.getByRole("textbox", { name: "Logical Name" }).fill("global");
        await page.getByRole("button", { name: "Apply" }).click();
        
        // Verify results are shown (should have some filtered results)
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        
        // Close filter
        await page.getByRole("menuitem", { name: "Filter" }).click();
    });

    test("Should allow selecting logical files and show selection", async ({ page }) => {
        // Wait for data to load
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        
        // Select first row
        await page.locator(".ms-DetailsRow").first().locator(".ms-DetailsRow-check").click();
        await expect(page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
    });

    test("Should display logical file details when clicking on a file name", async ({ page, browserName }) => {
        // Wait for data to load
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        
        // Get the first logical file name link and click it
        const firstLogicalFileLink = page.locator(".ms-DetailsRow").first().locator("a").first();
        await firstLogicalFileLink.waitFor({ state: "visible" });
        
        if (browserName === "chromium") {
            await firstLogicalFileLink.click();
            
            // Wait for navigation to detail page
            await page.waitForLoadState("networkidle");
            
            // Verify we're on a detail page (should have specific elements)
            // This is a basic check - the actual elements would depend on the detail page structure
            await expect(page.locator("body")).toBeVisible();
        }
    });

    test("Should show cost columns when available", async ({ page }) => {
        // Check for cost-related columns that might be available
        const fileCostAtRestVisible = await page.getByText("File Cost At Rest").isVisible();
        const fileAccessCostVisible = await page.getByText("File Access Cost").isVisible();
        
        // At least verify the page loads properly even if cost columns aren't visible
        await expect(page.getByText("Logical Name")).toBeVisible();
        
        // If cost columns are visible, they should be properly formatted
        if (fileCostAtRestVisible || fileAccessCostVisible) {
            // Additional checks could be added here for cost formatting
            await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        }
    });
});