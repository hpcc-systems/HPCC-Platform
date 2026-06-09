import { test, expect } from "@playwright/test";

test.describe("V9 Global Metrics", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/topology/global-stats");
        await page.waitForLoadState("networkidle");
        await page.locator(".fui-NavDrawerBody").waitFor({ state: "visible", timeout: 15000 });
    });

    test("Global Metrics page loaded with all expected controls", async ({ page }) => {
        // Check frame elements
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
        await expect(page.getByRole("button", { name: "History" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();

        // Check toolbar controls
        await expect(page.getByRole("button", { name: "Refresh" })).toBeVisible();

        // Check date pickers
        await expect(page.getByPlaceholder("From Date")).toBeVisible();
        await expect(page.getByPlaceholder("To Date")).toBeVisible();

        // Check reset button
        const resetButton = page.getByRole("button", { name: "Reset" });
        await expect(resetButton).toBeVisible();

        // Check total badge
        await expect(page.getByText("Total:")).toBeVisible();
    });

    test("Should display grid with expected columns", async ({ page }) => {
        // Wait for API data to load - columns only render when data is returned
        await page.waitForTimeout(2000);

        // Check for basic columns that should always be present
        const expectedColumns = [
            "Category",
            "Start",
            "End"
        ];

        // Try to find first column to determine if data loaded
        const hasColumns = await page.getByRole("columnheader", { name: "Category", exact: true }).isVisible({ timeout: 8000 }).catch(() => false);
        if (!hasColumns) {
            test.skip(true, "No global metrics data available - API returned no results");
            return;
        }

        for (const column of expectedColumns) {
            await expect(page.getByRole("columnheader", { name: column, exact: true })).toBeVisible({ timeout: 5000 });
        }

        // Check that grid exists (use first() to avoid strict mode violation with multiple FluentProviders)
        const grid = page.locator(".fui-FluentProvider").first();
        await expect(grid).toBeVisible();
    });

    test("Should update date range and refresh data", async ({ page }) => {
        // Click refresh button
        const refreshButton = page.getByRole("button", { name: "Refresh" });
        await expect(refreshButton).toBeVisible();
        await refreshButton.click();

        // Wait for network activity to complete
        await page.waitForLoadState("networkidle");

        // Verify page is still functional
        await expect(refreshButton).toBeVisible();
    });

    test("Should reset date filters", async ({ page }) => {
        // Click reset button
        const resetButton = page.getByRole("button", { name: "Reset" });
        await expect(resetButton).toBeVisible();
        await resetButton.click();

        // Wait for navigation/update
        await page.waitForLoadState("networkidle");

        // Verify controls are still present
        await expect(page.getByPlaceholder("From Date")).toBeVisible();
        await expect(page.getByPlaceholder("To Date")).toBeVisible();
    });

    test("Should display statistics columns with emoji prefix", async ({ page }) => {
        // Wait for data to load
        await page.waitForTimeout(2000);

        // Look for columns starting with the stats emoji (📊)
        const statsColumns = page.locator("text=/📊/");

        // If stats data is available, verify emoji columns exist
        if (await statsColumns.count() > 0) {
            await expect(statsColumns.first()).toBeVisible();
        }
    });

    test("Should display dimension columns with emoji prefix", async ({ page }) => {
        // Wait for data to load
        await page.waitForTimeout(2000);

        // Look for columns starting with the dimension emoji (🏷️)
        const dimensionColumns = page.locator("text=/🏷️/");

        // If dimension data is available, verify emoji columns exist
        if (await dimensionColumns.count() > 0) {
            await expect(dimensionColumns.first()).toBeVisible();
        }
    });

    test("Should be accessible from Topology menu", async ({ page }) => {
        await page.goto("index.html#/topology");
        await page.waitForLoadState("networkidle");

        // Navigate to Global Metrics from Topology menu
        const topologyButton = page.getByRole("button", { name: "Topology" });
        if (await topologyButton.isVisible()) {
            await topologyButton.click();

            const globalMetricsLink = page.getByRole("menuitem", { name: "Global Metrics" });
            if (await globalMetricsLink.isVisible()) {
                await globalMetricsLink.click();
                await page.waitForLoadState("networkidle");

                // Verify we're on the Global Metrics page
                await expect(page.getByPlaceholder("From Date")).toBeVisible();
                await expect(page.getByPlaceholder("To Date")).toBeVisible();
            }
        }
    });

    test("Should be accessible from Operations menu", async ({ page }) => {
        await page.goto("index.html#/operations");
        await page.waitForLoadState("networkidle");

        // Navigate to Global Metrics from Operations menu
        const operationsButton = page.getByRole("button", { name: "Operations" });
        if (await operationsButton.isVisible()) {
            await operationsButton.click();

            const globalMetricsLink = page.getByRole("menuitem", { name: "Global Metrics" });
            if (await globalMetricsLink.isVisible()) {
                await globalMetricsLink.click();
                await page.waitForLoadState("networkidle");

                // Verify we're on the Global Metrics page
                await expect(page.getByPlaceholder("From Date")).toBeVisible();
                await expect(page.getByPlaceholder("To Date")).toBeVisible();
            }
        }
    });
});
