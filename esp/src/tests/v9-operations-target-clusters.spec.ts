import { test, expect } from "./fixtures";

test.describe("V9 Target Clusters", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/operations/clusters");
        await page.waitForLoadState("networkidle");
    });

    test("Page loads with command bar and grid", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeVisible();
        await expect(page.getByRole("grid")).toBeVisible();
    });

    test("Column headers are visible", async ({ page }) => {
        for (const column of ["Name", "Node", "Platform", "Directory"]) {
            await expect(page.getByRole("columnheader", { name: column, exact: true })).toBeVisible();
        }
    });

    test("Preflight button is disabled when nothing is selected", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeDisabled();
    });

    test("Table rows load with data", async ({ page }) => {
        // Header row is index 0; first data row is index 1
        await expect(page.getByRole("row").nth(1)).toBeVisible();
    });

    test("Selecting a row enables Preflight", async ({ page }) => {
        const checkboxes = page.locator("input[type='checkbox']");
        const count = await checkboxes.count();
        if (count > 1) {
            // index 0 is the header select-all; index 1 is the first data row checkbox
            await checkboxes.nth(1).check();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeEnabled();
        }
    });

    test("Deselecting a row disables Preflight again", async ({ page }) => {
        const checkboxes = page.locator("input[type='checkbox']");
        const count = await checkboxes.count();
        if (count > 1) {
            await checkboxes.nth(1).check();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeEnabled();
            await checkboxes.nth(1).uncheck();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeDisabled();
        }
    });

    test("Select all checkbox selects all rows", async ({ page }) => {
        const checkboxes = page.locator("input[type='checkbox']");
        const count = await checkboxes.count();
        if (count > 1) {
            // First checkbox is the header select-all
            await checkboxes.first().check();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeEnabled();
            // All data-row checkboxes should now be checked
            for (let i = 1; i < count; i++) {
                await expect(checkboxes.nth(i)).toBeChecked();
            }
        }
    });

    test("Refresh reloads the table", async ({ page }) => {
        await page.getByRole("menuitem", { name: "Refresh" }).click();
        await page.waitForLoadState("networkidle");
        // After refresh the table should still be present and Preflight still disabled
        await expect(page.getByRole("grid")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeDisabled();
    });

    test("Cluster row can be expanded to show components", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();
        if (await expandButton.isVisible()) {
            const rowCountBefore = await page.getByRole("row").count();
            await expandButton.click();
            // The exact number of children is data-dependent; just assert more rows appeared
            await expect(page.getByRole("row")).not.toHaveCount(rowCountBefore, { timeout: 5000 });
        }
    });

    test("Expanded cluster can be collapsed", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();
        if (await expandButton.isVisible()) {
            const rowCountBefore = await page.getByRole("row").count();
            await expandButton.click();
            // Wait for children to appear
            await expect(page.getByRole("row")).not.toHaveCount(rowCountBefore, { timeout: 5000 });

            await expandButton.click();
            // All children should be hidden again — back to the original count
            await expect(page.getByRole("row")).toHaveCount(rowCountBefore, { timeout: 5000 });
        }
    });
});
