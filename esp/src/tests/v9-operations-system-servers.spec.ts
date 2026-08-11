import { test, expect } from "./fixtures";

test.describe("V9 System Servers", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/operations/servers");
        await page.waitForLoadState("networkidle");
    });

    test("Page loads with command bar and grid", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeVisible();
        await expect(page.getByRole("grid")).toBeVisible();
    });

    test("Column headers are visible", async ({ page }) => {
        for (const column of ["Name", "Queue", "Node", "Network Address", "Directory"]) {
            await expect(page.getByRole("columnheader", { name: column, exact: true })).toBeVisible();
        }
    });

    test("Preflight button is disabled when nothing is selected", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toHaveAttribute("aria-disabled", "true");
    });

    test("Table server rows load with data", async ({ page }) => {
        // Header row is index 0; first server row is index 1
        await expect(page.getByRole("row").nth(1)).toBeVisible();
    });

    test("Refresh reloads the table", async ({ page }) => {
        await page.getByRole("menuitem", { name: "Refresh" }).click();
        await page.waitForLoadState("networkidle");
        await expect(page.getByRole("grid")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toHaveAttribute("aria-disabled", "true");
    });

    test("Server row can be expanded to show machine children", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();

        if (await expandButton.isVisible()) {
            const rowCountBefore = await page.getByRole("row").count();
            await expandButton.click();
            // Children are pre-loaded — no extra network request needed
            await expect(page.getByRole("row")).not.toHaveCount(rowCountBefore, { timeout: 5000 });
        }
    });

    test("Expanded server can be collapsed", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();

        if (await expandButton.isVisible()) {
            const rowCountBefore = await page.getByRole("row").count();
            await expandButton.click();
            await expect(page.getByRole("row")).not.toHaveCount(rowCountBefore, { timeout: 5000 });

            await expandButton.click();
            await expect(page.getByRole("row")).toHaveCount(rowCountBefore, { timeout: 5000 });
        }
    });

    test("Selecting a machine row enables Preflight", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();

        if (await expandButton.isVisible()) {
            await expandButton.click();
            // Children are pre-loaded; checkboxes appear immediately for selectable machines
            const firstCheckbox = page.locator("input[type='checkbox']").first();
            await expect(firstCheckbox).toBeAttached({ timeout: 5000 });

            await firstCheckbox.check();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).not.toHaveAttribute("aria-disabled", "true");
        }
    });

    test("Deselecting a machine row disables Preflight again", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();

        if (await expandButton.isVisible()) {
            await expandButton.click();
            const firstCheckbox = page.locator("input[type='checkbox']").first();
            await expect(firstCheckbox).toBeAttached({ timeout: 5000 });

            await firstCheckbox.check();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).not.toHaveAttribute("aria-disabled", "true");

            await firstCheckbox.uncheck();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).toHaveAttribute("aria-disabled", "true");
        }
    });
});
