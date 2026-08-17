import { test, expect, openOverflowMenu } from "./fixtures";

test.describe("V9 Cluster Processes", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/operations/processes");
        await page.waitForLoadState("networkidle");
        await openOverflowMenu(page);
    });

    test("Page loads with command bar and grid", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeVisible();
        await expect(page.getByRole("grid")).toBeVisible();
    });

    test("Column headers are visible", async ({ page }) => {
        for (const column of ["Name", "Domain", "Platform", "Worker Number", "Channel", "Directory", "Log Directory"]) {
            await expect(page.getByRole("columnheader", { name: column, exact: true })).toBeVisible();
        }
    });

    test("Preflight button is disabled when nothing is selected", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toHaveAttribute("aria-disabled", "true");
    });

    test("Table cluster rows load with data", async ({ page }) => {
        // Header row is index 0; first cluster row is index 1
        await expect(page.getByRole("row").nth(1)).toBeVisible();
    });

    test("Refresh reloads the table", async ({ page }) => {
        await page.getByRole("menuitem", { name: "Refresh" }).click();
        await page.waitForLoadState("networkidle");
        await expect(page.getByRole("grid")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toHaveAttribute("aria-disabled", "true");
    });

    test("Cluster row can be expanded to lazy-load machine children", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();

        if (await expandButton.isVisible()) {
            const rowCountBefore = await page.getByRole("row").count();
            await expandButton.click();
            // Children load asynchronously via TpMachineQuery — wait for network
            await page.waitForLoadState("networkidle");
            await expect(page.getByRole("row")).not.toHaveCount(rowCountBefore, { timeout: 10000 });
        }
    });

    test("Expanded cluster can be collapsed", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();

        if (await expandButton.isVisible()) {
            const rowCountBefore = await page.getByRole("row").count();
            await expandButton.click();
            await page.waitForLoadState("networkidle");
            // Wait for children to appear
            await expect(page.getByRole("row")).not.toHaveCount(rowCountBefore, { timeout: 10000 });

            await expandButton.click();
            // All machine children should be hidden again
            await expect(page.getByRole("row")).toHaveCount(rowCountBefore, { timeout: 5000 });
        }
    });

    test("Selecting a machine row enables Preflight", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();

        if (await expandButton.isVisible()) {
            await expandButton.click();
            await page.waitForLoadState("networkidle");
            // Machine rows appear after expansion — wait for at least one checkbox
            const firstMachineCheckbox = page.locator("input[type='checkbox']").first();
            await expect(firstMachineCheckbox).toBeAttached({ timeout: 10000 });

            await firstMachineCheckbox.check();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).not.toHaveAttribute("aria-disabled", "true");
        }
    });

    test("Deselecting a machine row disables Preflight again", async ({ page }) => {
        const firstDataRow = page.getByRole("row").nth(1);
        const expandButton = firstDataRow.getByRole("button").first();

        if (await expandButton.isVisible()) {
            await expandButton.click();
            await page.waitForLoadState("networkidle");
            const firstMachineCheckbox = page.locator("input[type='checkbox']").first();
            await expect(firstMachineCheckbox).toBeAttached({ timeout: 10000 });

            await firstMachineCheckbox.check();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).not.toHaveAttribute("aria-disabled", "true");

            await firstMachineCheckbox.uncheck();
            await expect(page.getByRole("menuitem", { name: "Preflight" })).toHaveAttribute("aria-disabled", "true");
        }
    });
});
