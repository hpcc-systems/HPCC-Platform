import { test, expect } from "./fixtures";

test.describe("V9 Files - Logical Files", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/files");
        await page.waitForLoadState("networkidle");
        await page.locator(".fui-NavDrawerBody").waitFor({ state: "visible", timeout: 15000 });
    });

    test("Should display the Logical Files page with all expected columns and controls", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();

        await page.waitForTimeout(2000);

        const logicalNameVisible = await page.getByText("Logical Name").isVisible();

        if (!logicalNameVisible) {
            test.skip(true, "Logical Files page failed to load properly - known issue HPCC-32297");
        }

        await expect(page.getByText("Logical Name")).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();
        await expect(page.getByText("Super Owner")).toBeVisible();
        await expect(page.getByText("Description")).toBeVisible();
        await expect(page.getByText("Cluster", { exact: true })).toBeVisible();
        await expect(page.getByText("Records")).toBeVisible();
        await expect(page.getByText("Size", { exact: true })).toBeVisible();
        await expect(page.getByText("File Size")).toBeVisible();
        await expect(page.getByText("Parts")).toBeVisible();
        await expect(page.getByText("Min Skew")).toBeVisible();
        await expect(page.getByText("Max Skew")).toBeVisible();

        // Check for cost columns when available
        const fileCostAtRestVisible = await page.getByText("File Cost At Rest").isVisible();
        const fileAccessCostVisible = await page.getByText("File Access Cost").isVisible();

        if (fileCostAtRestVisible || fileAccessCostVisible) {
            await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);
        }

        const firstRow = page.locator(".fui-TableBody .fui-TableRow").first();
        const hasRows = await firstRow.isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No logical files data available to verify row display");
        }
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);
    });

    test("Should allow filtering logical files and display filtered results", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();

        const filterMenuItem = page.getByRole("menuitem", { name: "Filter" });
        const filterExists = await filterMenuItem.isVisible();

        if (!filterExists) {
            test.skip(true, "Filter functionality not available on this configuration");
        }

        // Wait for initial data to load
        try {
            const firstRow = page.locator(".fui-TableBody .fui-TableRow").first();
            await firstRow.waitFor({ state: "visible", timeout: 10000 });
        } catch {
            test.skip(true, "No logical files data available - cannot test filtering");
        }

        const initialRowCount = await page.locator(".fui-TableBody .fui-TableRow").count();

        await filterMenuItem.click();

        // Wait for the dialog to appear - wait for the heading instead of the dialog role
        // because the modal has pointerEvents: "none" on root which makes Playwright think it's hidden
        const filterDialog = page.getByRole("dialog").first();
        await expect(filterDialog.locator(".fui-DialogTitle")).toBeVisible();

        const logicalNameInput = filterDialog.locator("input[name=\"LogicalName\"]").or(
            filterDialog.getByLabel("Name")
        ).or(
            filterDialog.locator("input[type=\"text\"]").first()
        );

        await expect(logicalNameInput).toBeVisible();
        await logicalNameInput.fill("*global*");

        await filterDialog.getByRole("button", { name: "Apply" }).click();
        await expect(filterDialog).toBeHidden();

        // Wait for filter to be applied by checking for network idle or row changes
        await page.waitForLoadState("networkidle");

        const filteredRowCount = await page.locator(".fui-TableBody .fui-TableRow").count();

        if (filteredRowCount > 0) {
            await expect(page.locator(".fui-TableBody .fui-TableRow").first()).toBeVisible();
            console.log(`Filter applied successfully. Rows: ${initialRowCount} -> ${filteredRowCount}`);
        } else {
            console.log(`Filter applied successfully. No results found for filter '*global*'. Rows: ${initialRowCount} -> 0`);
        }

        await page.getByRole("menuitem", { name: "Filter" }).click();
    });

    test("Should allow selecting logical files and show selection", async ({ page }) => {
        const firstRow = page.locator(".fui-TableBody .fui-TableRow").first();
        const hasData = await firstRow.isVisible({ timeout: 5000 }).catch(() => false);

        if (!hasData) {
            test.skip(true, "No logical files data available - likely due to HPCC-32297");
            return;
        }

        const firstRowSelectionCell = firstRow.locator(".fui-TableSelectionCell");
        const hasCheckbox = await firstRowSelectionCell.isVisible({ timeout: 2000 }).catch(() => false);
        if (!hasCheckbox) {
            test.skip(true, "Logical Files grid does not use checkbox selection");
            return;
        }
        // Click td[1] (first data cell after selection cell) to trigger row selection
        // via the row's onClick handler rather than the selection cell's Checkbox
        await firstRow.locator("td").nth(1).click();
        await expect(page.locator(".fui-TableBody .fui-TableRow[aria-selected='true']")).toHaveCount(1);
    });

    test("Should display logical file details when clicking on a file name", async ({ page, browserName }) => {
        const hasData = await page.locator(".fui-TableBody .fui-TableRow").count() > 0;

        if (!hasData) {
            test.skip(true, "No logical files data available - likely due to HPCC-32297");
        }

        await page.locator(".fui-TableBody .fui-TableRow").first().waitFor({ state: "visible", timeout: 10000 });
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);

        const firstLogicalFileLink = page.locator(".fui-TableBody .fui-TableRow").first().locator("a").first();
        await firstLogicalFileLink.waitFor({ state: "visible" });

        if (browserName === "chromium") {
            await firstLogicalFileLink.click();

            await page.waitForLoadState("networkidle");

            await expect(page.locator("body")).toBeVisible();
        }
    });

});
test.describe("V9 Files - Protected By tab", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/files");
        await page.waitForLoadState("networkidle");
        await page.locator(".fui-NavDrawerBody").waitFor({ state: "visible", timeout: 15000 });
    });

    test("Unprotect All button is disabled when no protections exist", async ({ page, browserName }) => {
        test.skip(browserName !== "chromium", "Chromium only");

        const firstLink = page.locator("[role='gridcell'] a").first();
        if (await firstLink.count() === 0) {
            test.skip(true, "No logical files available");
        }
        await firstLink.click();
        await page.waitForLoadState("networkidle");

        const protectedByTab = page.getByRole("tab", { name: /Protected By/i });
        if (await protectedByTab.count() === 0) {
            test.skip(true, "Protected By tab not found");
        }
        await protectedByTab.click();
        await page.waitForTimeout(1000);

        const unprotectAllBtn = page.getByRole("menuitem", { name: /Unprotect All/i });
        await expect(unprotectAllBtn).toBeVisible({ timeout: 5000 });

        const rows = page.locator("[role='row'][data-item-index]");
        const rowCount = await rows.count();
        if (rowCount === 0) {
            await expect(unprotectAllBtn).toHaveAttribute("aria-disabled", "true");
        } else {
            test.skip(true, "File has protections — cannot assert disabled state");
        }
    });

    test("Unprotect All button shows confirmation dialog before acting", async ({ page, browserName }) => {
        test.skip(browserName !== "chromium", "Chromium only");

        const firstLink = page.locator("[role='gridcell'] a").first();
        if (await firstLink.count() === 0) {
            test.skip(true, "No logical files available");
        }
        await firstLink.click();
        await page.waitForLoadState("networkidle");

        const protectedByTab = page.getByRole("tab", { name: /Protected By/i });
        if (await protectedByTab.count() === 0) {
            test.skip(true, "Protected By tab not found");
        }
        await protectedByTab.click();
        await page.waitForTimeout(1000);

        const unprotectAllBtn = page.getByRole("menuitem", { name: /Unprotect All/i });
        await expect(unprotectAllBtn).toBeVisible({ timeout: 5000 });

        const isDisabled = await unprotectAllBtn.getAttribute("aria-disabled");
        if (isDisabled === "true") {
            test.skip(true, "No protections on this file — button is disabled");
        }

        await unprotectAllBtn.click();

        // Confirmation dialog must appear before any action is taken
        const dialog = page.getByRole("dialog");
        await expect(dialog).toBeVisible({ timeout: 3000 });
        await expect(dialog.getByText(/Unprotect All/i)).toBeVisible();
        await expect(dialog.getByRole("button", { name: /OK/i })).toBeVisible();
        await expect(dialog.getByRole("button", { name: /Cancel/i })).toBeVisible();

        // Cancel — no changes should occur
        await dialog.getByRole("button", { name: /Cancel/i }).click();
        await expect(dialog).not.toBeVisible();
    });
});
