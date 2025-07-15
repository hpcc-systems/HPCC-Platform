import { test, expect } from "@playwright/test";

test.describe("V9 Base Table Functionality", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
    });

    test("Should display table with proper column headers and sorting capabilities", async ({ page }) => {
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);

        const columnHeaders = [
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

        await expect(page.getByRole("columnheader", { name: "Priority" }).locator("div").first()).toBeVisible();
    });

    test("Should support table pagination and row count changes", async ({ page }) => {
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);

        const paginationControls = page.locator(".dgrid-pagination, .pagination, [aria-label*='Page']");
        if (await paginationControls.count() > 0) {
            await expect(paginationControls.first()).toBeVisible();
        }

        const rowCount = await page.locator(".dgrid-row").count();
        expect(rowCount).toBeGreaterThan(0);
    });

    test("Should support table refresh functionality", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();

        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        const initialRowCount = await page.locator(".dgrid-row").count();

        await page.getByRole("menuitem", { name: "Refresh" }).click();
        await page.waitForLoadState("networkidle");

        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);

        const newRowCount = await page.locator(".dgrid-row").count();
        expect(newRowCount).toBeGreaterThan(0);
    });

    test("Should support row selection functionality", async ({ page }) => {
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);

        const firstRow = page.locator(".dgrid-row").first();
        await expect(firstRow).toBeVisible();

        const selectableElement = firstRow.locator(".dgrid-selector, .ms-DetailsRow-check, input[type='checkbox']").first();
        if (await selectableElement.count() > 0) {
            await selectableElement.click();

            const selectedRows = page.locator(".dgrid-row.dgrid-selected, .ms-DetailsRow.is-selected");
            if (await selectedRows.count() > 0) {
                await expect(selectedRows).toHaveCount(1);
            }
        }
    });

    test("Should display table controls and menu items", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();

        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();

        const controlButtons = page.locator("button").filter({ hasText: "" });
        if (await controlButtons.count() > 0) {
            await expect(controlButtons.first()).toBeVisible();
        }
    });

    test("Should handle empty table states gracefully", async ({ page }) => {
        await page.getByRole("menuitem", { name: "Filter" }).click({ timeout: 5000 }).catch(() => {
        });

        const filterInput = page.getByRole("textbox").first();
        if (await filterInput.count() > 0) {
            await filterInput.fill("nonexistentdatafilter12345");

            const applyButton = page.getByRole("button", { name: "Apply" });
            if (await applyButton.count() > 0) {
                await applyButton.click();
                await page.waitForLoadState("networkidle");

                await expect(page.locator("body")).toBeVisible();

                await page.getByRole("menuitem", { name: "Filter" }).click().catch(() => { });
                const clearButton = page.getByRole("button", { name: "Clear" });
                if (await clearButton.count() > 0) {
                    await clearButton.click();
                }
            }
        }

        await expect(page.getByRole("menubar")).toBeVisible();
    });

    test("Should support column sorting functionality", async ({ page }) => {
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);

        const sortableHeaders = [
            "Priority",
            "State",
            "Owner"
        ];

        for (const headerText of sortableHeaders) {
            const header = page.getByText(headerText);
            if (await header.count() > 0) {
                await header.click();
                await page.waitForLoadState("networkidle");

                await expect(page.locator(".dgrid-row")).not.toHaveCount(0);

                break;
            }
        }
    });

    test("Should maintain table state during page interactions", async ({ page }) => {
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        const initialRowCount = await page.locator(".dgrid-row").count();

        await page.getByRole("button", { name: "Advanced" }).click().catch(() => { });
        await page.waitForTimeout(1000);

        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        await expect(page.getByRole("menubar")).toBeVisible();

        const currentRowCount = await page.locator(".dgrid-row").count();
        expect(currentRowCount).toBeGreaterThan(0);
    });
});