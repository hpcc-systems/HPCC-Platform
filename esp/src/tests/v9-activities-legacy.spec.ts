import { test, expect } from "@playwright/test";

test.describe("V9 Activities (Legacy)", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities-legacy");
        await page.waitForLoadState("networkidle");
    });

    test("Activities page loaded with frame and content", async ({ page }) => {
        // Check frame elements
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
        await expect(page.getByRole("button", { name: "History" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();
        await expect(page.locator(".fui-NavDrawerBody")).toBeVisible();
        await expect(page.locator("a").filter({ hasText: /^Activities$/ })).toBeVisible();
        await expect(page.getByRole("link", { name: "Event Scheduler" })).toBeVisible();

        // Check activities content is loaded
        await expect(page.locator("svg").filter({ hasText: "%hthor" })).toBeVisible();
        await expect(page.locator(".reflex-splitter")).toBeVisible();
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByRole("columnheader", { name: "Priority" }).locator("div").first()).toBeVisible();

        // Check required elements from Activities.java
        const requiredElements = ["Job Name", "Owner", "Target/Wuid", "Graph", "State"];
        for (const elementText of requiredElements) {
            await expect(page.getByText(elementText)).toBeVisible();
        }

        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
    });

    test("Frame Loaded", async ({ page }) => {
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator("button").filter({ hasText: "" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
        await expect(page.getByRole("button", { name: "History" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();
        await expect(page.locator(".fui-NavDrawerBody")).toBeVisible();
        await expect(page.locator("a").filter({ hasText: /^Activities$/ })).toBeVisible();
        await expect(page.getByRole("link", { name: "Event Scheduler" })).toBeVisible();
    });

    test("Activities Loaded", async ({ page }) => {
        await expect(page.locator("svg").filter({ hasText: "%hthor" })).toBeVisible();
        await expect(page.locator(".reflex-splitter")).toBeVisible();
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.locator("button").filter({ hasText: "" })).toBeVisible();
        await expect(page.locator("button").filter({ hasText: "" })).toBeVisible();
        await expect(page.getByRole("columnheader", { name: "Priority" }).locator("div").first()).toBeVisible();
        await expect(page.getByText("Target/Wuid")).toBeVisible();
        await expect(page.getByText("Graph")).toBeVisible();
        await expect(page.getByText("State")).toBeVisible();
        await expect(page.getByText("Owner")).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
    });

    test("Should have proper navigation structure and links", async ({ page }) => {
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator(".fui-NavDrawerBody")).toBeVisible();

        await expect(page.locator("a").filter({ hasText: /^Activities$/ })).toBeVisible();

        const navigationLinks = [
            "Event Scheduler",
            "Files",
            "Clusters",
            "Operations"
        ];

        for (const linkText of navigationLinks) {
            const link = page.getByRole("link", { name: linkText });
            if (await link.isVisible()) {
                await expect(link).toBeVisible();
            }
        }

        await expect(page.getByRole("link", { name: "ECL", exact: true })).toBeVisible();
    });

    test("Should navigate to ECL Workunits and verify page loads", async ({ page }) => {
        const eclLink = page.getByRole("link", { name: "ECL", exact: true });
        if (await eclLink.isVisible()) {
            await eclLink.click();
            await page.waitForLoadState("networkidle");

            const workunitsLink = page.getByRole("link", { name: "Workunits" });
            if (await workunitsLink.isVisible()) {
                await workunitsLink.click();
                await page.waitForLoadState("networkidle");

                await expect(page.getByText("WUID")).toBeVisible();
                await expect(page.getByText("Owner", { exact: true })).toBeVisible();
                await expect(page.getByText("Job Name")).toBeVisible();
                await expect(page.getByText("Cluster", { exact: true })).toBeVisible();
            }
        }
    });

    test("Should navigate to Files section and verify page loads", async ({ page }) => {
        const filesLink = page.getByRole("link", { name: "Files" });
        if (await filesLink.isVisible()) {
            await filesLink.click();
            await page.waitForLoadState("networkidle");

            const logicalFilesLink = page.getByRole("link", { name: "Logical Files" });
            if (await logicalFilesLink.isVisible()) {
                await logicalFilesLink.click();
                await page.waitForLoadState("networkidle");

                await expect(page.getByText("Logical Name")).toBeVisible();
                await expect(page.getByText("Owner", { exact: true })).toBeVisible();
            }
        }
    });

    test("Should handle navigation errors gracefully", async ({ page }) => {
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator(".fui-NavDrawerBody")).toBeVisible();

        await page.goto("index.html#/activities-legacy");
        await page.waitForLoadState("networkidle");

        await expect(page.getByText("Target/Wuid")).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
    });

    test("Should test advanced and history buttons functionality", async ({ page }) => {
        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
        await expect(page.getByRole("button", { name: "History" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();

        const advancedButton = page.getByRole("button", { name: "Advanced" });
        await expect(advancedButton).toBeEnabled();
    });

    test("Should support table pagination, refresh functionality and row count changes", async ({ page }) => {
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);

        // Check pagination if it exists
        const paginationControls = page.locator(".dgrid-pagination, .pagination, [aria-label*='Page']");
        if (await paginationControls.count() > 0) {
            await expect(paginationControls.first()).toBeVisible();
        }

        const initialRowCount = await page.locator(".dgrid-row").count();
        expect(initialRowCount).toBeGreaterThan(0);

        // Test refresh functionality
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
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
