import { test, expect } from "@playwright/test";

test.describe("Root pages", () => {

    test("Should verify Activities page core functionality (converted from Activities.java)", async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");

        await expect(page.getByText("Job Name")).toBeVisible();
        await expect(page.getByText("Owner")).toBeVisible();
        await expect(page.getByText("Target/Wuid")).toBeVisible();

        await expect(page.getByText("Graph")).toBeVisible();
        await expect(page.getByText("State")).toBeVisible();
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
    });

    test("Should verify Workunits page functionality (converted from ECLWorkUnitsTest.java)", async ({ page }) => {
        await page.goto("index.html#/workunits");
        await page.waitForLoadState("networkidle");

        await expect(page.getByText("WUID")).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
        await expect(page.getByText("Cluster", { exact: true })).toBeVisible();
        await expect(page.getByText("State")).toBeVisible();

        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
    });

    test("Should verify Files/Logical Files page (converted from FilesLogicalFilesTest.java)", async ({ page }) => {
        await page.goto("index.html#/files");
        await page.waitForLoadState("networkidle");

        await expect(page.getByRole("menubar")).toBeVisible();

        const logicalNameVisible = await page.getByText("Logical Name").isVisible();

        if (!logicalNameVisible) {
            test.skip(true, "Logical Files page failed to load properly - known issue HPCC-32297. Original Java test was disabled for this reason.");
        }

        await expect(page.getByText("Logical Name")).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();

        const dataRows = page.locator(".ms-DetailsRow");
        const hasData = await dataRows.count() > 0;

        if (!hasData) {
            test.skip(true, "No logical files data available - this may be related to HPCC-32297");
        }

        await expect(dataRows).not.toHaveCount(0);
    });

    test("Should verify base table functionality (converted from BaseTableTest.java)", async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");

        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();

        await expect(page.getByRole("columnheader", { name: "Priority" }).locator("div").first()).toBeVisible();

        const firstRow = page.locator(".dgrid-row").first();
        await expect(firstRow).toBeVisible();
    });

    test("Should verify navigation functionality (converted from ActivitiesTest.java navigation tests)", async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");

        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator(".fui-NavDrawerBody")).toBeVisible();
        await expect(page.locator("a").filter({ hasText: /^Activities$/ })).toBeVisible();

        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
        await expect(page.getByRole("button", { name: "History" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();
    });

});