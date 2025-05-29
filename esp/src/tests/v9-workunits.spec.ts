import { test, expect } from "@playwright/test";

test.describe("V9 Workunits", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/workunits");
        await page.waitForLoadState("networkidle");
    });

    test("Should display the Workunits page with all expected columns and controls", async ({ page }) => {
        expect(await page.getByRole("menubar")).toBeVisible();
        expect(await page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        expect(await page.getByText("WUID")).toBeVisible();
        expect(await page.getByText("Owner", { exact: true })).toBeVisible();
        expect(await page.getByText("Job Name")).toBeVisible();
        expect(await page.getByText("Cluster", { exact: true })).toBeVisible();
        await page.locator(".ms-DetailsRow").first().waitFor({ state: "visible" });
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Should filter workunits by Job Name and display filtered results", async ({ page }) => {
        expect(await page.getByRole("menubar")).toBeVisible();
        expect(await page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        const date = new Date();
        const month = date.getMonth() + 1 < 10 ? "0" + (date.getMonth() + 1) : date.getMonth() + 1;
        const day = date.getDate() < 10 ? "0" + date.getDate() : date.getDate();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("textbox", { name: "Job Name" }).fill("global.setup.ts");
        await page.getByRole("button", { name: "Apply" }).click();
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.getByRole("menuitem", { name: "Filter" }).click();
    });

    test("Should allow protecting and unprotecting a workunit and reflect lock status", async ({ page, browserName }) => {
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").first().locator(".ms-DetailsRow-check").click();
        expect(await page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        if (browserName === "chromium") {
            await page.getByRole("menuitem", { name: "Protect", exact: true }).click();
            expect(await page.locator(".ms-DetailsRow").first().locator("[data-icon-name=\"LockSolid\"]")).toBeVisible();
            await page.locator(".ms-DetailsRow").first().locator(".ms-DetailsRow-check").click();
            expect(await page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
            await page.getByRole("menuitem", { name: "Unprotect" }).click();
            expect(await page.locator(".ms-DetailsRow").first().locator("[data-icon-name=\"LockSolid\"]")).toHaveCount(0);
        }
    });

    test("Should set a completed workunit to failed and update its status", async ({ page, browserName }) => {
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").filter({ hasText: "completed" }).first().locator(".ms-DetailsRow-check").click();
        expect(await page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        if (browserName === "chromium") {
            await page.getByRole("menuitem", { name: "Set To Failed", exact: true }).click();
            expect(await page.locator(".ms-DetailsRow").first()).toContainText("failed");
        }
    });

    test.skip("Should delete a completed workunit and decrease the row count", async ({ page, browserName }) => {
        const wuCount = await page.locator(".ms-DetailsRow").count();
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").filter({ hasText: "completed" }).first().locator(".ms-DetailsRow-check").click();
        expect(await page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        if (browserName === "chromium") {
            await page.getByRole("menuitem", { name: "Delete", exact: true }).click();
            await page.getByRole("button", { name: "OK" }).click();
            expect(await page.locator(".ms-DetailsRow")).toHaveCount(wuCount - 1);
        }
    });

});
