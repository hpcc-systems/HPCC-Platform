import { test, expect } from "@playwright/test";

test.describe("V9 Workunits", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/workunits");
        await page.waitForLoadState("networkidle");
    });

    test("Loaded", async ({ page }) => {
        expect(await page.getByRole("menubar")).toBeVisible();
        expect(await page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        expect(await page.getByText("WUID")).toBeVisible();
        expect(await page.getByText("Owner", { exact: true })).toBeVisible();
        expect(await page.getByText("Job Name")).toBeVisible();
        expect(await page.getByText("Cluster", { exact: true })).toBeVisible();
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Filter", async ({ page }) => {
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
        await page.getByRole("button", { name: "Clear" }).click();
        const wuidField = await page.getByPlaceholder("W20200824-060035");
        wuidField.fill(`W${date.getFullYear()}${month}${day}*`);
        await page.getByRole("button", { name: "Apply" }).click();
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByPlaceholder("W20200824-060035").fill("W2023*");
        await page.getByRole("button", { name: "Apply" }).click();
        expect(await page.getByText("- 0 of 0 Rows")).toBeVisible();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("button", { name: "Clear" }).click();
        await page.getByRole("button", { name: "Apply" }).click();
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Protect / Unprotect", async ({ page, browserName }) => {
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

    test("Set a WU to failed", async ({ page, browserName }) => {
        expect(await page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").filter({ hasText: "completed" }).first().locator(".ms-DetailsRow-check").click();
        expect(await page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        if (browserName === "chromium") {
            await page.getByRole("menuitem", { name: "Set To Failed", exact: true }).click();
            expect(await page.locator(".ms-DetailsRow").first()).toContainText("failed");
        }
    });

    test.skip("Delete a WU", async ({ page, browserName }) => {
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
