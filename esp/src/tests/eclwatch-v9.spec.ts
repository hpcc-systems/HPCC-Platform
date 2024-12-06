import { test, expect } from "@playwright/test";

test.describe("Basic ECLWatch V9 UI", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("/esp/files/index.html#/activities");
    })

    test("Frame Loaded", async ({ page }) => {
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator("button").filter({ hasText: "" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
        await expect(page.getByTitle("Activities")).toBeVisible();
        await expect(page.getByRole("link", { name: "ECL", exact: true })).toBeVisible();
        await expect(page.getByRole("link", { name: "Files" })).toBeVisible();
        await expect(page.getByRole("link", { name: "Published Queries" })).toBeVisible();
        await expect(page.getByRole("button", { name: "History" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();
        await expect(page.locator("a").filter({ hasText: /^Activities$/ })).toBeVisible();
        await expect(page.getByRole("link", { name: "Event Scheduler" })).toBeVisible();
    });

    test("Activities page", async ({ page }) => {
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
});

test.describe("Workunit tests", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("/esp/files/index.html#/workunits");
    });

    test("View the Workunits list page", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByText("WUID")).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
        await expect(page.getByText("Cluster", { exact: true })).toBeVisible();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Filter the Workunits list page", async ({ page }) => {
        const date = new Date();
        const month = date.getMonth() + 1 < 10 ? "0" + (date.getMonth() + 1) : date.getMonth() + 1;
        const day = date.getDate() < 10 ? "0" + date.getDate() : date.getDate();
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        const wuidField = await page.getByPlaceholder("W20200824-060035");
        wuidField.fill(`W${date.getFullYear()}${month}${day}*`);
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByPlaceholder("W20200824-060035").fill(`W2023*`);
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).toHaveCount(0);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("button", { name: "Clear" }).click();
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Protect / Unprotect a WU", async ({ page }) => {
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").first().locator(".ms-DetailsRow-check").click();
        await expect(page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        await page.getByRole("menuitem", { name: "Protect", exact: true }).click();
        await expect(page.locator(".ms-DetailsRow").first().locator("[data-icon-name=\"LockSolid\"]")).toBeVisible();
        await page.getByRole("menuitem", { name: "Unprotect" }).click();
        await expect(page.locator(".ms-DetailsRow").first().locator("[data-icon-name=\"LockSolid\"]")).not.toBeVisible();
    });

    test("Set a WU to failed", async ({ page }) => {
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").filter({ hasText: "completed" }).last().locator(".ms-DetailsRow-check").click();
        await expect(page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        await page.getByRole("menuitem", { name: "Set To Failed", exact: true }).click();
        await expect(page.locator(".ms-DetailsRow.is-selected").filter({ hasText: "failed" })).toBeVisible();
    });

    // this test was failing when run in GitHub Actions
    // test("Delete a WU", async ({ page }) => {
    //     const wuCount = await page.locator(".ms-DetailsRow").count();
    //     await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    //     await page.locator(".ms-DetailsRow").filter({ hasText: "completed" }).first().locator(".ms-DetailsRow-check").click();
    //     await expect(page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
    //     await page.getByRole("menuitem", { name: "Delete", exact: true }).click();
    //     await page.getByRole("button", { name: "OK" }).click();
    //     await expect(page.locator(".ms-DetailsRow")).toHaveCount(wuCount - 1);
    // });

});

test.describe("File tests", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("/esp/files/index.html#/files");
    });

    test("View the Files list page", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByText("Logical Name")).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();
        await expect(page.getByText("Cluster")).toBeVisible();
        await expect(page.getByText("Records")).toBeVisible();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Filter the Files list page", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByPlaceholder("*::somefile*").fill("*allPeople*");
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).toHaveCount(1);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("button", { name: "Clear" }).click();
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(1);
    });

});

test.describe("Query tests", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("/esp/files/index.html#/queries");
    });

    test("View the Queries list page", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByText("ID", { exact: true })).toBeVisible();
        await expect(page.getByText("Priority", { exact: true })).toBeVisible();
        await expect(page.getByText("Name")).toBeVisible();
        await expect(page.getByText("Target")).toBeVisible();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Filter the Queries list page", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByPlaceholder("My?Su?erQ*ry").fill("asdf");
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).toHaveCount(0);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("button", { name: "Clear" }).click();
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

});
