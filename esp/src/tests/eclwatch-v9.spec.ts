import { test, expect } from "@playwright/test";

test.describe("ECLWatch V9", () => {

    test("Basic Frame", async ({ page }) => {
        await page.goto("/esp/files/index.html#/activities");
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

    test("Activities", async ({ page }) => {
        await page.goto("/esp/files/index.html#/activities");
        await page.getByTitle("Disk Usage").locator("i").click();
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
        await expect(page.getByRole("gridcell", { name: "HThorServer - hthor" })).toBeVisible();
        await expect(page.getByRole("gridcell", { name: "ThorMaster - thor", exact: true })).toBeVisible();
        await expect(page.getByRole("gridcell", { name: "ThorMaster - thor_roxie" })).toBeVisible();
        await expect(page.getByRole("gridcell", { name: "RoxieServer - roxie" })).toBeVisible();
        await expect(page.getByRole("gridcell", { name: "myeclccserver - hthor." })).toBeVisible();
        await expect(page.getByRole("gridcell", { name: "mydfuserver - dfuserver_queue" })).toBeVisible();
    });
});
