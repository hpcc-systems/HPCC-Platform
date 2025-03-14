import { test, expect } from "@playwright/test";

test.describe("V9 Activity", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
    });

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
});
