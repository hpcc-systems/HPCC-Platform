import { test, expect } from "./fixtures";

test.describe("V9 Topology", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/operations/topology/clusters");
        await page.waitForLoadState("networkidle");
    });

    test("Page loads with view tabs and toolbar", async ({ page }) => {
        await expect(page.getByRole("tab", { name: "Target Clusters" })).toBeVisible();
        await expect(page.getByRole("tab", { name: "System Servers" })).toBeVisible();
        await expect(page.getByRole("tab", { name: "Cluster Processes" })).toBeVisible();

        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeVisible();
    });

    test("Preflight button is disabled when no node is selected", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Preflight" })).toBeDisabled();
    });

    test("Detail pane shows placeholder when no node is selected", async ({ page }) => {
        await expect(page.getByText("Please select a target, service or machine.")).toBeVisible();
    });

    test("Target Clusters tree loads with content", async ({ page }) => {
        await expect(page.getByRole("tab", { name: "Target Clusters" })).toHaveAttribute("aria-selected", "true");
        await expect(page.getByTestId("topology-tree-row").first()).toBeVisible();
    });

    test("Clicking a tree node shows detail pane with Summary tab", async ({ page }) => {
        await page.getByTestId("topology-tree-row").first().click();

        await expect(page.getByRole("tab", { name: "Summary" })).toBeVisible();
        await expect(page.getByRole("tab", { name: "Configuration" })).toBeVisible();
        await expect(page.getByRole("tab", { name: "Logs" })).toBeVisible();
        await expect(page.getByRole("tab", { name: "Summary" })).toHaveAttribute("aria-selected", "true");

        await expect(page.getByText("Please select a target, service or machine.")).not.toBeVisible();
    });

    test("Switching to System Servers tab loads content", async ({ page }) => {
        await page.getByRole("tab", { name: "System Servers" }).click();
        await page.waitForLoadState("networkidle");

        await expect(page.getByRole("tab", { name: "System Servers" })).toHaveAttribute("aria-selected", "true");
        await expect(page.getByText("Please select a target, service or machine.")).toBeVisible();
    });

    test("Switching to Cluster Processes tab loads content", async ({ page }) => {
        await page.getByRole("tab", { name: "Cluster Processes" }).click();
        await page.waitForLoadState("networkidle");

        await expect(page.getByRole("tab", { name: "Cluster Processes" })).toHaveAttribute("aria-selected", "true");
        await expect(page.getByText("Please select a target, service or machine.")).toBeVisible();
    });

    test("Refresh button reloads the tree", async ({ page }) => {
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await page.getByRole("menuitem", { name: "Refresh" }).click();
        await page.waitForLoadState("networkidle");

        await expect(page.getByRole("tab", { name: "Target Clusters" })).toBeVisible();
        await expect(page.getByText("Please select a target, service or machine.")).toBeVisible();
    });

    test("Tree node chevron expands children", async ({ page }) => {
        const firstRow = page.getByTestId("topology-tree-row").first();
        const chevronButton = firstRow.getByRole("button").first();
        if (await chevronButton.isVisible()) {
            const countBefore = await page.getByTestId("topology-tree-row").count();
            await chevronButton.click();
            await expect(page.getByTestId("topology-tree-row")).not.toHaveCount(countBefore);
        }
    });

    test("URL updates when switching view tabs", async ({ page }) => {
        await page.getByRole("tab", { name: "System Servers" }).click();
        await expect(page).toHaveURL(/#\/operations\/topology\/servers/);

        await page.getByRole("tab", { name: "Cluster Processes" }).click();
        await expect(page).toHaveURL(/#\/operations\/topology\/processes/);

        await page.getByRole("tab", { name: "Target Clusters" }).click();
        await expect(page).toHaveURL(/#\/operations\/topology\/clusters/);
    });

    test("Direct navigation to servers tab works", async ({ page }) => {
        await page.goto("index.html#/operations/topology/servers");
        await page.waitForLoadState("networkidle");

        await expect(page.getByRole("tab", { name: "System Servers" })).toHaveAttribute("aria-selected", "true");
    });

    test("Direct navigation to processes tab works", async ({ page }) => {
        await page.goto("index.html#/operations/topology/processes");
        await page.waitForLoadState("networkidle");

        await expect(page.getByRole("tab", { name: "Cluster Processes" })).toHaveAttribute("aria-selected", "true");
    });
});
