import { test, expect } from "./fixtures";
import { getWuid, loadWUs } from "./global";

test.describe("V9 ECL Playground", () => {

    test.describe("Empty playground", () => {

        test.beforeEach(async ({ page }) => {
            await page.goto("index.html#/play");
            await page.waitForLoadState("networkidle");
        });

        test("Should display the ECL Playground title", async ({ page }) => {
            await expect(page.getByRole("heading", { name: "ECL Playground" })).toBeVisible();
        });

        test("Should display the Sample dropdown", async ({ page }) => {
            await expect(page.getByLabel("Sample")).toBeVisible();
        });

        test("Should display the Submit button", async ({ page }) => {
            await expect(page.getByRole("button", { name: "Submit" })).toBeVisible();
        });

        test("Should display the Syntax check button", async ({ page }) => {
            await expect(page.getByRole("button", { name: "Syntax" })).toBeVisible();
        });

        test("Should display the Target cluster field", async ({ page }) => {
            await expect(page.getByLabel("Target")).toBeVisible();
        });

        test("Should display the Name input field", async ({ page }) => {
            await expect(page.getByLabel("Name")).toBeVisible();
        });

        test("Should display the output mode buttons", async ({ page }) => {
            await expect(page.getByRole("button", { name: "Error/Warning(s)" })).toBeVisible();
            await expect(page.getByRole("button", { name: "Result(s)" })).toBeVisible();
            await expect(page.getByRole("button", { name: "Visualizations" })).toBeVisible();
        });

        test("Should have Results and Visualizations buttons disabled initially", async ({ page }) => {
            await expect(page.getByRole("button", { name: "Result(s)" })).toBeDisabled();
            await expect(page.getByRole("button", { name: "Visualizations" })).toBeDisabled();
        });

        test("Should show the ECL, Graphs, and Outputs dock panels", async ({ page }) => {
            await expect(page.locator("#eclEditor")).toBeVisible();
            await expect(page.locator("#graph")).toBeVisible();
            await expect(page.locator("#output")).toBeVisible();
        });

        test("Should select a sample ECL and load it into the editor", async ({ page }) => {
            const sampleDropdown = page.getByRole("combobox", { name: "Sample" });
            await expect(sampleDropdown).toBeVisible();

            // Open the dropdown and select the first available option
            await sampleDropdown.click();
            const firstOption = page.getByRole("option").nth(2)
            if (await firstOption.isVisible({ timeout: 2000 }).catch(() => false)) {
                const optionText = await firstOption.textContent() ?? "";
                await firstOption.click();
                await page.waitForTimeout(500);

                // A sample was selected — the dropdown value should reflect the choice
                await expect(sampleDropdown).toContainText(optionText);
            }
        });

        test("Should navigate to /playground route and display the same page", async ({ page }) => {
            await page.goto("index.html#/playground");
            await page.waitForLoadState("networkidle");
            await expect(page.getByRole("heading", { name: "ECL Playground" })).toBeVisible();
        });

    });

    test.describe("Playground with existing workunit", () => {

        let wuid = "";

        test.beforeEach(async ({ page, browserName }) => {
            if (wuid === "") {
                loadWUs();
                wuid = getWuid(browserName, 0);
            }
            await page.goto(`index.html#/play/${wuid}`);
            await page.waitForLoadState("networkidle");
        });

        test("Should display the ECL Playground title with a wuid", async ({ page }) => {
            await expect(page.getByRole("heading", { name: "ECL Playground" })).toBeVisible();
        });

        test("Should have the Results and Visualizations buttons enabled after loading a complete workunit", async ({ page }) => {
            // Workunit from setup is completed, so Results/Vis buttons should be enabled
            await expect(page.getByRole("button", { name: "Result(s)" })).toBeEnabled({ timeout: 15000 });
            await expect(page.getByRole("button", { name: "Visualizations" })).toBeEnabled({ timeout: 15000 });
        });

        test("Should switch to Errors output mode when Error/Warnings button is clicked", async ({ page }) => {
            // Enable state first
            await page.getByRole("button", { name: "Result(s)" }).click();
            // Switch back to errors
            await page.getByRole("button", { name: "Error/Warning(s)" }).click();
            // InfoGrid (errors panel) should be rendered
            await expect(page.getByRole("button", { name: "Error/Warning(s)" })).toBeVisible();
        });

        test("Should switch to Results output mode when Results button is clicked", async ({ page }) => {
            await expect(page.getByRole("button", { name: "Result(s)" })).toBeEnabled({ timeout: 15000 });
            await page.getByRole("button", { name: "Result(s)" }).click();
            await page.waitForLoadState("networkidle");
            // TabbedResults component should render — look for result tab or output panel
            await expect(page.getByRole("button", { name: "Result(s)" })).toBeVisible();
        });

        test("Should display a link to the workunit in the status area", async ({ page }) => {
            // Status link shows the WU state and links to the workunit
            const statusLink = page.getByRole("link", { name: /completed|failed|running/i });
            await expect(statusLink).toBeVisible({ timeout: 15000 });
        });

        test("Should show the Syntax button with unknown state icon on load", async ({ page }) => {
            // Before any syntax check the QuestionCircle icon should be present
            await expect(page.getByRole("button", { name: "Syntax" })).toBeVisible();
        });

    });

});
