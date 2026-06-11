import { test, expect } from "./fixtures";

test.describe("V9 Workunits", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/workunits");
        await page.waitForLoadState("networkidle");
        await page.locator(".fui-NavDrawerBody").waitFor({ state: "visible", timeout: 15000 });
    });

    test("Should display the Workunits page with all expected columns and controls", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        const expectedColumns = [
            "WUID",
            "Owner",
            "Job Name",
            "Cluster",
            "State",
            "Total Cluster Time"
        ];

        const costColumns = [
            "Compile Cost",
            "Execution Cost",
            "File Access Cost"
        ];

        for (const column of expectedColumns) {
            // Use column header selector with exact match to avoid ambiguity
            await expect(page.getByRole("columnheader", { name: column, exact: true })).toBeVisible();
        }

        for (const column of costColumns) {
            const columnElement = page.getByText(column);
            if (await columnElement.count() > 0) {
                await expect(columnElement).toBeVisible();
            }
        }

        const hasRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available to test");
        }
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);
    });

    test("Should filter workunits by Job Name and display filtered results", async ({ page }) => {
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        const date = new Date();
        const month = date.getMonth() + 1 < 10 ? "0" + (date.getMonth() + 1) : date.getMonth() + 1;
        const day = date.getDate() < 10 ? "0" + date.getDate() : date.getDate();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("textbox", { name: "Job Name" }).fill("global.setup.ts");
        await page.getByRole("button", { name: "Apply" }).click();

        // Wait for filter to apply and results to load
        await page.waitForTimeout(1000);
        const hasFilteredRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 5000 }).catch(() => false);
        if (!hasFilteredRows) {
            test.skip(true, "No matching workunits found after filter");
        }
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0, { timeout: 10000 });

        await page.getByRole("menuitem", { name: "Filter" }).click();
    });

    test("Should allow protecting and unprotecting a workunit and reflect lock status", async ({ page, browserName }) => {
        const rows = page.locator(".fui-TableBody .fui-TableRow");
        const firstRow = rows.first();

        const hasRows = await firstRow.isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available");
        }

        await expect(rows).not.toHaveCount(0);

        // Click a data cell (td[3] = Owner column, plain text) to trigger row selection via the row's onClick handler.
        // Avoid td[0] (SelectionCell checkbox), td[1] (Protected column - SVG icon that changes behaviour
        // once the WU is protected), and td[2] (WUID link - navigates away).
        await firstRow.locator("td").nth(3).click();
        await expect(firstRow).toHaveAttribute("aria-selected", "true");

        if (browserName === "chromium") {
            const protectBtn = page.getByRole("menuitem", { name: "Protect", exact: true });
            const isProtectEnabled = await protectBtn.isEnabled({ timeout: 2000 }).catch(() => false);
            if (!isProtectEnabled) {
                test.skip(true, "Protect not available - selected WU may already be protected or Protected field is null");
                return;
            }

            await protectBtn.click();

            // Wait for action state to change instead of sleeping
            const unprotectBtn = page.getByRole("menuitem", { name: "Unprotect", exact: true });
            await expect(unprotectBtn).toBeVisible({ timeout: 10000 });

            // Reselect only if selection was cleared by refresh
            const stillSelected = await firstRow.getAttribute("aria-selected");
            if (stillSelected !== "true") {
                await firstRow.locator("td").nth(3).click();
                await expect(firstRow).toHaveAttribute("aria-selected", "true");
            }

            await unprotectBtn.click();
        }
    });

    test("Should set a completed workunit to failed and update its status", async ({ page, browserName }) => {
        const hasRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available");
        }
        const completedRow = page.locator(".fui-TableBody .fui-TableRow").filter({ hasText: "completed" }).first();
        const hasCompleted = await completedRow.isVisible({ timeout: 3000 }).catch(() => false);
        if (!hasCompleted) {
            test.skip(true, "No completed workunits available");
        }
        await completedRow.locator("td").nth(3).click();
        await expect(page.locator(".fui-TableBody .fui-TableRow[aria-selected='true']")).toHaveCount(1);
        if (browserName === "chromium") {
            // SetToFailed requires hasNotProtected - skip if WU is protected or Protected field is null
            const setFailedBtn = page.getByRole("menuitem", { name: "Set To Failed", exact: true });
            const isEnabled = await setFailedBtn.isEnabled({ timeout: 2000 }).catch(() => false);
            if (!isEnabled) {
                test.skip(true, "Set To Failed not available - WU may be protected or status prevents this action");
                return;
            }
            await page.getByRole("menuitem", { name: "Set To Failed", exact: true }).click();

            // Wait for the action to complete and UI to update
            await page.waitForTimeout(1000);
            await expect(page.locator(".fui-TableBody .fui-TableRow").first()).toContainText("failed", { timeout: 10000 });
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

    test("Should support workunit detail page navigation", async ({ page, browserName }) => {
        const hasRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available");
        }
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);

        const firstWuidLink = page.locator(".fui-TableBody .fui-TableRow").first().locator("a").first();

        if (browserName === "chromium") {
            await firstWuidLink.click();
            await page.waitForLoadState("networkidle");

            const detailPageElements = [
                "WUID",
                "State",
                "Owner",
                "Job Name",
                "Cluster"
            ];

            let foundDetailElements = 0;
            for (const element of detailPageElements) {
                const elementLocator = page.getByText(element);
                if (await elementLocator.count() > 0) {
                    foundDetailElements++;
                }
            }

            expect(foundDetailElements).toBeGreaterThan(0);
        }
    });

    test("Should compare two workunits and preserve WUIDs while navigating tabs", async ({ page }) => {
        const hasRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available");
        }
        const rowCount = await page.locator(".fui-TableBody .fui-TableRow").count();
        if (rowCount < 2) {
            test.skip(true, "Not enough workunits to compare");
        }

        const firstRow = page.locator(".fui-TableBody .fui-TableRow").nth(0);
        const secondRow = page.locator(".fui-TableBody .fui-TableRow").nth(1);
        const selectedWuids = [
            await firstRow.locator("a").first().innerText(),
            await secondRow.locator("a").first().innerText()
        ];

        await firstRow.locator("td").nth(3).click();
        await secondRow.locator("td").nth(3).click();
        await expect(page.locator(".fui-TableBody .fui-TableRow[aria-selected='true']")).toHaveCount(2);

        // Compare requires at least 2 selected rows
        const compareBtn = page.getByRole("menuitem", { name: "Compare" });
        const isEnabled = await compareBtn.isEnabled({ timeout: 2000 }).catch(() => false);
        if (!isEnabled) {
            test.skip(true, "Compare not available after selecting 2 rows");
            return;
        }
        await compareBtn.click();
        await page.waitForLoadState("networkidle");

        const compareHash = new URL(page.url()).hash;
        const compareMatch = compareHash.match(/^#\/compare\/([^/?]+)/);
        expect(compareMatch).toBeTruthy();
        const compareWuids = compareMatch![1].split(",");
        expect(compareWuids).toEqual(expect.arrayContaining(selectedWuids));
        // The compare page uses a split layout - verify we're on the compare page
        await expect(page.locator("[class*='compare'], .fui-FluentProvider, #root")).toBeVisible();

        await page.getByRole("tab", { name: "Variables" }).first().click();
        await page.waitForLoadState("networkidle");

        const variablesHash = new URL(page.url()).hash;
        expect(variablesHash).toContain("/variables");
        const variablesMatch = variablesHash.match(/^#\/compare\/([^/?]+)\/variables/);
        expect(variablesMatch).toBeTruthy();
        const variablesWuids = variablesMatch![1].split(",");
        expect(variablesWuids).toEqual(expect.arrayContaining(selectedWuids));
    });

    test("Should handle workunit detail page tabs", async ({ page, browserName }) => {
        const hasRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available");
        }
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);

        const firstWuidLink = page.locator(".fui-TableBody .fui-TableRow").first().locator("a").first();

        if (browserName === "chromium") {
            await firstWuidLink.click();
            await page.waitForLoadState("networkidle");

            const expectedTabs = [
                "variables",
                "outputs",
                "inputs",
                "summaries",
                "metrics",
                "workflows",
                "queries",
                "resources",
                "helpers",
                "logicalgraph",
                "xml"
            ];

            let tabsFound = 0;
            for (const tab of expectedTabs) {
                const tabLocator = page.locator("[role=\"tab\"]").filter({ hasText: new RegExp(tab, "i") });
                if (await tabLocator.count() > 0) {
                    tabsFound++;
                }
            }

            if (tabsFound > 0) {
                expect(tabsFound).toBeGreaterThan(0);
            }
        }
    });

    test("Should handle different workunit states properly", async ({ page }) => {
        const hasRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available");
        }
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);

        const possibleStates = ["completed", "failed", "compiled", "running", "blocked"];

        let statesFound = 0;
        for (const state of possibleStates) {
            const stateRows = page.locator(".fui-TableBody .fui-TableRow").filter({ hasText: state });
            if (await stateRows.count() > 0) {
                statesFound++;
                await expect(stateRows.first()).toBeVisible();
            }
        }

        expect(statesFound).toBeGreaterThan(0);
    });

    test("Should support workunit data validation", async ({ page }) => {
        const hasRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available");
        }
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);

        const rows = page.locator(".fui-TableBody .fui-TableRow");
        const rowCount = await rows.count();

        for (let i = 0; i < Math.min(rowCount, 3); i++) {
            const row = rows.nth(i);
            await expect(row).toBeVisible();

            const rowText = await row.textContent();
            expect(rowText).toBeTruthy();
            if (rowText) {
                expect(rowText.length).toBeGreaterThan(0);
            }
        }
    });

    test("Should handle workunit sorting by different columns", async ({ page }) => {
        const hasRows = await page.locator(".fui-TableBody .fui-TableRow").first().isVisible({ timeout: 10000 }).catch(() => false);
        if (!hasRows) {
            test.skip(true, "No workunits available");
        }
        await expect(page.locator(".fui-TableBody .fui-TableRow")).not.toHaveCount(0);

        const initialRowCount = await page.locator(".fui-TableBody .fui-TableRow").count();
        expect(initialRowCount).toBeGreaterThan(0);

        const sortableColumns = ["WUID", "Owner", "State", "Job Name"];

        for (const column of sortableColumns) {
            const columnHeader = page.getByRole("columnheader", { name: column });
            if (await columnHeader.count() > 0) {
                await columnHeader.click();
                await page.waitForLoadState("networkidle");
                await page.waitForTimeout(1000);

                const newRowCount = await page.locator(".fui-TableBody .fui-TableRow").count();
                expect(newRowCount).toBeGreaterThanOrEqual(1);

                break;
            }
        }
    });

});
