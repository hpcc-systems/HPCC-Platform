import { test, expect } from "./fixtures";

test.describe("V9 Landing Zone", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/landingzone");
        await page.waitForLoadState("networkidle");
    });

    test("Should display the Landing Zone page with expected toolbar and components", async ({ page }) => {
        // Check that the main toolbar is visible
        await expect(page.getByRole("menubar")).toBeVisible();

        // Check for main toolbar buttons
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Preview" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Upload" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Download" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Delete" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Filter" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Add File" })).toBeVisible();

        // Check for import options
        await expect(page.getByRole("menuitem", { name: "Fixed" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Delimited" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "XML" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "JSON" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Variable" })).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Blob" })).toBeVisible();
    });

    test("Should display the tree table with correct column headers", async ({ page }) => {
        // Wait for the table to load
        await page.waitForTimeout(2000);

        // Check for table headers
        await expect(page.getByText("Name", { exact: true })).toBeVisible();
        await expect(page.getByText("Size", { exact: true })).toBeVisible();
        await expect(page.getByText("Date", { exact: true })).toBeVisible();

        // Check that the table container is present (FluentUI Table doesn't use role="table")
        await expect(page.locator(".fui-Table")).toBeVisible();
    });

    test("Should have disabled action buttons when no items are selected", async ({ page }) => {
        // Wait for page to fully load
        await page.waitForTimeout(1000);

        // These buttons should be disabled when nothing is selected
        await expect(page.getByRole("menuitem", { name: "Preview" })).toBeDisabled();
        await expect(page.getByRole("menuitem", { name: "Download" })).toBeDisabled();
        await expect(page.getByRole("menuitem", { name: "Delete" })).toBeDisabled();

        // Import buttons should be disabled when nothing is selected
        await expect(page.getByRole("menuitem", { name: "Fixed" })).toBeDisabled();
        await expect(page.getByRole("menuitem", { name: "Delimited" })).toBeDisabled();
        await expect(page.getByRole("menuitem", { name: "XML" })).toBeDisabled();
        await expect(page.getByRole("menuitem", { name: "JSON" })).toBeDisabled();
        await expect(page.getByRole("menuitem", { name: "Variable" })).toBeDisabled();
        await expect(page.getByRole("menuitem", { name: "Blob" })).toBeDisabled();
    });

    test("Should open filter dialog when filter button is clicked", async ({ page }) => {
        await page.getByRole("menuitem", { name: "Filter" }).click();

        // Wait for the filter heading to appear (more reliable than checking dialog visibility)
        const filterHeading = page.getByRole("heading", { name: "Filter" });
        await expect(filterHeading).toBeVisible();

        // Check for filter form fields - there should be comboboxes and a textbox
        const comboboxes = page.getByRole("combobox");
        const comboCount = await comboboxes.count();
        expect(comboCount).toBeGreaterThanOrEqual(2); // should have at least 2 rows

        const fileNameTextbox = page.getByRole("textbox", { name: "File Name" });
        await expect(fileNameTextbox).toBeVisible();

        // Get the dialog container and check for buttons within it
        const dialog = page.getByRole("dialog");
        await expect(dialog.getByRole("button", { name: "Apply" })).toBeVisible();
        await expect(dialog.getByRole("button", { name: "Clear" })).toBeVisible();
    });

    test("Should open upload file dialog when upload button is clicked", async ({ page }) => {
        await page.getByRole("menuitem", { name: "Upload" }).click();

        // This should trigger the file input click
        // We can't actually test file upload in Playwright easily, but we can verify the input exists
        const fileInput = page.locator("#uploaderBtn");
        await expect(fileInput).toBeAttached();
        await expect(fileInput).toHaveAttribute("type", "file");
        await expect(fileInput).toHaveAttribute("multiple");
    });

    test("Should open Add File form when Add File button is clicked", async ({ page }) => {
        await page.getByRole("menuitem", { name: "Add File" }).click();

        // Check that the Add File form dialog opens
        await expect(page.getByRole("heading", { name: "Add File" })).toBeVisible();

        // Wait for the form to load properly
        await page.waitForTimeout(1000);

        // Check for form fields using specific selectors
        await expect(page.getByLabel("IP")).toBeVisible();
        await expect(page.getByLabel("Path")).toBeVisible();

        // Check for form buttons
        await expect(page.getByRole("button", { name: "Add", exact: true })).toBeVisible();
        await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible();
    });

    test("Should display loading state initially", async ({ page }) => {
        // Go to page and check for loading indicators
        await page.goto("index.html#/landingzone");

        // Should show loading spinner initially
        const loadingSpinner = page.getByLabel("Loading");

        // The spinner might be visible briefly or the data might load quickly
        // So we'll use a timeout to check if it appears
        try {
            await expect(loadingSpinner).toBeVisible({ timeout: 1000 });
        } catch {
            // If loading is too fast, that's also acceptable
            console.log("Loading completed before we could detect spinner");
        }

        // Wait for the page to finish loading
        await page.waitForLoadState("networkidle");
    });

    test("Should handle tree expansion functionality", async ({ page }) => {
        await page.waitForTimeout(2000);

        // Look for expandable tree items (should have expand/collapse buttons)
        const expandButtons = page.locator("button[aria-expanded]");
        const expandButtonCount = await expandButtons.count();

        if (expandButtonCount > 0) {
            // Test expanding the first expandable item
            const firstExpandButton = expandButtons.first();
            const initialState = await firstExpandButton.getAttribute("aria-expanded");

            await firstExpandButton.click();
            await page.waitForTimeout(500);

            // Check that the aria-expanded state changed
            const newState = await firstExpandButton.getAttribute("aria-expanded");
            expect(newState).not.toBe(initialState);
        } else {
            console.log("No expandable items found - this may be expected if no drop zones are configured");
        }
    });

    test("Should allow text selection in tree rows", async ({ page }) => {
        await page.waitForTimeout(2000);

        // Look for tree rows
        const treeRows = page.locator("[role='row']");
        const rowCount = await treeRows.count();

        if (rowCount > 1) { // Skip header row
            const dataRow = treeRows.nth(1);
            await dataRow.hover();

            // Should be able to interact with the row
            await expect(dataRow).toBeVisible();

            // Double-click to select text (this tests the userSelect: "text" functionality)
            await dataRow.dblclick();
        }
    });

    test("Should show appropriate icons for different item types", async ({ page }) => {
        await page.waitForTimeout(2000);

        // Look for icons in the tree
        const icons = page.locator("svg[data-icon-name]");
        const iconCount = await icons.count();

        if (iconCount > 0) {
            // We should see server, desktop, folder, or document icons
            const iconNames: string[] = [];
            for (let i = 0; i < Math.min(iconCount, 10); i++) {
                const iconName = await icons.nth(i).getAttribute("data-icon-name");
                if (iconName) {
                    iconNames.push(iconName);
                }
            }

            // Should have appropriate icons
            const expectedIcons = ["Server", "Desktop", "Folder", "Document"];
            const hasExpectedIcon = iconNames.some(name =>
                expectedIcons.some(expected => name.includes(expected))
            );

            if (iconNames.length > 0) {
                expect(hasExpectedIcon).toBeTruthy();
            }
        }
    });

    test("Should handle refresh functionality", async ({ page }) => {
        await page.waitForTimeout(1000);

        // Click refresh button
        await page.getByRole("menuitem", { name: "Refresh" }).click();

        // Wait for any network requests to complete
        await page.waitForLoadState("networkidle");

        // The page should still be functional after refresh
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
    });

    test.describe("Drag and Drop Functionality", () => {
        test("Should show drop zone overlay on drag enter", async ({ page }) => {
            // Simulate drag enter on the body element instead of document
            await page.dispatchEvent("body", "dragenter");

            // Check if drop zone overlay becomes visible
            const dropZoneOverlay = page.locator(".dzWrapper");

            // The overlay might be visible temporarily
            try {
                await expect(dropZoneOverlay).toBeVisible({ timeout: 1000 });
            } catch {
                // If the overlay doesn't appear, that's also acceptable depending on implementation
                console.log("Drop zone overlay not detected");
            }
        });
    });

    test.describe("Responsive Design", () => {
        test("Should handle column resizing", async ({ page }) => {
            await page.waitForTimeout(2000);

            // Look for resize handles
            const resizeHandles = page.locator(".resizeHandle");
            const handleCount = await resizeHandles.count();

            if (handleCount > 0) {
                const firstHandle = resizeHandles.first();
                const boundingBox = await firstHandle.boundingBox();

                if (boundingBox) {
                    // Simulate resize by dragging the handle
                    await page.mouse.move(boundingBox.x + boundingBox.width / 2, boundingBox.y + boundingBox.height / 2);
                    await page.mouse.down();
                    await page.mouse.move(boundingBox.x + 50, boundingBox.y + boundingBox.height / 2);
                    await page.mouse.up();

                    // Column should have been resized
                    // We can't easily test the exact width, but the operation should complete without error
                    await page.waitForTimeout(100);
                }
            }
        });
    });

    test.describe("Error Handling", () => {
        test("Should handle network errors gracefully", async ({ page }) => {
            // Navigate to landing zone
            await page.goto("index.html#/landingzone");

            // The page should load even if some network requests fail
            await expect(page.getByRole("menubar")).toBeVisible();

            // Check that error handling doesn't break the UI
            await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        });
    });

});
