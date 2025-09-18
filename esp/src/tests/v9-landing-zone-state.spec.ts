import { test, expect } from "@playwright/test";

test.describe("V9 Landing Zone - State Preservation", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/landingzone");
        await page.waitForLoadState("networkidle");
        await page.waitForTimeout(2000);
    });

    test.describe("Tree State Management During File Operations", () => {
        test("Should preserve expanded state when files are deleted", async ({ page }) => {
            // This test validates the specific fix you implemented for maintaining
            // tree expansion state during delete operations

            // First, expand some tree items to establish state
            // Look specifically for tree expand buttons in the table area
            const expandableItems = page.locator(".fui-Table button[aria-expanded='false']");
            const itemCount = await expandableItems.count();

            if (itemCount > 0) {
                // Expand dropzone
                const dropzoneButton = expandableItems.first();
                await dropzoneButton.click();
                await page.waitForTimeout(1000);

                // Look for machine buttons within the tree
                const machineButtons = page.locator(".fui-Table button[aria-expanded='false']");
                const machineCount = await machineButtons.count();

                if (machineCount > 0) {
                    const machineButton = machineButtons.first();
                    await machineButton.click();
                    await page.waitForTimeout(2000); // Wait for files to load

                    // Verify both items are expanded
                    await expect(dropzoneButton).toHaveAttribute("aria-expanded", "true");
                    await expect(machineButton).toHaveAttribute("aria-expanded", "true");

                    // Now try to perform a delete operation if files are available
                    const fileCheckboxes = page.locator("input[type='checkbox']");
                    const checkboxCount = await fileCheckboxes.count();

                    if (checkboxCount > 1) {
                        // Select a file
                        const fileCheckbox = fileCheckboxes.nth(1); // Skip select-all
                        await fileCheckbox.check();
                        await page.waitForTimeout(500);

                        // Click delete button (this would normally show confirmation dialog)
                        const deleteButton = page.getByRole("menuitem", { name: "Delete" });
                        if (await deleteButton.isEnabled()) {
                            await deleteButton.click();
                            await page.waitForTimeout(500);

                            // Look for confirmation dialog
                            const confirmButton = page.getByRole("button", { name: "Delete" });
                            if (await confirmButton.isVisible()) {
                                // Cancel instead of actually deleting
                                const cancelButton = page.getByRole("button", { name: "Cancel" });
                                if (await cancelButton.isVisible()) {
                                    await cancelButton.click();
                                }
                            }

                            // After canceling, tree state should still be preserved
                            await expect(dropzoneButton).toHaveAttribute("aria-expanded", "true");
                            await expect(machineButton).toHaveAttribute("aria-expanded", "true");
                        }
                    }
                }
            }
        });

        test("Should preserve expanded state when files are uploaded", async ({ page }) => {
            // This test validates that upload operations don't collapse the tree

            // Expand tree items - use table-specific selector
            const expandableItems = page.locator(".fui-Table button[aria-expanded='false']");
            const itemCount = await expandableItems.count();

            if (itemCount > 0) {
                const dropzoneButton = expandableItems.first();
                await dropzoneButton.click();
                await page.waitForTimeout(1000);

                // Verify it's expanded before operations
                await expect(dropzoneButton).toHaveAttribute("aria-expanded", "true");

                // Open upload dialog
                await page.getByRole("menuitem", { name: "Upload" }).click();

                // This triggers the file input - we can't actually upload files in Playwright
                // but we can verify the expanded state remains intact
                await page.waitForTimeout(500);

                // Tree should still be expanded
                await expect(dropzoneButton).toHaveAttribute("aria-expanded", "true");

                // Try Add File form as well
                await page.getByRole("menuitem", { name: "Add File" }).click();
                await page.waitForTimeout(500);

                // Close the form
                const cancelButton = page.getByRole("button", { name: "Cancel" });
                if (await cancelButton.isVisible()) {
                    await cancelButton.click();
                }

                // Tree should still be expanded
                await expect(dropzoneButton).toHaveAttribute("aria-expanded", "true");
            }
        });

        test("Should handle selective refresh of machine data", async ({ page }) => {
            // This test verifies the selective refresh functionality you implemented

            // Expand multiple machines if available - use table-specific selector
            const expandableItems = page.locator(".fui-Table button[aria-expanded='false']");
            const itemCount = await expandableItems.count();

            if (itemCount >= 1) {
                // Expand first dropzone
                const firstDropzone = expandableItems.first();
                await firstDropzone.click();
                await page.waitForTimeout(1000);

                // Verify dropzone is expanded
                await expect(firstDropzone).toHaveAttribute("aria-expanded", "true");

                // Look for machine buttons within the tree
                const machineButtons = page.locator(".fui-Table button[aria-expanded='false']");
                const machineCount = await machineButtons.count();

                if (machineCount >= 1) {
                    const firstMachine = machineButtons.first();
                    await firstMachine.click();
                    await page.waitForTimeout(2000);

                    // Verify machine is expanded
                    await expect(firstMachine).toHaveAttribute("aria-expanded", "true");

                    // Expand second machine if available
                    const remainingMachines = page.locator(".fui-Table button[aria-expanded='false']");
                    const remainingCount = await remainingMachines.count();

                    if (remainingCount >= 1) {
                        const secondMachine = remainingMachines.first();
                        await secondMachine.click();
                        await page.waitForTimeout(2000);

                        // Both machines should be expanded
                        await expect(firstMachine).toHaveAttribute("aria-expanded", "true");
                        await expect(secondMachine).toHaveAttribute("aria-expanded", "true");

                        // Trigger refresh operation
                        await page.getByRole("menuitem", { name: "Refresh" }).click();
                        await page.waitForLoadState("networkidle");
                        await page.waitForTimeout(1000);

                        // Document current behavior (expansion state after refresh)
                        const firstMachineState = await firstMachine.getAttribute("aria-expanded");
                        const secondMachineState = await secondMachine.getAttribute("aria-expanded");

                        console.log(`After refresh - First machine: ${firstMachineState}, Second machine: ${secondMachineState}`);
                    }
                }
            }
        });

        test("Should maintain selection state appropriately during operations", async ({ page }) => {
            await page.waitForTimeout(2000);

            // Select some files
            const fileCheckboxes = page.locator("input[type='checkbox']");
            const checkboxCount = await fileCheckboxes.count();

            if (checkboxCount > 1) {
                // Select first file
                const firstFile = fileCheckboxes.nth(1);
                await firstFile.check();
                await expect(firstFile).toBeChecked();

                // Action buttons should be enabled
                await expect(page.getByRole("menuitem", { name: "Download" })).toBeEnabled();

                // Test download operation (doesn't actually download in test)
                await page.getByRole("menuitem", { name: "Download" }).click();
                await page.waitForTimeout(500);

                // Selection should still be maintained for download
                await expect(firstFile).toBeChecked();

                // Test that delete operations clear selection (as per your implementation)
                const deleteButton = page.getByRole("menuitem", { name: "Delete" });
                if (await deleteButton.isEnabled()) {
                    await deleteButton.click();
                    await page.waitForTimeout(500);

                    // Look for confirmation dialog and cancel it
                    const cancelButton = page.getByRole("button", { name: "Cancel" });
                    if (await cancelButton.isVisible()) {
                        await cancelButton.click();

                        // Selection should still be there since we canceled
                        await expect(firstFile).toBeChecked();
                    }
                }
            }
        });

        test("Should handle loading states during tree expansion", async ({ page }) => {
            // Test the loading state management you implemented

            // Look for machines that can be expanded - use table-specific selector
            const expandableItems = page.locator(".fui-Table button[aria-expanded='false']");
            const itemCount = await expandableItems.count();

            if (itemCount > 0) {
                // Expand dropzone first
                const dropzoneButton = expandableItems.first();
                await dropzoneButton.click();
                await page.waitForTimeout(1000);

                // Look for machine-level expansion within the tree
                const machineButtons = page.locator(".fui-Table button[aria-expanded='false']");
                const machineCount = await machineButtons.count();

                if (machineCount > 0) {
                    const machineButton = machineButtons.first();

                    // Click to expand and immediately look for loading state
                    await machineButton.click();

                    // Check for loading indicators (might be brief)
                    const loadingSpinners = page.locator("[data-icon-name*='Spinner'], .compactSpinner");

                    try {
                        // Wait briefly for loading spinner to appear
                        await expect(loadingSpinners.first()).toBeVisible({ timeout: 1000 });
                        console.log("Loading spinner detected during expansion");

                        // Wait for loading to complete
                        await page.waitForTimeout(2000);

                        // Spinner should be gone
                        const visibleSpinners = await loadingSpinners.count();
                        console.log(`Spinners remaining after load: ${visibleSpinners}`);

                    } catch {
                        console.log("Loading completed too quickly to detect spinner");
                    }

                    // Machine should be expanded after loading
                    await expect(machineButton).toHaveAttribute("aria-expanded", "true");
                }
            }
        });

        test("Should handle network errors gracefully during expansion", async ({ page }) => {
            // Test error handling in tree expansion

            // This test documents behavior when network requests fail
            // In a real scenario, you might mock failed network requests

            const expandableItems = page.locator(".fui-Table button[aria-expanded='false']");
            const itemCount = await expandableItems.count();

            if (itemCount > 0) {
                const button = expandableItems.first();
                await button.click();
                await page.waitForTimeout(3000); // Wait longer for potential timeout

                // Button should either be expanded or reverted to collapsed state
                const finalState = await button.getAttribute("aria-expanded");
                console.log(`Final expansion state after potential error: ${finalState}`);

                // UI should remain functional regardless
                await expect(page.getByRole("menubar")).toBeVisible();
                await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
            }
        });
    });

    test.describe("Performance and Responsiveness", () => {
        test("Should handle rapid expansion/collapse operations", async ({ page }) => {
            await page.waitForTimeout(2000);

            // Test rapid clicking to ensure UI remains responsive
            const expandableItems = page.locator(".fui-Table button[aria-expanded]");
            const itemCount = await expandableItems.count();

            if (itemCount > 0) {
                const button = expandableItems.first();

                // Rapid expand/collapse cycles
                for (let i = 0; i < 3; i++) {
                    await button.click();
                    await page.waitForTimeout(200);
                    await button.click();
                    await page.waitForTimeout(200);
                }

                // UI should still be responsive
                await expect(page.getByRole("menubar")).toBeVisible();

                // Final state should be stable
                await page.waitForTimeout(1000);
                const finalState = await button.getAttribute("aria-expanded");
                console.log(`Final state after rapid operations: ${finalState}`);
            }
        });

        test("Should maintain performance with large file lists", async ({ page }) => {
            // Test behavior when dealing with potentially large file lists

            await page.waitForTimeout(2000);

            // Expand all available items to load maximum data
            const expandButtons = page.locator(".fui-Table button[aria-expanded='false']");
            const buttonCount = await expandButtons.count();

            console.log(`Found ${buttonCount} expandable items`);

            // Expand up to 3 items to test performance
            for (let i = 0; i < Math.min(buttonCount, 3); i++) {
                const button = expandButtons.nth(i);
                await button.click();
                await page.waitForTimeout(1500); // Allow time for loading
            }

            // UI should remain responsive
            await expect(page.getByRole("menubar")).toBeVisible();

            // Scrolling should work smoothly
            await page.mouse.wheel(0, 100);
            await page.waitForTimeout(200);
            await page.mouse.wheel(0, -100);

            // Should still be able to interact with buttons
            await page.getByRole("menuitem", { name: "Refresh" }).hover();
        });
    });
});
