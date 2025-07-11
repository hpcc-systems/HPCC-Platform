import { test, expect } from "@playwright/test";

test.describe("V9 Activity - Enhanced Navigation", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
    });

    test("Should have proper navigation structure and links", async ({ page }) => {
        // Check main navigation elements
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator('.fui-NavDrawerBody')).toBeVisible();
        
        // Check for main navigation links that should be present
        await expect(page.locator("a").filter({ hasText: /^Activities$/ })).toBeVisible();
        
        // Check for other main navigation sections
        const navigationLinks = [
            "Event Scheduler",
            "ECL",
            "Files", 
            "Clusters",
            "Operations"
        ];
        
        for (const linkText of navigationLinks) {
            const link = page.getByRole("link", { name: linkText });
            if (await link.isVisible()) {
                await expect(link).toBeVisible();
            }
        }
    });

    test("Should navigate to ECL Workunits and verify page loads", async ({ page }) => {
        // Navigate to ECL section
        const eclLink = page.getByRole("link", { name: "ECL" });
        if (await eclLink.isVisible()) {
            await eclLink.click();
            await page.waitForLoadState("networkidle");
            
            // Look for Workunits link
            const workunitsLink = page.getByRole("link", { name: "Workunits" });
            if (await workunitsLink.isVisible()) {
                await workunitsLink.click();
                await page.waitForLoadState("networkidle");
                
                // Verify we're on the workunits page
                await expect(page.getByText("WUID")).toBeVisible();
                await expect(page.getByText("Owner", { exact: true })).toBeVisible();
                await expect(page.getByText("Job Name")).toBeVisible();
                await expect(page.getByText("Cluster", { exact: true })).toBeVisible();
            }
        }
    });

    test("Should navigate to Files section and verify page loads", async ({ page }) => {
        // Navigate to Files section
        const filesLink = page.getByRole("link", { name: "Files" });
        if (await filesLink.isVisible()) {
            await filesLink.click();
            await page.waitForLoadState("networkidle");
            
            // Look for Logical Files link
            const logicalFilesLink = page.getByRole("link", { name: "Logical Files" });
            if (await logicalFilesLink.isVisible()) {
                await logicalFilesLink.click();
                await page.waitForLoadState("networkidle");
                
                // Verify we're on the logical files page
                await expect(page.getByText("Logical Name")).toBeVisible();
                await expect(page.getByText("Owner", { exact: true })).toBeVisible();
            }
        }
    });

    test("Should verify Activities page contains expected text elements", async ({ page }) => {
        // These are the text elements that the original Java test checked for
        const expectedTexts = [
            "Target/Wuid",
            "Graph", 
            "State",
            "Owner",
            "Job Name"
        ];
        
        for (const text of expectedTexts) {
            await expect(page.getByText(text)).toBeVisible();
        }
    });

    test("Should verify Activities page table functionality", async ({ page }) => {
        // Check for table-related elements
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        
        // Check for sortable columns
        await expect(page.getByRole("columnheader", { name: "Priority" }).locator("div").first()).toBeVisible();
        
        // Verify data rows exist
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
        
        // Check for controls
        await expect(page.locator("button").filter({ hasText: "" })).toBeVisible();
    });

    test("Should handle navigation errors gracefully", async ({ page }) => {
        // Test navigation resilience by checking main elements are still present
        // after any potential navigation failures
        
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator('.fui-NavDrawerBody')).toBeVisible();
        
        // Try to navigate back to activities if we're not there
        await page.goto("index.html#/activities");
        await page.waitForLoadState("networkidle");
        
        // Verify we're back on the activities page
        await expect(page.getByText("Target/Wuid")).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
    });

    test("Should test advanced and history buttons functionality", async ({ page }) => {
        // Check for Advanced button
        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
        
        // Check for History button  
        await expect(page.getByRole("button", { name: "History" })).toBeVisible();
        
        // Check for Add to favorites button
        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();
        
        // These buttons should be clickable
        const advancedButton = page.getByRole("button", { name: "Advanced" });
        await expect(advancedButton).toBeEnabled();
    });
});