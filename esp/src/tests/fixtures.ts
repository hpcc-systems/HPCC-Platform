import { test as base, Page } from "@playwright/test";

// Extend base test with custom fixtures
export const test = base.extend({
    context: async ({ context }, use) => {
        // Disable CSS animations and transitions for faster, more stable tests
        await context.addInitScript(() => {
            const style = document.createElement("style");
            style.textContent = "* { transition: none !important; animation: none !important; }";
            document.head?.appendChild(style);
        });

        await use(context);
    }
});

// CommandBarV9 collapses toolbar items into a "More items" overflow menu when
// the toolbar is too narrow to fit them all. Open it (if present) so overflowed
// menuitems are also discoverable, avoiding viewport-width flakiness.
export async function openOverflowMenu(page: Page) {
    const overflowButton = page.locator("button[aria-label='More items']");
    if (await overflowButton.isVisible()) {
        await overflowButton.click();
    }
}

// TabList similarly collapses tabs into an overflow menu (aria-label "N more
// menu items", see OverflowTabList.tsx) when there are too many to fit. Click
// the tab directly if visible, otherwise open the overflow menu and click it
// there, avoiding viewport-width flakiness.
export async function clickTab(page: Page, tabName: string) {
    const directTab = page.getByRole("tab", { name: tabName });
    if (await directTab.isVisible({ timeout: 1000 }).catch(() => false)) {
        await directTab.click();
        return;
    }

    const overflowButton = page.getByRole("button", { name: /more menu items/i });
    if (await overflowButton.isVisible({ timeout: 1000 }).catch(() => false)) {
        await overflowButton.click();
        await page.getByRole("menuitem", { name: tabName }).click();
    } else {
        // Fallback to direct click if overflow menu not found
        await directTab.click();
    }
}

export { expect } from "@playwright/test";
