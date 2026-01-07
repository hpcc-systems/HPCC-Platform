import { test as base } from "@playwright/test";

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

export { expect } from "@playwright/test";
