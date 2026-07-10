import { test, expect } from "./fixtures";

test.describe("CodeMirror Find Dialog", () => {

    test.beforeEach(async ({ page }) => {
        await page.goto("index.html#/play");
        await page.waitForLoadState("networkidle");
        await page.locator(".fui-NavDrawerBody").waitFor({ state: "visible", timeout: 15000 });
    });

    test("Should display the Find button in the ECL editor toolbar", async ({ page }) => {
        await expect(page.getByRole("button", { name: "Find" })).toBeVisible();
    });

    test("Should open the find dialog when the Find button is clicked", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        await expect(page.getByRole("dialog", { name: "Find" })).toBeVisible();
    });

    test("Should display find input, Match Case and Match Whole Word options in the dialog", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        const dialog = page.getByRole("dialog", { name: "Find" });
        await expect(dialog.getByRole("textbox")).toBeVisible();
        await expect(dialog.getByRole("checkbox", { name: "Match Case" })).toBeVisible();
        await expect(dialog.getByRole("checkbox", { name: "Match Whole Word" })).toBeVisible();
    });

    test("Should display the Find Next button in the dialog", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        const dialog = page.getByRole("dialog", { name: "Find" });
        await expect(dialog.getByRole("button", { name: "Find Next" })).toBeVisible();
    });

    test("Should close the dialog when the Cancel button is clicked", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        const dialog = page.getByRole("dialog", { name: "Find" });
        await expect(dialog).toBeVisible();
        await dialog.getByRole("button", { name: "Cancel" }).click();
        await expect(dialog).not.toBeVisible();
    });

    test("Should close the dialog when Escape is pressed", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        const dialog = page.getByRole("dialog", { name: "Find" });
        await expect(dialog).toBeVisible();
        await dialog.getByRole("textbox").press("Escape");
        await expect(dialog).not.toBeVisible();
    });

    test("Should accept text typed into the find input", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        const dialog = page.getByRole("dialog", { name: "Find" });
        const input = dialog.getByRole("textbox");
        await input.fill("OUTPUT");
        await expect(input).toHaveValue("OUTPUT");
    });

    test("Should toggle the Match Case checkbox", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        const dialog = page.getByRole("dialog", { name: "Find" });
        const matchCase = dialog.getByRole("checkbox", { name: "Match Case" });
        await expect(matchCase).not.toBeChecked();
        await matchCase.click();
        await expect(matchCase).toBeChecked();
    });

    test("Should toggle the Match Whole Word checkbox", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        const dialog = page.getByRole("dialog", { name: "Find" });
        const matchWholeWord = dialog.getByRole("checkbox", { name: "Match Whole Word" });
        await expect(matchWholeWord).not.toBeChecked();
        await matchWholeWord.click();
        await expect(matchWholeWord).toBeChecked();
    });

    test("Should reopen the dialog when the Find button is clicked a second time after closing", async ({ page }) => {
        await page.getByRole("button", { name: "Find" }).click();
        const dialog = page.getByRole("dialog", { name: "Find" });
        await dialog.getByRole("button", { name: "Cancel" }).click();
        await expect(dialog).not.toBeVisible();
        await page.getByRole("button", { name: "Find" }).click();
        await expect(dialog).toBeVisible();
    });

});
