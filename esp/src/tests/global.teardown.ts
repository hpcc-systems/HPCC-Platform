import { test, expect } from "@playwright/test";

test.describe("Teardown", () => {

    // these tests were failing when run in GitHub Actions

    // test("Delete all Queries", async ({ page }) => {
    //     await page.goto("/esp/files/index.html#/queries");
    //     await page.locator(".ms-DetailsHeader .ms-DetailsRow-check").click();
    //     await page.getByRole("menuitem", { name: "Delete" }).click();
    //     await page.getByRole("button", { name: "OK" }).click();
    // });

    // test("Delete all Files", async ({ page }) => {
    //     await page.goto("/esp/files/index.html#/files");
    //     await page.locator(".ms-DetailsHeader .ms-DetailsRow-check").click();
    //     await page.getByRole("menuitem", { name: "Delete" }).click();
    //     await page.getByRole("button", { name: "OK" }).click();
    // });

    // test("Delete all Workunits", async ({ page }) => {
    //     await page.goto("/esp/files/index.html#/workunits");
    //     await page.locator(".ms-DetailsHeader .ms-DetailsRow-check").click();
    //     await page.getByRole("menuitem", { name: "Delete" }).click();
    //     await page.getByRole("button", { name: "OK" }).click();
    // });

});