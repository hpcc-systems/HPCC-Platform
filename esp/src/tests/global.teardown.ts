import { test as teardown } from "@playwright/test";
import { DFUArrayActions, Workunit, DFUService } from "@hpcc-js/comms";
import { baseURL } from "./global";

const dfuService = new DFUService({ baseUrl: baseURL });

teardown("Teardown", async ({ }) => {
    console.log("Teardown:");
    const wus = await Workunit.query({ baseUrl: baseURL }, { Jobname: "global.setup.ts" });
    for (const wu of wus) {
        console.log(`    ${wu.Wuid}`);
        for (const result of await wu.fetchResults()) {
            if (result.FileName) {
                console.log(`        ${result.FileName}`);
                const lf = await dfuService.DFUArrayAction({ Type: DFUArrayActions.Delete, LogicalFiles: { Item: [result.FileName] } });
            }
        }
        await wu.delete();
    }
    console.log("");
    // these tests were failing when run in GitHub Actions

    // test("Delete all Queries", async ({ page }) => {
    //     await page.goto("index.html#/queries");
    //     await page.locator(".ms-DetailsHeader .ms-DetailsRow-check").click();
    //     await page.getByRole("menuitem", { name: "Delete" }).click();
    //     await page.getByRole("button", { name: "OK" }).click();
    // });

    // test("Delete all Files", async ({ page }) => {
    //     await page.goto("index.html#/files");
    //     await page.locator(".ms-DetailsHeader .ms-DetailsRow-check").click();
    //     await page.getByRole("menuitem", { name: "Delete" }).click();
    //     await page.getByRole("button", { name: "OK" }).click();
    // });

    // test("Delete all Workunits", async ({ page }) => {
    //     await page.goto("index.html#/workunits");
    //     await page.locator(".ms-DetailsHeader .ms-DetailsRow-check").click();
    //     await page.getByRole("menuitem", { name: "Delete" }).click();
    //     await page.getByRole("button", { name: "OK" }).click();
    // });

});
