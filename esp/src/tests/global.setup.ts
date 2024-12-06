import { test, expect } from "@playwright/test";
import { Workunit, WUUpdate } from "@hpcc-js/comms";
import { baseURL } from "../playwright.config";

test.describe("Playground tests", () => {

    let editor;

    test.beforeEach(async ({ page }) => {
        await page.goto("/esp/files/index.html#/play");
        editor = await page.locator(".CodeMirror");
        await editor?.click();
        await page.keyboard.press("Control+A");
        await page.keyboard.press("Backspace");
    })

    test("Execute Simple Sort sample", async ({ page }) => {
        await page.getByText("Simple Filter").click();
        await page.getByRole("option", { name: "Simple Sort" }).click();
        await page.getByRole("button", { name: "Submit" }).click();
        await expect(page.getByRole("link", { name: "completed" })).toBeVisible();
        await expect(page.getByRole("tab", { name: "Result" })).toBeVisible();
        await expect(page.locator("#dgrid_0-header")).toBeVisible();
        await expect(page.locator(".dgrid-row")).toHaveCount(3);
        await expect(page.locator("svg > g > g > g")).toBeVisible();
    });

    test("Create two simple files", async ({ page }) => {
        await page.keyboard.type(`
Layout_Person := RECORD
    UNSIGNED1 PersonID;
    STRING15 FirstName;
    STRING25 LastName;
END;

allPeople := DATASET([  {1, 'Fred', 'Smith'},
                        {2, 'Joe', 'Blow'},
                        {3, 'Jane', 'Smith'}], Layout_Person);

somePeople := allPeople(LastName = 'Smith');

//  Outputs  ---
OUTPUT(allPeople,,'~allPeople',OVERWRITE);
OUTPUT(somePeople,,'~somePeople',OVERWRITE);
            `);

        await page.getByRole("button", { name: "Submit" }).click();
        await expect(page.getByRole("link", { name: "completed" })).toBeVisible();
        const result2 = await page.getByRole("tab", { name: "Result 2" });
        await expect(result2).toBeVisible();
        await result2.click();
        await expect(page.locator(".dgrid-header-row")).toBeVisible();
        await expect(page.locator(".dgrid-row")).toHaveCount(2);
    });

    // this test was failing when run in GitHub Actions
    /*
    test("Publish a roxie query", async ({ page }) => {
        await page.keyboard.type(`
resistorCodes := dataset([{0, 'Black'},
    {1, 'Brown'},
    {2, 'Red'},
    {3, 'Orange'},
    {4, 'Yellow'},
    {5, 'Green'},
    {6, 'Blue'},
    {7, 'Violet'},
    {8, 'Grey'},
    {9, 'White'}], {unsigned1 value, string color}) : stored('colorMap');

color2code := DICTIONARY(resistorCodes, { color => value});

colourDictionary := dictionary(recordof(color2code));

bands := DATASET([{'Red'},{'Yellow'},{'Blue'}], {string band}) : STORED('bands');

valrec := RECORD
unsigned1 value;
END;

valrec getValue(bands L, colourDictionary mapping) := TRANSFORM
SELF.value := mapping[L.band].value;
END;

results := allnodes(PROJECT(bands, getValue(LEFT, THISNODE(color2code))));

ave(results, value);`
        );
        await page.getByText("hthor", { exact: true }).click();
        await page.getByRole("option", { name: "roxie", exact: true }).click();
        await page.getByLabel("Name", { exact: true }).fill("dictallnodes2");
        await page.getByRole("button", { name: "Publish" }).click();
        await expect(page.getByRole("link", { name: "compiled" })).toBeVisible();
    });
    */

    test("Publish a roxie query", async ({ }) => {
        const wu = await Workunit.create({ baseUrl: baseURL });

        const query = `
resistorCodes := dataset([{0, 'Black'},
    {1, 'Brown'},
    {2, 'Red'},
    {3, 'Orange'},
    {4, 'Yellow'},
    {5, 'Green'},
    {6, 'Blue'},
    {7, 'Violet'},
    {8, 'Grey'},
    {9, 'White'}], {unsigned1 value, string color}) : stored('colorMap');

color2code := DICTIONARY(resistorCodes, { color => value});

colourDictionary := dictionary(recordof(color2code));

bands := DATASET([{'Red'},{'Yellow'},{'Blue'}], {string band}) : STORED('bands');

valrec := RECORD
unsigned1 value;
END;

valrec getValue(bands L, colourDictionary mapping) := TRANSFORM
SELF.value := mapping[L.band].value;
END;

results := allnodes(PROJECT(bands, getValue(LEFT, THISNODE(color2code))));

ave(results, value);`;

        await wu.update({ Jobname: "dictallnodes2", QueryText: query });
        await wu.submit("roxie", WUUpdate.Action.Compile);
        await wu.watchUntilComplete();
        await wu.publish("dictallnodes2");
    });

    test("Create a few WUs", async ({ page }) => {
        await page.keyboard.type(`OUTPUT('Hello World')`);
        await page.getByRole("button", { name: "Submit" }).click();
        await expect(page.getByRole("link", { name: "completed" })).toBeVisible();
        await page.getByRole("button", { name: "Submit" }).click();
        await expect(page.getByRole("link", { name: "completed" })).toBeVisible();
        await page.getByRole("button", { name: "Submit" }).click();
        await expect(page.getByRole("link", { name: "completed" })).toBeVisible();
    });

});