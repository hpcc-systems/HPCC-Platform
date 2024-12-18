import { test, expect } from "@playwright/test";

test.describe("Basic ECLWatch V9 UI", () => {

    test("Frame Loaded", async ({ page }) => {
        await page.goto("/esp/files/index.html#/activities");
        await expect(page.getByRole("link", { name: "ECL Watch" })).toBeVisible();
        await expect(page.locator("button").filter({ hasText: "" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Advanced" })).toBeVisible();
        await expect(page.getByTitle("Activities")).toBeVisible();
        await expect(page.getByRole("link", { name: "ECL", exact: true })).toBeVisible();
        await expect(page.getByRole("link", { name: "Files" })).toBeVisible();
        await expect(page.getByRole("link", { name: "Published Queries" })).toBeVisible();
        await expect(page.getByRole("button", { name: "History" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Add to favorites" })).toBeVisible();
        await expect(page.locator("a").filter({ hasText: /^Activities$/ })).toBeVisible();
        await expect(page.getByRole("link", { name: "Event Scheduler" })).toBeVisible();
    });

    test("Activities page", async ({ page }) => {
        await page.goto("/esp/files/index.html#/activities");
        await expect(page.locator("svg").filter({ hasText: "%hthor" })).toBeVisible();
        await expect(page.locator(".reflex-splitter")).toBeVisible();
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.locator("button").filter({ hasText: "" })).toBeVisible();
        await expect(page.locator("button").filter({ hasText: "" })).toBeVisible();
        await expect(page.getByRole("columnheader", { name: "Priority" }).locator("div").first()).toBeVisible();
        await expect(page.getByText("Target/Wuid")).toBeVisible();
        await expect(page.getByText("Graph")).toBeVisible();
        await expect(page.getByText("State")).toBeVisible();
        await expect(page.getByText("Owner")).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
        await expect(page.locator(".dgrid-row")).not.toHaveCount(0);
    });
});

test.describe("Playground tests", () => {

    test("Execute Simple Filter sample", async ({ page }) => {
        await page.goto("/esp/files/index.html#/play");
        await page.getByText("Simple Filter").click();
        await page.getByRole("option", { name: "Simple Sort" }).click();
        await page.getByRole("button", { name: "Submit" }).click();
        await expect(page.getByRole("link", { name: "completed" })).toBeVisible({ timeout: 10000 });
        await expect(page.getByRole("tab", { name: "Result" })).toBeVisible();
        await expect(page.locator("#dgrid_0-header")).toBeVisible();
        await expect(page.locator(".dgrid-row")).toHaveCount(3);
        await expect(page.locator("svg > g > g > g")).toBeVisible();
    });

    test("Create two simple files", async ({ page }) => {
        await page.goto("/esp/files/index.html#/play");
        const editor = await page.locator(".CodeMirror");
        await editor?.click();
        await page.keyboard.press("Control+A");
        await page.keyboard.press("Backspace");
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
        await expect(page.getByRole("link", { name: "completed" })).toBeVisible({ timeout: 10000 });
        const result2 = await page.getByRole("tab", { name: "Result 2" });
        await expect(result2).toBeVisible({ timeout: 10000 });
        await result2.click();
        await expect(page.locator(".dgrid-header-row")).toBeVisible({ timeout: 10000 });
        await expect(page.locator(".dgrid-row")).toHaveCount(2);
    });

    test("Publish a roxie query", async ({ page }) => {
        await page.goto("/esp/files/index.html#/play");
        const editor = await page.locator(".CodeMirror");
        await editor?.click();
        await page.keyboard.press("Control+A");
        await page.keyboard.press("Backspace");
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
        await expect(page.getByRole("link", { name: "compiled" })).toBeVisible({ timeout: 10000 });
    });

});

test.describe("Workunit tests", () => {

    test("View the Workunits list page", async ({ page }) => {
        await page.goto("/esp/files/index.html#/workunits");
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByText("WUID")).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();
        await expect(page.getByText("Job Name")).toBeVisible();
        await expect(page.getByText("Cluster", { exact: true })).toBeVisible();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Filter the Workunits list page", async ({ page }) => {
        const date = new Date();
        await page.goto("/esp/files/index.html#/workunits");
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByPlaceholder("W20200824-060035").fill(`W${date.getFullYear()}${date.getMonth() + 1}${date.getDate()}*`);
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByPlaceholder("W20200824-060035").fill(`W2023*`);
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).toHaveCount(0);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("button", { name: "Clear" }).click();
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Protect / Unprotect a WU", async ({ page }) => {
        await page.goto("/esp/files/index.html#/workunits");
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").first().locator(".ms-DetailsRow-check").click();
        await expect(page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        await page.getByRole("menuitem", { name: "Protect", exact: true }).click();
        await expect(page.locator(".ms-DetailsRow").first().locator("[data-icon-name=\"LockSolid\"]")).toBeVisible();
        await page.getByRole("menuitem", { name: "Unprotect" }).click();
        await expect(page.locator(".ms-DetailsRow").first().locator("[data-icon-name=\"LockSolid\"]")).not.toBeVisible();
    });

    test("Set a WU to failed", async ({ page }) => {
        await page.goto("/esp/files/index.html#/workunits");
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").filter({ hasText: "completed" }).first().locator(".ms-DetailsRow-check").click();
        await expect(page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        await page.getByRole("menuitem", { name: "Set To Failed", exact: true }).click();
        await expect(page.locator(".ms-DetailsRow.is-selected").filter({ hasText: "failed" })).toBeVisible();
    });

    test("Delete a WU", async ({ page }) => {
        await page.goto("/esp/files/index.html#/workunits");
        const wuCount = await page.locator(".ms-DetailsRow").count();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
        await page.locator(".ms-DetailsRow").filter({ hasText: "failed" }).first().locator(".ms-DetailsRow-check").click();
        await expect(page.locator(".ms-DetailsRow.is-selected")).toHaveCount(1);
        await page.getByRole("menuitem", { name: "Delete", exact: true }).click();
        await page.getByRole("button", { name: "OK" }).click();
        await expect(page.locator(".ms-DetailsRow")).toHaveCount(wuCount - 1);
    });

});

test.describe("File tests", () => {

    test("View the Files list page", async ({ page }) => {
        await page.goto("/esp/files/index.html#/files");
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByText("Logical Name")).toBeVisible();
        await expect(page.getByText("Owner", { exact: true })).toBeVisible();
        await expect(page.getByText("Cluster")).toBeVisible();
        await expect(page.getByText("Records")).toBeVisible();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Filter the Files list page", async ({ page }) => {
        await page.goto("/esp/files/index.html#/files");
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByPlaceholder("*::somefile*").fill("*allPeople*");
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).toHaveCount(1);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("button", { name: "Clear" }).click();
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(1);
    });

});

test.describe("Query tests", () => {

    test("View the Queries list page", async ({ page }) => {
        await page.goto("/esp/files/index.html#/queries");
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await expect(page.getByText("ID", { exact: true })).toBeVisible();
        await expect(page.getByText("Priority", { exact: true })).toBeVisible();
        await expect(page.getByText("Name")).toBeVisible();
        await expect(page.getByText("Target")).toBeVisible();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

    test("Filter the Queries list page", async ({ page }) => {
        await page.goto("/esp/files/index.html#/queries");
        await expect(page.getByRole("menubar")).toBeVisible();
        await expect(page.getByRole("menuitem", { name: "Refresh" })).toBeVisible();
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByPlaceholder("My?Su?erQ*ry").fill("asdf");
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).toHaveCount(0);
        await page.getByRole("menuitem", { name: "Filter" }).click();
        await page.getByRole("button", { name: "Clear" }).click();
        await page.getByRole("button", { name: "Apply" }).click();
        await expect(page.locator(".ms-DetailsRow")).not.toHaveCount(0);
    });

});
