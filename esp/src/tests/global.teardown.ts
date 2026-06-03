import { test as teardown } from "@playwright/test";
import { DFUArrayActions, Workunit, DFUWorkunit, DFUService } from "@hpcc-js/comms";
import { baseURL, userID, password, dfuState, loadDFUWUs } from "./global";

const dfuService = new DFUService({ baseUrl: baseURL, userID, password });

teardown("Teardown", async ({ }) => {
    console.log("Teardown WUs:");
    const wus = await Workunit.query({ baseUrl: baseURL, userID, password }, { Jobname: "global.setup.ts" });
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

    console.log("Teardown DFU WUs:");

    loadDFUWUs();
    for (const storedDFUDatas of Object.values(dfuState.dfuWus)) {
        for (const storedDFUData of storedDFUDatas) {
            console.log(`    ${storedDFUData.Wuid}`);
            const wu = await DFUWorkunit.attach({ baseUrl: baseURL, userID, password }, storedDFUData.Wuid);
            await wu.delete();
        }
    }

    console.log("");
});
