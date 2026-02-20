import { test as setup } from "@playwright/test";
import { Workunit, DFUWorkunit } from "@hpcc-js/comms";
import { baseURL, ecl, saveDFUWUs, saveWUs, setDFUWU, setWU } from "./global";

async function submit(browserName: string, ecl: string): Promise<Workunit> {
    const wu = await Workunit.submit({ baseUrl: baseURL, userID: browserName }, "thor", ecl);
    wu.update({ Jobname: "global.setup.ts" });
    console.log(`    ${wu.Wuid}`);
    return wu.watchUntilComplete();
}

let idx = 0;
async function despray(browserName: string, sourceLogicalName: string): Promise<DFUWorkunit> {
    const wu = await DFUWorkunit.despray({ baseUrl: baseURL, userID: browserName }, {
        destGroup: "mydropzone",
        destIP: ".",
        destPath: `/var/lib/HPCCSystems/mydropzone/${browserName}_${sourceLogicalName}_${++idx}`,
        sourceLogicalName,
        overwrite: false,
        SingleConnection: false,
        wrap: false
    });
    console.log(`    ${wu.ID}`);
    return wu.watchUntilComplete();
}

const browsers = ["chromium", "firefox", "webkit"];
setup("Setup", async () => {
    console.log("Setup WUs:");
    const jobs: Promise<Workunit>[] = [];
    browsers.forEach(browserName => {
        jobs.push(submit(browserName, ecl.helloWorld));
        jobs.push(submit(browserName, ecl.normDenorm));
    });
    const wus = await Promise.all(jobs);
    for (const wu of wus) {
        setWU(wu);
    }
    saveWUs();

    console.log("Setup DFU WUs:");
    const sprayJobs: Promise<Workunit>[] = [];
    browsers.forEach(browserName => {
        sprayJobs.push(despray(browserName, "thor::global::setup::ts::ParentDataReady"));
    });
    const dfuWus = await Promise.all(sprayJobs);
    for (const wu of dfuWus) {
        setDFUWU(wu);
    }
    saveDFUWUs();

    console.log("");
});
