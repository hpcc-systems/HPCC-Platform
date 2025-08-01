import { test as setup } from "@playwright/test";
import { Workunit } from "@hpcc-js/comms";
import { baseURL, ecl, saveWUs, setWU } from "./global";

async function submit(browserName, ecl: string): Promise<Workunit> {
    const wu = await Workunit.submit({ baseUrl: baseURL, userID: browserName }, "thor", ecl);
    wu.update({ Jobname: "global.setup.ts" });
    console.log(`    ${wu.Wuid}`);
    return wu.watchUntilComplete();
}

const browsers = ["chromium", "firefox", "webkit"];

setup("Setup", async () => {
    console.log("Setup:");
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
    console.log("");
});
