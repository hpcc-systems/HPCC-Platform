import { DFUWorkunit, Workunit } from "@hpcc-js/comms";
import * as fs from "fs";
import * as path from "path";

export let baseURL = "http://127.0.0.1:8080";
export function setBaseURL(baseUrl: string) {
    baseURL = baseUrl;
}

const WU_DATA_FILE = path.join(__dirname, "test-wus.json");
const DFU_WU_DATA_FILE = path.join(__dirname, "test-dfu-wus.json");

interface StoredWU {
    Wuid: string;
}

interface StoredData {
    baseURL: string;
    wus: { [browser: string]: StoredWU[] };
}

interface StoredDFUWU {
    Wuid: string;
}

interface StoredDFUData {
    baseURL: string;
    dfuWus: { [browser: string]: StoredDFUWU[] };
}

export const state: StoredData = { baseURL, wus: {} };
export const dfuState: StoredDFUData = { baseURL, dfuWus: {} };

export function loadWUs() {
    try {
        if (fs.existsSync(WU_DATA_FILE)) {
            const data = fs.readFileSync(WU_DATA_FILE, "utf8");
            const stored: StoredData = JSON.parse(data);
            if (stored.baseURL) {
                baseURL = stored.baseURL;
            }
            if (stored.wus) {
                state.wus = stored.wus;
            }
        }
    } catch (err) {
        console.warn("Failed to load WU data:", err);
    }
}

export function saveWUs() {
    try {
        const dataToSave: StoredData = {
            baseURL,
            wus: state.wus
        };
        fs.writeFileSync(WU_DATA_FILE, JSON.stringify(dataToSave, null, 2));
    } catch (err) {
        console.warn("Failed to save WU data:", err);
    }
}

export function loadDFUWUs() {
    try {
        if (fs.existsSync(DFU_WU_DATA_FILE)) {
            const data = fs.readFileSync(DFU_WU_DATA_FILE, "utf8");
            const stored: StoredDFUData = JSON.parse(data);
            if (stored.baseURL) {
                baseURL = stored.baseURL;
            }
            if (stored.dfuWus) {
                dfuState.dfuWus = stored.dfuWus;
            }
        }
    } catch (err) {
        console.warn("Failed to load DFU WU data:", err);
    }
}

export function saveDFUWUs() {
    try {
        const dataToSave: StoredDFUData = {
            baseURL,
            dfuWus: dfuState.dfuWus
        };
        fs.writeFileSync(DFU_WU_DATA_FILE, JSON.stringify(dataToSave, null, 2));
    } catch (err) {
        console.warn("Failed to save DFU WU data:", err);
    }
}

export function setWU(wu: Workunit) {
    if (!state.wus[wu.Owner]) {
        state.wus[wu.Owner] = [];
    }
    const storedWU: StoredWU = {
        Wuid: wu.Wuid
    };
    state.wus[wu.Owner].push(storedWU);
    console.log(`   ${wu.Owner} ${wu.Wuid}`);
}

type DFUWUIdentifier = {
    Wuid?: string;
    ID?: string;
    wuid?: string;
    id?: string;
};

function getDFUWuId(wu: DFUWorkunit): string {
    const candidate = (wu as DFUWUIdentifier).Wuid ?? (wu as DFUWUIdentifier).ID ?? (wu as DFUWUIdentifier).wuid ?? (wu as DFUWUIdentifier).id;
    if (!candidate) {
        throw new Error("DFU workunit identifier is missing");
    }
    return candidate;
}

export function setDFUWU(wu: DFUWorkunit) {
    if (!dfuState.dfuWus[wu.User]) {
        dfuState.dfuWus[wu.User] = [];
    }
    const storedWU: StoredDFUWU = {
        Wuid: getDFUWuId(wu)
    };
    dfuState.dfuWus[wu.User].push(storedWU);
    console.log(`   ${wu.User} ${storedWU.Wuid}`);
}

export function getWULength(browser: string) {
    loadWUs();
    return state.wus[browser]?.length ?? 0;
}

export function getDFUWULength(browser: string) {
    loadDFUWUs();
    return dfuState.dfuWus[browser]?.length ?? 0;
}

export function getWuid(browser: string, idx: number): string {
    const wuEntries = state.wus[browser];
    if (!wuEntries || idx >= wuEntries.length) {
        throw new Error(`No workunit found for browser "${browser}" at index ${idx}`);
    }
    const wuEntry = wuEntries[idx];
    return wuEntry.Wuid;
}

export function getDFUWuid(browser: string, idx: number): string {
    const wuEntries = dfuState.dfuWus[browser];
    if (!wuEntries || idx >= wuEntries.length) {
        throw new Error(`No DFU workunit found for browser "${browser}" at index ${idx}`);
    }
    const wuEntry = wuEntries[idx];
    return wuEntry.Wuid;
}

export namespace ecl {
    export const helloWorld = `\
#option('generateLogicalGraph', true);
OUTPUT('Hello World');
`;

    export const normDenorm = `\
#option('generateLogicalGraph', true);
ParentRec := RECORD
    INTEGER1  NameID;
    STRING20  Name;
END;

ChildRec := RECORD
    INTEGER1  NameID;
    STRING20  Addr;
END;

DenormedRec := RECORD
    ParentRec;
    INTEGER1 NumRows;
    DATASET(ChildRec) Children {MAXCOUNT(5)};
END;

NamesTable := DATASET([ {1, 'Gavin'}, 
                        {2, 'Liz'}, 
                        {3, 'Mr Nobody'}, 
                        {4, 'Anywhere'}], 
                      ParentRec);            

NormAddrs := DATASET([{1, '10 Malt Lane'},     
                      {2, '10 Malt Lane'},     
                      {2, '3 The cottages'},     
                      {4, 'Here'},     
                      {4, 'There'},     
                      {4, 'Near'},     
                      {4, 'Far'}], 
                     ChildRec);    

DenormedRec ParentLoad(ParentRec L) := TRANSFORM
    SELF.NumRows := 0;
    SELF.Children := [];
    SELF := L;
END;

Ptbl := PROJECT(NamesTable, ParentLoad(LEFT));
OUTPUT(Ptbl,, 'global::setup::ts::ParentDataReady', OVERWRITE);

DenormedRec DeNormThem(DenormedRec L, ChildRec R, INTEGER C) := TRANSFORM
    SELF.NumRows := C;
    SELF.Children := L.Children + R;
    SELF := L;
END;

DeNormedRecs := DENORMALIZE(Ptbl, NormAddrs, 
                            LEFT.NameID = RIGHT.NameID, 
                            DeNormThem(LEFT, RIGHT, COUNTER));

OUTPUT(DeNormedRecs,, 'global::setup::ts::NestedChildDataset', OVERWRITE);

ParentRec ParentOut(DenormedRec L) := TRANSFORM
    SELF := L;
END;

Pout := PROJECT(DeNormedRecs, ParentOut(LEFT));
OUTPUT(Pout,, 'global::setup::ts::ParentExtracted', OVERWRITE);

ChildRec NewChildren(DenormedRec L, INTEGER C) := TRANSFORM
    SELF := L.Children[C];
END;
NewChilds := NORMALIZE(DeNormedRecs, LEFT.NumRows, NewChildren(LEFT, COUNTER));

OUTPUT(NewChilds,, 'global::setup::ts::ChildrenExtracted', OVERWRITE);
`;
}
