import * as Observable from "dojo/store/Observable";
import * as ESPWorkunit from "src/ESPWorkunit";
import * as ESPDFUWorkunit from "src/ESPDFUWorkunit";
import * as ESPLogicalFile from "src/ESPLogicalFile";
import * as ESPQuery from "src/ESPQuery";
import { Memory } from "./Memory";
import * as WsWorkunits from "./WsWorkunits";
import * as FileSpray from "./FileSpray";
import * as WsDfu from "./WsDfu";
import nlsHPCC from "./nlsHPCC";

export type searchAllResponse = undefined | "ecl" | "dfu" | "file" | "query";

interface SearchParams {
    searchECL: boolean;
    searchECLText: boolean;
    searchDFU: boolean;
    searchFile: boolean;
    searchQuery: boolean;
    text: string;
    searchType?: string;
}

export class ESPSearch {

    protected _searchID = 0;
    protected id = 0;
    protected _rowID = 0;
    protected _searchText: string;

    store = Observable(new Memory());
    eclStore = Observable(new Memory("Wuid"));
    dfuStore = Observable(new Memory("ID"));
    fileStore = Observable(new Memory("__hpcc_id"));
    queryStore = Observable(new Memory("__hpcc_id"));

    constructor(private update: () => void) {
    }

    generateSearchParams(searchText: string): SearchParams {
        const searchParams: SearchParams = {
            searchECL: false,
            searchECLText: false,
            searchDFU: false,
            searchFile: false,
            searchQuery: false,
            text: searchText
        };

        if (searchText.indexOf("ecl:") === 0) {
            searchParams.searchECL = true;
            searchParams.searchECLText = true;
            searchParams.text = searchText.substring(4);
        } else if (searchText.indexOf("dfu:") === 0) {
            searchParams.searchDFU = true;
            searchParams.text = searchText.substring(4);
        } else if (searchText.indexOf("file:") === 0) {
            searchParams.searchFile = true;
            searchParams.text = searchText.substring(5);
        } else if (searchText.indexOf("query:") === 0) {
            searchParams.searchQuery = true;
            searchParams.text = searchText.substring(6);
        } else {
            searchParams.searchECL = true;
            searchParams.searchDFU = true;
            searchParams.searchFile = true;
            searchParams.searchQuery = true;
        }
        searchParams.text = searchParams.text.trim();

        return searchParams;
    }

    searchAll(searchText: string): any {

        this.store.setData([]);
        this.eclStore.setData([]);
        this.dfuStore.setData([]);
        this.fileStore.setData([]);
        this.queryStore.setData([]);

        const searchArray = [];
        const searchParams = this.generateSearchParams(searchText);

        if (searchParams.searchECL) {
            searchArray.push(WsWorkunits.WUQuery({ request: { Wuid: "*" + searchParams.text + "*" } }).then(response => {
                const results = this.parseWuQueryResponse(nlsHPCC.WUID, response);
                this.loadWUQueryResponse(results);
                return results;
            }));
            searchArray.push(WsWorkunits.WUQuery({ request: { Jobname: "*" + searchText + "*" } }).then(response => {
                const results = this.parseWuQueryResponse(nlsHPCC.JobName, response);
                this.loadWUQueryResponse(results);
                return results;
            }));
            searchArray.push(WsWorkunits.WUQuery({ request: { Owner: searchText } }).then(response => {
                const results = this.parseWuQueryResponse(nlsHPCC.Owner, response);
                this.loadWUQueryResponse(results);
                return results;
            }));
        }
        if (searchParams.searchECLText) {
            searchArray.push(WsWorkunits.WUQuery({ request: { ECL: searchParams.text } }).then(response => {
                const results = this.parseWuQueryResponse(nlsHPCC.ECL, response);
                this.loadWUQueryResponse(results);
                return results;
            }));
        }
        if (searchParams.searchDFU) {
            searchArray.push(FileSpray.GetDFUWorkunit({ request: { wuid: "*" + searchText + "*" } }).then(response => {
                const results = this.parseGetDFUWorkunitResponse(nlsHPCC.ECL, response);
                this.loadGetDFUWorkunitResponse(results);
                return results;
            }));
            searchArray.push(FileSpray.GetDFUWorkunits({ request: { Jobname: "*" + searchText + "*" } }).then(response => {
                const results = this.parseGetDFUWorkunitsResponse(nlsHPCC.JobName, response);
                this.loadGetDFUWorkunitsResponse(results);
                return results;
            }));
            searchArray.push(FileSpray.GetDFUWorkunits({ request: { Owner: searchText } }).then(response => {
                const results = this.parseGetDFUWorkunitsResponse(nlsHPCC.Owner, response);
                this.loadGetDFUWorkunitsResponse(results);
                return results;
            }));
        }
        if (searchParams.searchFile) {
            searchArray.push(WsDfu.DFUQuery({ request: { LogicalName: "*" + searchParams.text + "*" } }).then(response => {
                const results = this.parseDFUQueryResponse(nlsHPCC.LogicalName, response);
                this.loadDFUQueryResponse(results);
                return results;
            }));
            searchArray.push(WsDfu.DFUQuery({ request: { Owner: searchParams.text } }).then(response => {
                const results = this.parseDFUQueryResponse(nlsHPCC.Owner, response);
                this.loadDFUQueryResponse(results);
                return results;
            }));
        }
        if (searchParams.searchQuery) {
            searchArray.push(WsWorkunits.WUListQueries({ request: { QueryID: "*" + searchParams.text + "*" } }).then(response => {
                const results = this.parseWUListQueriesResponse(nlsHPCC.ID, response);
                this.loadWUListQueriesResponse(results);
                return results;
            }));
            searchArray.push(WsWorkunits.WUListQueries({ request: { QueryName: "*" + searchParams.text + "*" } }).then(response => {
                const results = this.parseWUListQueriesResponse(nlsHPCC.Name, response);
                this.loadWUListQueriesResponse(results);
                return results;
            }));
        }

        this.update();

        return Promise.all(searchArray);
    }

    parseWuQueryResponse(prefix, response) {
        return response?.WUQueryResponse?.Workunits?.ECLWorkunit.map(item => {
            return {
                storeID: ++this._rowID,
                id: item.Wuid,
                Type: nlsHPCC.ECLWorkunit,
                Reason: prefix,
                Summary: item.Wuid,
                _type: "Wuid",
                _wuid: item.Wuid,
                ...item
            };
        });
    }

    parseGetDFUWorkunitResponse(prefix, response) {
        const item = response?.GetDFUWorkunitResponse?.result;
        return (item && item.State !== 999) ? {
            storeID: ++this._rowID,
            id: item.ID,
            Type: nlsHPCC.DFUWorkunit,
            Reason: prefix,
            Summary: item.ID,
            _type: "DFUWuid",
            _wuid: item.ID,
            ...item
        } : undefined;
    }

    parseGetDFUWorkunitsResponse(prefix, response) {
        return response?.GetDFUWorkunitsResponse?.results?.DFUWorkunit.map(item => {
            return {
                storeID: ++this._rowID,
                id: item.ID,
                Type: nlsHPCC.DFUWorkunit,
                Reason: prefix,
                Summary: item.ID,
                _type: "DFUWuid",
                _wuid: item.ID,
                ...item
            };
        });
    }

    parseDFUQueryResponse(prefix, response) {
        return response?.DFUQueryResponse?.DFULogicalFiles?.DFULogicalFile.map(item => {
            if (item.isSuperfile) {
                return {
                    storeID: ++this._rowID,
                    id: item.Name,
                    Type: nlsHPCC.SuperFile,
                    Reason: prefix,
                    Summary: item.Name,
                    _type: "SuperFile",
                    _nodeGroup: item.NodeGroup,
                    _name: item.Name,
                    ...item
                };
            }
            return {
                storeID: ++this._rowID,
                id: item.NodeGroup + "::" + item.Name,
                Type: nlsHPCC.LogicalFile,
                Reason: prefix,
                Summary: item.Name + " (" + item.NodeGroup + ")",
                _type: "LogicalFile",
                _nodeGroup: item.NodeGroup,
                _name: item.Name,
                ...item
            };
        });
    }

    parseWUListQueriesResponse(prefix, response) {
        return response?.WUListQueriesResponse?.QuerysetQueries?.QuerySetQuery.map(item => {
            return {
                storeID: ++this._rowID,
                id: item.QuerySetId + "::" + item.Id,
                Type: nlsHPCC.Query,
                Reason: prefix,
                Summary: item.Name + " (" + item.QuerySetId + " - " + item.Id + ")",
                _type: "Query",
                _querySetId: item.QuerySetId,
                _id: item.Id,
                ...item
            };
        });
    }

    loadWUQueryResponse(results) {
        if (results) {
            results.forEach((item, idx) => {
                this.store.put(item, { overwrite: true });
                this.eclStore.put(ESPWorkunit.Get(item._wuid, item), { overwrite: true });
            });
            return results.length;
        }
        return 0;
    }

    loadGetDFUWorkunitResponse(workunit) {
        if (workunit) {
            this.store.put(workunit, { overwrite: true });
            this.dfuStore.put(ESPDFUWorkunit.Get(workunit._wuid, workunit), { overwrite: true });
            return 1;
        }
        return 0;
    }

    loadGetDFUWorkunitsResponse(results) {
        if (results) {
            results.forEach((item, idx) => {
                this.store.put(item, { overwrite: true });
                this.dfuStore.put(ESPDFUWorkunit.Get(item._wuid, item), { overwrite: true });
            });
            return results.length;
        }
        return 0;
    }

    loadDFUQueryResponse(items) {
        if (items) {
            items.forEach((item, idx) => {
                this.store.put(item, { overwrite: true });
                this.fileStore.put(ESPLogicalFile.Get(item._nodeGroup, item._name, item), { overwrite: true });
            });
            return items.length;
        }
        return 0;
    }

    loadWUListQueriesResponse(items) {
        if (items) {
            items.forEach((item, idx) => {
                this.store.put(item, { overwrite: true });
                this.queryStore.put(ESPQuery.Get(item._querySetId, item._id, item), { overwrite: true });
            });
            return items.length;
        }
        return 0;
    }
}

export function searchAll(searchText: string,
    loadWUQueryResponse: (prefix: string, response: any) => void,
    loadGetDFUWorkunitResponse: (prefix: string, response: any) => void,
    loadGetDFUWorkunitsResponse: (prefix: string, response: any) => void,
    loadDFUQueryResponse: (prefix: string, response: any) => void,
    loadWUListQueriesResponse: (prefix: string, response: any) => void,
    start: (searchCount: number) => void,
    done: (success: boolean) => void,
): any {

    const generateSearchParams = (searchText: string): SearchParams => {
        const searchParams: SearchParams = {
            searchECL: false,
            searchECLText: false,
            searchDFU: false,
            searchFile: false,
            searchQuery: false,
            text: searchText,
            searchType: ""
        };

        if (searchText.indexOf("ECL:") === 0) {
            searchParams.searchType = "ecl";
            searchParams.searchECL = true;
            searchParams.searchECLText = true;
            searchParams.text = searchText.substring(4);
        } else if (searchText.indexOf("DFU:") === 0) {
            searchParams.searchType = "dfu";
            searchParams.searchDFU = true;
            searchParams.text = searchText.substring(4);
        } else if (searchText.indexOf("FILE:") === 0) {
            searchParams.searchType = "file";
            searchParams.searchFile = true;
            searchParams.text = searchText.substring(5);
        } else if (searchText.indexOf("QUERY:") === 0) {
            searchParams.searchType = "query";
            searchParams.searchQuery = true;
            searchParams.text = searchText.substring(6);
        } else {
            searchParams.searchECL = true;
            searchParams.searchDFU = true;
            searchParams.searchFile = true;
            searchParams.searchQuery = true;
        }
        searchParams.text = searchParams.text.trim();

        return searchParams;
    }

    const searchArray = [];
    const searchParams = generateSearchParams(searchText);

    if (searchParams.searchECL) {
        searchArray.push(WsWorkunits.WUQuery({ request: { Wuid: "*" + searchParams.text + "*" } }).then(response => {
            loadWUQueryResponse(nlsHPCC.WUID, response);
        }));
        searchArray.push(WsWorkunits.WUQuery({ request: { Jobname: "*" + searchText + "*" } }).then(response => {
            loadWUQueryResponse(nlsHPCC.JobName, response);
        }));
        searchArray.push(WsWorkunits.WUQuery({ request: { Owner: searchText } }).then(response => {
            loadWUQueryResponse(nlsHPCC.Owner, response);
        }));
    }
    if (searchParams.searchECLText) {
        searchArray.push(WsWorkunits.WUQuery({ request: { ECL: searchParams.text } }).then(response => {
            loadWUQueryResponse(nlsHPCC.ECL, response);
        }));
    }
    if (searchParams.searchDFU) {
        searchArray.push(FileSpray.GetDFUWorkunit({ request: { wuid: "*" + searchText + "*" } }).then(response => {
            loadGetDFUWorkunitResponse(nlsHPCC.ECL, response);
        }));
        searchArray.push(FileSpray.GetDFUWorkunits({ request: { Jobname: "*" + searchText + "*" } }).then(response => {
            loadGetDFUWorkunitsResponse(nlsHPCC.JobName, response);
        }));
        searchArray.push(FileSpray.GetDFUWorkunits({ request: { Owner: searchText } }).then(response => {
            loadGetDFUWorkunitsResponse(nlsHPCC.Owner, response);
        }));
    }
    if (searchParams.searchFile) {
        searchArray.push(WsDfu.DFUQuery({ request: { LogicalName: "*" + searchParams.text + "*" } }).then(response => {
            loadDFUQueryResponse(nlsHPCC.LogicalName, response);
        }));
        searchArray.push(WsDfu.DFUQuery({ request: { Owner: searchParams.text } }).then(response => {
            loadDFUQueryResponse(nlsHPCC.Owner, response);
        }));
    }
    if (searchParams.searchQuery) {
        searchArray.push(WsWorkunits.WUListQueries({ request: { QueryID: "*" + searchParams.text + "*" } }).then(response => {
            loadWUListQueriesResponse(nlsHPCC.ID, response);
        }));
        searchArray.push(WsWorkunits.WUListQueries({ request: { QueryName: "*" + searchParams.text + "*" } }).then(response => {
            loadWUListQueriesResponse(nlsHPCC.Name, response);
        }));
    }

    start(searchArray.length);
    Promise.all(searchArray).then(function (results) {
        done(true);
    }, function (error) {
        done(false);
    });

    return searchParams.searchType;
}