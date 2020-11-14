import * as Memory from "dojo/store/Memory";
import * as Observable from "dojo/store/Observable";
import * as ESPWorkunit from "src/ESPWorkunit";
import * as ESPDFUWorkunit from "src/ESPDFUWorkunit";
import * as ESPLogicalFile from "src/ESPLogicalFile";
import * as ESPQuery from "src/ESPQuery";
import * as WsWorkunits from "./WsWorkunits";
import * as FileSpray from "./FileSpray";
import * as WsDfu from "./WsDfu";
import nlsHPCC from "./nlsHPCC";

export type searchAllResponse = undefined | "ecl" | "dfu" | "file" | "query";

export class ESPSearch {

    protected _searchID = 0;
    protected id = 0;
    protected _rowID = 0;
    protected _searchText: string;

    store = Observable(new Memory());
    eclStore = Observable(new Memory({ idProperty: "Wuid", data: [] }));
    dfuStore = Observable(new Memory({ idProperty: "ID", data: [] }));
    fileStore = Observable(new Memory({ idProperty: "__hpcc_id", data: [] }));
    queryStore = Observable(new Memory({ idProperty: "__hpcc_id", data: [] }));

    constructor(private begin: (searchCount: number) => void, private update: () => void, private end: (success: boolean) => void) {
    }

    searchAll(searchText: string): searchAllResponse {
        const searchID = ++this._searchID;
        ++this.id;

        this.store.setData([]);
        this.eclStore.setData([]);
        this.dfuStore.setData([]);
        this.fileStore.setData([]);
        this.queryStore.setData([]);

        let searchECL = false;
        let searchECLText = false;
        let searchDFU = false;
        let searchFile = false;
        let searchQuery = false;

        let retVal: searchAllResponse = undefined;

        if (searchText.indexOf("ecl:") === 0) {
            retVal = "ecl";
            searchECL = true;
            searchECLText = true;
            searchText = searchText.substring(4);
        } else if (searchText.indexOf("dfu:") === 0) {
            searchDFU = true;
            searchText = searchText.substring(4);
        } else if (searchText.indexOf("file:") === 0) {
            searchFile = true;
            searchText = searchText.substring(5);
        } else if (searchText.indexOf("query:") === 0) {
            searchQuery = true;
            searchText = searchText.substring(6);
        } else {
            searchECL = true;
            searchDFU = true;
            searchFile = true;
            searchQuery = true;
        }
        searchText = searchText.trim();

        const searchArray = [];
        if (searchECL) {
            searchArray.push(WsWorkunits.WUQuery({ request: { Wuid: "*" + searchText + "*" }, suppressExceptionToaster: true }).then(response => {
                if (searchID === this._searchID) {
                    this.loadWUQueryResponse(nlsHPCC.WUID, response);
                    this.update();
                }
            }));
            searchArray.push(WsWorkunits.WUQuery({ request: { Jobname: "*" + searchText + "*" } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadWUQueryResponse(nlsHPCC.JobName, response);
                    this.update();
                }
            }));
            searchArray.push(WsWorkunits.WUQuery({ request: { Owner: searchText } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadWUQueryResponse(nlsHPCC.Owner, response);
                    this.update();
                }
            }));
        }
        if (searchECLText) {
            searchArray.push(WsWorkunits.WUQuery({ request: { ECL: searchText } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadWUQueryResponse(nlsHPCC.ECL, response);
                    this.update();
                }
            }));
        }
        if (searchDFU) {
            searchArray.push(FileSpray.GetDFUWorkunit({ request: { wuid: "*" + searchText + "*" }, suppressExceptionToaster: true }).then(response => {
                if (searchID === this._searchID) {
                    this.loadGetDFUWorkunitResponse(nlsHPCC.WUID, response);
                    this.update();
                }
            }));
            searchArray.push(FileSpray.GetDFUWorkunits({ request: { Jobname: "*" + searchText + "*" } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadGetDFUWorkunitsResponse(nlsHPCC.JobName, response);
                    this.update();
                }
            }));
            searchArray.push(FileSpray.GetDFUWorkunits({ request: { Owner: searchText } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadGetDFUWorkunitsResponse(nlsHPCC.Owner, response);
                    this.update();
                }
            }));
        }
        if (searchFile) {
            searchArray.push(WsDfu.DFUQuery({ request: { LogicalName: "*" + searchText + "*" } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadDFUQueryResponse(nlsHPCC.LogicalName, response);
                    this.update();
                }
            }));
            searchArray.push(WsDfu.DFUQuery({ request: { Owner: searchText } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadDFUQueryResponse(nlsHPCC.Owner, response);
                    this.update();
                }
            }));
        }
        if (searchQuery) {
            searchArray.push(WsWorkunits.WUListQueries({ request: { QueryID: "*" + searchText + "*" } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadWUListQueriesResponse(nlsHPCC.ID, response);
                    this.update();
                }
            }));
            searchArray.push(WsWorkunits.WUListQueries({ request: { QueryName: "*" + searchText + "*" } }).then(response => {
                if (searchID === this._searchID) {
                    this.loadWUListQueriesResponse(nlsHPCC.Name, response);
                    this.update();
                }
            }));
        }

        //  Always true  ---
        this.begin(searchArray.length);
        Promise.all(searchArray).then(results => {
            if (searchID === this._searchID)
                this.end(true);
        }).catch(e => {
            if (searchID === this._searchID)
                this.end(false);

        });

        return retVal;
    }

    loadWUQueryResponse(prefix, response) {
        const workunits = response?.WUQueryResponse?.Workunits?.ECLWorkunit;
        if (workunits) {
            workunits.forEach((item, idx) => {
                this.store.add({
                    storeID: ++this._rowID,
                    id: this.id + item.Wuid,
                    Type: nlsHPCC.ECLWorkunit,
                    Reason: prefix,
                    Summary: item.Wuid,
                    _type: "Wuid",
                    _wuid: item.Wuid
                });
                this.eclStore.add(ESPWorkunit.Get(item.Wuid, item), { overwrite: true });
            });
            return workunits.length;
        }
        return 0;
    }

    loadGetDFUWorkunitResponse(prefix, response) {
        const workunit = response?.GetDFUWorkunitResponse?.result;
        if (workunit && workunit.State !== 999) {
            this.store.add({
                storeID: ++this._rowID,
                id: this.id + workunit.ID,
                Type: nlsHPCC.DFUWorkunit,
                Reason: prefix,
                Summary: workunit.ID,
                _type: "DFUWuid",
                _wuid: workunit.ID
            });
            this.dfuStore.add(ESPDFUWorkunit.Get(workunit.ID, workunit), { overwrite: true });
            return 1;
        }
        return 0;
    }

    loadGetDFUWorkunitsResponse(prefix, response) {
        const workunits = response?.GetDFUWorkunitsResponse?.results?.DFUWorkunit;
        if (workunits) {
            workunits.forEach((item, idx) => {
                this.store.add({
                    storeID: ++this._rowID,
                    id: this.id + item.ID,
                    Type: nlsHPCC.DFUWorkunit,
                    Reason: prefix,
                    Summary: item.ID,
                    _type: "DFUWuid",
                    _wuid: item.ID
                });
                this.dfuStore.add(ESPDFUWorkunit.Get(item.ID, item), { overwrite: true });
            });
            return workunits.length;
        }
        return 0;
    }

    loadDFUQueryResponse(prefix, response) {
        const items = response?.DFUQueryResponse?.DFULogicalFiles?.DFULogicalFile;
        if (items) {
            items.forEach((item, idx) => {
                if (item.isSuperfile) {
                    this.store.add({
                        storeID: ++this._rowID,
                        id: this.id + item.Name,
                        Type: nlsHPCC.SuperFile,
                        Reason: prefix,
                        Summary: item.Name,
                        _type: "SuperFile",
                        _name: item.Name
                    });
                } else {
                    this.store.add({
                        storeID: ++this._rowID,
                        id: this.id + item.Name,
                        Type: nlsHPCC.LogicalFile,
                        Reason: prefix,
                        Summary: item.Name + " (" + item.NodeGroup + ")",
                        _type: "LogicalFile",
                        _nodeGroup: item.NodeGroup,
                        _name: item.Name
                    });
                }
                this.fileStore.add(ESPLogicalFile.Get(item.NodeGroup, item.Name, item), { overwrite: true });
            });
            return items.length;
        }
        return 0;
    }

    loadWUListQueriesResponse(prefix, response) {
        const items = response?.WUListQueriesResponse?.QuerysetQueries?.QuerySetQuery;
        if (items) {
            items.forEach((item, idx) => {
                this.store.add({
                    storeID: ++this._rowID,
                    id: this.id + item.QuerySetId + "::" + item.Id,
                    Type: nlsHPCC.Query,
                    Reason: prefix,
                    Summary: item.Name + " (" + item.QuerySetId + " - " + item.Id + ")",
                    _type: "Query",
                    _querySetId: item.QuerySetId,
                    _id: item.Id
                });
                this.queryStore.add(ESPQuery.Get(item.QuerySetId, item.Id, item), { overwrite: true });
            });
            return items.length;
        }
        return 0;
    }
}

export function searchAll(searchText: string,
    loadWUQueryResponse: (what: string, response: any) => void,
    loadGetDFUWorkunitResponse: (what: string, response: any) => void,
    loadGetDFUWorkunitsResponse: (what: string, response: any) => void,
    loadDFUQueryResponse: (what: string, response: any) => void,
    loadWUListQueriesResponse: (what: string, response: any) => void,
    start: (searchCount: number) => void,
    done: (success: boolean) => void,
): searchAllResponse {

    let searchECL = false;
    let searchECLText = false;
    let searchDFU = false;
    let searchFile = false;
    let searchQuery = false;

    let retVal: searchAllResponse = undefined;

    if (searchText.indexOf("ecl:") === 0) {
        retVal = "ecl";
        searchECL = true;
        searchECLText = true;
        searchText = searchText.substring(4);
    } else if (searchText.indexOf("dfu:") === 0) {
        searchDFU = true;
        searchText = searchText.substring(4);
    } else if (searchText.indexOf("file:") === 0) {
        searchFile = true;
        searchText = searchText.substring(5);
    } else if (searchText.indexOf("query:") === 0) {
        searchQuery = true;
        searchText = searchText.substring(6);
    } else {
        searchECL = true;
        searchDFU = true;
        searchFile = true;
        searchQuery = true;
    }
    searchText = searchText.trim();

    const searchArray = [];
    if (searchECL) {
        searchArray.push(WsWorkunits.WUQuery({ request: { Wuid: "*" + searchText + "*" }, suppressExceptionToaster: true }).then(function (response) {
            loadWUQueryResponse(nlsHPCC.WUID, response);
        }));
        searchArray.push(WsWorkunits.WUQuery({ request: { Jobname: "*" + searchText + "*" } }).then(function (response) {
            loadWUQueryResponse(nlsHPCC.JobName, response);
        }));
        searchArray.push(WsWorkunits.WUQuery({ request: { Owner: searchText } }).then(function (response) {
            loadWUQueryResponse(nlsHPCC.Owner, response);
        }));
    }
    if (searchECLText) {
        searchArray.push(WsWorkunits.WUQuery({ request: { ECL: searchText } }).then(function (response) {
            loadWUQueryResponse(nlsHPCC.ECL, response);
        }));
    }
    if (searchDFU) {
        searchArray.push(FileSpray.GetDFUWorkunit({ request: { wuid: "*" + searchText + "*" }, suppressExceptionToaster: true }).then(function (response) {
            loadGetDFUWorkunitResponse(nlsHPCC.WUID, response);
        }));
        searchArray.push(FileSpray.GetDFUWorkunits({ request: { Jobname: "*" + searchText + "*" } }).then(function (response) {
            loadGetDFUWorkunitsResponse(nlsHPCC.JobName, response);
        }));
        searchArray.push(FileSpray.GetDFUWorkunits({ request: { Owner: searchText } }).then(function (response) {
            loadGetDFUWorkunitsResponse(nlsHPCC.Owner, response);
        }));
    }
    if (searchFile) {
        searchArray.push(WsDfu.DFUQuery({ request: { LogicalName: "*" + searchText + "*" } }).then(function (response) {
            loadDFUQueryResponse(nlsHPCC.LogicalName, response);
        }));
        searchArray.push(WsDfu.DFUQuery({ request: { Owner: searchText } }).then(function (response) {
            loadDFUQueryResponse(nlsHPCC.Owner, response);
        }));
    }
    if (searchQuery) {
        searchArray.push(WsWorkunits.WUListQueries({ request: { QueryID: "*" + searchText + "*" } }).then(function (response) {
            loadWUListQueriesResponse(nlsHPCC.ID, response);
        }));
        searchArray.push(WsWorkunits.WUListQueries({ request: { QueryName: "*" + searchText + "*" } }).then(function (response) {
            loadWUListQueriesResponse(nlsHPCC.Name, response);
        }));
    }

    start(searchArray.length);
    Promise.all(searchArray).then(function (results) {
        done(true);
    }, function (error) {
        done(false);
    });

    return retVal;
}