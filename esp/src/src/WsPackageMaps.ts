import * as lang from "dojo/_base/lang";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";

import * as ESPRequest from "./ESPRequest";

const i18n = nlsHPCC;

export function PackageMapQuery(params) {
    return ESPRequest.send("WsPackageProcess", "ListPackages", params);
}

export function errorMessageCallback(callback, error) {
    if (callback && callback.error) {
        callback.error(error);
    }
}

export function getPackage(params) {
    return ESPRequest.send("WsPackageProcess", "GetPackage", {
        request: {
            Target: params.target,
            Process: params.process
        }
    });
}

export function getPackageMapById(params) {
    return ESPRequest.send("WsPackageProcess", "GetPackageMapById", {
        request: {
            PackageMapId: params.packageMap
        }
    });
}

export function GetPackageMapByIdUpdated(params) {
    return ESPRequest.send("WsPackageProcess", "GetPackageMapById", params);
}

export function RemovePartFromPackageMap(params) {
    return ESPRequest.send("WsPackageProcess", "RemovePartFromPackageMap", params);
}

export function AddPartToPackageMap(params) {
    return ESPRequest.send("WsPackageProcess", "AddPartToPackageMap", params);
}

export function AddPackage(params) {
    return ESPRequest.send("WsPackageProcess", "AddPackage", params);
}

export function GetPartFromPackageMap(params) {
    return ESPRequest.send("WsPackageProcess", "GetPartFromPackageMap", params);
}

export function GetPackageMapSelectTargets(params) {
    return ESPRequest.send("WsPackageProcess", "GetPackageMapSelectOptions", {
        request: {
            IncludeTargets: params.request.IncludeTargets
        }
    });
}

export function GetPackageMapSelectProcesses(params) {
    return ESPRequest.send("WsPackageProcess", "GetPackageMapSelectOptions", {
        request: {
            IncludeProcesses: params.request.IncludeProcesses
        }
    });
}

export function GetPackageMapSelectProcessFilter(params) {
    return ESPRequest.send("WsPackageProcess", "GetPackageMapSelectOptions", {
        request: {
            IncludeProcessFilters: params.request.IncludeProcessFilters
        }
    });
}

// Not used for now. May be used later.
export function listProcessFilters(callback) {
    const context = this;
    return ESPRequest.send("WsPackageProcess", "ListProcessFilters", {
        request: {},
        load(response) {
            if (!lang.exists("ListProcessFiltersResponse.ProcessFilters", response))
                callback.load(i18n.NoContent);
            else
                callback.load(response.ListProcessFiltersResponse.ProcessFilters);
        },
        error(err) {
            context.errorMessageCallback(callback, err);
        }
    });
}

export function validatePackage(params) {
    const request = { Target: params.target };
    if (params.packageMap)
        request["PMID"] = params.packageMap;
    if (params.process)
        request["Process"] = params.process;
    if (params.content)
        request["Info"] = params.content;
    if (params.active)
        request["Active"] = params.active;

    return ESPRequest.send("WsPackageProcess", "ValidatePackage", {
        request
    });
}

export function activatePackageMap(params) {
    return ESPRequest.send("WsPackageProcess", "ActivatePackage", params);
}

export function deactivatePackageMap(params) {
    return ESPRequest.send("WsPackageProcess", "DeActivatePackage", params);
}

export function deletePackageMap(params) {
    return ESPRequest.send("WsPackageProcess", "DeletePackage", params);
}
