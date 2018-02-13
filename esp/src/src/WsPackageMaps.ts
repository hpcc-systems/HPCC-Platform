import * as lang from "dojo/_base/lang";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";
import * as arrayUtil from "dojo/_base/array";

import * as ESPRequest from "./ESPRequest";

var i18n = nlsHPCC;

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

export function GetPartFromPackageMap(params) {
    return ESPRequest.send("WsPackageProcess", "GetPartFromPackageMap", params);
}

export function GetPackageMapSelectOptions(params) {
    return ESPRequest.send("WsPackageProcess", "GetPackageMapSelectOptions", {
        request: {
            IncludeTargets: params.includeTargets,
            IncludeProcesses: params.includeProcesses,
            IncludeProcessFilters: params.includeProcessFilters
        }
    });
}

//Not used for now. May be used later.
export function listProcessFilters(callback) {
    var context = this;
    return ESPRequest.send("WsPackageProcess", "ListProcessFilters", {
        request: {},
        load: function (response) {
            if (!lang.exists("ListProcessFiltersResponse.ProcessFilters", response))
                callback.load(i18n.NoContent);
            else
                callback.load(response.ListProcessFiltersResponse.ProcessFilters);
        },
        error: function (err) {
            context.errorMessageCallback(callback, err);
        }
    });
}

export function validatePackage(params) {
    var request = { Target: params.target };
    if (params.packageMap)
        request['PMID'] = params.packageMap;
    if (params.process)
        request['Process'] = params.process;
    if (params.content)
        request['Info'] = params.content;
    if (params.active)
        request['Active'] = params.active;

    return ESPRequest.send("WsPackageProcess", "ValidatePackage", {
        request: request
    });
}

export function activatePackageMap(packageMaps) {
    return ESPRequest.send("WsPackageProcess", "ActivatePackage", {
        request: {
            Target: packageMaps[0].Target,
            Process: packageMaps[0].Process,
            PackageMap: packageMaps[0].Id
        }
    });
}
export function deactivatePackageMap(packageMaps) {
    return ESPRequest.send("WsPackageProcess", "DeActivatePackage", {
        request: {
            Target: packageMaps[0].Target,
            Process: packageMaps[0].Process,
            PackageMap: packageMaps[0].Id
        }
    });
}
export function deletePackageMap(packageMaps) {
    var request = {};
    arrayUtil.forEach(packageMaps, function (item, idx) {
        request["PackageMaps.PackageMapEntry." + idx + ".Id"] = item.Id;
        request["PackageMaps.PackageMapEntry." + idx + ".Target"] = item.Target;
        request["PackageMaps.PackageMapEntry." + idx + ".Process"] = item.Process;
    });
    lang.mixin(request, {
        "PackageMaps.PackageMapEntry.itemcount": packageMaps.length
    });
    return ESPRequest.send("WsPackageProcess", "DeletePackage", {
        request: request
    });
}
