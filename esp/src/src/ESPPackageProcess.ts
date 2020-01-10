import * as declare from "dojo/_base/declare";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";
import * as Observable from "dojo/store/Observable";
import * as ESPRequest from "./ESPRequest";

const Store = declare([ESPRequest.Store], {
    i18n: nlsHPCC,
    service: "WsPackageProcess",
    action: "ListPackages",
    responseQualifier: "ListPackagesResponse.PackageMapList.PackageListMapData",
    idProperty: "Id",
    startProperty: "PageStartFrom",
    countProperty: "PageSize",
    SortbyProperty: "SortBy"
});

export function CreatePackageMapQueryObjectStore(options) {
    const store = new Store(options);
    return Observable(store);
}
