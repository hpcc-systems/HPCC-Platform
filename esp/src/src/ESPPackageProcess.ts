import * as declare from "dojo/_base/declare";
import * as Observable from "dojo/store/Observable";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";
import * as ESPRequest from "./ESPRequest";

var Store = declare([ESPRequest.Store], {
    i18n: nlsHPCC,
    service: "WsPackageProcess",
    action: "ListPackages",
    responseQualifier: "ListPackagesResponse.PackageMapList.PackageListMapData",
    responseTotalQualifier: "ListPackagesResponse.PackageMapList.PackageListMapData",
    idProperty: "__hpcc_id"
});
export function CreatePackageMapQueryObjectStore(options) {
    var store = new Store(options);
    return Observable(store);
}