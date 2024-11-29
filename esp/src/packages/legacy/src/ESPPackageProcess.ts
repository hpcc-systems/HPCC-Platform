import * as Observable from "dojo/store/Observable";
import * as ESPRequest from "./ESPRequest";

class Store extends ESPRequest.Store {
    service = "WsPackageProcess";
    action = "ListPackages";
    responseQualifier = "ListPackagesResponse.PackageMapList.PackageListMapData";
    responseTotalQualifier = undefined;
    idProperty = "Id";

    startProperty = "PageStartFrom";
    countProperty = "PageSize";

    SortbyProperty = "SortBy";
}

export function CreatePackageMapQueryObjectStore(options) {
    const store = new Store(options);
    return new Observable(store);
}
