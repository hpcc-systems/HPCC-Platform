﻿import { Observable } from "src-dojo/index";
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

export function CreatePackageMapQueryObjectStore() {
    const store = new Store();
    return new Observable(store);
}
