/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/Deferred",
    "dojo/_base/array",
    "dojo/store/util/QueryResults",

    "hpcc/ESPRequest"
], function (declare, lang, i18n, nlsHPCC, Deferred, arrayUtil, QueryResults,
    ESPRequest) {
    var i18n = nlsHPCC;
    return {
        PackageMapQuery: function (params) {
            return ESPRequest.send("WsPackageProcess", "ListPackages", params);
        },

        errorMessageCallback: function (callback, error)        {
            if (callback && callback.error) {
                callback.error(error);
            }
        },

        getPackage: function (params) {
            return ESPRequest.send("WsPackageProcess", "GetPackage", {
                request: {
                    Target: params.target,
                    Process: params.process
                }
            });
        },

        getPackageMapById: function (params) {
            return ESPRequest.send("WsPackageProcess", "GetPackageMapById", {
                request: {
                    PackageMapId: params.packageMap
                }
            });
        },

        GetPackageMapByIdUpdated: function (params) {
            return ESPRequest.send("WsPackageProcess", "GetPackageMapById", params);
        },

        RemovePartFromPackageMap: function (params) {
            return ESPRequest.send("WsPackageProcess", "RemovePartFromPackageMap", params);
        },

        AddPartToPackageMap: function (params) {
            return ESPRequest.send("WsPackageProcess", "AddPartToPackageMap", params);
        },

        GetPartFromPackageMap: function (params) {
            return ESPRequest.send("WsPackageProcess", "GetPartFromPackageMap", params);
        },

        GetPackageMapSelectOptions: function (params) {
            return ESPRequest.send("WsPackageProcess", "GetPackageMapSelectOptions", {
                request: {
                    IncludeTargets: params.includeTargets,
                    IncludeProcesses: params.includeProcesses,
                    IncludeProcessFilters: params.includeProcessFilters
                }
            });
        },

        //Not used for now. May be used later.
        listProcessFilters: function (callback) {
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
        },

        validatePackage: function ( params) {
            var request = { Target: params.target };
            if ( params.packageMap )
                request['PMID'] = params.packageMap;
            if ( params.process )
                request['Process'] = params.process;
            if ( params.content )
                request['Info'] = params.content;
            if ( params.active )
                request['Active'] = params.active;

            return ESPRequest.send("WsPackageProcess", "ValidatePackage", {
                request: request
            });
        },

        activatePackageMap: function (packageMaps) {
            return ESPRequest.send("WsPackageProcess", "ActivatePackage", {
                request: {
                    Target: packageMaps[0].Target,
                    Process: packageMaps[0].Process,
                    PackageMap: packageMaps[0].Id
                }
            });
        },
        deactivatePackageMap: function (packageMaps) {
            return ESPRequest.send("WsPackageProcess", "DeActivatePackage", {
                request: {
                    Target: packageMaps[0].Target,
                    Process: packageMaps[0].Process,
                    PackageMap: packageMaps[0].Id
                }
            });
        },
        deletePackageMap: function (packageMaps) {
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
    };
});
