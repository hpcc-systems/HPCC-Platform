/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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
    "dojo/_base/Deferred",
    "dojo/_base/array",
    "dojo/store/util/QueryResults",

    "hpcc/ESPRequest"
], function (declare, lang, Deferred, arrayUtil, QueryResults,
    ESPRequest) {
    return {
        PackageMapQuery: function (params) {
            return ESPRequest.send("WsPackageProcess", "ListPackages", params);
        },

        errorMessageCallback: function (callback, message, errorStack)        {
            if (callback && callback.error) {
                callback.error(message, errorStack);
            }
        },

        checkExceptions: function (callback, response) {
            if (!lang.exists("Exceptions.Exception", response))
                return true;
            var exceptionCode = response.Exceptions.Exception[0].Code;
            var exceptionMSG = response.Exceptions.Exception[0].Message;
            this.errorMessageCallback(callback, "Exception:", "Code:"+exceptionCode);
            return false;
        },

        checkStatus: function (callback, hasStatus, status) {
            if (hasStatus && (status.Code == 0))
                return true;
            if (callback && callback.error) {
                if (!hasStatus)
                    this.errorMessageCallback(callback, "(Invalid response)", "");
                else
                    this.errorMessageCallback(callback, status.Description, "");
            }
            return false;
        },

        getPackageMapById: function (params, callback) {
            var request = {
                PackageMapId: params.packageMap
            };

            var context = this;
            return ESPRequest.send("WsPackageProcess", "GetPackageMapById", {
                request: request,
                load: function (response) {
                    if (context.checkExceptions(callback, response) &&
                        context.checkStatus(callback, lang.exists("GetPackageMapByIdResponse.status", response),
                        response.GetPackageMapByIdResponse.status))
                    {
                        if (!lang.exists("GetPackageMapByIdResponse.Info", response))
                            callback.load("(No content)");
                        else
                            callback.load(response.GetPackageMapByIdResponse.Info);
                    }
                },
                error: function (err) {
                    context.errorMessageCallback(callback, err.message, err.stack);
                }
            });
        },

        GetPackageMapSelectOptions: function (params, callback) {
            var request = {
                IncludeTargets: params.includeTargets,
                IncludeProcesses: params.includeProcesses,
                IncludeProcessFilters: params.includeProcessFilters
            };
            var context = this;
            return ESPRequest.send("WsPackageProcess", "GetPackageMapSelectOptions", {
                request: {},
                load: function (response) {
                    if (context.checkExceptions(callback, response) &&
                        context.checkStatus(callback, lang.exists("GetPackageMapSelectOptionsResponse.status", response),
                        response.GetPackageMapSelectOptionsResponse.status))
                    {
                        callback.load(response.GetPackageMapSelectOptionsResponse);
                    }
                },
                error: function (err) {
                    context.errorMessageCallback(callback, err.message, err.stack);
                }
            });
        },

        listProcessFilters: function (callback) {
            var context = this;
            return ESPRequest.send("WsPackageProcess", "ListProcessFilters", {
                request: {},
                load: function (response) {
                    if (context.checkExceptions(callback, response) &&
                        context.checkStatus(callback, lang.exists("ListProcessFiltersResponse.status", response),
                        response.ListProcessFiltersResponse.status))
                    {
                        if (!lang.exists("ListProcessFiltersResponse.ProcessFilters", response))
                            callback.load("(No content)");
                        else
                            callback.load(response.ListProcessFiltersResponse.ProcessFilters);
                    }
                },
                error: function (err) {
                    context.errorMessageCallback(callback, err.message, err.stack);
                }
            });
        },

        validatePackage: function ( params, callback) {
            var request = { Target: params.target };
            if ( params.packageMap )
                request['PMID'] = params.packageMap;
            if ( params.process )
                request['Process'] = params.process;
            if ( params.content )
                request['Info'] = params.content;
            if ( params.active )
                request['Active'] = params.active;

            var context = this;
            return ESPRequest.send("WsPackageProcess", "ValidatePackage", {
                request: request,
                load: function (response) {
                    if (context.checkExceptions(callback, response) &&
                        context.checkStatus(callback, lang.exists("ValidatePackageResponse.status", response),
                        response.ValidatePackageResponse.status))
                    {
                        //console.log(response.ValidatePackageResponse);
                        callback.load(response.ValidatePackageResponse);
                    }
                },
                error: function (err) {
                    context.errorMessageCallback(callback, err.message, err.stack);
                }
            });
        },

        activatePackageMap: function (packageMaps, callback) {
            var request = {
                Target: packageMaps[0].Target,
                Process: packageMaps[0].Process,
                PackageMap: packageMaps[0].Id
            };

            var context = this;
            return ESPRequest.send("WsPackageProcess", "ActivatePackage", {
                request: request,
                load: function (response) {
                    if (context.checkExceptions(callback, response) &&
                        context.checkStatus(callback, lang.exists("ActivatePackageResponse.status", response),
                        response.ActivatePackageResponse.status))
                    {
                        callback.load(response.ActivatePackageResponse);
                    }
                },
                error: function (err) {
                    context.errorMessageCallback(callback, err.message, err.stack);
                }
            });
        },
        deactivatePackageMap: function (packageMaps, callback) {
            var request = {
                Target: packageMaps[0].Target,
                Process: packageMaps[0].Process,
                PackageMap: packageMaps[0].Id
            };

            var context = this;
            return ESPRequest.send("WsPackageProcess", "DeActivatePackage", {
                request: request,
                load: function (response) {
                    if (context.checkExceptions(callback, response) &&
                        context.checkStatus(callback, lang.exists("DeActivatePackageResponse.status", response),
                        response.DeActivatePackageResponse.status))
                    {
                        callback.load(response.DeActivatePackageResponse);
                    }
                },
                error: function (err) {
                    context.errorMessageCallback(callback, err.message, err.stack);
                }
            });
        },
        deletePackageMap: function (packageMaps, callback) {
            var context = this;
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
                request: request,
                load: function (response) {
                    if (context.checkExceptions(callback, response) &&
                        context.checkStatus(callback, lang.exists("DeletePackageResponse.status", response),
                        response.DeletePackageResponse.status))
                    {
                        callback.load(response.DeletePackageResponse);
                    }
                },
                error: function (err) {
                    context.errorMessageCallback(callback, err.message, err.stack);
                }
            });
        }
    };
});
