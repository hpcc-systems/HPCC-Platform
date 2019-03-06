import * as arrayUtil from "dojo/_base/array";
import * as domConstruct from "dojo/dom-construct";
import * as entities from "dojox/html/entities";

declare const dojoConfig;
declare const ActiveXObject;
declare const require;

export function xmlEncode(str) {
    str = "" + str;
    return entities.encode(str);
}

export function xmlEncode2(str) {
    str = "" + str;
    return str.replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\n/g, '&#10;')
        .replace(/\r/g, '&#13;')
        ;
}

export function decodeHtml(html) {
    var txt = document.createElement("textarea");
    txt.innerHTML = html;
    return txt.value;
}

export function parseXML(val) {
    var xmlDoc;
    if ((window as any).DOMParser) {
        var parser = new DOMParser();
        xmlDoc = parser.parseFromString(val, "text/xml");
    } else {
        xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
        xmlDoc.async = false;
        xmlDoc.loadXML(val);
    }
    return xmlDoc;
}

export function csvEncode(cell) {
    if (!isNaN(cell)) return cell;
    return '"' + String(cell).replace('"', '""') + '"';
}

export function espTime2Seconds(duration) {
    if (!duration) {
        return 0;
    } else if (!isNaN(duration)) {
        return parseFloat(duration);
    }
    //  GH:  <n>ns or <m>ms or <s>s or [<d> days ][<h>:][<m>:]<s>[.<ms>]
    var nsIndex = duration.indexOf("ns");
    if (nsIndex !== -1) {
        return parseFloat(duration.substr(0, nsIndex)) / 1000000000;
    }
    var msIndex = duration.indexOf("ms");
    if (msIndex !== -1) {
        return parseFloat(duration.substr(0, msIndex)) / 1000;
    }
    var sIndex = duration.indexOf("s");
    if (sIndex !== -1 && duration.indexOf("days") === -1) {
        return parseFloat(duration.substr(0, sIndex));
    }

    var dayTimeParts = duration.split(" days ");
    var days = parseFloat(dayTimeParts.length > 1 ? dayTimeParts[0] : 0.0);
    var time = dayTimeParts.length > 1 ? dayTimeParts[1] : dayTimeParts[0];
    var secs = 0.0;
    var timeParts = time.split(":").reverse();
    for (var j = 0; j < timeParts.length; ++j) {
        secs += parseFloat(timeParts[j]) * Math.pow(60, j);
    }
    return (days * 24 * 60 * 60) + secs;
}

export function espTime2SecondsTests() {
    var tests = [
        { str: "1.1s", expected: 1.1 },
        { str: "2.2ms", expected: 0.0022 },
        { str: "3.3ns", expected: 0.0000000033 },
        { str: "4.4", expected: 4.4 },
        { str: "5:55.5", expected: 355.5 },
        { str: "6:06:06.6", expected: 21966.6 },
        { str: "6:06:6.6", expected: 21966.6 },
        { str: "6:6:6.6", expected: 21966.6 },
        { str: "7 days 7:07:7.7", expected: 630427.7 }
    ];
    tests.forEach(function (test, idx) {
        if (espTime2Seconds(test.str) !== test.expected) {
            console.log("espTime2SecondsTests failed with " + espTime2Seconds(test.str) + " !== " + test.expected);
        }
    }, this);
}

export function convertedSize(intsize: number): string {
    const unitConversion = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    const x = Math.floor(Math.log(intsize) / Math.log(1024));
    return (intsize / Math.pow(1024, x)).toFixed(2) + " " + unitConversion[x];
}

export function unitTest(size, unit) {
    var nsIndex = size.indexOf(unit);
    if (nsIndex !== -1) {
        return parseFloat(size.substr(0, nsIndex));
    }
    return -1;
}

export function espSize2Bytes(size) {
    if (!size) {
        return 0;
    } else if (!isNaN(size)) {
        return parseFloat(size);
    }
    var retVal = unitTest(size, "Kb");
    if (retVal >= 0) {
        return retVal * 1024;
    }
    var retVal = unitTest(size, "Mb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 2);
    }
    var retVal = unitTest(size, "Gb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 3);
    }
    var retVal = unitTest(size, "Tb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 4);
    }
    var retVal = unitTest(size, "Pb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 5);
    }
    var retVal = unitTest(size, "Eb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 6);
    }
    var retVal = unitTest(size, "Zb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 7);
    }
    var retVal = unitTest(size, "b");
    if (retVal >= 0) {
        return retVal;
    }
    return 0;
}

export function espSize2BytesTests() {
    var tests = [
        { str: "1", expected: 1 },
        { str: "1b", expected: 1 },
        { str: "1Kb", expected: 1 * 1024 },
        { str: "1Mb", expected: 1 * 1024 * 1024 },
        { str: "1Gb", expected: 1 * 1024 * 1024 * 1024 },
        { str: "1Tb", expected: 1 * 1024 * 1024 * 1024 * 1024 },
        { str: "1Pb", expected: 1 * 1024 * 1024 * 1024 * 1024 * 1024 },
        { str: "1Eb", expected: 1 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 },
        { str: "1Zb", expected: 1 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 }
    ];
    tests.forEach(function (test, idx) {
        if (espSize2Bytes(test.str) !== test.expected) {
            console.log("espSize2BytesTests failed with " + test.str + "(" + espSize2Bytes(test.str) + ") !== " + test.expected);
        }
    }, this);
}

export function espSkew2Number(skew) {
    if (!skew) {
        return 0;
    }
    return parseFloat(skew);
}

export function espSkew2NumberTests() {
    var tests = [
        { str: "", expected: 0 },
        { str: "1", expected: 1 },
        { str: "10%", expected: 10 },
        { str: "-10%", expected: -10 }
    ];
    tests.forEach(function (test, idx) {
        if (espSkew2Number(test.str) !== test.expected) {
            console.log("espSkew2NumberTests failed with " + test.str + "(" + espSkew2Number(test.str) + ") !== " + test.expected);
        }
    }, this);
}

export function downloadToCSV(grid, rows, fileName) {
    var csvContent = "";
    var headers = grid.columns;
    var container = [];
    var headerNames = [];

    for (var key in headers) {
        if (headers[key].selectorType !== 'checkbox') {
            if (!headers[key].label) {
                var str = csvEncode(headers[key].field);
                headerNames.push(str);
            } else {
                var str = csvEncode(headers[key].label);
                headerNames.push(str);
            }
        }
    }
    container.push(headerNames);

    arrayUtil.forEach(rows, function (cells, idx) {
        container.push(cells);
    });

    arrayUtil.forEach(container, function (header, idx) {
        var dataString = header.join(",");
        csvContent += dataString + "\n";
    });

    var download = function (content, fileName, mimeType) {
        var a = document.createElement('a');
        mimeType = mimeType || 'application/octet-stream';

        if (navigator.msSaveBlob) { // IE10
            return navigator.msSaveBlob(new Blob([content], { type: mimeType }), fileName);
        } else if ('download' in a) {
            a.href = 'data:' + mimeType + ',' + encodeURIComponent(content);
            a.setAttribute('download', fileName);
            document.body.appendChild(a);
            setTimeout(function () {
                a.click();
                document.body.removeChild(a);
            }, 66);
            return true;
        } else {
            var f = document.createElement('iframe');
            document.body.appendChild(f);
            f.src = 'data:' + mimeType + ',' + encodeURIComponent(content);

            setTimeout(function () {
                document.body.removeChild(f);
            }, 333);
            return true;
        }
    }
    download(csvContent, fileName, 'text/csv');
}

/* alphanum.js (C) Brian Huisman
 * Based on the Alphanum Algorithm by David Koelle
 * The Alphanum Algorithm is discussed at http://www.DaveKoelle.com
 *
 * Distributed under same license as original
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

/* ********************************************************************
 * Alphanum sort() function version - case sensitive
 *  - Slower, but easier to modify for arrays of objects which contain
 *    string properties
 *
 */
export function alphanum(a, b) {
    function chunkify(t) {
        var tz = [];
        var x = 0, y = -1, n = false, i, j;

        while (i = (j = t.charAt(x++)).charCodeAt(0)) {
            var m = (i == 46 || (i >= 48 && i <= 57));
            if (m !== n) {
                tz[++y] = "";
                n = m;
            }
            tz[y] += j;
        }
        return tz;
    }

    var aa = chunkify(a);
    var bb = chunkify(b);

    for (var x = 0; aa[x] && bb[x]; x++) {
        if (aa[x] !== bb[x]) {
            var c = Number(aa[x]), d = Number(bb[x]);
            if (c == aa[x] && d == bb[x]) {
                return c - d;
            } else return (aa[x] > bb[x]) ? 1 : -1;
        }
    }
    return aa.length - bb.length;
}


/* ********************************************************************
 * Alphanum sort() function version - case insensitive
 *  - Slower, but easier to modify for arrays of objects which contain
 *    string properties
 *
 */
export function alphanumCase(a, b) {
    function chunkify(t) {
        var tz = [];
        var x = 0, y = -1, n = false, i, j;

        while (i = (j = t.charAt(x++)).charCodeAt(0)) {
            var m = (i == 46 || (i >= 48 && i <= 57));    // jshint ignore:line
            if (m !== n) {
                tz[++y] = "";
                n = m;
            }
            tz[y] += j;
        }
        return tz;
    }

    var aa = chunkify(a.toLowerCase());
    var bb = chunkify(b.toLowerCase());

    for (var x = 0; aa[x] && bb[x]; x++) {
        if (aa[x] !== bb[x]) {
            var c = Number(aa[x]), d = Number(bb[x]);
            if (c == aa[x] && d == bb[x]) {   // jshint ignore:line
                return c - d;
            } else return (aa[x] > bb[x]) ? 1 : -1;
        }
    }
    return aa.length - bb.length;
}

export function alphanumSort(arr, col, caseInsensitive, reverse: boolean = false) {
    if (arr && arr instanceof Array) {
        arr.sort(function (l, r) {
            if (caseInsensitive) {
                return alphanumCase(r[col], l[col]) * (reverse ? -1 : 1);
            }
            return alphanum(l[col], r[col]) * (reverse ? -1 : 1);
        });
    }
}

export function stringLowerSort(arr: object[], col: string) {
    arr.sort((a: { [col: string]: any }, b: { [col: string]: any }) => {
        const c: string = ("" + a[col]).toLowerCase();
        const d: string = ("" + b[col]).toLowerCase();
        return c.localeCompare(d);
    });
}

export function resolve(hpccWidget, callback) {
    function doLoad(widget) {
        if (widget[hpccWidget]) {
            widget = widget[hpccWidget];
        }
        if (widget.fixCircularDependency) {
            widget = widget.fixCircularDependency;
        }
        callback(widget);
    }

    switch (hpccWidget) {
        case "ActivityWidget":
            require(["hpcc/ActivityWidget"], doLoad);
            break;
        case "CurrentUserDetailsWidget":
            require(["hpcc/CurrentUserDetailsWidget"], doLoad);
            break;
        case "DelayLoadWidget":
            require(["hpcc/DelayLoadWidget"], doLoad);
            break;
        case "DFUQueryWidget":
            require(["hpcc/DFUQueryWidget"], doLoad);
            break;
        case "DFUSearchWidget":
            require(["hpcc/DFUSearchWidget"], doLoad);
            break;
        case "DFUWUDetailsWidget":
            require(["hpcc/DFUWUDetailsWidget"], doLoad);
            break;
        case "DiskUsageWidget":
            require(["hpcc/DiskUsageWidget"], doLoad);
            break;
        case "DiskUsageDetails":
            require(["hpcc/DiskUsageDetails"], doLoad);
            break;
        case "viz/DojoD3Choropleth":
            require(["hpcc/viz/DojoD3Choropleth"], doLoad);
            break;
        case "viz/DojoD32DChart":
            require(["hpcc/viz/DojoD32DChart"], doLoad);
            break;
        case "viz/DojoD3NDChart":
            require(["hpcc/viz/DojoD3NDChart"], doLoad);
            break;
        case "DynamicESDLDefinitionDetailsWidget":
            require(["hpcc/DynamicESDLDefinitionDetailsWidget"], doLoad);
            break;
        case "DynamicESDLDefinitionQueryWidget":
            require(["hpcc/DynamicESDLDefinitionQueryWidget"], doLoad);
            break;
        case "DynamicESDLDetailsWidget":
            require(["hpcc/DynamicESDLDetailsWidget"], doLoad);
            break;
        case "DynamicESDLMethodWidget":
            require(["hpcc/DynamicESDLMethodWidget"], doLoad);
            break;
        case "DynamicESDLQueryWidget":
            require(["hpcc/DynamicESDLQueryWidget"], doLoad);
            break;
        case "ECLPlaygroundResultsWidget":
            require(["hpcc/ECLPlaygroundResultsWidget"], doLoad);
            break;
        case "ECLPlaygroundWidget":
            require(["hpcc/ECLPlaygroundWidget"], doLoad);
            break;
        case "ECLSourceWidget":
            require(["hpcc/ECLSourceWidget"], doLoad);
            break;
        case "EventScheduleWorkunitWidget":
            require(["hpcc/EventScheduleWorkunitWidget"], doLoad);
            break;
        case "FileBelongsToWidget":
            require(["hpcc/FileBelongsToWidget"], doLoad);
            break;
        case "FileHistoryWidget":
            require(["hpcc/FileHistoryWidget"], doLoad);
            break;
        case "FilePartsWidget":
            require(["hpcc/FilePartsWidget"], doLoad);
            break;
        case "FilterDropDownWidget":
            require(["hpcc/FilterDropDownWidget"], doLoad);
            break;
        case "FullResultWidget":
            require(["hpcc/FullResultWidget"], doLoad);
            break;
        case "GetDFUWorkunitsWidget":
            require(["hpcc/GetDFUWorkunitsWidget"], doLoad);
            break;
        case "GetNumberOfFilesToCopyWidget":
            require(["hpcc/GetNumberOfFilesToCopyWidget"], doLoad);
            break;
        case "GraphPageWidget":
            require(["hpcc/GraphPageWidget"], doLoad);
            break;
        case "GraphsWUWidget":
            require(["hpcc/GraphsWUWidget"], doLoad);
            break;
        case "GraphsQueryWidget":
            require(["hpcc/GraphsQueryWidget"], doLoad);
            break;
        case "GraphsLFWidget":
            require(["hpcc/GraphsLFWidget"], doLoad);
            break;
        case "GraphTreeWidget":
            require(["src/GraphTreeWidget"], doLoad);
            break;
        case "GraphTree7Widget":
            require(["src/GraphTree7Widget"], doLoad);
            break;
        case "Graph7Widget":
            require(["src/Graph7Widget"], doLoad);
            break;
        case "GridDetailsWidget":
            require(["hpcc/GridDetailsWidget"], doLoad);
            break;
        case "GroupDetailsWidget":
            require(["hpcc/GroupDetailsWidget"], doLoad);
            break;
        case "HelpersWidget":
            require(["hpcc/HelpersWidget"], doLoad);
            break;
        case "HexViewWidget":
            require(["hpcc/HexViewWidget"], doLoad);
            break;
        case "HPCCPlatformECLWidget":
            require(["hpcc/HPCCPlatformECLWidget"], doLoad);
            break;
        case "HPCCPlatformFilesWidget":
            require(["hpcc/HPCCPlatformFilesWidget"], doLoad);
            break;
        case "HPCCPlatformMainWidget":
            require(["hpcc/HPCCPlatformMainWidget"], doLoad);
            break;
        case "HPCCPlatformOpsWidget":
            require(["hpcc/HPCCPlatformOpsWidget"], doLoad);
            break;
        case "HPCCPlatformRoxieWidget":
            require(["hpcc/HPCCPlatformRoxieWidget"], doLoad);
            break;
        case "HPCCPlatformServicesPluginWidget":
            require(["hpcc/HPCCPlatformServicesPluginWidget"], doLoad);
            break;
        case "HPCCPlatformWidget":
            require(["hpcc/HPCCPlatformWidget"], doLoad);
            break;
        case "IFrameWidget":
            require(["hpcc/IFrameWidget"], doLoad);
            break;
        case "InfoGridWidget":
            require(["hpcc/InfoGridWidget"], doLoad);
            break;
        case "JSGraphWidget":
            require(["hpcc/JSGraphWidget"], doLoad);
            break;
        case "LFDetailsWidget":
            require(["hpcc/LFDetailsWidget"], doLoad);
            break;
        case "LibrariesUsedWidget":
            require(["hpcc/LibrariesUsedWidget"], doLoad);
            break;
        case "LogsWidget":
            require(["hpcc/HelpersWidget"], doLoad);
            break;
        case "LogWidget":
            require(["hpcc/LogWidget"], doLoad);
            break;
        case "LZBrowseWidget":
            require(["hpcc/LZBrowseWidget"], doLoad);
            break;
        case "MemberOfWidget":
            require(["hpcc/MemberOfWidget"], doLoad);
            break;
        case "MembersWidget":
            require(["hpcc/MembersWidget"], doLoad);
            break;
        case "MonitoringWidget":
            require(["hpcc/MonitoringWidget"], doLoad);
            break;
        case "PackageMapDetailsWidget":
            require(["hpcc/PackageMapDetailsWidget"], doLoad);
            break;
        case "PackageMapPartsWidget":
            require(["hpcc/PackageMapPartsWidget"], doLoad);
            break;
        case "PackageMapQueryWidget":
            require(["hpcc/PackageMapQueryWidget"], doLoad);
            break;
        case "PackageMapValidateContentWidget":
            require(["hpcc/PackageMapValidateContentWidget"], doLoad);
            break;
        case "PackageMapValidateWidget":
            require(["hpcc/PackageMapValidateWidget"], doLoad);
            break;
        case "PackageSourceWidget":
            require(["hpcc/PackageSourceWidget"], doLoad);
            break;
        case "PermissionsWidget":
            require(["hpcc/PermissionsWidget"], doLoad);
            break;
        case "PreflightDetailsWidget":
            require(["hpcc/PreflightDetailsWidget"], doLoad);
            break;
        case "QuerySetDetailsWidget":
            require(["hpcc/QuerySetDetailsWidget"], doLoad);
            break;
        case "QuerySetErrorsWidget":
            require(["hpcc/QuerySetErrorsWidget"], doLoad);
            break;
        case "QuerySetLogicalFilesWidget":
            require(["hpcc/QuerySetLogicalFilesWidget"], doLoad);
            break;
        case "QuerySetQueryWidget":
            require(["hpcc/QuerySetQueryWidget"], doLoad);
            break;
        case "QuerySetSuperFilesWidget":
            require(["hpcc/QuerySetSuperFilesWidget"], doLoad);
            break;
        case "QueryTestWidget":
            require(["hpcc/QueryTestWidget"], doLoad);
            break;
        case "RequestInformationWidget":
            require(["hpcc/RequestInformationWidget"], doLoad);
            break;
        case "ResourcesWidget":
            require(["hpcc/ResourcesWidget"], doLoad);
            break;
        case "ResultsWidget":
            require(["hpcc/ResultsWidget"], doLoad);
            break;
        case "ResultWidget":
            require(["hpcc/ResultWidget"], doLoad);
            break;
        case "SearchResultsWidget":
            require(["hpcc/SearchResultsWidget"], doLoad);
            break;
        case "SelectionGridWidget":
            require(["hpcc/SelectionGridWidget"], doLoad);
            break;
        case "SFDetailsWidget":
            require(["hpcc/SFDetailsWidget"], doLoad);
            break;
        case "ShowAccountPermissionsWidget":
            require(["hpcc/ShowAccountPermissionsWidget"], doLoad);
            break;
        case "ShowIndividualPermissionsWidget":
            require(["hpcc/ShowIndividualPermissionsWidget"], doLoad);
            break;
        case "ShowInheritedPermissionsWidget":
            require(["hpcc/ShowInheritedPermissionsWidget"], doLoad);
            break;
        case "ShowPermissionsWidget":
            require(["hpcc/ShowPermissionsWidget"], doLoad);
            break;
        case "SourceFilesWidget":
            require(["hpcc/SourceFilesWidget"], doLoad);
            break;
        case "TargetComboBoxWidget":
            require(["hpcc/TargetComboBoxWidget"], doLoad);
            break;
        case "TargetSelectWidget":
            require(["hpcc/TargetSelectWidget"], doLoad);
            break;
        case "TimingGridWidget":
            require(["hpcc/TimingGridWidget"], doLoad);
            break;
        case "TimingPageWidget":
            require(["hpcc/TimingPageWidget"], doLoad);
            break;
        case "TimingTreeMapWidget":
            require(["hpcc/TimingTreeMapWidget"], doLoad);
            break;
        case "TopologyDetailsWidget":
            require(["hpcc/TopologyDetailsWidget"], doLoad);
            break;
        case "TopologyWidget":
            require(["hpcc/TopologyWidget"], doLoad);
            break;
        case "TpClusterInfoWidget":
            require(["hpcc/TpClusterInfoWidget"], doLoad);
            break;
        case "TpThorStatusWidget":
            require(["hpcc/TpThorStatusWidget"], doLoad);
            break;
        case "UserDetailsWidget":
            require(["hpcc/UserDetailsWidget"], doLoad);
            break;
        case "UserQueryWidget":
            require(["hpcc/UserQueryWidget"], doLoad);
            break;
        case "VariablesWidget":
            require(["hpcc/VariablesWidget"], doLoad);
            break;
        case "VizWidget":
            require(["hpcc/VizWidget"], doLoad);
            break;
        case "WorkflowsWidget":
            require(["hpcc/WorkflowsWidget"], doLoad);
            break;
        case "WUDetailsWidget":
            require(["hpcc/WUDetailsWidget"], doLoad);
            break;
        case "WUQueryWidget":
            require(["hpcc/WUQueryWidget"], doLoad);
            break;
        case "WUStatsWidget":
            require(["hpcc/WUStatsWidget"], doLoad);
            break;
        case "XrefDetailsWidget":
            require(["hpcc/XrefDetailsWidget"], doLoad);
            break;
        case "XrefDirectoriesWidget":
            require(["hpcc/XrefDirectoriesWidget"], doLoad);
            break;
        case "XrefErrorsWarningsWidget":
            require(["hpcc/XrefErrorsWarningsWidget"], doLoad);
            break;
        case "XrefFoundFilesWidget":
            require(["hpcc/XrefFoundFilesWidget"], doLoad);
            break;
        case "XrefLostFilesWidget":
            require(["hpcc/XrefLostFilesWidget"], doLoad);
            break;
        case "XrefOrphanFilesWidget":
            require(["hpcc/XrefOrphanFilesWidget"], doLoad);
            break;
        case "XrefQueryWidget":
            require(["hpcc/XrefQueryWidget"], doLoad);
            break;
        case "GangliaWidget":
            require(["ganglia/GangliaWidget"], doLoad);
            break;
        default:
            console.log("case \"" + hpccWidget + "\":\n" +
                "    require([\"hpcc/" + hpccWidget + "\"], doLoad);\n" +
                "    break;\n");
    }
}

export function getURL(name) {
    return dojoConfig.urlInfo.resourcePath + "/" + name;
}

export function getImageURL(name) {
    return this.getURL("img/" + name);
}

export function getImageHTML(name, tooltip?) {
    return "<img src='" + this.getImageURL(name) + "'" + (tooltip ? " title='" + tooltip + "'" : "") + " class='iconAlign'/>";
}

export function debounce(func, threshold, execAsap) {
    var timeout;
    return function debounced() {
        var obj = this, args = arguments;
        function delayed() {
            if (!execAsap)
                func.apply(obj, args);
            timeout = null;
        }
        if (timeout)
            clearTimeout(timeout);
        else if (execAsap)
            func.apply(obj, args);
        timeout = setTimeout(delayed, threshold || 100);
    }
}

export function DynamicDialogForm(object) {
    var table = domConstruct.create("table", {});

    for (var key in object) {
        var tr = domConstruct.create("tr", {}, table);
        if (object.hasOwnProperty(key)) {
            var td = domConstruct.create("td", {
                style: "width: 30%;"
            }, tr);
            domConstruct.create("label", {
                innerHTML: object[key]['label']
            }, td);
            var td1 = domConstruct.create("td", {
                style: "width: 100%;"
            }, tr);
            this.key = object[key]['widget'].placeAt(td1);
        }
    }
    return table;
}

export class Persist {

    private id: string;

    constructor(id) {
        this.id = "hpcc__Persist" + id + "_";
    }
    remove(key) {
        if (typeof (Storage) !== "undefined") {
            localStorage.removeItem(this.id + key);
        }
    }
    set(key, val) {
        if (typeof (Storage) !== "undefined") {
            localStorage.setItem(this.id + key, val);
        }
    }
    setObj(key, val) {
        this.set(key, JSON.stringify(val));
    }
    get(key, defValue?) {
        if (typeof (Storage) !== "undefined") {
            var retVal = localStorage.getItem(this.id + key);
            return retVal === null ? defValue : retVal;
        }
        return "";
    }
    getObj(key, defVal?) {
        try {
            return JSON.parse(this.get(key, defVal));
        } catch (e) {
            return {};
        }
    }
    exists(key) {
        if (typeof (Storage) !== "undefined") {
            var retVal = localStorage.getItem(this.id + key);
            return retVal === null;
        }
        return false;
    }
}
