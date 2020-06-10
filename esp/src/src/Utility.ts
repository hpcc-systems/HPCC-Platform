import { Palette } from "@hpcc-js/common";
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
    return str.replace(/&/g, "&amp;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&apos;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\n/g, "&#10;")
        .replace(/\r/g, "&#13;")
        ;
}

export function decodeHtml(html) {
    const txt = document.createElement("textarea");
    txt.innerHTML = html;
    return txt.value;
}

export function parseXML(val) {
    let xmlDoc;
    if ((window as any).DOMParser) {
        const parser = new DOMParser();
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

export function espTime2Seconds(duration?: string) {
    if (!duration) {
        return 0;
    } else if (!isNaN(+duration)) {
        return parseFloat(duration);
    }
    const re = /(?:(?:(\d+).days.)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+\.\d+|\d+)s))|(?:(\d+\.\d+|\d+)ms|(\d+\.\d+|\d+)us|(\d+\.\d+|\d+)ns)/;
    const match = re.exec(duration);
    if (!match) return 0;
    const days = +match[1] || 0;
    const hours = +match[2] || 0;
    const mins = +match[3] || 0;
    const secs = +match[4] || 0;
    const ms = +match[5] || 0;
    const us = +match[6] || 0;
    const ns = +match[7] || 0;
    return (days * 24 * 60 * 60) + (hours * 60 * 60) + (mins * 60) + secs + ms / 1000 + us / 1000000 + ns / 1000000000;
}

export function espTime2SecondsTests() {
    const tests = [
        { str: "1.1s", expected: 1.1 },
        { str: "2.2ms", expected: 0.0022 },
        { str: "3.3ns", expected: 0.0000000033 },
        { str: "4.4", expected: 4.4 },
        { str: "5m55.5s", expected: 355.5 },
        { str: "6h06m06.6s", expected: 21966.6 },
        { str: "6h06m6.6s", expected: 21966.6 },
        { str: "6h6m6.6s", expected: 21966.6 },
        { str: "7 days 7h07m7.7s", expected: 630427.7 }
    ];
    tests.forEach(function (test, idx) {
        if (espTime2Seconds(test.str) !== test.expected) {
            console.log("espTime2SecondsTests failed with " + espTime2Seconds(test.str) + " !== " + test.expected);
        }
    }, this);
}

export function convertedSize(intsize: number): string {
    const unitConversion = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    if (intsize === null || intsize === undefined) {
        return "";
    } else {
        const x = Math.floor(Math.log(intsize) / Math.log(1024));
        return (intsize / Math.pow(1024, x)).toFixed(2) + " " + unitConversion[x];
    }
}

export function returnOSName(OS: number) {
    switch (OS) {
        case 0:
            return "Windows";
        case 1:
            return "Solaris";
        case 2:
            return "Linux";
    }
}

export function valueCleanUp(intsize): string {
    if (intsize === null || intsize === undefined) {
        return "";
    } else {
        return intsize;
    }
}

export function removeSpecialCharacters(stringToConvert): string {
    // eslint-disable-next-line no-useless-escape
    return stringToConvert.replace(/[\!\@\#\$\%\^\&\*\)\(\+\=\.\<\>\{\}\[\]\:\;\'\"\|\~\`\_\-]/g, "");
}

export function unitTest(size, unit) {
    const nsIndex = size.indexOf(unit);
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
    let retVal = unitTest(size, "Kb");
    if (retVal >= 0) {
        return retVal * 1024;
    }
    retVal = unitTest(size, "Mb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 2);
    }
    retVal = unitTest(size, "Gb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 3);
    }
    retVal = unitTest(size, "Tb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 4);
    }
    retVal = unitTest(size, "Pb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 5);
    }
    retVal = unitTest(size, "Eb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 6);
    }
    retVal = unitTest(size, "Zb");
    if (retVal >= 0) {
        return retVal * Math.pow(1024, 7);
    }
    retVal = unitTest(size, "b");
    if (retVal >= 0) {
        return retVal;
    }
    return 0;
}

export function espSize2BytesTests() {
    const tests = [
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
    const tests = [
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
    let csvContent = "";
    const headers = grid.columns;
    const container = [];
    const headerNames = [];

    for (const key in headers) {
        if (headers[key].selectorType !== "checkbox") {
            if (!headers[key].label) {
                const str = csvEncode(headers[key].field);
                headerNames.push(str);
            } else {
                const str = csvEncode(headers[key].label);
                headerNames.push(str);
            }
        }
    }
    container.push(headerNames);

    arrayUtil.forEach(rows, function (cells, idx) {
        container.push(cells);
    });

    arrayUtil.forEach(container, function (header, idx) {
        const dataString = header.join(",");
        csvContent += dataString + "\n";
    });

    const download = function (content, fileName, mimeType) {
        const a = document.createElement("a");
        mimeType = mimeType || "application/octet-stream";

        if (navigator.msSaveBlob) { // IE10
            return navigator.msSaveBlob(new Blob([content], { type: mimeType }), fileName);
        } else if ("download" in a) {
            a.href = "data:" + mimeType + "," + encodeURIComponent(content);
            a.setAttribute("download", fileName);
            document.body.appendChild(a);
            setTimeout(function () {
                a.click();
                document.body.removeChild(a);
            }, 66);
            return true;
        } else {
            const f = document.createElement("iframe");
            document.body.appendChild(f);
            f.src = "data:" + mimeType + "," + encodeURIComponent(content);

            setTimeout(function () {
                document.body.removeChild(f);
            }, 333);
            return true;
        }
    };
    download(csvContent, fileName, "text/csv");
}

export function isObjectEmpty(obj) {
    for (const prop in obj) {
        if (obj.hasOwnProperty(prop))
            return false;
    }
    return true;
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
        const tz = [];
        let x = 0;
        let y = -1;
        let n = false;
        let i;
        let j;

        // eslint-disable-next-line no-cond-assign
        while (i = (j = t.charAt(x++)).charCodeAt(0)) {
            // tslint:disable-next-line: triple-equals
            const m = (i == 46 || (i >= 48 && i <= 57));
            if (m !== n) {
                tz[++y] = "";
                n = m;
            }
            tz[y] += j;
        }
        return tz;
    }

    const aa = chunkify(a);
    const bb = chunkify(b);

    for (let x = 0; aa[x] && bb[x]; x++) {
        if (aa[x] !== bb[x]) {
            const c = Number(aa[x]);
            const d = Number(bb[x]);
            // tslint:disable-next-line: triple-equals
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
        const tz = [];
        let x = 0;
        let y = -1;
        let n = false;
        let i;
        let j;

        // eslint-disable-next-line no-cond-assign
        while (i = (j = t.charAt(x++)).charCodeAt(0)) {
            // tslint:disable-next-line: triple-equals
            const m = (i == 46 || (i >= 48 && i <= 57));    // jshint ignore:line
            if (m !== n) {
                tz[++y] = "";
                n = m;
            }
            tz[y] += j;
        }
        return tz;
    }

    const aa = chunkify(a.toLowerCase());
    const bb = chunkify(b.toLowerCase());

    for (let x = 0; aa[x] && bb[x]; x++) {
        if (aa[x] !== bb[x]) {
            const c = Number(aa[x]);
            const d = Number(bb[x]);
            // tslint:disable-next-line: triple-equals
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
        case "ClusterProcessesQueryWidget":
            require(["hpcc/ClusterProcessesQueryWidget"], doLoad);
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
        case "DataPatternsWidget":
            require(["src/DataPatternsWidget"], doLoad);
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
        case "ECLArchiveWidget":
            require(["src/ECLArchiveWidget"], doLoad);
            break;
        case "ECLSourceWidget":
            require(["hpcc/ECLSourceWidget"], doLoad);
            break;
        case "EventScheduleWorkunitWidget":
            require(["hpcc/EventScheduleWorkunitWidget"], doLoad);
            break;
        case "FileBloomsWidget":
            require(["hpcc/FileBloomsWidget"], doLoad);
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
        case "FileProtectListWidget":
            require(["hpcc/FileProtectListWidget"], doLoad);
            break;
        case "FilterDropDownWidget":
            require(["hpcc/FilterDropDownWidget"], doLoad);
            break;
        case "FullResultWidget":
            require(["hpcc/FullResultWidget"], doLoad);
            break;
        case "GangliaWidget":
            require(["ganglia/GangliaWidget"], doLoad);
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
        case "GraphsWidget":
            //  ECLIDE Backward Compatibility  ---
            require(["hpcc/GraphsWUWidget"], doLoad);
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
        case "LogVisualizationWidget":
            require(["hpcc/LogVisualizationWidget"], doLoad);
            break;
        case "LZBrowseWidget":
            require(["hpcc/LZBrowseWidget"], doLoad);
            break;
        case "MachineInformationWidget":
            require(["hpcc/MachineInformationWidget"], doLoad);
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
        case "SystemServersQueryWidget":
            require(["hpcc/SystemServersQueryWidget"], doLoad);
            break;
        case "TargetClustersQueryWidget":
            require(["hpcc/TargetClustersQueryWidget"], doLoad);
            break;
        case "TargetComboBoxWidget":
            require(["hpcc/TargetComboBoxWidget"], doLoad);
            break;
        case "TargetSelectWidget":
            require(["hpcc/TargetSelectWidget"], doLoad);
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
        default:
            console.log("case \"" + hpccWidget + "\":\n" +
                "    require([\"hpcc/" + hpccWidget + "\"], doLoad);\n" +
                "    break;\n");
    }
}

export function getURL(name) {
    return dojoConfig.urlInfo.resourcePath + "/" + name;
}

export function pathTail(path: string) {
    //  Assuming we need to support windows paths as well...
    const pathParts = path.split("\\").join("/").split("/");
    return pathParts.pop();
}

export function getImageURL(name) {
    return this.getURL("img/" + name);
}

export function getImageHTML(name, tooltip?) {
    return "<img src='" + this.getImageURL(name) + "'" + (tooltip ? " title='" + tooltip + "'" : "") + " class='iconAlign'/>";
}

export function debounce(func, threshold, execAsap) {
    let timeout;
    return function debounced() {
        const context = this;
        const args = arguments;
        function delayed() {
            if (!execAsap)
                func.apply(context, args);
            timeout = null;
        }
        if (timeout)
            clearTimeout(timeout);
        else if (execAsap)
            func.apply(context, args);
        timeout = setTimeout(delayed, threshold || 100);
    };
}

export function DynamicDialogForm(object) {
    const table = domConstruct.create("table", {});

    for (const key in object) {
        const tr = domConstruct.create("tr", {}, table);
        if (object.hasOwnProperty(key)) {
            const td = domConstruct.create("td", {
                style: "width: 30%;"
            }, tr);
            domConstruct.create("label", {
                innerHTML: object[key]["label"]
            }, td);
            const td1 = domConstruct.create("td", {
                style: "width: 100%;"
            }, tr);
            this.key = object[key]["widget"].placeAt(td1);
        }
    }
    return table;
}

export function DynamicDialogTable(headingsArr, rows) {
    const table = domConstruct.create("table", {
        style: "border-collapse: collapse; width: 100%;"
    });

    const headingTr = domConstruct.create("tr", {
        style: "border: 1px solid #dddddd;"
    }, table);

    arrayUtil.forEach(headingsArr, function (row, idx) {
        //  @ts-ignore
        const th = domConstruct.create("th", {
            innerHTML: row,
            style: "text-align: left; padding-left:5px;"
        }, headingTr);
    });

    arrayUtil.forEach(rows, function (row, idx) {
        const tr = domConstruct.create("tr", {
            style: "padding: 5px 0 5px 0;"
        }, table);
        for (const key in row) {
            //  @ts-ignore
            const td = domConstruct.create("td", {
                innerHTML: key === "ServiceName" ? "<a href=" + row.Protocol + "://" + location.hostname + ":" + row.Port + " target='_blank'>" + row[key] + "</a>" : row[key], // TODO improve the ability to add link in any cell
                style: "style: width: 30%; padding: 5px 0 5px 5px; border: 1px solid #dddddd;"
            }, tr);
        }
    });
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
            const retVal = localStorage.getItem(this.id + key);
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
            const retVal = localStorage.getItem(this.id + key);
            return retVal === null;
        }
        return false;
    }
}

export function textColor(backgroundColor: string): string {
    return Palette.textColor(backgroundColor);
}
