import { format as d3Format, Palette } from "@hpcc-js/common";
import { Level, join } from "@hpcc-js/util";
import { arrayUtil, domConstruct } from "src-dojo/index";
import nlsHPCC from "./nlsHPCC";

declare const dojoConfig;
declare const ActiveXObject;

export function encodeXML(str) {
    str = "" + str;

    const xmlEntities: Record<string, string> = {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        "\"": "&quot;",
        "'": "&apos;",
        "\n": "&#10;",
        "\r": "&#13;"
    };

    return str.replace(/[&"'<>\n\r]/g, (match) => xmlEntities[match]);
}

export function encodeHTML(str?: string): string {
    if (!str) return str || "";

    const htmlEntities: Record<string, string> = {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        "\"": "&quot;",
        "'": "&apos;",
        "\u00A0": "&nbsp;", // Non-breaking space (char code 160)
        "\n": "&#10;",
        "\r": "&#13;"
    };

    return str.replace(/[&<>"'\u00A0\n\r]/g, (match) => htmlEntities[match]);
}

export function decodeHTML(str?: string): string {
    if (!str) return str || "";

    const htmlEntities: Record<string, string> = {
        "&amp;": "&",
        "&lt;": "<",
        "&gt;": ">",
        "&quot;": "\"",
        "&apos;": "'",
        "&nbsp;": "\u00A0", // Non-breaking space (char code 160)
        "&#10;": "\n",
        "&#13;": "\r"
    };

    return str.replace(/&(?:amp|lt|gt|quot|apos|nbsp);|&#(?:10|13);/g, (match) => htmlEntities[match]);
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
    if (cell === undefined) return "";
    return '"' + String(cell).replace(/"/g, '""') + '"';
}

export function espTime2Seconds(duration?: string) {
    if (!duration) {
        return 0;
    } else if (!isNaN(+duration)) {
        return parseFloat(duration);
    } else if (duration.length > 32) {
        throw new Error("Input too long");
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

export function convertedSize(intsize: number, postfix: string = ""): string {
    const unitConversion = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    if (isNaN(intsize) || intsize < 1) {
        return "";
    } else {
        const x = intsize > 0 ? Math.floor(Math.log(intsize) / Math.log(1024)) : 0;
        return (intsize / Math.pow(1024, x)).toFixed(2) + " " + unitConversion[x] + postfix;
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

export interface Column {
    selectorType?: string;
    id?: string;
    csvFormatter?: (value: any, row: any) => string;
    field: string;
    label: string;
}
export type ColumnMap = { [id: string]: Column };
export function formatAsDelim(columns: ColumnMap, rows: any, delim = ",") {
    const container: string[] = [];
    const headerNames: string[] = [];

    for (const key in columns) {
        if (key !== columns[key].id && columns[key].selectorType !== "checkbox") {
            if (!columns[key].label) {
                const str = csvEncode(columns[key].field);
                headerNames.push(str);
            } else {
                const str = csvEncode(columns[key].label);
                headerNames.push(str);
            }
        }
    }
    container.push(headerNames.join(delim));

    rows.forEach(row => {
        const cells: any[] = [];
        for (const key in columns) {
            if (key !== columns[key].id && columns[key].selectorType !== "checkbox") {
                let value = row[key];
                if ("csvFormatter" in columns[key]) {
                    value = columns[key].csvFormatter(row[key], row);
                }
                const cell = row[columns[key].field] ?? value;
                cells.push(csvEncode(cell ?? ""));
            }
        }
        container.push(cells.join(delim));
    });

    return container.join("\n");
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
        container.push(cells.map(cell => csvEncode(cell ?? "")));
    });

    arrayUtil.forEach(container, function (header, idx) {
        const dataString = header.join(",");
        csvContent += dataString + "\n";
    });

    const download = function (content, fileName, mimeType) {
        const a = document.createElement("a");
        mimeType = mimeType || "application/octet-stream";

        // @ts-expect-error
        if (navigator.msSaveBlob) { // IE10
            // @ts-expect-error
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

export function isObjectEmpty(obj: object): boolean {
    return Object.keys(obj).length === 0;
}
//  -----------------------------------------------------------------------------------------------
//  Modified from alphanum-sort:  https://github.com/TrySound/alphanum-sort © Bogdan Chadkin
//  The MIT License (MIT)
const zero = "0".charCodeAt(0);
const plus = "+".charCodeAt(0);
const minus = "-".charCodeAt(0);

function isWhitespace(code: number) {
    return code <= 32;
}

function isDigit(code: number) {
    return 48 <= code && code <= 57;
}

function isSign(code: number) {
    return code === minus || code === plus;
}

function compare(a, b, opts: { sign: boolean }) {
    const checkSign = opts.sign;
    let ia = 0;
    let ib = 0;
    const ma = a.length;
    const mb = b.length;
    let ca, cb; // character code
    let za, zb; // leading zero count
    let na, nb; // number length
    let sa, sb; // number sign
    let ta, tb; // temporary
    let bias;

    while (ia < ma && ib < mb) {
        ca = a.charCodeAt(ia);
        cb = b.charCodeAt(ib);
        za = zb = 0;
        na = nb = 0;
        sa = sb = true;
        bias = 0;

        // skip over leading spaces
        while (isWhitespace(ca)) {
            ia += 1;
            ca = a.charCodeAt(ia);
        }
        while (isWhitespace(cb)) {
            ib += 1;
            cb = b.charCodeAt(ib);
        }

        // skip and save sign
        if (checkSign) {
            ta = a.charCodeAt(ia + 1);
            if (isSign(ca) && isDigit(ta)) {
                if (ca === minus) {
                    sa = false;
                }
                ia += 1;
                ca = ta;
            }
            tb = b.charCodeAt(ib + 1);
            if (isSign(cb) && isDigit(tb)) {
                if (cb === minus) {
                    sb = false;
                }
                ib += 1;
                cb = tb;
            }
        }

        // compare digits with other symbols
        if (isDigit(ca) && !isDigit(cb)) {
            return -1;
        }
        if (!isDigit(ca) && isDigit(cb)) {
            return 1;
        }

        // compare negative and positive
        if (!sa && sb) {
            return -1;
        }
        if (sa && !sb) {
            return 1;
        }

        // count leading zeros
        while (ca === zero) {
            za += 1;
            ia += 1;
            ca = a.charCodeAt(ia);
        }
        while (cb === zero) {
            zb += 1;
            ib += 1;
            cb = b.charCodeAt(ib);
        }

        // count numbers
        while (isDigit(ca) || isDigit(cb)) {
            if (isDigit(ca) && isDigit(cb) && bias === 0) {
                if (sa) {
                    if (ca < cb) {
                        bias = -1;
                    } else if (ca > cb) {
                        bias = 1;
                    }
                } else {
                    if (ca > cb) {
                        bias = -1;
                    } else if (ca < cb) {
                        bias = 1;
                    }
                }
            }
            if (isDigit(ca)) {
                ia += 1;
                na += 1;
                ca = a.charCodeAt(ia);
            }
            if (isDigit(cb)) {
                ib += 1;
                nb += 1;
                cb = b.charCodeAt(ib);
            }
        }

        // compare number length
        if (sa) {
            if (na < nb) {
                return -1;
            }
            if (na > nb) {
                return 1;
            }
        } else {
            if (na > nb) {
                return -1;
            }
            if (na < nb) {
                return 1;
            }
        }

        // compare numbers
        if (bias) {
            return bias;
        }

        // compare leading zeros
        if (sa) {
            if (za > zb) {
                return -1;
            }
            if (za < zb) {
                return 1;
            }
        } else {
            if (za < zb) {
                return -1;
            }
            if (za > zb) {
                return 1;
            }
        }

        // compare ascii codes
        if (ca < cb) {
            return -1;
        }
        if (ca > cb) {
            return 1;
        }

        ia += 1;
        ib += 1;
    }

    // compare length
    if (ma < mb) {
        return -1;
    }
    if (ma > mb) {
        return 1;
    }
    return 0;
}
//  -----------------------------------------------------------------------------------------------

export function onDomMutate(domNode, callback, observerOpts) {
    observerOpts = observerOpts || { attributes: true, attributeFilter: ["style"] };
    const observer = new MutationObserver(mutations => {
        if (domNode.offsetParent === null) return;
        observer.disconnect();
        if (typeof callback === "function") {
            callback();
        }
    });
    observer.observe(domNode, observerOpts);
}

export function alphanumCompare(_l, _r, caseInsensitive: boolean = true, reverse: boolean = true): number {
    const l = caseInsensitive && typeof _l === "string" ? _l.toLocaleLowerCase() : _l;
    const r = caseInsensitive && typeof _r === "string" ? _r.toLocaleLowerCase() : _r;
    const cmp = compare(l, r, { sign: false });
    if (cmp !== 0) {
        return cmp * (reverse ? -1 : 1);
    }
    return 0;
}

export function createAlphanumSortFunc(cols: string[], caseInsensitive: boolean, reverse: boolean = false) {
    return function (l, r) {
        for (let i = 0; i < cols.length; ++i) {
            const col = cols[i];
            const cmp = alphanumCompare(l[col], r[col], caseInsensitive, reverse);
            if (cmp !== 0) {
                return cmp;
            }
        }
        return 0;
    };
}

export function copyToClipboard(value: string) {
    navigator?.clipboard?.writeText(value);
}

export function alphanumSort(arr, col, caseInsensitive, reverse: boolean = false) {
    if (arr && arr instanceof Array) {
        arr.sort(createAlphanumSortFunc(col, caseInsensitive, reverse));
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
        widget = widget.default ?? widget;
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
            import("hpcc/ActivityWidget").then(doLoad);
            break;
        case "ClusterProcessesQueryWidget":
            import("hpcc/ClusterProcessesQueryWidget").then(doLoad);
            break;
        case "CurrentUserDetailsWidget":
            import("hpcc/CurrentUserDetailsWidget").then(doLoad);
            break;
        case "DelayLoadWidget":
            import("hpcc/DelayLoadWidget").then(doLoad);
            break;
        case "DFUQueryWidget":
            import("hpcc/DFUQueryWidget").then(doLoad);
            break;
        case "DFUSearchWidget":
            import("hpcc/DFUSearchWidget").then(doLoad);
            break;
        case "DFUWUDetailsWidget":
            import("hpcc/DFUWUDetailsWidget").then(doLoad);
            break;
        case "DiskUsageWidget":
            import("hpcc/DiskUsageWidget").then(doLoad);
            break;
        case "DiskUsageDetails":
            import("hpcc/DiskUsageDetails").then(doLoad);
            break;
        case "ComponentUsageDetails":
            import("hpcc/ComponentUsageDetails").then(doLoad);
            break;
        case "viz/DojoD3Choropleth":
            import("hpcc/viz/DojoD3Choropleth").then(doLoad);
            break;
        case "viz/DojoD32DChart":
            import("hpcc/viz/DojoD32DChart").then(doLoad);
            break;
        case "viz/DojoD3NDChart":
            import("hpcc/viz/DojoD3NDChart").then(doLoad);
            break;
        case "DataPatternsWidget":
            import("src/DataPatternsWidget").then(doLoad);
            break;
        case "DynamicESDLDefinitionDetailsWidget":
            import("hpcc/DynamicESDLDefinitionDetailsWidget").then(doLoad);
            break;
        case "DynamicESDLDefinitionQueryWidget":
            import("hpcc/DynamicESDLDefinitionQueryWidget").then(doLoad);
            break;
        case "DynamicESDLDetailsWidget":
            import("hpcc/DynamicESDLDetailsWidget").then(doLoad);
            break;
        case "DynamicESDLMethodWidget":
            import("hpcc/DynamicESDLMethodWidget").then(doLoad);
            break;
        case "DynamicESDLQueryWidget":
            import("hpcc/DynamicESDLQueryWidget").then(doLoad);
            break;
        case "ECLPlaygroundResultsWidget":
            import("hpcc/ECLPlaygroundResultsWidget").then(doLoad);
            break;
        case "ECLPlaygroundWidget":
            import("hpcc/ECLPlaygroundWidget").then(doLoad);
            break;
        case "ECLArchiveWidget":
            import("src/ECLArchiveWidget").then(doLoad);
            break;
        case "ECLSourceWidget":
            import("hpcc/ECLSourceWidget").then(doLoad);
            break;
        case "EventScheduleWorkunitWidget":
            import("hpcc/EventScheduleWorkunitWidget").then(doLoad);
            break;
        case "FileBloomsWidget":
            import("hpcc/FileBloomsWidget").then(doLoad);
            break;
        case "FileBelongsToWidget":
            import("hpcc/FileBelongsToWidget").then(doLoad);
            break;
        case "FileHistoryWidget":
            import("hpcc/FileHistoryWidget").then(doLoad);
            break;
        case "FilePartsWidget":
            import("hpcc/FilePartsWidget").then(doLoad);
            break;
        case "FileProtectListWidget":
            import("hpcc/FileProtectListWidget").then(doLoad);
            break;
        case "FilterDropDownWidget":
            import("hpcc/FilterDropDownWidget").then(doLoad);
            break;
        case "FullResultWidget":
            import("hpcc/FullResultWidget").then(doLoad);
            break;
        case "GangliaWidget":
            import("ganglia/GangliaWidget").then(doLoad);
            break;
        case "GetDFUWorkunitsWidget":
            import("hpcc/GetDFUWorkunitsWidget").then(doLoad);
            break;
        case "GetNumberOfFilesToCopyWidget":
            import("hpcc/GetNumberOfFilesToCopyWidget").then(doLoad);
            break;
        case "GraphPageWidget":
            import("hpcc/GraphPageWidget").then(doLoad);
            break;
        case "GraphsWidget":
            //  ECLIDE Backward Compatibility  ---
            import("hpcc/GraphsWUWidget").then(doLoad);
            break;
        case "GraphsWUWidget":
            import("hpcc/GraphsWUWidget").then(doLoad);
            break;
        case "GraphsQueryWidget":
            import("hpcc/GraphsQueryWidget").then(doLoad);
            break;
        case "GraphsLFWidget":
            import("hpcc/GraphsLFWidget").then(doLoad);
            break;
        case "GraphTree7Widget":
            import("src/GraphTree7Widget").then(doLoad);
            break;
        case "Graph7Widget":
            import("src/Graph7Widget").then(doLoad);
            break;
        case "GridDetailsWidget":
            import("hpcc/GridDetailsWidget").then(doLoad);
            break;
        case "GroupDetailsWidget":
            import("hpcc/GroupDetailsWidget").then(doLoad);
            break;
        case "HelpersWidget":
            import("hpcc/HelpersWidget").then(doLoad);
            break;
        case "HexViewWidget":
            import("hpcc/HexViewWidget").then(doLoad);
            break;
        case "HPCCPlatformECLWidget":
            import("hpcc/HPCCPlatformECLWidget").then(doLoad);
            break;
        case "HPCCPlatformFilesWidget":
            import("hpcc/HPCCPlatformFilesWidget").then(doLoad);
            break;
        case "HPCCPlatformMainWidget":
            import("hpcc/HPCCPlatformMainWidget").then(doLoad);
            break;
        case "HPCCPlatformOpsWidget":
            import("hpcc/HPCCPlatformOpsWidget").then(doLoad);
            break;
        case "HPCCPlatformRoxieWidget":
            import("hpcc/HPCCPlatformRoxieWidget").then(doLoad);
            break;
        case "HPCCPlatformServicesPluginWidget":
            import("hpcc/HPCCPlatformServicesPluginWidget").then(doLoad);
            break;
        case "HPCCPlatformWidget":
            import("hpcc/HPCCPlatformWidget").then(doLoad);
            break;
        case "IFrameWidget":
            import("hpcc/IFrameWidget").then(doLoad);
            break;
        case "InfoGridWidget":
            import("hpcc/InfoGridWidget").then(doLoad);
            break;
        case "JSGraphWidget":
            import("hpcc/JSGraphWidget").then(doLoad);
            break;
        case "LFDetailsWidget":
            import("hpcc/LFDetailsWidget").then(doLoad);
            break;
        case "LibrariesUsedWidget":
            import("hpcc/LibrariesUsedWidget").then(doLoad);
            break;
        case "LogsWidget":
            import("hpcc/HelpersWidget").then(doLoad);
            break;
        case "LogWidget":
            import("hpcc/LogWidget").then(doLoad);
            break;
        case "LogVisualizationWidget":
            import("hpcc/LogVisualizationWidget").then(doLoad);
            break;
        case "LZBrowseWidget":
            import("hpcc/LZBrowseWidget").then(doLoad);
            break;
        case "MachineInformationWidget":
            import("hpcc/MachineInformationWidget").then(doLoad);
            break;
        case "MemberOfWidget":
            import("hpcc/MemberOfWidget").then(doLoad);
            break;
        case "MembersWidget":
            import("hpcc/MembersWidget").then(doLoad);
            break;
        case "PackageMapDetailsWidget":
            import("hpcc/PackageMapDetailsWidget").then(doLoad);
            break;
        case "PackageMapPartsWidget":
            import("hpcc/PackageMapPartsWidget").then(doLoad);
            break;
        case "PackageMapQueryWidget":
            import("hpcc/PackageMapQueryWidget").then(doLoad);
            break;
        case "PackageMapValidateContentWidget":
            import("hpcc/PackageMapValidateContentWidget").then(doLoad);
            break;
        case "PackageMapValidateWidget":
            import("hpcc/PackageMapValidateWidget").then(doLoad);
            break;
        case "PackageSourceWidget":
            import("hpcc/PackageSourceWidget").then(doLoad);
            break;
        case "PermissionsWidget":
            import("hpcc/PermissionsWidget").then(doLoad);
            break;
        case "PreflightDetailsWidget":
            import("hpcc/PreflightDetailsWidget").then(doLoad);
            break;
        case "QuerySetDetailsWidget":
            import("hpcc/QuerySetDetailsWidget").then(doLoad);
            break;
        case "QuerySetErrorsWidget":
            import("hpcc/QuerySetErrorsWidget").then(doLoad);
            break;
        case "QuerySetLogicalFilesWidget":
            import("hpcc/QuerySetLogicalFilesWidget").then(doLoad);
            break;
        case "QuerySetQueryWidget":
            import("hpcc/QuerySetQueryWidget").then(doLoad);
            break;
        case "QuerySetSuperFilesWidget":
            import("hpcc/QuerySetSuperFilesWidget").then(doLoad);
            break;
        case "QueryTestWidget":
            import("hpcc/QueryTestWidget").then(doLoad);
            break;
        case "RequestInformationWidget":
            import("hpcc/RequestInformationWidget").then(doLoad);
            break;
        case "ResourcesWidget":
            import("hpcc/ResourcesWidget").then(doLoad);
            break;
        case "ResultsWidget":
            import("hpcc/ResultsWidget").then(doLoad);
            break;
        case "ResultWidget":
            import("hpcc/ResultWidget").then(doLoad);
            break;
        case "SearchResultsWidget":
            import("hpcc/SearchResultsWidget").then(doLoad);
            break;
        case "SelectionGridWidget":
            import("hpcc/SelectionGridWidget").then(doLoad);
            break;
        case "SFDetailsWidget":
            import("hpcc/SFDetailsWidget").then(doLoad);
            break;
        case "ShowAccountPermissionsWidget":
            import("hpcc/ShowAccountPermissionsWidget").then(doLoad);
            break;
        case "ShowIndividualPermissionsWidget":
            import("hpcc/ShowIndividualPermissionsWidget").then(doLoad);
            break;
        case "ShowInheritedPermissionsWidget":
            import("hpcc/ShowInheritedPermissionsWidget").then(doLoad);
            break;
        case "ShowPermissionsWidget":
            import("hpcc/ShowPermissionsWidget").then(doLoad);
            break;
        case "SourceFilesWidget":
            import("hpcc/SourceFilesWidget").then(doLoad);
            break;
        case "SummaryStatsQueryWidget":
            import("hpcc/SummaryStatsQueryWidget").then(doLoad);
            break;
        case "SystemServersQueryWidget":
            import("hpcc/SystemServersQueryWidget").then(doLoad);
            break;
        case "TargetClustersQueryWidget":
            import("hpcc/TargetClustersQueryWidget").then(doLoad);
            break;
        case "TargetComboBoxWidget":
            import("hpcc/TargetComboBoxWidget").then(doLoad);
            break;
        case "TargetSelectWidget":
            import("hpcc/TargetSelectWidget").then(doLoad);
            break;
        case "TimingPageWidget":
            import("hpcc/TimingPageWidget").then(doLoad);
            break;
        case "TimingTreeMapWidget":
            import("hpcc/TimingTreeMapWidget").then(doLoad);
            break;
        case "TopologyDetailsWidget":
            import("hpcc/TopologyDetailsWidget").then(doLoad);
            break;
        case "TopologyWidget":
            import("hpcc/TopologyWidget").then(doLoad);
            break;
        case "TpClusterInfoWidget":
            import("hpcc/TpClusterInfoWidget").then(doLoad);
            break;
        case "TpThorStatusWidget":
            import("hpcc/TpThorStatusWidget").then(doLoad);
            break;
        case "UserDetailsWidget":
            import("hpcc/UserDetailsWidget").then(doLoad);
            break;
        case "UserQueryWidget":
            import("hpcc/UserQueryWidget").then(doLoad);
            break;
        case "VariablesWidget":
            import("hpcc/VariablesWidget").then(doLoad);
            break;
        case "VizWidget":
            import("hpcc/VizWidget").then(doLoad);
            break;
        case "WorkflowsWidget":
            import("hpcc/WorkflowsWidget").then(doLoad);
            break;
        case "ProcessesWidget":
            import("hpcc/ProcessesWidget").then(doLoad);
            break;
        case "WUDetailsWidget":
            import("hpcc/WUDetailsWidget").then(doLoad);
            break;
        case "WUQueryWidget":
            import("hpcc/WUQueryWidget").then(doLoad);
            break;
        case "XrefDetailsWidget":
            import("hpcc/XrefDetailsWidget").then(doLoad);
            break;
        case "XrefDirectoriesWidget":
            import("hpcc/XrefDirectoriesWidget").then(doLoad);
            break;
        case "XrefErrorsWarningsWidget":
            import("hpcc/XrefErrorsWarningsWidget").then(doLoad);
            break;
        case "XrefFoundFilesWidget":
            import("hpcc/XrefFoundFilesWidget").then(doLoad);
            break;
        case "XrefLostFilesWidget":
            import("hpcc/XrefLostFilesWidget").then(doLoad);
            break;
        case "XrefOrphanFilesWidget":
            import("hpcc/XrefOrphanFilesWidget").then(doLoad);
            break;
        case "XrefQueryWidget":
            import("hpcc/XrefQueryWidget").then(doLoad);
            break;
        default:
            console.log("case \"" + hpccWidget + "\":\n" +
                "    import(\"hpcc/" + hpccWidget + "\").then(doLoad);\n" +
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

export function joinPath(pathSegment, pathSep: string = "/") {
    let path = join(pathSegment);
    if (!path.endsWith(pathSep)) {
        path += pathSep;
    }
    return path;
}

export function normalizePath(path: string) {
    if (!path) return "";
    return path?.endsWith("/") ? path.slice(0, -1) : path;
}

export function getImageURL(name) {
    return getURL("img/" + name);
}

export function getImageHTML(name, tooltip?) {
    return "<img src='" + getImageURL(name) + "'" + (tooltip ? " title='" + tooltip + "'" : "") + " class='iconAlign'/>";
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
        if (key in object) {
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
        // @ts-expect-error
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
            // @ts-expect-error
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

function toCSVCell(str) {
    str = "" + str;
    const mustQuote = (str.indexOf(",") >= 0 || str.indexOf("\"") >= 0 || str.indexOf("\r") >= 0 || str.indexOf("\n") >= 0);
    if (mustQuote) {
        let retVal = "\"";
        for (let i = 0; i < str.length; ++i) {
            const c = str.charAt(i);
            retVal += c === "\"" ? "\"\"" : c;

        }
        retVal += "\"";
        return retVal;
    }
    return str;
}

function csvFormatHeader(data, delim) {
    let retVal = "";
    if (data.length) {
        for (const key in data[0]) {
            if (retVal.length)
                retVal += delim;
            retVal += key;
        }
    }
    return retVal;
}

function csvFormatRow(row, idx, delim) {
    let retVal = "";
    for (const key in row) {
        if (retVal.length)
            retVal += delim;
        retVal += toCSVCell(row[key]);
    }
    return retVal;
}

function csvFormatFooter(data) {
    return "";
}

export function toCSV(data, delim = ",") {
    let retVal = csvFormatHeader(data, delim) + "\n";
    data.forEach((item, idx) => {
        retVal += csvFormatRow(item, idx, delim) + "\n";
    });
    retVal += csvFormatFooter(data);
    return retVal;
}

function downloadText(content: string, fileName: string, type: "csv" | "plain" = "csv") {
    const textBlob = new Blob([content], { type: `text/${type}` });
    const link = document.createElement("a");
    link.setAttribute("download", fileName);
    link.setAttribute("href", window.URL.createObjectURL(textBlob));
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

export function downloadCSV(content: string, fileName: string) {
    downloadText(content, fileName, "csv");
}

export function downloadPlain(content: string, fileName: string) {
    downloadText(content, fileName, "plain");
}

const d3FormatNum = d3Format(",");

export function parseCookies(): Record<string, any> {
    const cookies = {};
    document.cookie.split(";").map(pair => {
        const [key, ...values] = pair.split("=");
        cookies[key.trim()] = values.join("=");
    });
    return cookies;
}

export function deleteCookie(name: string) {
    const expireDate = new Date();
    expireDate.setSeconds(expireDate.getSeconds() + 1);
    document.cookie = `${name}=; domain=${window.location.hostname}; expires=${expireDate.toUTCString()}`;
}

const d3FormatDecimal = d3Format(",.2f");
const d3FormatInt = d3Format(",.0f");

export function formatDecimal(num: number): string {
    if (!num) return "";
    if (isNaN(num)) return num.toString();
    return d3FormatDecimal(num);
}

export function formatNum(num: number): string {
    if (!num) return "";
    if (isNaN(num)) return num.toString();
    return d3FormatNum(num);
}

export function safeFormatNum(num: number): string {
    if (!num) return "";
    if (isNaN(num)) return num.toString();
    if (num < 0) return nlsHPCC.NotAvailable;
    return d3FormatInt(num);
}

export function formatNums(obj) {
    for (const key in obj) {
        obj[key] = formatNum(obj[key]);
    }
    return obj;
}

export function isNumeric(n: string | undefined | null | number) {
    return !isNaN(parseFloat(n as string)) && isFinite(n as number);
}

export function formatLine(labelTpl, obj): string {
    let retVal = "";
    let lpos = labelTpl.indexOf("%");
    let rpos = -1;
    let replacementFound = lpos >= 0 ? false : true;  //  If a line has no symbols always include it, otherwise only include that line IF a replacement was found  ---
    while (lpos >= 0) {
        retVal += labelTpl.substring(rpos + 1, lpos);
        rpos = labelTpl.indexOf("%", lpos + 1);
        if (rpos < 0) {
            console.log("Invalid Label Template");
            break;
        }
        const key = labelTpl.substring(lpos + 1, rpos);
        replacementFound = replacementFound || !!obj[labelTpl.substring(lpos + 1, rpos)];
        retVal += !key ? "%" : (obj[labelTpl.substring(lpos + 1, rpos)] || "");
        lpos = labelTpl.indexOf("%", rpos + 1);
    }
    retVal += labelTpl.substring(rpos + 1, labelTpl.length);
    return replacementFound ? retVal : "";
}

export function format(labelTpl, obj) {
    labelTpl = labelTpl.split("\\n").join("\n");

    const lines = labelTpl.split("\n");
    const result: string[] = [];

    for (const line of lines) {
        const formattedLine = formatLine(line, obj);
        if (formattedLine.trim().length > 0) {
            result.push(decodeHTML(formattedLine));
        }
    }

    return result.join("\n");
}

const TEN_TRILLION = 10000000000000;
export function nanosToMillis(timestamp: number): number {
    if (timestamp > TEN_TRILLION) {
        return Math.round(timestamp / 1000000);
    } else {
        return timestamp;
    }
}

export function timestampToDate(timestamp: number): Date {
    const millis = nanosToMillis(timestamp);
    return new Date(millis);
}

export function formatDateString(dateStr: string): string {
    const matches = dateStr.match(/([0-9]{4}(?:-[0-9]{1,2})+)([T\s])((?:[0-9]{1,2}:)+[0-9]{1,2}\.[0-9]{1,3})(Z*)/);
    if (matches) {
        return `${matches[1]}T${matches[3]}${matches[4] ? matches[4] : "Z"}`;
    }
    return dateStr;
}

export function logColor(level: Level): { background: string, foreground: string } {
    const colors = {
        background: "transparent",
        foreground: "inherit"
    };

    switch (level) {
        case Level.debug:
            colors.background = "var(--colorStatusSuccessBackground1)";
            colors.foreground = "var(--colorStatusSuccessForeground1)";
            break;
        case Level.info:
        case Level.notice:
            break;
        case Level.warning:
            colors.background = "var(--colorStatusWarningBackground1)";
            colors.foreground = "var(--colorStatusWarningForeground1)";
            break;
        case Level.error:
            colors.background = "var(--colorStatusDangerBackground1)";
            colors.foreground = "var(--colorStatusDangerForeground1)";
            break;
        case Level.critical:
        case Level.alert:
        case Level.emergency:
            colors.background = "var(--colorStatusDangerBackground2)";
            colors.foreground = "var(--colorStatusDangerForeground2)";
            break;
    }

    return colors;
}

export function themeIsDark() {
    if (typeof window === "undefined" || typeof document === "undefined") return false;
    return document.body.classList.contains("flat-dark");
}

export function wrapStringWithTag(string, tag = "span") {
    let retVal = string;
    const unallowedTags = ["script", "style", "link", "a", "input", "form", "img", "video", "iframe", "frameset"];
    if (!unallowedTags.includes(tag)) {
        const elm = document.createElement(tag);
        elm.innerText = string;
        retVal = elm.outerHTML;
    }
    return retVal;
}

export function isSpill(sourceKind: string, targetKind: string): boolean {
    return sourceKind === "2" || targetKind === "71";
}

export function wuidToDate(wuid: string): string {
    return `${wuid.substring(1, 5)}-${wuid.substring(5, 7)}-${wuid.substring(7, 9)}`;
}

export function wuidToTime(wuid: string): string {
    return `${wuid.substring(10, 12)}:${wuid.substring(12, 14)}:${wuid.substring(14, 16)}`;
}

export function wuidToDateTime(wuid: string): Date {
    return new Date(`${wuidToDate(wuid)}T${wuidToTime(wuid)}Z`);
}

export function removeAllExcept(arr: any, keysToKeep: string[]): void {
    for (const key of Object.keys(arr)) {
        if (keysToKeep.indexOf(key) < 0) {
            delete arr[key];
        }
    }
}

function pad(n: number): string {
    return n.toString().padStart(2, "0");
}

export function formatDate(date: Date, useUTC: boolean): string {
    const mm = pad(useUTC ? date.getUTCMonth() + 1 : date.getMonth() + 1);
    const dd = pad(useUTC ? date.getUTCDate() : date.getDate());
    const yyyy = useUTC ? date.getUTCFullYear() : date.getFullYear();
    const hh = pad(useUTC ? date.getUTCHours() : date.getHours());
    const min = pad(useUTC ? date.getUTCMinutes() : date.getMinutes());
    const sec = pad(useUTC ? date.getUTCSeconds() : date.getSeconds());
    return `${yyyy}-${mm}-${dd} ${hh}:${min}:${sec}`;
}