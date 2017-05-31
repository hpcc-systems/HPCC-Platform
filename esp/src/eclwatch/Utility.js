define([
    "dojo/_base/array",
    "dojox/html/entities"
], function (arrayUtil, entities) {

    function xmlEncode(str) {
        str = "" + str;
        return entities.encode(str);
    }

    function xmlEncode2(str) {
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

    function parseXML (val) {
        var xmlDoc;
        if (window.DOMParser) {
            var parser = new DOMParser();
            xmlDoc = parser.parseFromString(val,"text/xml");
        } else {
            xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
            xmlDoc.async = false;
            xmlDoc.loadXML(val);
        }
        return xmlDoc;
    }

    function csvEncode(cell) {
        if (!isNaN(cell)) return cell;
        return '"' + String(cell).replace('"', '""') + '"';
    }

    function espTime2Seconds(duration) {
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

    function espTime2SecondsTests() {
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

    function unitTest(size, unit) {
        var nsIndex = size.indexOf(unit);
        if (nsIndex !== -1) {
            return parseFloat(size.substr(0, nsIndex));
        }
        return -1;
    }

    function espSize2Bytes(size) {
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

    function espSize2BytesTests() {
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
                console.log("espSize2BytesTests failed with " + test.str + "(" +espSize2Bytes(test.str) + ") !== " + test.expected);
            }
        }, this);
    }

    function espSkew2Number(skew) {
        if (!skew) {
            return 0;
        }
        return parseFloat(skew);
    }

    function espSkew2NumberTests() {
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

    function downloadToCSV (grid, rows, fileName) {
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

            arrayUtil.forEach(rows, function (cells, idx){
                container.push(cells);
            });

            arrayUtil.forEach(container, function (header, idx) {
                var dataString = header.join(",");
                csvContent += dataString + "\n";
            });

            var download = function(content, fileName, mimeType) {
            var a = document.createElement('a');
            mimeType = mimeType || 'application/octet-stream';

            if (navigator.msSaveBlob) { // IE10
                return navigator.msSaveBlob(new Blob([content], { type: mimeType }), fileName);
            } else if ('download' in a) {
                a.href = 'data:' + mimeType + ',' + encodeURIComponent(content);
                a.setAttribute('download', fileName);
                document.body.appendChild(a);
                setTimeout(function() {
                  a.click();
                  document.body.removeChild(a);
                }, 66);
                return true;
              } else {
                var f = document.createElement('iframe');
                document.body.appendChild(f);
                f.src = 'data:' + mimeType + ',' + encodeURIComponent(content);

                setTimeout(function() {
                  document.body.removeChild(f);
                }, 333);
                return true;
              }
            }
            download(csvContent,  fileName, 'text/csv');
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
    function alphanum(a, b) {
        function chunkify(t) {
            var tz = [];
            var x = 0, y = -1, n = 0, i, j;

            while (i = (j = t.charAt(x++)).charCodeAt(0)) {
                var m = (i == 46 || (i >= 48 && i <= 57));  // jshint ignore:line
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
                if (c == aa[x] && d == bb[x]) {   // jshint ignore:line
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
    function alphanumCase(a, b) {
        function chunkify(t) {
            var tz = [];
            var x = 0, y = -1, n = 0, i, j;

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

    function alphanumSort(arr, col, caseInsensitive, reverse) {
        if (arr && arr instanceof Array) {
            arr.sort(function (l, r) {
                if (caseInsensitive) {
                    return alphanumCase(r[col], l[col]) * (reverse ? -1 : 1);
                }
                return alphanum(l[col], r[col]) * (reverse ? -1 : 1);
            });
        }
    }

    return {
        espTime2Seconds: espTime2Seconds,
        espSize2Bytes: espSize2Bytes,
        espSkew2Number: espSkew2Number,
        xmlEncode: xmlEncode,
        xmlEncode2: xmlEncode2,
        alphanumSort: alphanumSort,
        downloadToCSV: downloadToCSV,
        parseXML: parseXML
    }
});
