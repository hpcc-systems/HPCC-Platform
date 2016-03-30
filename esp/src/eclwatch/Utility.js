define([
    "dojox/html/entities"
], function (entities) {

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

    return {
        espTime2Seconds: espTime2Seconds,
        espSize2Bytes: espSize2Bytes,
        xmlEncode: xmlEncode,
        xmlEncode2: xmlEncode2
    }
});
