define([], function () {
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

    return {
        espTime2Seconds: espTime2Seconds
    }
});
