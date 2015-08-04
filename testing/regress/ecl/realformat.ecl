/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//Windows and linux format floating point numbers with > 15 places differently, so only check the significant digits
checkSignificant(real value, unsigned width, unsigned places, string expected) := FUNCTION
    formatted := REALFORMAT(value, width, places);
    RETURN PARALLEL(
        OUTPUT(LENGTH(formatted) = width);
        OUTPUT(TRIM(formatted, LEFT, RIGHT)[1..LENGTH(expected)] = expected);
    );
END;


doFormat(real value, string expected1, string expected2, string expected3) := PARALLEL(
    REALFORMAT(value, 0, 0);
    REALFORMAT(value, 0, 1);
    REALFORMAT(value, 1, 0);
    REALFORMAT(value, 1, 1);
    REALFORMAT(value, 2, 1);
    REALFORMAT(value, 3, 1);
    REALFORMAT(value, 3, 4);
    REALFORMAT(value, 5, 1);
    checkSignificant(value, 99, 10, expected1);
    checkSignificant(value, 2000, 10, expected2);
    checkSignificant(value, 2000, 1000, expected3);
);

doFormat(0.0,                   '0.0000000000', '0.0000000000',     '0.000000000000000');
doFormat(0.1,                   '0.1000000000', '0.1000000000',     '0.100000000000000');
doFormat(-0.1,                  '-0.1000000000', '-0.1000000000',  '-0.100000000000000');
doFormat(2.1,                   '2.1000000000', '2.1000000000',     '2.10000000000000');
doFormat(-2.1,                  '-2.1000000000', '-2.1000000000',  '-2.10000000000000');
doFormat(23.1,                  '23.1000000000', '23.1000000000',  '23.1000000000000');
doFormat(-23.1,                 '-23.1000000000', '-23.100000000','-23.1000000000000');

// only coompare 14 significant digits since linux outputs exactly at 12345678901234499... and does not round up the 15th digit
doFormat(1.23456789012345e200,  '**************', '12345678901234', '12345678901234');
doFormat(-1.23456789012345E200, '**************', '-12345678901234', '-12345678901234');
