/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

// HPCC-34182

IMPORT Std;

ReplaceTest(UTF8 s) := FUNCTION
    RETURN  Std.Uni.CleanSpaces(REGEXREPLACE(u'\r\n', S, u' '));
END;

UTF8 text_1 := 'gavin\r\nhalliday' : STORED('x');

regexRes_1 := ReplaceTest(text_1);
finalRes_1 := IF(regexRes_1 = u'gavin halliday', 'PASSED', 'FAILED');

OUTPUT(finalRes_1, NAMED('result_1'));

//------------------

FindSetTest(UTF8 s) := FUNCTION
    RETURN REGEXFINDSET(u'\\w+', s);
END;

UTF8 text_2 := 'gavin halliday' : STORED('y');

regexRes_2 := FindSetTest(text_2);
finalRes_2 := IF(regexRes_2 = [u'gavin', u'halliday'], 'PASSED', 'FAILED');

OUTPUT(finalRes_2, NAMED('result_2'));

//------------------

ReplaceVarTest(STRING s) := FUNCTION
    RETURN REGEXREPLACE((VARSTRING)'\r\n', S, ' ');
END;

STRING text_3 := 'gavin\r\nhalliday' : STORED('z');

regexRes_3 := ReplaceVarTest(text_3);
finalRes_3 := IF(regexRes_3 = 'gavin halliday', 'PASSED', 'FAILED');

OUTPUT(finalRes_3, NAMED('result_3'));
