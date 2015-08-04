/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

PATTERN ws := PATTERN('[ \t\r\n]+');
PATTERN digit := PATTERN('[0-9]');
PATTERN decim := '.' digit*;
PATTERN sign := ws? ('+' | '-');
PATTERN operator := ws? ('+' | '-' | '*' | '/');
PATTERN number := ws? ((digit+ decim?) | decim);
PATTERN op := ws? '(';
PATTERN cp := ws? ')';
RULE expression := sign? (number | (op SELF cp)) (operator sign? (number | (op SELF cp)))*;
//RULE expr := FIRST expression ws? LAST;

layout_in := RECORD
    STRING100 line;
END;

b := DATASET([
{'1 + 2'},
{'4-(57)'},
{'34.6'},
{'(5 /6) * -(6 +-7)'},
{'((8) * (1))'},
{'((1))'},
{'(8-9'}
], layout_in);

layout_result := RECORD
    STRING1 valid_expression := IF (MATCHED(exprESSION), 'Y', 'N');
    STRING20 match := MATCHTEXT(exprESSION);
END;

results := PARSE(b, line, exprESSION, layout_result, MAX, MANY, NOT MATCHED, PARSE);
OUTPUT(results);