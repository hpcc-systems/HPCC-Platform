/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

PATTERN ws := PATTERN('[ \t\r\n]+');
PATTERN digit := PATTERN('[0-9]');
PATTERN decim := '.' digit*;
PATTERN sign := ('+' | '-');
PATTERN operator :=  ('+' | '-' | '*' | '/');
token number :=  ((digit+ decim?) | decim);
PATTERN op := '(';
PATTERN cp := ')';
RULE expression := sign? (number | (op SELF cp)) (operator sign? (number | (op SELF cp)))*;
RULE expr := FIRST expression ws? LAST;

layout_in := RECORD
    STRING line;
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

results := PARSE(b, line, expr, layout_result, MAX, MANY, NOT MATCHED, PARSE,skip(ws));
OUTPUT(results);