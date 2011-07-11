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

reverseString4 := TYPE
   shared STRING4 REV(STRING4 S) := S[4] + S[3] + S[2] +S[1];
   EXPORT STRING4 LOAD(STRING4 S) := REV(S);
   EXPORT STRING4 STORE(STRING4 S) := REV(S);
END;
LAYOUT:= RECORD
reverseString4 FIELD;
end;
TEST_STRING := dataset([{'ABCD'}], LAYOUT);
CASTED:=(string4)TEST_STRING.FIELD;
output(TEST_STRING, {FIELD, CASTED});
