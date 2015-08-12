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
