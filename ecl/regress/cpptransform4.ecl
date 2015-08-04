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

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

outRecord := RECORD
               STRING fullname{maxlength(30)};
             END;

transform(outRecord) createName(string s1, string s2) := BEGINC++
    size32_t len1 = rtlTrimStrLen(lenS1, s1);
    size32_t len2 = rtlTrimStrLen(lenS2, s2);
    size32_t len = len1+1+len2+1;
    byte * self = __self.ensureCapacity(sizeof(size32_t)+len,"");
    *(size32_t *)self = len;
    memcpy(self+sizeof(size32_t), s1, len1);
    memcpy(self+sizeof(size32_t)+len1, " ", 1);
    memcpy(self+sizeof(size32_t)+len1+1, s2, len2);
    memcpy(self+sizeof(size32_t)+len1+1+len2, "\n", 1);
    return sizeof(size32_t)+len;
ENDC++;

output(project(namesTable2, createName(LEFT.surname, left.forename)));
