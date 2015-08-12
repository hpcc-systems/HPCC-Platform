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


codePageText(varstring codepage, unsigned4 len) := TYPE
        export integer physicallength := len;
        export unicode load(data s) := TOUNICODE(s[1..len], codepage);
        export data store(unicode s) := FROMUNICODE(s, codepage)[1..len];
    END;


externalRecord :=
    RECORD
        unsigned8 id;
        codePageText('utf' + '-16be',20) firstname;
        codePageText('utf8',20) lastname;
        string40 addr;
    END;

internalRecord :=
    RECORD
        unsigned8 id;
        unicode10 firstname;
        unicode20 lastname;
        string40 addr;
    END;

external := DATASET('input', externalRecord, THOR);

internalRecord t(externalRecord l) :=
    TRANSFORM
        SELF := l;
    END;

cleaned := PROJECT(external, t(LEFT));

output(cleaned);
