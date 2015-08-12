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


mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('keyed.d00',mainRecord,THOR);

i0 := index(mainTable, { surname, forename , filepos } ,'i0');
i1 := index(mainTable, { surname, forename }, { filepos } ,'i1');
i2 := index(mainTable, { surname }, { forename, filepos } ,'i2');
i3 := index(mainTable, { forename }, { surname, filepos } ,'i3');
i4 := index(mainTable, { sequence }, { surname, filepos } ,'i4');
i5 := index(mainTable, { sequence }, { surname, filepos } ,'i5');

BUILDINDEX(i0,overwrite);
BUILDINDEX(i1,overwrite);
BUILDINDEX(i2,overwrite);       // should have diff crcs
BUILDINDEX(i3,overwrite);       // should have diff crcs
BUILDINDEX(i4,overwrite);       // should have diff crcs
BUILDINDEX(i5,overwrite);
