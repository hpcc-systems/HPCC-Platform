/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//UseStandardFiles
//UseIndexes
//nothor

krec := RECORD
    STRING20 lname;
END;

d1 := DATASET([{'BAYLISS'}, {'SMITH'}, {'DOLSON'}, {'XXXXXXX'}], {STRING20 lname});

orec := RECORD
    STRING20 fname;
    STRING20 lname;
END;

orec xfm1(d1 l) := TRANSFORM
    SELF.fname := (SORT(DG_indexFile(dg_lastName=l.lname), dg_firstName))[1].dg_firstName;
    SELF := l;
    END;

orec xfm2(d1 l) := TRANSFORM
    SELF.fname := (SORT(DG_indexFile(dg_lastName=l.lname), -dg_firstName))[1].dg_firstName;
    SELF := l;
    END;

orec xfm3(d1 l) := TRANSFORM
    SELF.fname := (SORT(DG_indexFile((dg_lastName=l.lname) AND (dg_firstName >= 'D')), dg_firstName))[1].dg_firstName;
    SELF := l;
    END;

OUTPUT(PROJECT(d1, xfm1(LEFT)));
OUTPUT(PROJECT(d1, xfm2(LEFT)));
OUTPUT(PROJECT(d1, xfm3(LEFT)));
OUTPUT(DG_indexFile, { DG_firstname, DG_lastname, DG_prange, filepos} );
