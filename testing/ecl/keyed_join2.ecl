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

lhs := DATASET([{['Anderson', 'Taylor']}], {SET OF STRING25 Lnames{MAXLENGTH(100)}});

{STRING15 Fname, string15 LName} xfm(DG_FetchIndex1 r) := TRANSFORM
    SELF.Fname := r.Fname;
    SELF.Lname := r.Lname;
END;

j1 := JOIN(lhs, DG_FetchIndex1, RIGHT.Lname IN LEFT.Lnames, xfm(RIGHT));

#if (useLocal)
OUTPUT(SORT(j1, lname, fname), {fname});
#else
OUTPUT(j1, {fname});
#end
