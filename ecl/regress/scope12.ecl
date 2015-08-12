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

#option ('optimizeGraph', false);
#option ('foldAssign', false);
//Deliberatly nasty to tie myself in knots.

countRecord := RECORD
unsigned8           xcount;
                END;

parentRecord :=
                RECORD
unsigned8           id;
boolean                isSpecial;
countRecord            xfirst;
DATASET(countRecord)   children;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


x := parentDataset(parentDataset[2] != parentDataset[1]);
output(x);
