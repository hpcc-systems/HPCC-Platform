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

childRecord := RECORD
unsigned4           person_id;
string20            per_surname;
dataset(countRecord) counts;
    END;

parentRecord :=
                RECORD
unsigned8           id;
DATASET(childRecord)   children;
DATASET(countRecord)   parentCounts;
                END;

megaRecord :=   RECORD
unsigned8               id;
dataset(parentRecord)   xx;
parentRecord            yy;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


megaRecord ammend(childrecord l) := TRANSFORM
    SELF.id := l.person_id;
    SELF.xx := row(parentDataset);          // MORE: Not really legal, should generate a scope error since parentDataset is a row.
    SELF.yy := [];
            END;

projected := project(parentDataset.children, ammend(LEFT));
deduped := dedup(projected, id);

filtered := parentDataset(count(deduped) > 0);

output(filtered);

boolean cond := true : stored('cond');

megaRecord ammend2(parentRecord p, childrecord l) := TRANSFORM
    SELF.id := l.person_id;
    SELF.xx := [];
    SELF.yy := if(cond, p, p);
            END;

projected2(parentDataset p) := project(p.children, ammend2(p, LEFT));
deduped2 := dedup(projected2(row(parentDataset)), id);

filtered2 := parentDataset(count(deduped2) > 0);

output(filtered2);
