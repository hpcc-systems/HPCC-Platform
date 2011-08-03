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
