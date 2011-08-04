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


export check := SERVICE
 unsigned8 localHash(const string src) : eclrtl,library='eclrtl',entrypoint='checkLocalHash';
END;

mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
string20            alias;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

sequenceRecord := RECORD
        mainTable.sequence;
        mainTable.surname;
        mainTable.alias;
        mainTable.filepos;
    END;

nameKey := INDEX(mainTable, { surname, forename, filepos }, 'name.idx');
sequenceKey := INDEX(mainTable, sequenceRecord, 'sequence.idx');


peopleRecord := RECORD
integer8        id;
string20        addr;
            END;

//peopleDataset := DATASET([{3000,'London'}], peopleRecord);
peopleDataset := DATASET([{3000,'London'},{3500,'Miami'},{30,'Houndslow'}], peopleRecord);


joinedRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
string20            addr;
unsigned8           pos;
        END;

joinedRecord doJoin(peopleRecord l, sequenceRecord r) := TRANSFORM
    SELF := l;
    SELF.sequence := r.sequence;
    SELF.surname := r.surname;
    SELF.pos := r.filepos;
    SELF.forename := '';
    END;

FilledRecs := join(peopleDataset, sequenceKey, left.id <> 0 AND left.id=right.sequence AND check.localHash(left.addr) = check.localHash(right.alias) AND right.alias <> '',doJoin(left,right), left outer);
output(FilledRecs);
