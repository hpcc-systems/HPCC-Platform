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

AkaRecord := 
  RECORD
    string20 forename;
    string20 surname;
  END;

outputPersonRecord := 
  RECORD
    unsigned id;
    DATASET(AkaRecord) children;
  END;

inputPersonRecord := 
  RECORD
    unsigned id;
    string20 forename;
    string20 surname;
  END;

/*
inPeople := DATASET (
  [{1,'Gavin','Halliday'},{1,'Gavin', 'Hall'},{1,'Gawain',''},
   {2,'Liz','Halliday'},{2,'Elizabeth', 'Halliday'},
{2,'Elizabeth','MaidenName'},
   {3,'Lorraine','Chapman'},
   {4,'Richard','Chapman'},{4,'John','Doe'}], inputPersonRecord);
*/
inPeople := DATASET ([{1,'Gavin','Halliday'}], inputPersonRecord);

outputPersonRecord makeChildren(outputPersonRecord l, outputPersonRecord r) := 
TRANSFORM
    SELF.id := l.id;
    SELF.children := l.children + 
                     row({r.children[1].forename, r.children[1].surname}, AkaRecord) +
                     row(r.children[3], AkaRecord) +
                     row({r.children(forename<>'')[1].forename, r.children(forename<>'')[1].surname}, AkaRecord) +
                     row({r.children(forename<>'')[3].forename, r.children(forename<>'')[3].surname}, AkaRecord);
  END;

outputPersonRecord makeFatRecord(inputPersonRecord l) := TRANSFORM
    SELF.id := l.id;
    SELF.children := dataset([{ l.forename, l.surname }], AkaRecord);
  END;

fatIn := PROJECT(inPeople, MAKEFATRECORD(LEFT));

r := ROLLUP(fatIn, id, makeChildren(LEFT, RIGHT), local);

output(r);
