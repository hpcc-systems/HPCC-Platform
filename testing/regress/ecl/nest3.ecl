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

inPeople := DATASET (
  [{1,'Gavin','Halliday'},{1,'Gavin', 'Hall'},{1,'Gawain',''},
   {2,'Liz','Halliday'},{2,'Elizabeth', 'Halliday'},{2,'Elizabeth','MaidenName'},
   {3,'Lorraine','Chapman'},
   {4,'Richard','Chapman'},{4,'John','Doe'}], inputPersonRecord);

outputPersonRecord makeChildren(outputPersonRecord l, outputPersonRecord r) := TRANSFORM
    SELF.id := l.id;
    SELF.children := l.children + row({r.children[1].forename, r.children[1].surname}, AkaRecord);
  END;

outputPersonRecord makeFatRecord(inputPersonRecord l) := TRANSFORM
    SELF.id := l.id;
    SELF.children := dataset([{ l.forename, l.surname }], AkaRecord);
  END;

fatIn := PROJECT(inPeople, MAKEFATRECORD(LEFT));

r := ROLLUP(fatIn, id, makeChildren(LEFT, RIGHT), local);

output(r);
