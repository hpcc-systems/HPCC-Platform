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

//nothor

#option ('newChildQueries', true)

export namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

export namesTable := dataset('x',namesRecord,FLAT);
export namesTable2 := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

two := 2 : stored('two');

export namesRecord thetf(namesRecord l, namesRecord r) := TRANSFORM
 SELF.age := r.age + l.age;
 SELF := l;
END;

export dostuff1(DATASET(namesRecord) ds) := FUNCTION
 sds := SORT(ds, surname);
 ta := ITERATE(sds, thetf(LEFT,RIGHT));
 RETURN ta;
END;

export dostuff2(DATASET(namesRecord) ds) := FUNCTION
 sds := SORT(ds, forename);
 ta := ITERATE(sds, thetf(LEFT,RIGHT));
 RETURN ta;
END;

export dostuff3(DATASET(namesRecord) ds) := FUNCTION
 ddd := DEDUP(ds, forename);
 sds := SORT(ddd, surname, age);
 ta := ITERATE(sds, thetf(LEFT,RIGHT));
 RETURN ta;
END;

doit(DATASET(namesRecord) ds) := FUNCTION
  allit := dostuff1(ds) + dostuff2(ds);
  RETURN allit;
END;


export matches(dataset(namesRecord) ds, unsigned anum = 38) := MODULE

  lhs := dostuff1(ds);
  _rhs := dostuff2(ds);
  rhs := dostuff3(_rhs);
  j := JOIN(lhs, rhs, LEFT.surname = RIGHT.surname);
  export result := IF(COUNT(lhs)<10,j,rhs);
END;


loopres := loop(namesTable2, 10, matches(rows(left),3).result);

OUTPUT(loopres);
OUTPUT(loopres(age<10));

