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

#option ('newChildQueries', true);

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
  export result := IF(COUNT(lhs)>10,lhs,rhs);

//  export result := JOIN(lhs, rhs, LEFT.surname = RIGHT.surname);
END;


output(loop(namesTable2, 10, matches(rows(left),3).result));


// OUTPUT(IF(COUNT(dostuff1(namesTable2))>20,dostuff1(namesTable2),dostuff2(namesTable2)));

