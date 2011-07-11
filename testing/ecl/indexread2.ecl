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

//nothor
//UseStandardFiles
//UseIndexes

// Very obscure code to try and ensure that a temporary result is evaluated in a child graph that needs to be serialized to the slave

in := dataset(['Anderson','Taylor','Smith'], { string15 FName });

childResultRecord := record
  STRING25 Lname;
  STRING15 Fname;
  unsigned cnt;
end;

resultRecord := record
  STRING15 Fname;
  dataset(childResultRecord) children;
end;

resultRecord t1(in l) := transform
    deduped := table(dedup(sort(DG_FetchIndex1(LName != l.FName), Lname, FName), LName), { __filepos, LName, FName });
    cntDedup := count(deduped);

    childResultRecord t2(deduped l, DG_FetchIndex1 r, unsigned cnt) := transform
        SELF := l;
        SELF.cnt := cnt;
        END;

    self := l;
    joinedChildren := JOIN(deduped, DG_FetchIndex1, left.LName = right.LName and left.FName = right.FName and right.__filepos != cntDedup, t2(left, right, 0));
    self.children := sort(joinedChildren, Lname, FName);
    end;

p1 := project(in, t1(left));
output(p1);



