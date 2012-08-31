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



