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

recL := record
  VARSTRING name {MAXLENGTH(11)};
  unsigned  val;
END;

recR := record
  VARSTRING name {MAXLENGTH(12)};
  unsigned2     val;
END;

recO := record
  STRING10  lname;
  STRING11  rname;
END;


dsL := DATASET([
  {'BABAK',1},
  {'BABY',2},
  {'BARBARB',3},
  {'BEBO',4}
], recL);

dsR := DATASET([
  {'BABARA',5},
  {'BABATUNDE',6},
  {'BABETTE',7},
  {'BABS',8},
  {'BARBAR',9},
  {'BARBARBA',10},
  {'BARBARBO',11},
  {'BARBER',12},
  {'BETTY',13}
], recR);

recO T(recL l,recR r) := TRANSFORM
  self.lname := l.name;
  self.rname := r.name;
END;


J1 := JOIN(dsL,dsR,left.name[1..*]=right.name[3..*] and left.val<right.val,T(LEFT,RIGHT), ATMOST(left.name[1..*]=right.name[3..*],3), LOCAL);

output(J1);


