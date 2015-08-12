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

// test join without transform

r1 := RECORD
  string20 surname := '?????????????';
  string10 forename := '?????????????';
  integer2 age := 25;
END;

r2 := RECORD
  string30 addr := 'Unknown';
  string20 surname;
END;

JoinRecord := RECORD
  string20 surname := '?????????????';
  string10 forename := '?????????????';
  integer2 age := 25;
  string30 addr := 'Unknown';
  // dup:  string20 surname;
END;

namesTable := dataset([{'Smithe','Pru',10}], r1);
addressTable := dataset([{'Hawthorn','10 Slapdash Lane'}], r2);

JoinRecord tranx(r1 l, r2 r) := TRANSFORM
  SELF := l;
  SELF := r;
END;

join_with_trans := join (namesTable, addressTable,
  LEFT.surname[1..10] = RIGHT.surname[1..10] AND
  LEFT.surname[11..16] = RIGHT.surname[11..16] AND
  LEFT.forename[1] <> RIGHT.addr[1],
  tranx(LEFT,RIGHT),LEFT RIGHT OUTER);

output(join_with_trans,,'out.d00');

