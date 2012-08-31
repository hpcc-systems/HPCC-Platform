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

import std.system.thorlib;

namesRecord := RECORD
string3     id;
string10    surname ;
string10    forename;
string2     nl;
  END;

natRecord := RECORD
string3     id;
string10    nationality ;
string2     nl;
  END;

nameAndNatRecord := RECORD
string3     id := 1;
string10    surname := '' ;
string10    forename := '';
string10    nationality := '' ;
string2     nl := '';
  END;

names := DATASET('in.d00', namesRecord, FLAT);
nationalities := DATASET('nat.d00', natRecord, FLAT);

namesRecord JoinTransform (namesRecord l, namesRecord r)
:=
    TRANSFORM
                   self.forename := thorlib.logicalToPhysical('testc.d00', true);
                   self := r;
    END;

snames := ITERATE(names, JoinTransform(LEFT,RIGHT));

output(snames, , 'out.d00');
