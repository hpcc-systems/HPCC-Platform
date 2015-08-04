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

//BUF: #13104 - binding parameter causes type of a transform record to change.

export BestByDate(unsigned6 myDid) := FUNCTION
rec := { string20 surname, unsigned6 did; };
ds10 := dataset('ds', rec, thor);
f := ds10(did = myDid);
t2 := table(f, { surname, did, myDid });

typeof(t2) t(t2 l) := transform
      SELF.surname := trim(l.surname) + 'x';
      SELF := l;
  END;

return project(t2, t(LEFT));

END;

output(BestByDate(000561683165));

