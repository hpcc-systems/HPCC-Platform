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


inrec := RECORD
  unsigned i;
  unsigned n;
END;

outrec := RECORD
  unsigned result;
  unsigned lhs;
  unsigned rhs;
END;

leftDs := DATASET([{1,1},{2,2},{3,3}], inrec);
rightDs := DATASET([{1,10},{2,20},{3,30}], inrec);

c := combine(leftDs, rightDs, TRANSFORM(outRec, SELF.result := LEFT.n + RIGHT.n; SELF.lhs := LEFT.n; SELF.rhs := RIGHT.n), LOCAL);
NOTHOR(OUTPUT(c));
