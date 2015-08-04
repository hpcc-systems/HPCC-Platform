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

inf := dataset('xxx::yyy', {unsigned8 did, string9 ssn}, THOR);

Layout_PairMatch := record
  unsigned1  pflag;
  end;

sm_rec := record
    inf.ssn;
  end;

me_use := table(inf,{ inf.ssn; });   // works

Layout_PairMatch tra(me_use ll, me_use r) := transform
  self.pflag := 1;
  end;

mu1 := me_use(ssn<>'');

j := join(mu1,mu1, left.ssn=right.ssn, tra(left,right));

output(j,,'TEMP:idRules_0');