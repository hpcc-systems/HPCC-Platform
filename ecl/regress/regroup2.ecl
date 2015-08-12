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


inrec := record
unsigned6 did;
    end;

outrec := record(inrec)
string20        name;
unsigned        score;
          end;

ds := dataset('ds', inrec, thor);

dsg := group(ds, row);

c1 := soapcall(dsg, 'remote1', 'service', inrec, transform(left), dataset(outrec));
c2 := soapcall(dsg, 'remote2', 'service', inrec, transform(left), dataset(outrec));
c3 := soapcall(dsg, 'remote3', 'service', inrec, transform(left), dataset(outrec));

//perform 3 soap calls in parallel
combined := regroup(c1, c2, c3);

// choose the best 5 results for each input row
best := topn(combined, 5, -score);

output(best);
