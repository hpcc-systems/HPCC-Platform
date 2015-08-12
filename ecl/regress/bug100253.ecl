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

layout := {
    unsigned1 id,
    string s1,
};

layout2 := {
    unsigned1 id,
    string s2,
};

ds1 := dataset([{1,'1'},{2,'2'}], layout);
ds2 := dataset([{1,'one'},{2,'two'}], layout2);

ds1_dist := distribute(ds1, id);
ds2_dist := distribute(ds2, id);

// FAIL causes compiler to lose knowledge of distribution
ds1A := if(exists(ds1_dist(s1 = '1')),
           ds1_dist,
           fail(ds1_dist, 'message'));

// The graph shows that this join is NOT local
ds3 := join(ds1A, ds2_dist,
            left.id = right.id);
output(ds3);

// compiler retains knowledge of distribution
ds1B := if(exists(ds1_dist(s1 = '1')),
           ds1_dist,
           ds1_dist(s1 = '2'));

// The graph shows that this join IS local.
ds4 := join(ds1B, ds2_dist,
            left.id = right.id);
output(ds4);
