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
    unsigned u1 := 0,
    string1 s1 := 'a',
};

// without NOFOLD, works as expected
ds := nofold(dataset([{0},{0},{0},{0}], layout));

ds_dist := distribute(ds, random());
ds_cnt_project := project(ds_dist,
                          transform(layout,
                            self.u1 := counter));

// dataset is not in expected order b/c DISTRIBUTE occurs after PROJECT in the graph
output(ds_cnt_project);

