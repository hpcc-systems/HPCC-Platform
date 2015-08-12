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


idRecord := RECORD
    UNSIGNED f2;
END;

parentRecord := RECORD
   UNSIGNED f1;
   DATASET(idRecord) children;
END;

ds1 := DATASET([
    {1, [1,2,3] },
    {2, [2,3,4] },
    {3, [3] }], parentRecord);
    
ds2 := DATASET([
    {1, [5, 3, 8]},
    {2, [1,6] },
    {3, []}], parentRecord);
    
j1 := JOIN(ds1, ds2, LEFT.f1 = RIGHT.f1 AND EXISTS(JOIN(LEFT.children, RIGHT.children, LEFT.f2 = RIGHT.f2)));

jchild(parentRecord l, parentRecord r) := JOIN(l.children, r.children, LEFT.f2 = RIGHT.f2);
j2 := JOIN(ds1, ds2, LEFT.f1 = RIGHT.f1 AND EXISTS(jchild(LEFT, RIGHT)));


//LEFT is not valid as the second argument to the join until the right argument has been processed!
j3 := JOIN(ds1, ds2, LEFT.f1 = RIGHT.f1 AND EXISTS(JOIN(LEFT.children, LEFT.children(f2 > 10), LEFT.f2 = RIGHT.f2)));

sequential(
    output(j1);
    output(j2);
    output(j3);
);
