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

rec1 := record
    string20    ReferenceCode := '1';
    string20    BillingCode := '2';
end;

rec2 := record
rec1        child1;
unsigned8   value;
        end;

rec3 := record
unsigned8   value;
rec2        child2;
        end;


ds := dataset('ds', rec3, thor);

rec3.child2.child1 t(rec3 l) := transform
    self := l.child2.child1;
    end;

output(project(ds, t(LEFT)));

