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

myRecord := RECORD
    UNSIGNED4 uid;
END;

doAction(set of dataset(myRecord) allInputs) := function
    set of integer emptyIntegerSet := [];
    inputs := RANGE(allInputs, emptyIntegerSet);
    return JOIN(inputs, stepped(left.uid = right.uid), transform(left), sorted(uid));
end;
nullInput := dataset([], myRecord);

s := GRAPH(nullInput, 1, doAction(rowset(left)));

output(s);
