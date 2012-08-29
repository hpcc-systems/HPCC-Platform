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


ds := dataset([1], { integer SomeValue; });

SomeValue := 100;

myModule(integer SomeValue2) := module

    export integer SomeValue := 110;

    export anotherFunction(integer SomeValue2) := function

        integer SomeValue := 120;

        result := table(ds, {
                        sum(group, someValue),      // 1
                        sum(group, ^.someValue),    // 120
                        sum(group, ^^.someValue),   // 110
                        sum(group, ^^^.someValue),  // 100
                        sum(group, someValue2),     // 84
                        sum(group, ^.someValue2),   // 84           // careful... first just removes parameter scope
                        sum(group, ^^.someValue2),  // 1000
                        0});

        return result;
    end;

    export result := anotherFunction(84);
end;

output(myModule(1000).result);

