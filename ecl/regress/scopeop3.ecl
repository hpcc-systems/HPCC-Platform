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


ds := dataset([1], { integer SomeValue; });

SomeValue := 100;

myModule(integer SomeValue2) := module

    export integer SomeValue := 110;

    export anotherFunction(integer SomeValue2) := SomeValue2 * ^.SomeValue2;            //  this is an example that doesn't work.  Should choose a better parameter name...
end;

output(myModule(1000).anotherFunction(20));

