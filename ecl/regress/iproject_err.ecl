/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

myModule := MODULE

    EXPORT value1 := 100;
    EXPORT value2 := 200;
    EXPORT value3(unsigned i) := 300;   // invalid - no default value supplied
    EXPORT value4() := 400;

END;

myInterface := INTERFACE

    EXPORT value1 := 0;
    EXPORT value2() := 0;
    EXPORT value3 := 0;
    EXPORT value4() := 0;

END;



display(myInterface x) := FUNCTION
    RETURN
        ORDERED(
            OUTPUT(x.value1);
            OUTPUT(x.value2());
            OUTPUT(x.value3);
            OUTPUT(x.value4());
        );
END;


mappedModule := PROJECT(myModule, myInterface);
display(mappedModule);