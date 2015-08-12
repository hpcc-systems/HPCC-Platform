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


//No virtual records
rec := record
unsigned    value;
        end;

optionsClass := module,virtual
export firstItem    := 1;
export lastItem     := 2;
export values       := dataset([1,2,3],rec);
export result       := nofold(values)[firstItem..lastItem];
                end;

derivedClass := module(optionsClass)
export lastItem     := 3;
                end;


myDerivedClass := module(derivedClass)
export dataset values           := dataset([100,101,120], rec);
                end;



displayValues(optionsClass options) :=
    output(options.values[options.firstItem..options.lastItem]);

//Check correct values picked up
displayValues(optionsClass);
displayValues(derivedClass);
displayValues(myDerivedClass);

//Requires virtual binding to work correctly
output(optionsClass.result);
output(derivedClass.result);
output(myDerivedClass.result);

//Check compatible...
outputValues(derivedClass options) := displayValues(options);


outputValues(myDerivedClass);
