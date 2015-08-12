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

TypeHelper :=
                    SERVICE
unsigned4               getPascalLength(const data physical) : eclrtl,library='eclrtl',entrypoint='rtlGetPascalLength';
string                  pascalToString(const data src) : eclrtl,library='eclrtl',entrypoint='rtlPascalToString';
data                    stringToPascal(const string src) : eclrtl,library='eclrtl',entrypoint='rtlStringToPascal';
                    END;


PascalString := TYPE
export integer              physicalLength(data x) := TypeHelper.getPascalLength(x);
export string               load(data x)    := (string)TypeHelper.pascalToString(x);
export data                 store(string x) := TypeHelper.stringToPascal(x);
                END;



testRecord := RECORD
PascalString                f1;
PascalString                f2;
                END;

testDataset := DATASET('invar.d00', testRecord, FLAT);

outRecord :=    RECORD
varstring100        f1 := testDataset.f1;
varstring100        f2 := testDataset.f2;
                END;


output (testDataset,outRecord,'outvar.d00');

