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
string                  padTruncString(unsigned4 len, const string text) : eclrtl,library='eclrtl',entrypoint='rtlPadTruncString';
unsigned4               getPascalLength(const string physical) : eclrtl,library='eclrtl',entrypoint='rtlGetPascalLength';
string                  pascalToString(const string src) : eclrtl,library='eclrtl',entrypoint='rtlPascalToString';
string                  stringToPascal(const string src) : eclrtl,library='eclrtl',entrypoint='rtlStringToPascal';
integer                 bcdToInteger(const string x) : eclrtl,library='eclrtl',entrypoint='rtlBcdToInteger';
string                  integerToBcd(unsigned4 digits, integer value) : eclrtl,library='eclrtl',entrypoint='rtlIntegerToBcd';
string4                 integerToBcdFixed(unsigned4 digits, integer value) : eclrtl,library='eclrtl',entrypoint='rtlIntegerToBcdFixed';
                    END;

//Variable length record processing

//Problems:
//1. Length isn't associated with the datatype, so cannot really do an assignment
//2. Implies value returned from store is truncated by length
//3. length from physicalLength() is passed to load()
//4. parameter not needed on physicalLength


VariableString(integer len) :=
                TYPE
export integer              physicalLength(string physical) := (integer)len;
export string               load(string physical) := physical;
export string               store(string logical) := TypeHelper.padTruncString(len, logical);
                END;


//Problems:
//1. Parameter passed to length doesn't know the length - need to pass 0.
//2. Hard/Impossible to code in ECL.
//3. Result of store is copied without clipping etc.

PascalString := TYPE
export integer              physicalLength(string x) := TypeHelper.getPascalLength(x);
export string               load(string x)  := (string)TypeHelper.pascalToString(x);
export string               store(string x) := (string)TypeHelper.stringToPascal(x);
                END;



IbmDecimal(integer digits) :=
                TYPE
export integer              physicalLength(string x) := (integer) ((digits+1)/2);
export integer              load(string x) := (integer)TypeHelper.bcdToInteger(x);
export string               store(integer x) := TypeHelper.integerToBcd(digits, x);
                END;


IbmDecimal8 :=
                TYPE
export integer              load(string4 x) := (integer)TypeHelper.bcdToInteger(x);
export string4              store(integer x) := TypeHelper.integerToBcdFixed(8, x);
                END;

