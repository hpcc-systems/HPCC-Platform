/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

