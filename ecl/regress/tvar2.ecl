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
unsigned4               getPascalLength(const data physical) : eclrtl,library='eclrtl',entrypoint='rtlGetPascalLength';
string                  pascalToString(const data src) : eclrtl,library='eclrtl',entrypoint='rtlPascalToString';
data                    stringToPascal(const string src) : eclrtl,library='eclrtl',entrypoint='rtlStringToPascal';
integer                 bcdToInteger(const data x) : eclrtl,library='eclrtl',entrypoint='rtlBcdToInteger';
string                  integerToBcd(unsigned4 digits, integer value) : eclrtl,library='eclrtl',entrypoint='rtlIntegerToBcd';
data4                   integerToBcdFixed(unsigned4 digits, integer value) : eclrtl,library='eclrtl',entrypoint='rtlIntegerToBcdFixed';
                    END;

//Variable length record processing

//Problems:
//1. Length isn't associated with the datatype, so cannot really do an assignment
//2. Implies value returned from store is truncated by length
//3. length from loadLength() is passed to load()
//4. parameter not needed on loadLength


VariableString(integer len) :=
                TYPE
export integer              physicalLength(string physical) := (integer)len;
export string               load(string physical) := (string)physical;
export string               store(string logical) := TypeHelper.padTruncString(len, logical);
                END;

PascalString := TYPE
export integer              physicalLength(data x) := TypeHelper.getPascalLength((data)x);
export string               load(data x)    := (string)TypeHelper.pascalToString((data)x);
export data                 store(string x) := TypeHelper.stringToPascal(x);
                END;



//Problems:


testRecord := RECORD
string10      f1;
                END;

testDataset := DATASET('invar.d00', testRecord, FLAT);


outRecord3 :=   RECORD
PascalString    f1 := testDataset.f1;
                END;

output (testDataset,outRecord3,'outvar.d00');
