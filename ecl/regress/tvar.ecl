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
data                    padTruncString(unsigned4 len, const string text) : eclrtl,library='eclrtl',entrypoint='rtlPadTruncString';
unsigned4               getPascalLength(const data physical) : eclrtl,library='eclrtl',entrypoint='rtlGetPascalLength';
string                  pascalToString(const data src) : eclrtl,library='eclrtl',entrypoint='rtlPascalToString';
data                    stringToPascal(const string src) : eclrtl,library='eclrtl',entrypoint='rtlStringToPascal';
integer                 bcdToInteger(const data x) : eclrtl,library='eclrtl',entrypoint='rtlBcdToInteger';
data                    integerToBcd(unsigned4 digits, integer value) : eclrtl,library='eclrtl',entrypoint='rtlIntegerToBcd';
data4                   integerToBcdFixed(unsigned4 digits, integer value) : eclrtl,library='eclrtl',entrypoint='rtlIntegerToBcdFixed';
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
export string               store(string logical) := (string)TypeHelper.padTruncString(len, logical);
                END;


//Problems:
//1. Parameter passed to length doesn't know the length - need to pass 0.
//2. Hard/Impossible to code in ECL.
//3. Result of store is copied without clipping etc.

PascalString := TYPE
export integer              physicalLength(string x) := TypeHelper.getPascalLength((data)x);
export string               load(string x)  := (string)TypeHelper.pascalToString((data)x);
export string               store(string x) := (string)TypeHelper.stringToPascal(x);
                END;



IbmDecimal(integer digits) :=
                TYPE
export integer              physicalLength(string x) := (integer) ((digits+1)/2);
export integer              load(string x) := (integer)TypeHelper.bcdToInteger((data)x);
export string               store(integer x) := (string)TypeHelper.integerToBcd(digits, x);
                END;


IbmDecimal8 :=
                TYPE
export integer              load(string4 x) := (integer)TypeHelper.bcdToInteger((data4)x);
export string4              store(integer x) := (string4)TypeHelper.integerToBcdFixed(8, x);
                END;


testRecord := RECORD
integer                     f1len;
varstring100                f1;
//VariableString(self.F1len)    f1;
PascalString                f2;
IbmDecimal(8)               f3;
IbmDecimal8                 f4;
                END;

/*
testRecord := RECORD
PascalString        f1;
PascalString        f2;
                END;
*/

testDataset := DATASET('invar.d00', testRecord, FLAT);

outRecord2 :=   RECORD
varstring1      f1 := (varstring1)map(1=1=>(string)10,(string)20);
varstring1      f2 := (varstring1)map(1=1=>(varstring)10,'x');
integer         f3 := length(map(1=2=>(string)12,(string)55));
                END;

outRecord :=    RECORD
varstring100        f1 := testDataset.f1;
varstring100        f2 := testDataset.f2;
integer             f3 := testDataset.f3;
integer             f4 := testDataset.f4;
                END;

outRecord3 :=   RECORD
varstring100        f1 := testDataset.f1;
VariableString(8)   f2 := testDataset.f2;
integer             f3 := testDataset.f3;
integer             f4 := testDataset.f4;
                END;

output (testDataset,outRecord3,'outvar.d00');

//output (testDataset,{sizeof(testDataset),SIZEOF(f2)},'outvar.d00');
