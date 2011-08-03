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

