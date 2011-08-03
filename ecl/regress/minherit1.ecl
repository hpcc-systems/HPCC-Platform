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
