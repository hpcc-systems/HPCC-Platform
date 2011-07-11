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


ds := dataset([1], { integer SomeValue; });

SomeValue := 100;

myModule(integer SomeValue) := module

    export integer SomeValue := 110;

    export anotherFunction(integer SomeValue) := function
        
        integer SomeValue := 120;

        result := table(ds, { 
                        sum(group, someValue),
                        sum(group, ^.someValue),
                        sum(group, ^^.someValue),
                        sum(group, ^^^.someValue),
                        0});

        return result;
    end;

    export result := anotherFunction(84);
end;

output(myModule(1000).result);

