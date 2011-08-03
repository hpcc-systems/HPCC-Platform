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

    export anotherFunction(integer SomeValue) := function

        result := table(ds, {
                        sum(group, someValue),          // 1
                        sum(group, ^.someValue),        // 84
                        sum(group, ^^.someValue),       // 1000
                        sum(group, ^^^.someValue),      // 100
                        0});

        return result;
    end;

    export result := anotherFunction(84);
end;

output(myModule(1000).result);

