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




AddNumbers(integer x, integer y) := define function return x+y; end;
AddNumbers2(integer x, integer y) := define x+y;

unknown := 99 : stored('unknown');


AddUnknown(integer x) := define function return x+unknown; end;

one := 1 : stored('one');
two := 2 : stored('two');


func2(unsigned x, unsigned y) := AddUnknown(x * y);

output(AddNumbers(one, two));
output(AddUnknown(one));
output(func2(two, two));


output(AddNumbers(1, 2));
output(AddNumbers2(AddNumbers(3, 4),AddNumbers(5,6)));
output(AddUnknown(1));
output(func2(2, 2));
