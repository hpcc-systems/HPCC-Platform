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



assert([1] = [1] + []);
assert([1, 2] = [1] + [2]);
assert([1, 2] = [[1], [2]]);
assert([1, 2, 3] = [[1], [[2], 3]]);

integer stored_9 := 9 : stored('stored_9');
set of integer stored_fib := [1,2,3,5,8] : stored('stored_fib');

assert([stored_9] + [4] + stored_fib = [stored_9, 4, stored_fib]);
assert([stored_9] + ([4] + stored_fib) = [stored_9, 4, stored_fib]);
