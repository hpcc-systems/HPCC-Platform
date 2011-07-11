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

test( arg1, arg2='[]' ) := MACRO
    output('in macro test. arg1 = ' + arg1 );
    #uniquename(c)
    %c% := (string)count(arg2);
    output( 'You entered ' + %c% + ' items in arg2');
ENDMACRO;

// "Too many actual parameters supplied to macro test: expected 2, given 3"
test('a', ['spam','eggs'] );

// no problem:
test('b', ['spam'] );

// no problem:
setArg := ['spam','eggs'];
test('c', setArg );


