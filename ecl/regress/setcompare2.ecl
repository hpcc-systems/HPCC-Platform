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

the_set := [4264,10];


result1 := IF( the_set != [0,12],
                       'the sets are different',
                       'the sets are the same'
                     );


result2 := IF( the_set = [0,12],
                       'the sets are the same',
                       'the sets are different'
                      );

OUTPUT( result1 );
OUTPUT( result2 );

output('Following are true');
output(the_set = [4264,10]);
output(the_set >= [4264,10]);
output(the_set <= [4264,10]);
output(the_set != [0,12]);
output(the_set > [0,12]);
output(the_set >= [0,12]);

output('Following are false');
output(the_set > [4264,10]);
output(the_set < [4264,10]);
output(the_set != [4264,10]);
output(the_set = [0,12]);
output(the_set < [0,12]);
output(the_set <= [0,12]);
