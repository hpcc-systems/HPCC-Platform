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


myrec := record
    varstring64 thing1;
    end;

myset := nofold(dataset([{'0123456789'},
                  {'0123456789 '},
                  {'012345678'}],myrec));

filename := 'RTTEMP::foo';

output_set := output(myset,,filename,OVERWRITE);

my_file := dataset(filename,myrec,THOR);

output_value0 := output(myset(  thing1 = '0123456789'));
output_value1 := output(my_file(thing1 = '0123456789'));
output_value2 := output(myset(  thing1 = V'0123456789'));
output_value3 := output(my_file(thing1 = V'0123456789'));

sequential(output_set,output_value0,output_value1,output_value2,output_value3);

// Output from output_value0 does not match output from output_value1.  Please explain.
// This issue occurs in build 471
