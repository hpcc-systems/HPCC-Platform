/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
