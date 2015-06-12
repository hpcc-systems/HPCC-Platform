/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

//derived from birp3.eclxml
//An example of an index read being used in two different conditional contexts
//If the engines and code generator supported lazy evaluation, the code could
//be generated once and executed on demand.

i := index({unsigned f1}, {string f2}, 'i');

inRec := RECORD
    UNSIGNED id;
END;

outRec := RECORD
    STRING text;
END;

test1 := false : stored('test1');
test2 := 0 : stored('test2');

outRec t(inRec l) := TRANSFORM

    mappedValue := i(f1 = l.id)[1].f2;

    val1 := 'Z' + IF(test1, mappedValue, '');

    val2 := CASE(test2, 1 => 'z',
                     2 => '?',
                     3 => 'x' + mappedValue ,
                     '');

    SELF.text := val1 + val2;
END;

inDs := DATASET([1,2,3], {inRec});

output(PROJECT(inDs, t(LEFT)));
