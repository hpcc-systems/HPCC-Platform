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

//Variation on loop3 except the rhs is a row rather than a dataset.  It makes the code cleaner,
//but still ugly because of count(rows(left))

stepRecord :=
            record
string          next;
            end;

stackRecord :=
            record
unsigned        value;
            end;

stateRecord :=
            record
unsigned        step;
            end;

actions := dataset(['10','20','*','15','10','+','-','.'], stepRecord);


processNext(string next, dataset(stackRecord) in) := FUNCTION

        unsigned tos := count(in);

        RETURN CASE(next,
                       '+'=>in[1..tos-2] + row(transform(stackRecord, self.value := in[tos-1].value+in[tos].value)),
                       '*'=>in[1..tos-2] + row(transform(stackRecord, self.value := in[tos-1].value*in[tos].value)),
                       '-'=>in[1..tos-2] + row(transform(stackRecord, self.value := in[tos-1].value-in[tos].value)),
                       '~'=>in[1..tos-1] + row(transform(stackRecord, self.value := -in[tos].value)),
                       in + row(transform(stackRecord, self.value := (integer)next)));
    END;


initialRows := dataset([], stackRecord);
initialState := row({1}, stateRecord);

result := LOOP(initialRows, initialState,
                actions[right.step].next != '.',
                processNext(actions[right.step].next, rows(left)),
                transform(stateRecord, self.step := right.step+1));


output(result[1].value);
