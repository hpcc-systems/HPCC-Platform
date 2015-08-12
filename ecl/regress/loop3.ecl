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

//Exploration of using a two parameter LOOP statement with semantics
//while(cond) left' = f(left,right) right' = g(left,right)
//conclusion - it would probably be ugly because of the count(left) kind of semantics.
//also loop4 is preferred because less messy.

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

actions := dataset(['10','20','*',15,10,'+','-','.'], stepRecord);


stateRow processNext(stateRow in, dataset(stackRecord) in) := FUNCTION

        string next := actions[in.step];
        unsigned tos = count(in.stack);

        RETURN CASE(next,
                       '+'=>in[1..tos-2] + row(stackRecord, self.value := in[tos-1].value+in[tos].value);
                       '*'=>in[1..tos-2] + row(stackRecord, self.value := in[tos-1].value*in[tos].value);
                       '-'=>in[1..tos-2] + row(stackRecord, self.value := in[tos-1].value-in[tos].value);
                       '~'=>in[1..tos-1] + row(stackRecord, self.value := -in[tos].value);
                       in + row(stackRecord, self.value = (integer)next));
    END;


initialRows := dataset([], stackRecord);
initialState := dataset([{1}], stateRecord);

result := LOOP(initialRows, initialState,
                actions[rows(right)[1].state] != '.',
                processNext(actions[rows(right)[1].step], rows(left)),
                project(rows(right),transform(stateRecord, self.step = right.step+1)))


output(result[1].value);
