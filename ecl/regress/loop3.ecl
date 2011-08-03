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
