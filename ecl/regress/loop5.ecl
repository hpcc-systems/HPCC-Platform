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
initialState := dataset([1], stateRecord);

result := LOOP(initialRows, initialState, 
                actions[right.step].next != '.', 
                processNext(actions[right.step].next, rows(left)),
                project(rows(right), transform(stateRecord, self.step := left.step+1)));


output(result[1].value);
