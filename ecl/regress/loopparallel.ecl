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

#option ('targetClusterType', 'roxie');

numInitialRecord := 1000;
filterSuccess := 100;
secondExpand := 2000;
finalNumber := 40;

rec := record
unsigned    id1;
unsigned    id2;
unsigned    score;
        end;





dataset(rec) processLoop(dataset(rec) input, unsigned step) := function

    //create lots of inital records
    dataset(rec) stepOne() := function
        initial := dataset([{0,0,step}], rec);
        return normalize(initial, numInitialRecord, transform(rec, self.id1 := counter; self := []));
    end;

    //reduce the number right down again
    dataset(rec) stepTwo() := function
        return input(id1 % filterSuccess = 1);
    end;

    //now expand them up again
    dataset(rec) stepThree() := function
        return normalize(input, numInitialRecord, transform(rec, self.id1 := left.id1; self.id2 := counter; self.score := left.id1 - counter; ));
    end;

    //filter back down
    dataset(rec) stepFour() := function
        return topn(input, finalNumber, score);
    end;

    return case(step,
        1=>stepOne(),
        2=>stepTwo(),
        3=>stepThree(),
        4=>stepFour()
        );

end;



initial := dataset([], rec);
results := LOOP(initial, 4, processLoop(rows(left), counter), parallel([2,2]));
output(results);

results2 := LOOP(initial, 4, (left.id1 > counter), processLoop(rows(left), counter), parallel([2],2));
output(results2);
