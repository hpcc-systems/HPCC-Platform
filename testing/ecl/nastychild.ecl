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

//Deliberately try and create a race condition between the two graph inputs, so that if the child queries are
//accidentally shared you'll get complaints about results being used before they are defined
//nothor
//nothorlcr

export rtl := SERVICE
 unsigned4 sleep(unsigned4 dxelay) : eclrtl,library='eclrtl',entrypoint='rtlSleep';
END;

numIterations := 100;
blockSize := 8;

rec := { unsigned id, string name{maxlength(50)} };

masterrec := { unsigned id, dataset(rec) children{maxcount(500)} };


getInput(unsigned graphCounter) := function

    delta := [2,4];
    ds0 := dataset([1],{ unsigned id });
    ds1 := normalize(ds0, NumIterations, transform({unsigned id}, self.id := counter));

    createChildren(unsigned id, unsigned parentCounter) := function
        timeCounter := graphCounter * (blockSize DIV 2) + parentCounter;
        sleepTime := IF(timeCounter % blockSize >= blockSize DIV 2, 1, 5);
        ds := dataset([id], { unsigned xcount; });
        norm := normalize(nofold(ds), (((left.xcount-1)%4)+1) * 10, transform(rec, self.id := counter; self.name := (string)counter; ));
        p := project(nofold(norm), transform(rec, self.name := left.name + '!' + (string)sleepTime + IF(rtl.Sleep(sleepTime)=1, 'x', 'y') + (string)(count(norm) + id); self := left));
        return p;
    end;

    ds2 := nofold(project(ds1, transform(masterrec, self.id := left.id + graphCounter * numIterations; self.children := createChildren(left.id, counter))));
    
    return ds2;
end;


initialResults := dataset([], masterrec);

processStage(unsigned c, set of dataset(masterrec) _in) := 
    CASE(c, 1=>getInput(c),
            2=>getInput(c),
            3=>_in[1]+_in[2]);

results := graph(initialResults, 3, processStage(counter, rowset(left)), parallel);

output(sort(results,id));
