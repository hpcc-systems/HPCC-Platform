/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
