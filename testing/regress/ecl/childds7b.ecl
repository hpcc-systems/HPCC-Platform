/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

idRec := { unsigned id; };
mainRec := { unsigned seq, dataset(idRec) ids };

idRec createId(unsigned id) := TRANSFORM
    SELF.id := id;
END; 

mainRec createMain(unsigned c, unsigned num) := TRANSFORM
    SELF.seq := c;
    SELF.ids := DATASET(num, createId(c + (COUNTER-1)));
END;

ds := NOFOLD(DATASET(4, createMain(COUNTER, 3)));

boolean assertTrue(boolean x, const varstring msg = 'Condition should have been true') := BEGINC++
    #option pure
    if (!x)
        rtlFail(0, msg);
    return x;
ENDC++;

trueValue := true : stored('trueValue');
falseValue := false : stored('falseValue');

evalFilter(mainRec l) := FUNCTION

    dedupIds := l.ids;
    sortedIds := nofold(sort(l.ids, id));
    bad := sortedIds(assertTrue(id > 10, 'seq > 10'));
    cntGood := COUNT(sortedIds(id != 10000));
    cntBad1 := COUNT(bad);
    
    f := IF(trueValue, dedupIds(id != cntGood), NOFOLD(dedupIds(id != cntBad1)));
    RETURN COUNT(NOFOLD(f)) != 0;
END;

mainRec t(mainRec l) := TRANSFORM,SKIP(NOT evalFilter(l))
    SELF := l;
END;

output(PROJECT(ds, t(LEFT)));
