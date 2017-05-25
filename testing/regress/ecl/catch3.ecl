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

//nohthor
//nothor

MyRec := RECORD
    STRING50 Value1;
    unsigned Value2;
END;

ds := DATASET([
           {'C',1},
           {'C',2},
           {'C',3},
           {'C',4},
           {'C',5},
     {'X',1},
     {'A',1}],MyRec);

MyRec FailTransform := transform
  self.value1 := FAILMESSAGE[1..17];
  self.value2 := FAILCODE
END;

limited := LIMIT(ds, 2);

recovered := CATCH(limited, SKIP);

recovered2 := CATCH(limited, onfail(FailTransform));

recovered3 := CATCH(CATCH(limited, FAIL(1, 'Failed, dude')), onfail(FailTransform));

OUTPUT(recovered);
OUTPUT(recovered2);
OUTPUT(recovered3);

// What about exceptions in child queries

MyRec childXform(MyRec l, unsigned lim) := TRANSFORM
    SELF.value2 := (SORT(LIMIT(ds(value1=l.value1), lim), value2))[1].value2;
    SELF := l;
    END;


failingChild := project(ds,childxform(LEFT, 2));
passingChild := project(ds,childxform(LEFT, 20));

output(CATCH(failingChild, onfail(FailTransform)));
output(CATCH(passingChild, onfail(FailTransform)));

// What about exceptions in dependencies?

Value2Max := MAX(limited, value2);

output(CATCH(ds(value2 = value2Max), onfail(FailTransform)));
