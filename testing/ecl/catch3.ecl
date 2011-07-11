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
