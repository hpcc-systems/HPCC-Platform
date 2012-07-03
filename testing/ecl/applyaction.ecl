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

fibRecord := 
            RECORD
integer         fib1 := 1;
integer         fib2 := 1;
integer         mycounter := 100;
string20        extra := 'xx';
            END;

fibTable := dataset([{9},{8},{7},{6}],fibrecord);

fibRecord makeFibs(fibRecord l, fibRecord r) := TRANSFORM
    SELF.fib1 := r.fib1 + r.fib2;
    SELF.fib2 := r.fib2 + r.fib1 + r.fib2;
    SELF.mycounter := l.mycounter + 1;
    SELF := r;
            END;

doDisplay(string x) := output(dataset([x], { string line}),named('results'),extend);

ret := iterate(fibTable, makeFibs(LEFT, RIGHT));
apply(ret, 
    doDisplay((string)fib1 + ','),
    doDisplay((string)fib2 + ','),
    doDisplay((string)mycounter),
    doDisplay(extra),
    before(doDisplay('Begin Apply....')),
    after(doDisplay('...End Apply'))
    );
