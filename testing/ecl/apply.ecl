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

//noroxie

import lib_logging;
import lib_workunitservices;

#option('slaveDaliClient', true);

export Display := 
    SERVICE
        unsigned4 echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlDisplay';
    END;

fibRecord := 
            RECORD
integer         fib1 := 1;
integer         fib2 := 1;
integer         mycounter := 100;
string20        extra := 'xx';
            END;

fibTable := dataset([{9},{8},{7},{6}],fibrecord);

fibRecord makeFibs(fibRecord l, fibRecord r) := TRANSFORM
    SELF.fib1 := l.fib1 + l.fib2;
    SELF.fib2 := l.fib2 + l.fib1 + l.fib2;
    SELF.mycounter := l.mycounter + 1;
    SELF := r;
            END;

//doDisplay(string x) := EVALUATE(display.echo(x));
doDisplay(string x) := logging.addWorkunitInformation(x);

ret := iterate(fibTable, makeFibs(LEFT, RIGHT));

SEQUENTIAL(
 apply(ret, 
    doDisplay((string)fib1 + ','),
    doDisplay((string)fib2 + ','),
    doDisplay((string)mycounter),
    doDisplay(extra),
    before(doDisplay('Begin Apply....')),
    after(doDisplay('...End Apply'))
    ),
 OUTPUT(workunitservices.WorkunitMessages(WORKUNIT), { message })
);
