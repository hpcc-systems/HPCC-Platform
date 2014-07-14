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

//Named so that it is executed early on, leaving time for other queries to overlap when running parallel 

//no point testing this lots of times - testing the workflow engine once in hthor is enough
//nothor
//noroxie
//nothorlcr

ds := DATASET(['one','two','three'], { string text; });

p := ds : PERSIST('p1');

msg := DATASET(['Success'],{string text; });

//There should be 3 entries - once for each time it succeeds.

calcedP := p : success(output(msg,NAMED('log'),extend)); 

output(calcedP,NAMED('result'),extend) : when(cron('* * * * *'),count(3));
