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


r := RECORD
string date;
string line;
decimal8_2 value;
decimal8_2 total;
   END;


ds := PIPE('cmd /C type c:\\temp\\acc.csv', r, csv);
output(ds, { date, ',', line, ',', value, ',', total, '\n'} );
output(TABLE(ds, { line, ',', cnt := COUNT(group), ',', sum_value := sum(group, value), '\n'    }, line)(cnt > 1));

