/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

timeRecord :=
            RECORD
unsigned2 hour;
unsigned2 min;
unsigned2 second;
           END;

TRANSFORM(timeRecord) getTime(unsigned delta) := BEGINC++
struct timeRecord_t
{
    unsigned short hour;
    unsigned short min;
    unsigned short second;
};
#body
    timeRecord_t * result = reinterpret_cast<timeRecord_t *>(__self.getSelf());
    result->hour = 3;
    result->min = 10;
    result->second = 12+delta;
    return (size32_t)sizeof(timeRecord_t);
ENDC++;

output(ROW(getTime(10)));
output(ROW(getTime(20)));
