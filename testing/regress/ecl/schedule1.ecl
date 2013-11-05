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
//nothor
//nothorlcr

sequential(
        output('Line1');
        notify(EVENT('TestScheduleEvent', '<Event><name>Gavin</name><action>check</action><from>' + WORKUNIT + '</from></Event>'));
        output('Line2')
) : when(cron('* * * * *'),count(1));


sequential(
        output('Line3');
        output('Perform ' + EVENTEXTRA('action') + ' for ' + EVENTEXTRA('name'));
        notify(EVENT('TestScheduleResult', '<Event><name>' + EVENTEXTRA('name') + '</name><result>ok</result></Event>'), EVENTEXTRA('from'));
        output('Line4')
) : when('TestScheduleEvent', count(1));

sequential(
        output('Line5');
        output(EVENTEXTRA('name') + ':' + EVENTEXTRA('result'))
) : when('TestScheduleResult', count(1));

