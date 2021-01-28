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

//nothor

//This is a obscure test for the event generation and processing capabilities of the workflow engine.

//If two instances of this query are run at the same time it can cause problems.  Both will generate an
//event, but the event from one workunit will be sent to both workunits.  The respoonse in stage 2 is
//coded to be sent back to the workunit that generated it.  This means the 1st workunit receives two result
//events, and the 2nd workunit receives none.

//Ideally there would be a filter on the event that would allow an event from a different workunit to be
//rejected.

//To avoid problems in the regression suite (when roxie and hthor are run at the same time) use an
//event name that is based on the target platform.

myEventName := 'TestScheduleEvent' + __TARGET_PLATFORM__;

sequential(
        output('Line1');
        notify(EVENT(myEventName, '<Event><name>Gavin</name><action>check</action><from>' + WORKUNIT + '</from></Event>'));
        output('Line2')
) : when(cron('* * * * *'),count(1));


sequential(
        output('Line3');
        output('Perform ' + EVENTEXTRA('action') + ' for ' + EVENTEXTRA('name'));
        notify(EVENT('TestScheduleResult', '<Event><name>' + EVENTEXTRA('name') + '</name><result>ok</result></Event>'), EVENTEXTRA('from'));
        output('Line4')
) : when(myEventName, count(1));

sequential(
        output('Line5');
        output(EVENTEXTRA('name') + ':' + EVENTEXTRA('result'))
) : when('TestScheduleResult', count(1));

