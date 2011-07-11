/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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

//noroxie
//nothor
//nothorlcr

sequential(
        output('Line1');
        notify(EVENT('TestScheduleEvent', '<Event><name>Gavin</name><action>check</action><from>' + WORKUNIT + '</from></Event>'));
        output('Line2');
        Wait('TestScheduleResult');
        output('Line5');
        output(EVENTEXTRA('name') + ':' + EVENTEXTRA('result'))
);// : when(cron('* * * * *'),count(1));


sequential(
        output('Line3');
        output('Perform ' + EVENTEXTRA('action') + ' for ' + EVENTEXTRA('name'));
        notify(EVENT('TestScheduleResult', '<Event><name>' + EVENTEXTRA('name') + '</name><result>ok</result></Event>'), EVENTEXTRA('from'));
        output('Line4')
) : when('TestScheduleEvent', count(1));

