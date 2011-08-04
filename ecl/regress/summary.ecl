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


/* Demo hql file using the CSV stuff to summarise my account transactions.... */

accountRecord :=
            RECORD
string10        strDate;
string40        category;
real8           credit;
decimal8_2      debit;
            END;

accountTable := dataset('gavin::account',accountRecord,CSV);

accountRecord cleanCategory(accountRecord l) := TRANSFORM
  SELF.category := l.category[1..LENGTH(TRIM(l.category))-1];
  SELF := l;
  END;

cleaned := project(accountTable, cleanCategory(LEFT));

summaryTable := table(cleaned, { value := SUM(group,credit) - SUM(group,debit), cnt := COUNT(GROUP), category}, category);

sortedSummary := sort(summaryTable, -value);

output(sortedSummary,,'gavin::x::summary',CSV(TERMINATOR('\r\n')));
