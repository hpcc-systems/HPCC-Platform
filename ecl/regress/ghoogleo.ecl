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

#option ('newQueries', true);

import ghoogle;
import lib_stringLib;

ghoogle.ghoogleDefine()


//Optimizer for queries....



q1 := dataset([
        CmdReadWord(1, 0, 0, 'david'),
        CmdReadWord(2, 0, 0, 'goliath'),
        CmdTermAndTerm(1, 2),
        CmdParagraphFilter(3, 1, 2)
        ]);

q2 := dataset([
        CmdReadWord(1, 0, 0, 'abraham'),
        CmdReadWord(2, 0, 0, 'jesus'),
        CmdTermAndNotTerm(1, 2)
        ]);

q3 := dataset([
        CmdReadWord(1, 0, 0, 'abraham'),
        CmdReadWord(2, 0, 0, 'ruth'),
        CmdTermAndTerm(1, 2),
        CmdAtleastTerm(3, [2], 1)
        ]);

q4 := dataset([
        CmdReadWord(1, 0, 0, 'abraham'),
        CmdTermAndWord(2, 1, 0, 'ruth')
        ]);

searchDefinition := q3;

projectStageNums := project(searchDefinition, transform(searchRecord, SELF.stage := COUNTER; SELF := LEFT));


annotated := annotateQuery(projectStageNums);
optimized := optimizeQuery(annotated);
output(projectStageNums, named('Original'));
output(annotated, named('Annotated'));
output(optimized, named('optimized'));
