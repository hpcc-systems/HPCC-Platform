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
