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

#ifndef _THGRAPHMANAGER_HPP
#define _THGRAPHMANAGER_HPP

class CSDSServerStatus;
interface IException;
CSDSServerStatus &queryServerStatus();
CSDSServerStatus &openThorServerStatus();
void closeThorServerStatus();
void thorMain(ILogMsgHandler *logHandler, const char *workunit, const char *graphName);

enum ThorExitCodes { TEC_Clean, TEC_CtrlC, TEC_Idle, TEC_Watchdog, TEC_SlaveInit, TEC_Swap, TEC_DaliDown };

void abortThor(IException *e, unsigned errCode, bool abortCurrentJob=true);
void setExitCode(int code);
int queryExitCode();

#endif
