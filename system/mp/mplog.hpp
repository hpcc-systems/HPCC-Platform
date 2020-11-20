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

// Documentation for this file can be found at http://mgweb.mg.seisint.com/WebHelp/mp/html/mplog_hpp.html

#ifndef MPLOG_HPP
#define MPLOG_HPP

#ifndef mp_decl
#define mp_decl DECL_IMPORT
#endif

#include "jlog.hpp"
#include "mpbase.hpp"

// PARENT-SIDE HELPER FUNCTIONS

extern mp_decl bool connectLogMsgManagerToChild(INode * childNode);
extern mp_decl bool connectLogMsgManagerToChildOwn(INode * childNode);
extern mp_decl bool disconnectLogMsgManagerFromChild(INode * childNode);
extern mp_decl bool disconnectLogMsgManagerFromChildOwn(INode * childNode);
extern mp_decl void startLogMsgChildReceiver();

// CHILD-SIDE HELPER FUNCTIONS

extern mp_decl bool connectLogMsgManagerToParent(INode * parentNode);
extern mp_decl bool connectLogMsgManagerToParentOwn(INode * parentNode);
extern mp_decl bool disconnectLogMsgManagerFromParent(INode * parentNode);
extern mp_decl bool disconnectLogMsgManagerFromParentOwn(INode * parentNode);
extern mp_decl void startLogMsgParentReceiver();

extern mp_decl void stopLogMsgReceivers();

/*
  Order of operations on start-up and shut-down:
  
  startMPServer(port);
  startLogMsgChildReceiver();           (if manager will have children)
  startLogMsgParentReceiver();          (if manager will have parents)
  
  ...
  
  stopMPServer();
*/


#endif
