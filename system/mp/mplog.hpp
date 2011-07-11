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

// Documentation for this file can be found at http://mgweb.mg.seisint.com/WebHelp/mp/html/mplog_hpp.html

#ifndef MPLOG_HPP
#define MPLOG_HPP

#ifndef mp_decl
#define mp_decl __declspec(dllimport)
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

// LISTENER HELPER FUNCTIONS

extern mp_decl ILogMsgListener * startLogMsgListener();
extern mp_decl void stopLogMsgListener();
extern mp_decl bool connectLogMsgListenerToChild(INode * childNode);
extern mp_decl bool connectLogMsgListenerToChildOwn(INode * childNode);
extern mp_decl bool disconnectLogMsgListenerFromChild(INode * childNode);
extern mp_decl bool disconnectLogMsgListenerFromChildOwn(INode * childNode);

#endif
