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

// LISTENER HELPER FUNCTIONS

extern mp_decl ILogMsgListener * startLogMsgListener();
extern mp_decl void stopLogMsgListener();
extern mp_decl bool connectLogMsgListenerToChild(INode * childNode);
extern mp_decl bool connectLogMsgListenerToChildOwn(INode * childNode);
extern mp_decl bool disconnectLogMsgListenerFromChild(INode * childNode);
extern mp_decl bool disconnectLogMsgListenerFromChildOwn(INode * childNode);

#ifdef DEBUG
    #include <iostream>
    #include <sstream>
    #include <cstdlib>
    #include <stdarg.h>
    #include <utility>
    #include <iostream>
    #include <cxxabi.h>
    #include <execinfo.h>
    #define _nl std::endl
    extern thread_local int debug_counter;
    extern thread_local int debug_thread_id;
    extern int global_proc_rank;
//    int _getFuncName(char* &funcName, int &funcLength, int skipDepth);
    void trace_print_func_data(std::stringstream &stream, int first_arg);
    std::string trace_prefix(int skipDepth = 0);
    template<typename First, typename ...Rest>
    void trace_print_func_data(std::stringstream &stream, int first_arg, First && first, Rest && ...rest){
        stream << ((first_arg)?"":",") << first;
        trace_print_func_data(stream, 0,std::forward<Rest>(rest)...);
    }
    template<typename ...Rest>
    void trace_print_func(const char* funcName, Rest && ...rest){
        std::stringstream stream;
        stream << trace_prefix(1) << "["<<funcName << "]"<<": (";
        trace_print_func_data(stream, 1, std::forward<Rest>(rest)...);
        std::cout<<stream.str();
    }
//    template<typename ...Rest>
//    void trace_print_func(Rest && ...rest){
//        std::stringstream stream;
//        stream << trace_prefix() << "(";
//        trace_print_func_data(stream, 1, std::forward<Rest>(rest)...);
//        std::cout<<stream.str();
//    }

    #define _T(x) { std::stringstream stream; stream << trace_prefix() << x << _nl; std::cout << stream.str(); }
    #define _TF(...) trace_print_func(__VA_ARGS__)
#else
    #define _nl
    #define _T(x)
    #define _TF(...)
#endif


#endif
