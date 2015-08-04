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

#include "jthread.hpp"
#include "jlib.hpp"

extern bool dispatch_abort;

class c_dispatch : public Thread
{ public:
  CriticalSection cs;
  bool done;
  char * ex1;
    char * id;
  bool is_done();
  c_dispatch() { done=false; ex1=0; id=0;}
  virtual void action()=0;
  virtual int run();
  virtual ~c_dispatch();
};


class m_dispatch
{ unsigned max_disp;
  char * last_error;
  public:
  c_dispatch **d;
  void dispatch(c_dispatch * disp);
  bool all_done(bool no_abort);
  bool all_done_ex(bool no_abort);
  void stop();
  void clear_error();
  m_dispatch(unsigned max_thread);
  ~m_dispatch();
};




  
