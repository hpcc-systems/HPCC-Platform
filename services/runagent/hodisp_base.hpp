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




  
