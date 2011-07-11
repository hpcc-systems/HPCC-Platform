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

#include "hrpc.hpp"
#include "hodisp_base.hpp"
#include "jsocket.hpp"
#include "jkeyboard.hpp"


bool dispatch_abort=false;



void m_dispatch::dispatch(c_dispatch * disp)
{ while (1)
  {
    unsigned i=0;
    for (i=0; i<max_disp; i++)
    {  if (d[i]&&d[i]->is_done()) 
       { if (d[i]->ex1)
         { char * t=last_error;
           last_error=alloccat(d[i]->ex1,"\n",t,0);
                     //traceft("m_dispatch error added %s\n",d[i]->ex1);
           if (t) free(t);
         }
         if (!d[i]->join(2000))
            traceft("m_dispatch join failed");
         d[i]->Release();
         d[i]=0;
       }
    }
    for (i=0; i<max_disp; i++)
    {  if (!d[i])
       { d[i]=disp;
         d[i]->start();
         return;
       }
    }
    MilliSleep(1000);
  }
}

bool m_dispatch::all_done(bool no_abort)
{ try
  { bool all_d=false;
    traceft("all_done start\n");
    while (!all_d)
    { all_d=true;
      if (!no_abort&&dispatch_abort) return false;
      for (unsigned i=0; i<max_disp; i++)
      { if (d[i])
        { if (d[i]->is_done()) 
          { if (d[i]->ex1)
            { char * t=last_error;
              last_error=alloccat(d[i]->ex1,"\n",t,0);
              if (t) free(t);
            }
            if (!d[i]->join(2000))
                traceft("all_done join failed\n");

            d[i]->Release();  
            d[i]=0; 
          } else
          { all_d=false;
            //traceft("nd %i ",i);
          }
        }
      } 
      //traceft("Sleep %i\n",all_d);
      MilliSleep(1000);
    }
    traceft("all_done end\n");
  } catch (...)
  { traceft("unknown exception in all_done\n");
  }
  return true;
}


bool user_continue() {
  keyboard kb;
  bool res=false;
  printf("\n ignore exception y/n\n");
    for (int i=10; i>0; i--) {
      printf(" %i \r",i);
        sleep(1);
        if(kb.kbhit()) {
            char t = kb.getch();
            if (t=='y'||t=='Y') { 
             res=true;
             break;
            } 
            else if (t=='n'||t=='N') {
             res=false;
             break;
            }
        }
    }
    printf("\n");
  return res;
}

bool m_dispatch::all_done_ex(bool no_abort)
{ bool res=all_done(no_abort);
  if (last_error&&!user_continue()) throw MakeStringException(1, last_error);
  return res;
}

void m_dispatch::stop(){};

m_dispatch::m_dispatch(unsigned max_d)
{ max_disp=max_d;
  unsigned si=sizeof(c_dispatch *)*max_d;
  d=(c_dispatch**)malloc(si);
  memset(d,0,si); 
  last_error=0;
}

m_dispatch::~m_dispatch()
{ free(d);
  if (last_error) free(last_error);
  
}

void m_dispatch::clear_error()
{ if (last_error) free(last_error);
  last_error=0;
}

bool c_dispatch::is_done()
{ bool res; 
  cs.enter(); 
  res=done; 
  cs.leave();  
  return res;
};



c_dispatch::~c_dispatch() 
{ if (ex1) free(ex1); 
};


int c_dispatch::run()
{ try
  { action();
  } catch (char *s )
  { ex1=strdup(s);
  } catch(IHRPC_Exception *e) 
  { StringBuffer s;
    ex1=alloccat(e->errorMessage(s).toCharArray()," ",id," ",0);
    e->Release();
  } catch(IJSOCK_Exception *e) 
  { StringBuffer s;
    ex1=alloccat(e->errorMessage(s).toCharArray()," ",id," ",0);
    e->Release();
  } 
  catch (IException *e)
  { StringBuffer s;
    ex1=alloccat(e->errorMessage(s).toCharArray()," ",id," ",0);
    e->Release();
  }
  catch (...)
  { traceft("unknown exception in c_dispatch::run\n");
  }
  if (ex1) traceft("%s exception %s\n",id?id:"",ex1);
  //traceft("c_dispatch run done\n");
  cs.enter();
  done=true;
  cs.leave();
  return 0;
}

