#ifndef SAUTIL_HPP
#define SAUTIL_HPP

#include "jtime.hpp"

class CSashaSchedule
{
    CCronAtSchedule at;
    CDateTime atnext;
    bool atenabled;
    unsigned mininterval;
    unsigned last;
    CriticalSection sect;
    bool triggered;
    unsigned maxduration;
    unsigned throttle;
public:

    CSashaSchedule()
    {
        mininterval = 0;
        last = 0;
        atenabled = false;
        triggered = false;
        maxduration = 0;
        throttle = 0;
    }

    void init(IPropertyTree *props, unsigned definterval, unsigned initinterval=0)
    {
        CriticalBlock block(sect);
        StringBuffer ats;
        atenabled = false;
        if (props->getProp("@at",ats)) {
            bool all = true;
            for (unsigned i=0;i<ats.length();i++) 
                if (!isspace(ats.charAt(i))&&(ats.charAt(i)!='*'))
                    all = false;
            if (!all) {
                at.set(ats.str());
                CDateTime now;
                now.setNow();
                at.next(now,atnext,false);
                atenabled = true;
            }
        }
        mininterval  = props->getPropInt("@interval",definterval);
        if (initinterval)
            last = msTick()-(60*60*1000*initinterval);
        else
            last = 0;
        maxduration = props->getPropInt("@duration",0)*1000*60*60*1000;
        throttle = props->getPropInt("@throttle",0);
        if (throttle>=100)
            throttle = 99;
    }

    bool ready()
    {
        CriticalBlock block(sect);
        if (triggered) 
            triggered = false;
        else {
            if (!mininterval)
                return false;
            if (last&&((msTick()-last)/(60*60*1000)<mininterval))
                return false;
            if (atenabled) {
                CDateTime atnow;
                atnow.setNow();
                time_t now = atnow.getSimple();
                time_t next = atnext.getSimple();
                if (now<next) {
                    if (next-now>30) 
                        return false;
                    // 
                    Sleep((unsigned)(next-now)*1000);
                }
                at.next(atnow,atnext,true);
            }
        }
        last = msTick();
        if (last==0)
            last++;
        return true;
    }

    void triggerNow()
    {
        CriticalBlock block(sect);
        triggered = true;
    }

    bool checkDurationAndThrottle(unsigned start,unsigned start1, std::atomic<bool> &stopped)
    {
        unsigned tt = 0;
        if (throttle) {
            tt = (throttle*(msTick()-start1))/(100-throttle);
            if (tt>1000)
                PROGLOG("Throttling %d",tt);
        }
        do {
            if (stopped)
                return false;
            if (tt>1000) {
                Sleep(1000);
                tt -= 1000;
            }
            else if (tt) {
                Sleep(tt);
                tt = 0;
            }
            if (maxduration) {
                if (msTick()-start>maxduration) {
                    PROGLOG("Maximum duration (%d) exceeded", maxduration);
                    return false;
                }
            }
        } while (tt);
        return true;
    }
};

extern void operationStarted(const char *msg);
extern void operationFinished(const char *msg);

extern unsigned clustersToGroups(IPropertyTree *envroot,const StringArray &cmplst,StringArray &cnames,StringArray &groups,bool *done);
extern unsigned clustersToGroups(IPropertyTree *envroot,const StringArray &cmplst,StringArray &groups,bool *done);

#endif
