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


#ifndef JTIME_IPP
#define JTIME_IPP

#include "jtime.hpp"

class jlib_decl CScmDateTime : implements IJlibDateTime, public CInterface
{
public:
    CScmDateTime();
    IMPLEMENT_IINTERFACE

    virtual IStringVal & getString(IStringVal & str) const;
    virtual IStringVal & getDateString(IStringVal & str) const;
    virtual IStringVal & getTimeString(IStringVal & str) const;
    virtual void getDate(unsigned & _year, unsigned & _month, unsigned & _day) const;
    virtual void getTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec, int & localToGmtDelta) const;

    virtual IStringVal & getGmtString(IStringVal & str) const;
    virtual IStringVal & getGmtDateString(IStringVal & str) const;
    virtual IStringVal & getGmtTimeString(IStringVal & str) const;
    virtual void getGmtDate(unsigned & _year, unsigned & _month, unsigned & _day) const;
    virtual void getGmtTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec) const;

    virtual IStringVal & getLocalString(IStringVal & str) const;
    virtual IStringVal & getLocalDateString(IStringVal & str) const;
    virtual IStringVal & getLocalTimeString(IStringVal & str) const;
    virtual void getLocalDate(unsigned & _year, unsigned & _month, unsigned & _day) const;
    virtual void getLocalTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec) const;

    virtual int compare(const IJlibConstDateTime & other) const;

//Modification
    virtual void setString(const char * pstr);
    virtual void setDateTime(unsigned _year, unsigned _month, unsigned _day, unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec, int localToGmtDelta);

    virtual void setGmtString(const char * pstr);
    virtual void setGmtDateString(const char * pstr);
    virtual void setGmtTimeString(const char * pstr);
    virtual void setGmtDate(unsigned _year, unsigned _month, unsigned _day);
    virtual void setGmtTime(unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec);

    virtual void setLocalString(const char * pstr);
    virtual void setLocalDateString(const char * pstr);
    virtual void setLocalTimeString(const char * pstr);
    virtual void setLocalDate(unsigned _year, unsigned _month, unsigned _day);
    virtual void setLocalTime(unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec);

    void setNow();
    void setSimpleGmt(time_t simple);
    void setSimpleLocal(time_t simple); // time_t has no zone, so these differ only in value of utcToLocalDelta stored

protected:
    CDateTime cdt;
    int utcToLocalDelta;                    // delta gmt->local of timezone, for set/get which don't specify gmt or local
};

#endif
