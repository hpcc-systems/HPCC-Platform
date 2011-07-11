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


#ifndef JTIME_IPP
#define JTIME_IPP

#include "jtime.hpp"

class jlib_decl CScmDateTime : public CInterface, implements IJlibDateTime
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
