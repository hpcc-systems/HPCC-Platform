/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


#ifndef JTIME_HPP
#define JTIME_HPP

#include "jlib.hpp"

#include "jiface.hpp"

interface IJlibConstDateTime : extends IInterface
{
    virtual IStringVal & getString(IStringVal & str) const = 0;
    virtual IStringVal & getDateString(IStringVal & str) const = 0;
    virtual IStringVal & getTimeString(IStringVal & str) const = 0;
    virtual void getDate(unsigned & _year, unsigned & _month, unsigned & _day) const = 0;
    virtual void getTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec, int & localToGmtDelta) const = 0;
    virtual IStringVal & getGmtString(IStringVal & str) const = 0;
    virtual IStringVal & getGmtDateString(IStringVal & str) const = 0;
    virtual IStringVal & getGmtTimeString(IStringVal & str) const = 0;
    virtual void getGmtDate(unsigned & _year, unsigned & _month, unsigned & _day) const = 0;
    virtual void getGmtTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec) const = 0;
    virtual IStringVal & getLocalString(IStringVal & str) const = 0;
    virtual IStringVal & getLocalDateString(IStringVal & str) const = 0;
    virtual IStringVal & getLocalTimeString(IStringVal & str) const = 0;
    virtual void getLocalDate(unsigned & _year, unsigned & _month, unsigned & _day) const = 0;
    virtual void getLocalTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec) const = 0;
    virtual int compare(const IJlibConstDateTime & other) const = 0;
};



interface IJlibDateTime : extends IJlibConstDateTime
{
    virtual void setString(const char * pstr) = 0;
    virtual void setDateTime(unsigned _year, unsigned _month, unsigned _day, unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec, int localToGmtDelta) = 0;
    virtual void setGmtString(const char * pstr) = 0;
    virtual void setGmtDateString(const char * pstr) = 0;
    virtual void setGmtTimeString(const char * pstr) = 0;
    virtual void setGmtDate(unsigned _year, unsigned _month, unsigned _day) = 0;
    virtual void setGmtTime(unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec) = 0;
    virtual void setLocalString(const char * pstr) = 0;
    virtual void setLocalDateString(const char * pstr) = 0;
    virtual void setLocalTimeString(const char * pstr) = 0;
    virtual void setLocalDate(unsigned _year, unsigned _month, unsigned _day) = 0;
    virtual void setLocalTime(unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec) = 0;
};


class jlib_decl CDateTime
{
public:
    CDateTime() { clear(); }
    CDateTime(CDateTime const & other) { set(other); }
    CDateTime(MemoryBuffer & src) { deserialize(src); }

    void deserialize(MemoryBuffer & src);
    void serialize(MemoryBuffer & dst) const;
    hash64_t getHash(hash64_t init) const;

    CDateTime & operator=(CDateTime const & other) { set(other); return *this; }
    void clear();
    void set(CDateTime const & other);
    void set(unsigned year, unsigned month, unsigned day, unsigned hour, unsigned minute, unsigned second, unsigned nano = 0, bool local = false);
    void setDate(unsigned year, unsigned month, unsigned day); // Sets to midnight UTC on date given
    void setTime(unsigned hour, unsigned minute, unsigned second, unsigned nano = 0, bool local = false); // Leaves the date along, set to the time given
    void set(time_t simple);
    void setFromFILETIME(__int64 fileTime);

    void setString(char const * str, char const * * end = NULL, bool local = false); // Sets to date and time given as yyyy-mm-ddThh:mm:ss[.nnnnnnnnn]
    void setDateString(char const * str, char const * * end = NULL); // Sets to midnight UTC on date given as yyyy-mm-dd
    void setTimeString(char const * str, char const * * end = NULL, bool local = false); // Leaves the date alone, sets to the time given as hh:mm:ss[.nnnnnnnnn]
    void setNow();
    void adjustTime(int deltaMins);

    void getDate(unsigned & year, unsigned & month, unsigned & day, bool local = false) const;
    void getTime(unsigned & hour, unsigned & minute, unsigned & second, unsigned & nano, bool local = false) const;
    time_t getSimple() const;
    StringBuffer & getString(StringBuffer & str, bool local = false) const;
    StringBuffer & getDateString(StringBuffer & str, bool local = false) const;
    StringBuffer & getTimeString(StringBuffer & str, bool local = false) const;

    bool isNull() const;
    bool equals(CDateTime const & cdt, bool compareNanosec = true) const;
    int compare(CDateTime const & cdt, bool compareNanosec = true) const;
    int compareDate(CDateTime const & cdt) const;

    //NB operator forms ignore nanoseconds, use named methods for other behaviour
    bool operator==(CDateTime const & cdt) const { return equals(cdt, false); }
    bool operator!=(CDateTime const & cdt) const { return !equals(cdt, false); }
    bool operator<(CDateTime const & cdt) const { return (compare(cdt, false) < 0); }
    bool operator<=(CDateTime const & cdt) const { return (compare(cdt, false) <= 0); }
    bool operator>(CDateTime const & cdt) const { return (compare(cdt, false) > 0); }
    bool operator>=(CDateTime const & cdt) const { return (compare(cdt, false) >= 0); }

    int queryUtcToLocalDelta() const;

private:
    void setFromUtcTm(struct tm const & ts);
    void getToUtcTm(struct tm & ts) const;

private:
    short utc_year;
    byte utc_mon;
    byte utc_mday;
    byte utc_hour;
    byte utc_min;
    byte utc_sec;
    unsigned nanosec;
};

class jlib_decl CCronAtSchedule
{
    // all arrays are in order
    UnsignedArray minutes;  // 0-59   
    UnsignedArray hours;    // 0-23   
    UnsignedArray days;     // 1-31   
    UnsignedArray months;   // 1-12   
    UnsignedArray dows;     // 0-6    0=sunday, 6=saturday

    bool match(UnsignedArray &a,unsigned v,unsigned &next);
    bool matchDay(unsigned yr, unsigned mon, unsigned dy, unsigned &nextdy);

public:
    const char *set(const char *spec);  // returns past final param
    void next(const CDateTime &fromdt, CDateTime &nextdt, bool greater=false);
};

interface ICronTable : public IInterface
{
    interface Transaction : public IInterface
    {
        //for safety, will rollback on destruction, so need explicit commit()
        virtual void add(char const * spec, char const * tag) = 0;
        virtual unsigned remove(char const * tag) = 0;
        virtual unsigned unremove(char const * tag) = 0;
        virtual void removeall() = 0;
        virtual void commit() = 0;
        virtual void rollback() = 0;
    };

    virtual void add(char const * spec, char const * tag) = 0;
    virtual unsigned remove(char const * tag) = 0;
    virtual void removeall() = 0;
    virtual Transaction * getTransactionFrame() = 0; //can only have one frame at a time, subsequent attempts return NULL
    virtual unsigned next(CDateTime const & fromdt, CDateTime & nextdt, StringArray & tags) = 0;
};

extern jlib_decl ICronTable * createCronTable();

class CTimeMon
{
public:
    unsigned t;
    unsigned timeout;

    CTimeMon() { t = msTick(); }

    CTimeMon(unsigned _timeout) { reset(_timeout); }
    void reset(unsigned _timeout) { timeout=_timeout; t = msTick(); }

    unsigned elapsed()
    {
        return msTick()-t;
    }
    bool timedout(unsigned *remaining=NULL)
    {
        if ((int)timeout<0) {       
            if (remaining)
                *remaining = (unsigned)-1;
            return false;
        }
        unsigned e = elapsed(); 
        if (e>=timeout) {
            if (remaining)
                *remaining = 0;
            return true;
        }
        if (remaining)
            *remaining = timeout-e;
        return false;
    }
};

extern jlib_decl IJlibDateTime * createDateTime();
extern jlib_decl IJlibDateTime * createDateTimeNow();
extern jlib_decl void serializeDateTime(MemoryBuffer & tgt);
extern jlib_decl IJlibDateTime * deserializeDateTime(MemoryBuffer & src);

extern jlib_decl void testTiming();
extern jlib_decl void testCronTable();

extern jlib_decl IJlibDateTime * createDateTimeFromLocal(time_t lt);
extern jlib_decl time_t createLocalFromDateTime(IJlibDateTime const * dt);

extern jlib_decl void timetToIDateTime(CDateTime * target, time_t time);
extern jlib_decl time_t timetFromIDateTime(const CDateTime * source);


#ifndef __GNUC__ //in GNU C, all these functions are defined --- elsewhere, we'll have to fake them
extern jlib_decl struct tm * gmtime_r(time_t const * simple, struct tm * utc);
extern jlib_decl struct tm * localtime_r(time_t const * simple, struct tm * local);
extern jlib_decl time_t timegm(struct tm * utc);
extern jlib_decl time_t timelocal(struct tm * local);
#endif

#endif

