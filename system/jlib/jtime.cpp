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


#include "platform.h"

#include "jmisc.hpp"
#include "jtime.ipp"
#include "jexcept.hpp"
#include "jerror.hpp"

#ifndef __GNUC__

#if _WIN32 //this appears to be the best way of controlling timezone information used by mktime

void setUtcTZ()
{
    _wputenv(L"TZ=UTC0UTC");
    _tzset();
}

wchar_t localTZ[80];

void setLocalTZ()
{
    _wputenv(localTZ);
    _tzset();
}

MODULE_INIT(INIT_PRIORITY_JTIME)
{
    TIME_ZONE_INFORMATION localInfo;
    GetTimeZoneInformation(&localInfo);
    wchar_t * finger = localTZ;
    int biasHour = localInfo.Bias / 60;
    int biasMin = abs(localInfo.Bias) % 60;
    wcsncpy(finger, L"TZ=", 3);
    finger += 3;
    wcsncpy(finger, localInfo.StandardName, 3);
    finger += 3;
    if(biasMin)
        finger += swprintf(finger, L"%d:%02d", biasHour, biasMin);
    else
        finger += swprintf(finger, L"%d", biasHour);
    wcsncpy(finger, localInfo.DaylightName, 3);
    finger += 3;
    *finger = 0;
    setLocalTZ();
    return true;
}

#else //not GNU or WIN32, assume standard behaviour (untested)

StringBuffer localTZ;

void setUtcTZ()
{
    localTZ.clear().append(getenv("TZ"));
    setenv("TZ", "UTC");
}

void setLocalTZ()
{
    setenv("TZ", localTZ.str());
}

#endif //_WIN32

Mutex timeMutex;

struct tm * gmtime_r(time_t const * simple, struct tm * utc)
{
    synchronized procedure(timeMutex);
    struct tm * ts = gmtime(simple);
    if(!ts) return NULL;
    memcpy(utc, ts, sizeof(struct tm));
    return utc;
}

struct tm * localtime_r(time_t const * simple, struct tm * local)
{
    synchronized procedure(timeMutex);
    struct tm * ts = localtime(simple);
    if(!ts) return NULL;
    memcpy(local, ts, sizeof(struct tm));
    return local;
}

time_t timegm(struct tm * utc)
{
    synchronized procedure(timeMutex);
    setUtcTZ(); //YUCK, but this is apparently standard practice where timegm is not available
    time_t simple = mktime(utc);
    setLocalTZ();
    return simple;
}

time_t timelocal(struct tm * local)
{
    synchronized procedure(timeMutex);
    return mktime(local); //mktime is more common (but less descriptive) name for timelocal
}

#endif //__GNUC__

static unsigned readDigits(char const * & str, unsigned numDigits)
{
    unsigned ret = 0;
    while(numDigits--)
    {
        char c = *str++;
        if(!isdigit(c))
            throwError1(JLIBERR_BadlyFormedDateTime, str);
        ret  = ret * 10 + (c - '0');
    }
    return ret;
}

static void checkChar(char const * & str, char required)
{
    char c = *str++;
    if(c != required)
        throwError1(JLIBERR_BadlyFormedDateTime, str);
}

void CDateTime::setFromUtcTm(struct tm const & ts)
{
    utc_year = ts.tm_year;
    utc_mon = ts.tm_mon;
    utc_mday = ts.tm_mday;
    utc_hour = ts.tm_hour;
    utc_min = ts.tm_min;
    utc_sec = ts.tm_sec;
}

void CDateTime::getToUtcTm(struct tm & ts) const
{
    ts.tm_year = utc_year;
    ts.tm_mon = utc_mon;
    ts.tm_mday = utc_mday;
    ts.tm_hour = utc_hour;
    ts.tm_min = utc_min;
    ts.tm_sec = utc_sec;
    ts.tm_isdst = 0;
}

void CDateTime::deserialize(MemoryBuffer &src)
{
    src.read(utc_year).read(utc_mon).read(utc_mday).read(utc_hour).read(utc_min).read(utc_sec).read(nanosec);
    utc_year -= 1900;
    utc_mon -= 1;
}

void CDateTime::serialize(MemoryBuffer &dst) const
{
    short year = 1900+utc_year;
    byte mon = 1+utc_mon;
    dst.append(year).append(mon).append(utc_mday).append(utc_hour).append(utc_min).append(utc_sec).append(nanosec);
}

// See http://www.isthe.com/chongo/tech/comp/fnv/index.html and eclrtl.cpp

#define FNV_64_PRIME I64C(0x100000001b3U)
#define APPLY_FNV64(hval, next) { hval *= FNV_64_PRIME; hval ^= next; }

hash64_t CDateTime::getHash(hash64_t hash) const
{
    APPLY_FNV64(hash, utc_sec);
    APPLY_FNV64(hash, utc_min);
    APPLY_FNV64(hash, utc_hour);
    APPLY_FNV64(hash, utc_mday);
    APPLY_FNV64(hash, utc_mon);
    APPLY_FNV64(hash, utc_year);
    APPLY_FNV64(hash, nanosec);
    return hash;
}

void CDateTime::clear()
{
    utc_sec = 0;
    utc_min = 0;
    utc_hour = 0;
    utc_mday = 1;
    utc_mon = 0;
    utc_year = 0;
    nanosec = 0;
}

void CDateTime::set(CDateTime const & other)
{
    utc_sec = other.utc_sec;
    utc_min = other.utc_min;
    utc_hour = other.utc_hour;
    utc_mday = other.utc_mday;
    utc_mon = other.utc_mon;
    utc_year = other.utc_year;
    nanosec = other.nanosec;
}

void CDateTime::set(time_t simple)
{
    struct tm ts;
    gmtime_r(&simple, &ts);
    setFromUtcTm(ts);
}

void CDateTime::setString(char const * str, char const * * end, bool local)
{
    if (!str||!*str) {
        clear();
        return;
    }
    unsigned year = readDigits(str, 4);
    checkChar(str, '-');
    unsigned month = readDigits(str, 2);
    checkChar(str, '-');
    unsigned day = readDigits(str, 2);
    checkChar(str, 'T');
    unsigned hour = readDigits(str, 2);
    checkChar(str, ':');
    unsigned minute = readDigits(str, 2);
    checkChar(str, ':');
    unsigned sec = readDigits(str, 2);
    unsigned nano = 0;
    if(*str == '.')
    {
        unsigned digits;
        for(digits = 0; digits < 9; digits++)
        {
            char c = *++str;
            if(!isdigit(c)) break;
            nano = nano * 10 + (c - '0');
        }
        while(digits++<9)
            nano *= 10;
    }
    if(end) *end = str;
    set(year, month, day, hour, minute, sec, nano, local);
}

void CDateTime::setDateString(char const * str, char const * * end)
{
    unsigned year = readDigits(str, 4);
    checkChar(str, '-');
    unsigned month = readDigits(str, 2);
    checkChar(str, '-');
    unsigned day = readDigits(str, 2);
    if(end) *end = str;
    set(year, month, day, 0, 0, 0, 0, false);
}

void CDateTime::setTimeString(char const * str, char const * * end, bool local)
{
    unsigned year;
    unsigned month;
    unsigned day;
    getDate(year, month, day, false);
    unsigned hour = readDigits(str, 2);
    checkChar(str, ':');
    unsigned minute = readDigits(str, 2);
    checkChar(str, ':');
    unsigned sec = readDigits(str, 2);
    unsigned nano = 0;
    if(*str == '.')
    {
        unsigned digits;
        for(digits = 0; digits < 9; digits++)
        {
            char c = *++str;
            if(!isdigit(c)) break;
            nano = nano * 10 + (c - '0');
        }
        while(digits++<9)
            nano *= 10;
    }
    if(end) *end = str;
    set(year, month, day, hour, minute, sec, nano, local);
}

void CDateTime::set(unsigned year, unsigned month, unsigned day, unsigned hour, unsigned minute, unsigned second, unsigned nano, bool local)
{
    if(local)
    {
        struct tm local;
        local.tm_year = year - 1900;
        local.tm_mon = month - 1;
        local.tm_mday = day;
        local.tm_hour = hour;
        local.tm_min = minute;
        local.tm_sec = second;
        local.tm_isdst = -1;
        time_t simple = timelocal(&local);
        set(simple);
    }
    else
    {
        utc_year = year - 1900;
        utc_mon = month - 1;
        utc_mday = day;
        utc_hour = hour;
        utc_min = minute;
        utc_sec = second;
    }
    nanosec = nano;
}

void CDateTime::setDate(unsigned year, unsigned month, unsigned day)
{
    set(year, month, day, 0, 0, 0, 0, false);
}

void CDateTime::setTime(unsigned hour, unsigned minute, unsigned second, unsigned nano, bool local)
{
    unsigned year;
    unsigned month;
    unsigned day;
    getDate(year, month, day, false);
    set(year, month, day, hour, minute, second, nano, local);
}

//FILETIME is a large integer that represents the number of 100 nanosecond
//intervals since January 1, 1601 (UTC), also known as a FILETIME value.
void CDateTime::setFromFILETIME(__int64 fileTime)
{
    __int64 secsAfterADEpoch = fileTime / 10000000;
    __int64 AD2Unix = ((1970-1601) * 365 - 3 + ((1970-1601)/4) ) * (__int64)86400;
    set(secsAfterADEpoch - AD2Unix);
}

void CDateTime::setNow()
{
    time_t simple;
    time(&simple);
    set(simple);
}

void CDateTime::adjustTime(int deltaMins)
{
    time_t simple = getSimple();
    simple += deltaMins * 60;
    set(simple);
}

void CDateTime::getDate(unsigned & year, unsigned & month, unsigned & day, bool local) const
{
    if(local)
    {
        time_t simple = getSimple();
        struct tm local;
        localtime_r(&simple, &local);
        year = local.tm_year + 1900;
        month = local.tm_mon + 1;
        day = local.tm_mday;
    }
    else
    {
        year = utc_year + 1900;
        month = utc_mon + 1;
        day = utc_mday;
    }
}

void CDateTime::getTime(unsigned & hour, unsigned & minute, unsigned & second, unsigned & nano, bool local) const
{
    if(local)
    {
        time_t simple = getSimple();
        struct tm local;
        localtime_r(&simple, &local);
        hour = local.tm_hour;
        minute = local.tm_min;
        second = local.tm_sec;
    }
    else
    {
        hour = utc_hour;
        minute = utc_min;
        second = utc_sec;
    }
    nano = nanosec;
}

time_t CDateTime::getSimple() const
{
    struct tm ts;
    getToUtcTm(ts);
    return timegm(&ts);
}

StringBuffer & CDateTime::getString(StringBuffer & str, bool local) const
{
    if(isNull()) return str;
    char buff[64]; // allow extra for invalid dates
    char * finger = buff;
    if(local)
    {
        time_t simple = getSimple();
        struct tm local;
        localtime_r(&simple, &local);
        finger += sprintf(finger, "%04d-%02d-%02dT%02d:%02d:%02d", local.tm_year+1900, local.tm_mon+1, local.tm_mday, local.tm_hour, local.tm_min, local.tm_sec);
    }
    else
        finger += sprintf(finger, "%04d-%02d-%02dT%02d:%02d:%02d", utc_year+1900, utc_mon+1, utc_mday, utc_hour, utc_min, utc_sec);
    if(nanosec) finger += sprintf(finger, ".%06u", nanosec/1000);
    return str.append(buff);
}

StringBuffer & CDateTime::getDateString(StringBuffer & str, bool local) const
{
    if(isNull()) return str;
    char buff[64]; // allow extra for invalid dates
    if(local)
    {
        time_t simple = getSimple();
        struct tm local;
        localtime_r(&simple, &local);
        sprintf(buff, "%04d-%02d-%02d", local.tm_year+1900, local.tm_mon+1, local.tm_mday);
    }
    else
        sprintf(buff, "%04d-%02d-%02d", utc_year+1900, utc_mon+1, utc_mday);
    return str.append(buff);
}

StringBuffer & CDateTime::getTimeString(StringBuffer & str, bool local) const
{
    if(isNull()) return str;
    char buff[64];  // allow extra for invalid dates
    char * finger = buff;
    if(local)
    {
        time_t simple = getSimple();
        struct tm local;
        localtime_r(&simple, &local);
        finger += sprintf(finger, "%02d:%02d:%02d", local.tm_hour, local.tm_min, local.tm_sec);
    }
    else
        finger += sprintf(finger, "%02d:%02d:%02d", utc_hour, utc_min, utc_sec);
    if(nanosec) finger += sprintf(finger, ".%06u", nanosec/1000);
    return str.append(buff);
}

bool CDateTime::isNull() const
{
    if(utc_year || utc_mon || (utc_mday-1) || utc_hour || utc_min || utc_sec || nanosec)
        return false;
    return true;
}

bool CDateTime::equals(CDateTime const & cdt, bool compareNanosec) const
{
    time_t thisSimple = getSimple();
    time_t thatSimple = cdt.getSimple();
    return ((thisSimple == thatSimple) && ((compareNanosec) ? (nanosec == cdt.nanosec) : true));
}

int CDateTime::compare(CDateTime const & cdt, bool compareNanosec) const
{
    time_t thisSimple = getSimple();
    time_t thatSimple = cdt.getSimple();
    if(thisSimple != thatSimple) return ((thisSimple > thatSimple) ? +1 : -1);
    if(compareNanosec && (nanosec != cdt.nanosec)) return ((nanosec > cdt.nanosec) ? +1 : -1);
    return 0;
}

int CDateTime::compareDate(CDateTime const & cdt) const
{
    if(utc_year != cdt.utc_year) return ((utc_year > cdt.utc_year) ? +1 : -1);
    if(utc_mon != cdt.utc_mon) return ((utc_mon > cdt.utc_mon) ? +1 : -1);
    if(utc_mday != cdt.utc_mday) return ((utc_mday > cdt.utc_mday) ? +1 : -1);
    return 0;
}

int CDateTime::queryUtcToLocalDelta() const
{
    struct tm ts;
    getToUtcTm(ts);
    time_t correct = timegm(&ts);
    time_t shifted = timelocal(&ts);
    return ((int)(correct - shifted))/60;
}

//---------------------------------------------------------------------------

CScmDateTime::CScmDateTime()
{
    utcToLocalDelta = 0;
}

IStringVal & CScmDateTime::getString(IStringVal & str) const
{
    StringBuffer temp;
    CDateTime local(cdt);
    local.adjustTime(utcToLocalDelta);
    local.getString(temp);
    if (utcToLocalDelta == 0)
        temp.append("Z");
    else
    {
        int value = utcToLocalDelta;
        if (value > 0)
            temp.append('+');
        else
        {
            value = -value;
            temp.append('-');
        }
        temp.appendf("%02d:%02d", value / 60, value % 60);
    }
    str.set(temp.str());
    return str;
}

int CScmDateTime::compare(const IJlibConstDateTime & other) const
{
    unsigned year, month, day, hour, min, sec, nanosec;
    other.getGmtDate(year, month, day);
    other.getGmtTime(hour, min, sec, nanosec);
    CDateTime otherCDT;
    otherCDT.set(year, month, day, hour, min, sec, nanosec);
    return cdt.compare(otherCDT);
}

IStringVal & CScmDateTime::getDateString(IStringVal & str) const
{
    StringBuffer temp;
    CDateTime local(cdt);
    local.adjustTime(utcToLocalDelta);
    local.getDateString(temp);
    str.set(temp.str());
    return str;
}

IStringVal & CScmDateTime::getTimeString(IStringVal & str) const
{
    StringBuffer temp;
    CDateTime local(cdt);
    local.adjustTime(utcToLocalDelta);
    local.getTimeString(temp);
    str.set(temp.str());
    return str;
}

void CScmDateTime::getDate(unsigned & _year, unsigned & _month, unsigned & _day) const
{
    CDateTime local(cdt);
    local.adjustTime(utcToLocalDelta);
    local.getDate(_year, _month, _day);
}

void CScmDateTime::getTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec, int & localToGmtDelta) const
{
    CDateTime local(cdt);
    local.adjustTime(utcToLocalDelta);
    local.getTime(_hour, _min, _sec, _nanosec);
    localToGmtDelta = -utcToLocalDelta;
}


IStringVal & CScmDateTime::getGmtString(IStringVal & str) const
{
    StringBuffer temp;
    str.set(cdt.getString(temp).str());
    return str;
}

IStringVal & CScmDateTime::getGmtDateString(IStringVal & str) const
{
    StringBuffer temp;
    str.set(cdt.getDateString(temp).str());
    return str;
}

IStringVal & CScmDateTime::getGmtTimeString(IStringVal & str) const
{
    StringBuffer temp;
    str.set(cdt.getTimeString(temp).str());
    return str;
}

void CScmDateTime::getGmtDate(unsigned & _year, unsigned & _month, unsigned & _day) const
{
    cdt.getDate(_year, _month, _day);
}

void CScmDateTime::getGmtTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec) const
{
    cdt.getTime(_hour, _min, _sec, _nanosec);
}

IStringVal & CScmDateTime::getLocalString(IStringVal & str) const
{
    StringBuffer temp;
    str.set(cdt.getString(temp, true).str());
    return str;
}

IStringVal & CScmDateTime::getLocalDateString(IStringVal & str) const
{
    StringBuffer temp;
    str.set(cdt.getDateString(temp, true).str());
    return str;
}

IStringVal & CScmDateTime::getLocalTimeString(IStringVal & str) const
{
    StringBuffer temp;
    str.set(cdt.getTimeString(temp, true).str());
    return str;
}

void CScmDateTime::getLocalDate(unsigned & _year, unsigned & _month, unsigned & _day) const
{
    cdt.getDate(_year, _month, _day, true);
}

void CScmDateTime::getLocalTime(unsigned & _hour, unsigned & _min, unsigned & _sec, unsigned & _nanosec) const
{
    cdt.getTime(_hour, _min, _sec, _nanosec, true);
}


void CScmDateTime::setString(const char * pstr)
{
    char const * end;
    cdt.setString(pstr, &end, false);
    char sign = *end;
    if (toupper(sign) == 'Z')
    {
        utcToLocalDelta = 0;
        end++;
    }
    else if ((sign == '-') || (sign == '+'))
    {
        end++;
        int delta = readDigits(end, 2);
        if (*end++ != ':')
            throwError1(JLIBERR_BadlyFormedDateTime, pstr);
        delta = delta * 60 + readDigits(end, 2);
        if (sign == '-')
            delta = -delta;
        utcToLocalDelta = delta;
        cdt.adjustTime(-delta);
    }
    if (*end != 0)
        throwError1(JLIBERR_BadlyFormedDateTime, pstr);
}


void CScmDateTime::setDateTime(unsigned _year, unsigned _month, unsigned _day, unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec, int localToGmtDelta)
{
    utcToLocalDelta = -localToGmtDelta;
    cdt.set(_year, _month, _day, _hour, _min, _sec, _nanosec);
    cdt.adjustTime(localToGmtDelta);
}

void CScmDateTime::setGmtString(const char * pstr)
{
    utcToLocalDelta = 0;
    cdt.setString(pstr);
}

void CScmDateTime::setGmtDateString(const char * pstr)
{
    utcToLocalDelta = 0;
    cdt.setDateString(pstr);
}

void CScmDateTime::setGmtTimeString(const char * pstr)
{
    utcToLocalDelta = 0;
    cdt.setTimeString(pstr);
}

void CScmDateTime::setGmtDate(unsigned _year, unsigned _month, unsigned _day)
{
    utcToLocalDelta = 0;
    cdt.setDate(_year, _month, _day);
}

void CScmDateTime::setGmtTime(unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec)
{
    utcToLocalDelta = 0;
    cdt.setTime(_hour, _min, _sec, _nanosec);
}

void CScmDateTime::setLocalString(const char * pstr)
{
    cdt.setString(pstr, NULL, true);
    utcToLocalDelta = cdt.queryUtcToLocalDelta();
}

void CScmDateTime::setLocalDateString(const char * pstr)
{
    cdt.setDateString(pstr, NULL);
    utcToLocalDelta = cdt.queryUtcToLocalDelta();
}

void CScmDateTime::setLocalTimeString(const char * pstr)
{
    cdt.setTimeString(pstr, NULL, true);
    utcToLocalDelta = cdt.queryUtcToLocalDelta();
}

void CScmDateTime::setLocalDate(unsigned _year, unsigned _month, unsigned _day)
{
    cdt.setDate(_year, _month, _day);
    utcToLocalDelta = cdt.queryUtcToLocalDelta();
}

void CScmDateTime::setLocalTime(unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec)
{
    cdt.setTime(_hour, _min, _sec, _nanosec, true);
    utcToLocalDelta = cdt.queryUtcToLocalDelta();
}

void CScmDateTime::setNow()
{
    cdt.setNow();
}

void CScmDateTime::setSimpleGmt(time_t simple)
{
    cdt.set(simple);
    utcToLocalDelta = 0;
}

void CScmDateTime::setSimpleLocal(time_t simple)
{
    cdt.set(simple);
    utcToLocalDelta = cdt.queryUtcToLocalDelta();
}

IJlibDateTime * createDateTime()
{
    return new CScmDateTime;
}


IJlibDateTime * createDateTimeNow()
{
    CScmDateTime * dt = new CScmDateTime;
    dt->setNow();
    return dt;
}

static bool isLeapYear (unsigned yr)
{
   return (yr%400==0)||((yr%4==0)&&(yr%100!=0));
}


static unsigned daysInMonth(unsigned y, unsigned m)
{
    unsigned int days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if ((m==2)&&isLeapYear(y))
        return 29;
    if(m < 1 || m > 12)
        throw MakeStringException(0, "month should between 1 and 12");
    return days[m - 1];
}

static unsigned dayOfWeek(unsigned y, unsigned m, unsigned d)
{
    if (m < 3) {
        m += 13;
        y--;
    }
    else  
        m++;
    return (d+26*m/10+y+y/4-y/100+y/400+6)%7;
}

static unsigned decodeDay(const char *day)
{ // not this uses sun as 0
    if (strlen(day)>=3) {
        const char *p="sunmontuewedthufrisat";
        for (unsigned i=0;i<7;i++) {
            if (memicmp(p,day,3)==0)
                return i;
            p += 3;
        }
    }
    return NotFound;
}


static unsigned decodeMon(const char *mon)
{
    if (strlen(mon)>=3) {
        const char *p="janfebmaraprmayjunjulaugsepoctnovdec";
        for (unsigned i=1;i<=12;i++) {
            if (memicmp(p,mon,3)==0)
                return i;
            p += 3;
        }
    }
    return NotFound;
}



static void carryTimeInc(unsigned &yr, unsigned &mon, unsigned &dy, unsigned &hr, unsigned &min)
{
    hr += (min/60);
    min %= 60;
    dy += (hr/24);
    hr %= 24;
    if (mon==0)
        mon++;
    if (dy==0)
        dy++;
    if (mon<=12) {
        if (dy<=daysInMonth(yr,mon))
            return;
    }
    loop {
        yr += (mon-1)/12;
        mon = (mon-1)%12+1;
        unsigned dinm = daysInMonth(yr,mon);
        if (dy<=dinm)
            break;
        mon++;
        dy -= dinm;
    }
}

inline const char *getnum(const char *s, unsigned &n,unsigned first,unsigned last)
{
    // assumes first char is digit
    n = *s-'0';
    s++;
    while (isdigit(*s)) {
        n = n*10+(*s-'0');
        s++;
    }
    if (n<first)
        n = first;
    else if (n>last)
        n = (n-first)%(last-first+1)+first; // bit over the top but does sunday as 7
    return s;
}

inline const char *getnumorname(const char *s, unsigned &n,unsigned first,unsigned last)
{
    n = NotFound;
    if (isdigit(*s))
        return getnum(s,n,first,last);
    if (last==6)  // dow 
        n = decodeDay(s);
    else if (last==12)  // mon 
        n = decodeMon(s);
    if (n!=NotFound)
        s+=3;
    return s;
}       

static int cmpval(unsigned const *a,unsigned const *b)
{
    if (*a>*b) return 1;
    if (*a<*b) return -1;
    return 0;
}

static const char *parseCronItem(const char *s,UnsignedArray &a,unsigned first,unsigned last)
{
    a.kill();
    unsigned n;
    bool added;
    if (s) {
        if (*s=='*') {
            s++;
            if (*s=='/') {
                s++;
                if (isdigit(*s)) {
                    s = getnum(s,n,first,last);
                    if (n)
                        for (unsigned i=first;i<=last;i+=n)
                            a.bAdd(i,cmpval,added);
                }
            }
        }
        else {
            loop {
                s = getnumorname(s,n,first,last);
                if (n!=NotFound) {
                    if (*s=='-') { // range
                        s++;
                        unsigned n2;
                        s = getnumorname(s,n2,first,last);
                        if (n2==NotFound) 
                            n2 = last;
                        unsigned inc = 1;
                        if (*s=='/') { // inc
                            s++;
                            if (isdigit(*s)) 
                                s = getnum(s,inc,1,last);
                        }
                        if (n <= n2)
                        {
                            for (; n<=n2; n+=inc)
                                a.bAdd(n,cmpval,added);
                        }
                        else
                        {
                            unsigned idx;
                            for (idx=n; idx<=last; idx+=inc)
                                a.bAdd(idx,cmpval,added);
                            for (idx-=(last-first+1); idx<=n2; idx+=inc)
                                a.bAdd(idx,cmpval,added);
                        }
                    }
                    else
                        a.bAdd(n,cmpval,added);
                }
                else if (*s==',') 
                    s++;
                else
                    break;
            }
        }
        while (isspace(*s))
            s++;                
    }
    return s;
}


const char *CCronAtSchedule::set(const char *spec)
{
    if (spec)
        while (isspace(*spec))
            spec++;
    spec = parseCronItem(spec,minutes,0,59);
    spec = parseCronItem(spec,hours,0,23);
    spec = parseCronItem(spec,days,1,31);
    spec = parseCronItem(spec,months,1,12);
    return parseCronItem(spec,dows,0,6);
}

bool CCronAtSchedule::match(UnsignedArray &a,unsigned v,unsigned &next)
{
    if (a.ordinality()==0) {
        next = v;
        return true;
    }
    ForEachItemIn(i,a) {
        unsigned n = a.item(i);
        if (n>=v) {
            next = n;
            return true;
        }
    }
    return false;
}

bool CCronAtSchedule::matchDay(unsigned yr, unsigned mon, unsigned dy, unsigned &nextdy)
{
    // first find matching day
    unsigned d=0;
    unsigned dinm=daysInMonth(yr,mon);
    if (days.ordinality()==0) {
        if (dows.ordinality()==0) {
            nextdy = dy;
            return true;
        }
    }
    else {
        ForEachItemIn(i,days) {
            unsigned d1 = days.item(i);
            if (d1>dinm)
                break;
            if (d1>=dy) {
                d = d1;
                break;
            }
        }
    }
    if (dows.ordinality()!=0) {
        unsigned dow = dayOfWeek(yr,mon,dy);
        ForEachItemIn(i,dows) {
            unsigned dw = dows.item(i);
            unsigned d2 = dy+(dw+7-dow)%7;
            if ((d2<=dinm)&&((d==0)||(d2<d)))
                d = d2;
        }
    }
    if (d!=0) {
        nextdy = d;
        return true;
    }
    return false;
}



void CCronAtSchedule::next(const CDateTime &fromdt, CDateTime &nextdt, bool greater)
{
    unsigned hr; 
    unsigned min; 
    unsigned sec;
    unsigned nano;
    fromdt.getTime(hr, min, sec, nano);
    sec = 0;
    nano = 0;
    unsigned yr; 
    unsigned mon;
    unsigned dy;
    fromdt.getDate(yr, mon, dy);
    if (greater)
        min++;
    for (unsigned i=0;i<60*24*12;i++) {                 // this is just a catch to stop impossible dates infinite looping
        carryTimeInc(yr, mon, dy, hr, min);
        unsigned nextmon;
        if (!match(months,mon,nextmon)) {
            yr++;
            hr = 0;
            min = 0;
            dy = 1;
            mon = months.item(0);
            continue;
        }
        if (nextmon!=mon) {
            mon = nextmon;
            dy = 1;
            hr = 0;
            min = 0;
        }
        unsigned nextdy;
        if (!matchDay(yr,mon,dy,nextdy)) {
            hr = 0;
            min = 0;
            mon++;
            dy = 1;
            continue;
        }
        if (nextdy!=dy) {
            hr = 0;
            min = 0;
            dy = nextdy;
        }
        unsigned nexthr;
        if (!match(hours,hr,nexthr)) {
            min = 0;
            dy++;
            hr = 0;
            continue;
        }
        if (nexthr!=hr) {
            min = 0;
            hr = nexthr;
        }
        unsigned nextmin;
        if (!match(minutes,min,nextmin)) {
            hr++;
            min = 0;
            continue;
        }
        min = nextmin;
        break;
    }
    nextdt.set(yr,mon,dy,hr,min,sec,nano);      
}

class CronTableItem
{
public:
    CronTableItem(CronTableItem * _prev, CronTableItem * _next, char const * spec, char const * _tag, bool inframe) : prev(_prev), next(_next), tag(_tag), markDelete(false), markNew(inframe)
    {
        char const * specend = cron.set(spec);
        if (*specend)
            throw MakeStringException(0, "Bad cron spec %s", spec);
    }
    CronTableItem * prev;
    CronTableItem * next;
    StringAttr tag;
    CCronAtSchedule cron;
    bool markDelete;
    bool markNew;
};

class CCronTable : public CInterface, implements ICronTable
{
private:
    class XFrame : public CInterface, implements ICronTable::Transaction
    {
    public:
        XFrame(CCronTable * _owner) : owner(_owner) {}
        ~XFrame() { CriticalBlock block(owner->crit); dorollback(); owner->hasframe = false; }
        IMPLEMENT_IINTERFACE;

        virtual void add(char const * spec, char const * tag)
        {
            CriticalBlock block(owner->crit);
            owner->doadd(spec, tag, true);
        }

        virtual unsigned remove(char const * tag)
        {
            CriticalBlock block(owner->crit);
            CronTableItem * finger = owner->head;
            unsigned count = 0;
            while(finger)
            {
                if(strcmp(finger->tag.get(), tag)==0)
                {
                    finger->markDelete = true;
                    count++;
                }
                finger = finger->next;
            }
            return count;
        }

        virtual unsigned unremove(char const * tag)
        {
            CriticalBlock block(owner->crit);
            CronTableItem * finger = owner->head;
            unsigned count = 0;
            while(finger)
            {
                if(strcmp(finger->tag.get(), tag)==0)
                {
                    finger->markDelete = false;
                    count++;
                }
                finger = finger->next;
            }
            return count;
        }

        virtual void removeall()
        {
            CriticalBlock block(owner->crit);
            CronTableItem * finger = owner->head;
            while(finger)
            {
                finger->markDelete = true;
                finger = finger->next;
            }
        }

        virtual void commit()
        {
            CriticalBlock block(owner->crit);
            CronTableItem * finger = owner->head;
            while(finger)
            {
                CronTableItem * next = finger->next;
                if(finger->markDelete)
                    owner->doremove(finger);
                else
                    finger->markNew = false;
                finger = next;
            }
        }

        virtual void rollback() { CriticalBlock block(owner->crit); dorollback(); }

    private:
        void dorollback()
        {
            CronTableItem * finger = owner->head;
            while(finger)
            {
                CronTableItem * next = finger->next;
                if(finger->markNew)
                    owner->doremove(finger);
                else
                    finger->markDelete = false;
                finger = next;
            }
        }

    private:
        CCronTable * owner;
    };

public:
    CCronTable() : head(NULL), tail(NULL), hasframe(false) {}
    ~CCronTable()
    {
        kill();
    }
    IMPLEMENT_IINTERFACE;

    virtual void add(char const * spec, char const * tag)
    {
        CriticalBlock block(crit);
        doadd(spec, tag, false);
    }

    virtual unsigned remove(char const * tag)
    {
        CriticalBlock block(crit);
        CronTableItem * finger = head;
        unsigned count = 0;
        while(finger)
        {
            if(hasframe)
            {
                while(finger && finger->markNew) finger = finger->next;
                if(!finger) break;
            }
            CronTableItem * next = finger->next;
            if(strcmp(finger->tag.get(), tag)==0)
            {
                doremove(finger);
                count++;
            }
            finger = next;
        }
        return count;
    }

    virtual void removeall()
    {
        CriticalBlock block(crit);
        if(hasframe)
        {
            CronTableItem * finger = head;
            while(finger)
            {
                CronTableItem * next = finger->next;
                if(!finger->markNew)
                    doremove(finger);
                finger = next;
            }
        }
        else
        {
            kill();
        }
    }

    virtual unsigned next(CDateTime const & fromdt, CDateTime & nextdt, StringArray & tags)
    {
        CriticalBlock block(crit);
        tags.kill();
        if(!head) return 0;
        head->cron.next(fromdt, nextdt, true);
        tags.append(head->tag.get());
        CronTableItem * finger = head->next;
        CDateTime fingerdt;
        while(finger)
        {
            if(hasframe)
            {
                while(finger && finger->markNew) finger = finger->next;
                if(!finger) break;
            }
            finger->cron.next(fromdt, fingerdt, true);
            int cmp = fingerdt.compare(nextdt);
            if(cmp<=0)
            {
                if(cmp<0)
                {
                    nextdt.set(fingerdt);
                    tags.kill();
                }
                tags.append(finger->tag.get());
            }
            finger = finger->next;
        }
        return tags.ordinality();
    }

    virtual Transaction * getTransactionFrame()
    {
        CriticalBlock block(crit);
        if(hasframe) return NULL;
        hasframe = true;
        return new XFrame(this);
    }

private:
    void kill()
    {
        CronTableItem * del;
        while(head)
        {
            del = head;
            head = head->next;
            delete del;
        }
        head = tail = NULL;
    }

    void doadd(char const * spec, char const * tag, bool inframe)
    {
        CronTableItem * newtail = new CronTableItem(tail, NULL, spec, tag, inframe);
        if(tail)
        {
            tail->next = newtail;
            tail = newtail;
        }
        else
        {
            head = tail = newtail;
        }
    }

    void doremove(CronTableItem * finger)
    {
        if(finger->prev)
            finger->prev->next = finger->next;
        else
            head = finger->next;
        if(finger->next)
            finger->next->prev = finger->prev;
        else
            tail = finger->prev;
        delete finger;
    }

private:
    friend class XFrame;
    CronTableItem * head;
    CronTableItem * tail;
    CriticalSection crit;
    bool hasframe;
};

ICronTable * createCronTable() { return new CCronTable(); }

#if 0

void testTiming()
{
    Owned<IJlibDateTime> ts = createDateTime();
    StringAttr result;
    StringAttrAdaptor ret(result);

    if(timezone != -20700)
        assertex(!"Please set your OS to Nepalese (Kathmandu) time to run this test");
    ts->setDateTime(2001,1,1,19,56,23,0,-180);
    ts->getString(ret);
    assertex(strcmp(result, "2001-01-01T19:56:23+03:00") == 0);
    ts->getGmtString(ret);
    assertex(strcmp(result, "2001-01-01T16:56:23") == 0);
    ts->getLocalString(ret);
    assertex(strcmp(result, "2001-01-01T22:41:23") == 0);

    ts->setDateTime(2004,2,28,23,56,23,0,180);
    ts->getString(ret);
    assertex(strcmp(result, "2004-02-28T23:56:23-03:00") == 0);
    ts->getGmtString(ret);
    assertex(strcmp(result, "2004-02-29T02:56:23") == 0);
    ts->getLocalString(ret);
    assertex(strcmp(result, "2004-02-29T08:41:23") == 0);

    ts->setDateTime(2003,2,28,23,56,23,0,180);
    ts->getString(ret);
    assertex(strcmp(result, "2003-02-28T23:56:23-03:00") == 0);
    ts->getGmtString(ret);
    assertex(strcmp(result, "2003-03-01T02:56:23") == 0);
    ts->getLocalString(ret);
    assertex(strcmp(result, "2003-03-01T08:41:23") == 0);

    ts->setDateTime(2004,2,28,23,56,23,0,0);
    ts->getString(ret);
    assertex(strcmp(result, "2004-02-28T23:56:23Z") == 0);

    ts->setDateTime(1970,0,1,0,0,0,0,0);
    ts->setString("2003-02-28T23:56:23-03:00");
    ts->getString(ret);
    assertex(strcmp(result, "2003-02-28T23:56:23-03:00") == 0);

    ts->setDateTime(0,0,0,0,0,0,0,0);
    ts->setString("2003-02-28T23:56:23");
    ts->getString(ret);
    assertex(strcmp(result, "2003-02-28T23:56:23Z") == 0);

    ts->setDateTime(0,0,0,0,0,0,0,0);
    ts->setString("2003-02-28T23:56:23Z");
    ts->getString(ret);
    assertex(strcmp(result, "2003-02-28T23:56:23Z") == 0);

    ts->setDateTime(0,0,0,0,0,0,0,0);
    ts->setGmtString("2003-02-28T23:56:23");
    ts->getString(ret);
    assertex(strcmp(result, "2003-02-28T23:56:23Z") == 0);

    ts->setDateTime(0,0,0,0,0,0,0,0);
    ts->setLocalString("2003-02-28T23:56:23");
    ts->getString(ret);
    assertex(strcmp(result, "2003-02-28T23:56:23+05:45") == 0);
    ts->getLocalString(ret);
    assertex(strcmp(result, "2003-02-28T23:56:23") == 0);
    ts->getGmtString(ret);
    assertex(strcmp(result, "2003-02-28T18:11:23") == 0);

    ts->setDateTime(0,0,0,0,0,0,0,0);
    ts->setLocalString("2003-07-28T23:56:23");
    ts->getGmtString(ret);
    assertex(strcmp(result, "2003-07-28T17:11:23") == 0);
}

void testCronTable()
{
    Owned<ICronTable> crontab(createCronTable());
    crontab->add("30 * * * *", "on the half hour");
    crontab->add("* * * * *", "every minute");
    crontab->add("*/2 * * * *", "every even minute");
    crontab->add("0 * * * *", "on the hour");
    crontab->add("0 0 */2 * *", "every other midnight");
    crontab->add("0 2,7-9 * * *", "two, seven, eight, and nine o'clock");
    CDateTime in;
    CDateTime key;
    CDateTime out;
    StringArray tags;
    unsigned ret;

    in.set(2004, 12, 6, 17, 32, 30);
    key.set(2004, 12, 6, 17, 33, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==1);
    assertex(strcmp(tags.item(0), "every minute")==0);

    in.set(2004, 12, 6, 17, 33, 0);
    key.set(2004, 12, 6, 17, 34, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==2);
    assertex(strcmp(tags.item(0), "every minute")==0);
    assertex(strcmp(tags.item(1), "every even minute")==0);

    in.set(2004, 12, 6, 17, 59, 0);
    key.set(2004, 12, 6, 18, 0, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==3);
    assertex(strcmp(tags.item(0), "every minute")==0);
    assertex(strcmp(tags.item(1), "every even minute")==0);
    assertex(strcmp(tags.item(2), "on the hour")==0);

    in.set(2004, 12, 6, 18, 29, 0);
    key.set(2004, 12, 6, 18, 30, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==3);
    assertex(strcmp(tags.item(0), "on the half hour")==0);
    assertex(strcmp(tags.item(1), "every minute")==0);
    assertex(strcmp(tags.item(2), "every even minute")==0);

    in.set(2004, 12, 6, 23, 59, 0);
    key.set(2004, 12, 7, 0, 0, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==4);
    assertex(strcmp(tags.item(0), "every minute")==0);
    assertex(strcmp(tags.item(1), "every even minute")==0);
    assertex(strcmp(tags.item(2), "on the hour")==0);
    assertex(strcmp(tags.item(3), "every other midnight")==0);

    in.set(2004, 12, 7, 23, 59, 0);
    key.set(2004, 12, 8, 0, 0, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==3);
    assertex(strcmp(tags.item(0), "every minute")==0);
    assertex(strcmp(tags.item(1), "every even minute")==0);
    assertex(strcmp(tags.item(2), "on the hour")==0);

    in.set(2004, 12, 6, 1, 59, 0);
    key.set(2004, 12, 6, 2, 0, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==4);
    assertex(strcmp(tags.item(0), "every minute")==0);
    assertex(strcmp(tags.item(1), "every even minute")==0);
    assertex(strcmp(tags.item(2), "on the hour")==0);
    assertex(strcmp(tags.item(3), "two, seven, eight, and nine o'clock")==0);

    in.set(2004, 12, 6, 7, 59, 0);
    key.set(2004, 12, 6, 8, 0, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==4);
    assertex(strcmp(tags.item(0), "every minute")==0);
    assertex(strcmp(tags.item(1), "every even minute")==0);
    assertex(strcmp(tags.item(2), "on the hour")==0);
    assertex(strcmp(tags.item(3), "two, seven, eight, and nine o'clock")==0);

    crontab->remove("on the hour");
    in.set(2004, 12, 6, 7, 59, 0);
    key.set(2004, 12, 6, 8, 0, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==3);
    assertex(strcmp(tags.item(0), "every minute")==0);
    assertex(strcmp(tags.item(1), "every even minute")==0);
    assertex(strcmp(tags.item(2), "two, seven, eight, and nine o'clock")==0);

    {
        Owned<ICronTable::Transaction> frame(crontab->getTransactionFrame());
        frame->remove("every minute");
        //rolls back
    }
    in.set(2004, 12, 6, 7, 59, 0);
    key.set(2004, 12, 6, 8, 0, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==3);
    assertex(strcmp(tags.item(0), "every minute")==0);
    assertex(strcmp(tags.item(1), "every even minute")==0);
    assertex(strcmp(tags.item(2), "two, seven, eight, and nine o'clock")==0);

    {
        Owned<ICronTable::Transaction> frame(crontab->getTransactionFrame());
        frame->remove("every minute");
        frame->commit();
    }
    in.set(2004, 12, 6, 7, 59, 0);
    key.set(2004, 12, 6, 8, 0, 0);
    ret = crontab->next(in, out, tags);
    assertex(out == key);
    assertex(ret==2);
    assertex(strcmp(tags.item(0), "every even minute")==0);
    assertex(strcmp(tags.item(1), "two, seven, eight, and nine o'clock")==0);
}

#endif

IJlibDateTime * createDateTimeFromLocal(time_t lt)
{
    CScmDateTime * dt = new CScmDateTime();
    dt->setSimpleLocal(lt);
    return dt;
}

time_t createLocalFromDateTime(IJlibDateTime const * dt)
{
    unsigned year;
    unsigned month;
    unsigned mday;
    unsigned hour;
    unsigned min;
    unsigned sec;
    unsigned nanosec;
    dt->getLocalDate(year, month, mday);
    dt->getLocalTime(hour, min, sec, nanosec);
    tm ts;
    ts.tm_year = year - 1900;
    ts.tm_mon = month - 1;
    ts.tm_mday = mday;
    ts.tm_hour = hour;
    ts.tm_min = min;
    ts.tm_sec = sec;
    ts.tm_isdst = -1; // leave determination of DST to RTL - hope this is ok
    return mktime(&ts);
}

void timetToIDateTime(CDateTime * target, time_t time)
{
    if (target)
    {
        struct tm tm_r;
        struct tm * gmt = gmtime_r(&time, &tm_r);
        target->setDate(gmt->tm_year + 1900, gmt->tm_mon + 1, gmt->tm_mday);
        target->setTime(gmt->tm_hour, gmt->tm_min, gmt->tm_sec, 0);
    }
}


time_t timetFromIDateTime(const CDateTime * source)
{
    if (source == NULL)
        return (time_t) 0;
    
    unsigned bluff;
    struct tm ttm;
    // better fix: change the signature to unsigned's ?? 
    source->getDate((unsigned &)ttm.tm_year, (unsigned &)ttm.tm_mon, (unsigned &)ttm.tm_mday);
    source->getTime((unsigned &)ttm.tm_hour, (unsigned &)ttm.tm_min, (unsigned &)ttm.tm_sec, bluff);
    ttm.tm_isdst = -1;

    if(ttm.tm_year >= 1900)
        ttm.tm_year -= 1900;

    ttm.tm_mon -= 1;

    time_t time = timegm(&ttm);
    if (time == (time_t)-1)
        time = 0;
    return time;
}

