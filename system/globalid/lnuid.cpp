/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#if defined(__linux__) || defined(__APPLE__)
#include <sys/time.h>
#elif _WIN32
#include <windows.h>
#include <wincrypt.h>
#include <chrono>
#endif

/**
* This is an implementation of Globally Unique Transaction ID’s.
* Note: This class currently generates unique id on OS's that supports device /dev/urandom.
* @author - Amol Patwardhan
* Version: 1.0
*/

#include <iostream>
#include <stdio.h>
#include <string.h>
#include <vector>
#include <assert.h>
#include <stdio.h>
#include <time.h>
#include <sstream>
#include <ctime>
#include "lnuid.h"
#include "jlog.hpp"

using namespace std;

namespace ln_uid {

    /** All alphanumeric characters except for "0", "I", "O", and "l" */
    const char* pszBase58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    //Random byte count
    const unsigned int random_byte_count = 11;
    //Random byte count
    const unsigned int time_byte_count = 5;

    //NOTE: This variable is used to offset dropped 41st bit from time component.
    //      This variable adds time till last 41st bit flip on 03-Nov-2004.
    //      This value should be updated later to handle next bit flip on 07-Sep-2039.
    unsigned long long  int OFFSET = (unsigned long long  int) 1099511627776;


    ln_uid_t &createUniqueId(ln_uid_t &out)
    {
#if defined(__linux__) || defined(__APPLE__)
        struct timeval tv;
        gettimeofday(&tv, NULL);

        unsigned long long  int millisecondsSinceEpochTrim =
            (unsigned long long int)(tv.tv_sec) * 1000 +
            (unsigned long long int)(tv.tv_usec) / 1000;

#elif _WIN32
        unsigned long long  int millisecondsSinceEpochTrim =
            std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
#else
       #error "Unimplemented"
#endif
        unsigned char timedata[time_byte_count];

        timedata[0] = (int)((millisecondsSinceEpochTrim >> 32) & 0xFF);
        timedata[1] = (int)((millisecondsSinceEpochTrim >> 24) & 0xFF);
        timedata[2] = (int)((millisecondsSinceEpochTrim >> 16) & 0xFF);
        timedata[3] = (int)((millisecondsSinceEpochTrim >> 8) & 0XFF);
        timedata[4] = (int)((millisecondsSinceEpochTrim & 0XFF));

        unsigned char randomdata[random_byte_count];

#if defined(__linux__) || defined(__APPLE__)
        FILE *fp;
        fp = fopen("/dev/urandom", "r");
        if (!fp || fread(&randomdata, 1, random_byte_count, fp) != random_byte_count)
        {
            // Should never happen, but if it does log it and ignore
            OERRLOG("Could not read data from /dev/urandom");
        }
        if (fp)
            fclose(fp);
#elif _WIN32
        HCRYPTPROV hProvider;

        CryptAcquireContext(&hProvider, 0, 0, PROV_RSA_FULL, CRYPT_VERIFYCONTEXT);
        CryptGenRandom(hProvider, random_byte_count, randomdata);
#else
       #error "Unimplemented"
#endif

        for(unsigned int i=0; i <time_byte_count; i++)
            out[i] = timedata[i];

        for(unsigned int i=5, j=0; i <uid_size; i++, j++)
            out[i] = randomdata[j];

        return out;
    }

    string createUniqueIdString()
    {
        ln_uid_t ln_uid = {};
        createUniqueId(ln_uid);
        return uniqueIdToString(ln_uid);
    }

    string uniqueIdToString(const ln_uid_t &uid)
    {
        // Skip and count leading zeroes.
        int zeroes = 0;
        int length = 0;
        int counter = 0;
        while (counter != uid_size && uid[counter] == 0)
        {
            counter++;
            zeroes++;
        }
        // Allocate enough space in big-endian base58 representation.
        int size = (uid_size) * 138 / 100 + 1; // log(256) / log(58), rounded up.
        vector<unsigned char> b58(size);

        // Process the bytes.
        while (counter != uid_size)
        {
            int carry = uid[counter];
            int i = 0;
            // Apply "b58 = b58 * 256 + ch".
            for (vector<unsigned char>::reverse_iterator it = b58.rbegin(); (carry != 0 || i < length) && (it != b58.rend()); it++, i++)
            {
                carry += 256 * (*it);
                *it = carry % 58;
                carry /= 58;
            }

            assert(carry == 0);
            length = i;
            counter++;
        }

        // Skip leading zeroes in base58 result.
        vector<unsigned char>::iterator it = b58.begin() + (size - length);
        while (it != b58.end() && *it == 0)
            it++;

        // Translate the result into a string.
        string str;
        str.reserve(zeroes + (b58.end() - it));
        str.assign(zeroes, '1');
        while (it != b58.end())
            str += pszBase58[*(it++)];
        return str;
    }

    ln_uid_t &uniqueIdFromString(const char* uid, ln_uid_t &out)
    {
        // Skip leading spaces. psz
        while (*uid && isspace(*uid))
            uid++;
        // Skip and count leading '1's.
        int zeroes = 0;
        while (*uid == '1')
        {
            zeroes++;
            uid++;
        }
        // Allocate enough space in big-endian base256 representation.
        vector<unsigned char> b256(strlen(uid) * 733 / 1000 + 1); // log(58) / log(256), rounded up.

        // Process the characters.
        while (*uid && !isspace(*uid))
        {
            // Decode base58 character
            const char* ch = strchr(pszBase58, *uid);
            if (ch == NULL)
               return out;

            // Apply "b256 = b256 * 58 + ch".
            int carry = ch - pszBase58;
            for (vector<unsigned char>::reverse_iterator it = b256.rbegin(); it != b256.rend(); it++)
            {
                carry += 58 * (*it);
                *it = carry % 256;
                carry /= 256;
            }
            assert(carry == 0);
            uid++;
        }

        // Skip trailing spaces.
        while (isspace(*uid))
            uid++;
        if (*uid != 0)
            return out;
        // Skip leading zeroes in b256.
        vector<unsigned char>::iterator it = b256.begin();
        while (it != b256.end() && *it == 0)
            it++;
        vector<unsigned char> vch;

        // Copy result into output vector.
        vch.reserve(zeroes + (b256.end() - it));
        vch.assign(zeroes, 0x00);
        while (it != b256.end())
            vch.push_back(*(it++));

        for(unsigned int i=0; i <vch.size(); i++)
            out[i] = vch[i];

        return out;
    }

    time_t timeFromUniqueId(const char* uid)
    {
        ln_uid_t uid_ch = {};
        uniqueIdFromString(uid, uid_ch);
        return timeFromUniqueId(uid_ch);
    }

    time_t timeFromUniqueId(const ln_uid_t &uid)
    {

        unsigned long long  int timepart = 0;
        for(unsigned int i=0; i< time_byte_count; i++ )
            timepart = (timepart << 8) + (uid[i] & 0xff);

        timepart = timepart + OFFSET;
        time_t seconds = (time_t)(timepart/1000);

        return seconds;
    }

    void getUniqueIdRange(time_t start, time_t end, ln_uid_t &uid_start, ln_uid_t &uid_end)
    {
        unsigned long long int startMilliSecondsSinceEpochTrim = (unsigned long long int)(start) * 1000;
        unsigned long long int endMilliSecondsSinceEpochTrim = (unsigned long long int)(end) * 1000;

        unsigned char startTimeData[time_byte_count], endTimeData[time_byte_count];

        startTimeData[0] = (int)((startMilliSecondsSinceEpochTrim >> 32) & 0xFF) ;
        startTimeData[1] = (int)((startMilliSecondsSinceEpochTrim >> 24) & 0xFF) ;
        startTimeData[2] = (int)((startMilliSecondsSinceEpochTrim >> 16) & 0xFF) ;
        startTimeData[3] = (int)((startMilliSecondsSinceEpochTrim >> 8) & 0XFF);
        startTimeData[4] = (int)((startMilliSecondsSinceEpochTrim & 0XFF));

        endTimeData[0] = (int)((endMilliSecondsSinceEpochTrim >> 32) & 0xFF) ;
        endTimeData[1] = (int)((endMilliSecondsSinceEpochTrim >> 24) & 0xFF) ;
        endTimeData[2] = (int)((endMilliSecondsSinceEpochTrim >> 16) & 0xFF) ;
        endTimeData[3] = (int)((endMilliSecondsSinceEpochTrim >> 8) & 0XFF);
        endTimeData[4] = (int)((endMilliSecondsSinceEpochTrim & 0XFF));

        unsigned char blankedRandomdata[random_byte_count], filledRandomData[random_byte_count];

        for(unsigned int i=0; i <random_byte_count; i++)
        {
            blankedRandomdata[i] = 0x00;
            filledRandomData[i] = 0xFF;
        }

        for(unsigned int i=0; i <time_byte_count; i++)
            uid_start[i] = startTimeData[i] ;

        for(unsigned int i=time_byte_count, j=0; i <uid_size; i++, j++)
            uid_start[i] = blankedRandomdata[j] ;

        for(unsigned int i=0; i <time_byte_count; i++)
            uid_end[i] = endTimeData[i] ;

        for(unsigned int i=time_byte_count, j=0; i <uid_size; i++, j++)
            uid_end[i] = filledRandomData[j] ;
    }

    void getUniqueIdDateRange(const char *start, const char *end, ln_uid_t &uid_start, ln_uid_t &uid_end)
    {
        tm startDateTime;
        tm endDateTime;

        time_t t1, t2, start_time, end_time;
        t1 = time(NULL);
        t2 = time(NULL);

#if defined(__linux__) || defined(__APPLE__)
        localtime_r(&t1, &startDateTime);
        strptime(start, "%Y-%m-%d %H:%M:%S", &startDateTime);

#elif _WIN32
        int Y1, m1, d1, H1, M1, S1;

        localtime_s( &startDateTime, &t1);
        sscanf_s(start, "%4d-%2d-%2d %2d:%2d:%2d", &Y1, &m1, &d1, &H1, &M1, &S1);
        startDateTime.tm_year = Y1 - 1900;
        startDateTime.tm_mon = m1 - 1;
        startDateTime.tm_mday = d1;
        startDateTime.tm_hour = H1;
        startDateTime.tm_min = M1;
        startDateTime.tm_sec = S1;
#else
       #error "Unimplemented"
#endif

#if defined(__linux__) || defined(__APPLE__)
        localtime_r(&t2, &endDateTime);
        strptime(end, "%Y-%m-%d %H:%M:%S", &endDateTime);

#elif _WIN32
        localtime_s(&endDateTime, &t2);
        int Y2, m2, d2, H2, M2, S2;
        sscanf_s(end, "%4d-%2d-%2d %2d:%2d:%2d", &Y2, &m2, &d2, &H2, &M2, &S2);
        endDateTime.tm_year = Y2 - 1900;
        endDateTime.tm_mon = m2 - 1;
        endDateTime.tm_mday = d2;
        endDateTime.tm_hour = H2;
        endDateTime.tm_min = M2;
        endDateTime.tm_sec = S2;
#else
       #error "Unimplemented"
#endif

        start_time = mktime(&startDateTime) ;
        end_time = mktime(&endDateTime) ;
        cout << "Start time:" << "UTC OFFSET:"<< get_utc_offset () << ":" <<start << ":" << start_time << endl;
        cout << "End time:" << end << ":" << end_time << endl;

        struct tm tm;
        char sb[100];
#if defined(__linux__) || defined(__APPLE__)
        localtime_r(&start_time, &tm);
        sprintf(sb, "now: %4d-%02d-%02d %02d:%02d:%02d\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec);
#elif _WIN32
        localtime_s(&tm, &start_time);
        sprintf_s(sb, "now: %4d-%02d-%02d %02d:%02d:%02d\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec);
#else
       #error "Unimplemented"
#endif
        cout << "DateTime is " << sb << endl;

        getUniqueIdRange(start_time, end_time, uid_start, uid_end);

    }

    bool sameUniqueId(const ln_uid_t &uid1, const ln_uid_t &uid2)
    {
        return memcmp(uid1, uid2, sizeof uid2)==0;
    }

    void copyUniqueId(ln_uid_t &to, const ln_uid_t &from)
    {
        memcpy (to, from, sizeof from);
    }


/*  returns the utc timezone offset (e.g. -8 hours for PST)
*/
    int get_utc_offset()
    {
        time_t zero = 24 * 60 * 60L;
        struct tm timeptr;
        int gmtime_hours;

        /* get the local time for Jan 2, 1900 00:00 UTC */
#if defined(__linux__) || defined(__APPLE__)
        localtime_r(&zero, &timeptr);
#elif _WIN32
        localtime_s(&timeptr, &zero);
#else
       #error "Unimplemented"
#endif

        gmtime_hours = timeptr.tm_hour;

/* if the local time is the "day before" the UTC, subtract 24 hours
   from the hours to get the UTC offset
*/
        if (timeptr.tm_mday < 2)
            gmtime_hours -= 24;

        return gmtime_hours;
    }
};
