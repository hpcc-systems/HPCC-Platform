#ifndef _METAPHONE_H
#define _METAPHONE_H

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////

#include "platform.h"
#include "dmetaphone.hpp"
#include "cstring.h"
#include <string.h>

//#include <varargs.h>
//#define false FALSE
//#define true TRUE

namespace nsDmetaphone {

#ifdef _MSC_VER
//Disable warnings about cString not matching the export specification of MString,
//because all methods of cString are inline, so they will be compiled into each
//plugin.
#pragma warning(push)
#pragma warning(disable: 4251 4275)
#endif

class DMETAPHONE_API MString : public cString
{
        int     length = 0;
        int     last = 0;
        bool    alternate = false;
        cString primary, secondary;

public:
        MString();
        MString(const char*);
        MString& operator =(const char *Str)
        {
            Set(Str);
            return(*this);
        }
        MString(const cString&);
        bool SlavoGermanic();
        bool IsVowel(int at);
        inline void MetaphAdd(const char* main);
        inline void MetaphAdd(const char main);
        inline void MetaphAdd(const char* main, const char* alt);
        bool StringAt(int start, int length, ... );
        void DoubleMetaphone(cString &metaph, cString &metaph2);
        char GetAt( int x )
        {
            return Ptr[x];
        }
        bool Find(char c)
        {
            return (strchr(Ptr, c) != NULL);
        }
        bool Find(const char * str)
        {
            return (strstr(Ptr, str) != NULL);
        }
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

}//namespace

#endif
