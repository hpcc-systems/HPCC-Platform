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



#ifndef JREGEXP_INCL
#define JREGEXP_INCL

#include <limits.h>

#include "jiface.hpp"
#include "jstring.hpp"

/*********************** Regular Expression Class ***********************/

class RECOMP;

#define RE_ALL      UINT_MAX

// WARNING - this is not threadsafe - it is up to the caller to ensure that a RegExpr is accessed from only one thread at a time 
// that INCLUDES the simple find() method

class jlib_decl RegExpr
{
public:

  RegExpr();
  ~RegExpr();
  RegExpr(const char *exp, bool nocase = false);

  bool init(const char *re, bool nocase = false);
  // Compiles the regular expression ready for Find
  // if nocase = 1 the matching is case insensitive (where possible)

  const char * find(const char *str,size32_t from=0,size32_t len=RE_ALL,size32_t maxlen=0);
  // finds the first occurrence of the RE in string
  //  (positioned after or at 'from' within 'len'
  //  (len = RE_ALL) means to end of the string))
  //  maxlen is only used for clarion strings (should be 0 otherwise)
  //  returns position of first match in string if found
  //  or NULL if not found

  size32_t findlen(unsigned n=0);
  // size of string (or n'th sub-string (n>0)) last matched using find

  const char * findstr(StringBuffer &s,unsigned n=0);
  // returns string last matched (n = 0) or substring n (n>0)

  const char *findnext();
  // repeat last find from after end of last successful find
  //  returns position of first match in string if found
  //  or NULL if not found

  void replace(const char *s,size32_t maxlen,unsigned n = 0);
  // replaces string (or n'th sub-string (n>0)) previously found
  // by find or findnext by 's'
  // can only be called after a successful find/findnext
  // maxlen is the maximum size of the result string after replacement

  const char * substitute(StringBuffer &s,const char *mask,...) __attribute__((format(printf,3,4)));;
  // (for DAB)
  // Creates a string from mask (and following parameters) where mask is
  // a 'sprintf' string with the addition that *after* the sprintf
  // any embedded strings of the form '&n&' are expanded to n'th sub-string
  // previously found by find/findnext ('&0&' is the entire found string)

  void kill();
  // releases extra storage used by RegularExpressionClass
  // (called by destructor)

protected:
  RECOMP *re;

};

inline bool isWildString(const char *s)
{
    if (s && *s) {
        do {
            if ('?'==*s || '*'==*s)
                return true;
        }
        while (*++s);
    }
    return false;
}

bool jlib_decl WildMatch(const char *src, int srclen, const char *pat, int patlen,bool nocase);
bool jlib_decl WildMatch(const char *src, const char *pat, bool nocase=false);
bool jlib_decl WildMatchReplace(const char *src, const char *pat, const char *repl, bool nocase, StringBuffer &out);
bool jlib_decl SoundexMatch(const char *src, const char *pat);
bool jlib_decl containsWildcard(const char * pattern);


class jlib_decl StringMatcher
{
public:
    StringMatcher();
    ~StringMatcher();

    void addEntry(const char * text, unsigned action);
    void addEntry(unsigned len, const char * text, unsigned action);
    unsigned getMatch(unsigned maxLength, const char * text, unsigned & matchLen);
    bool queryAddEntry(unsigned len, const char * text, unsigned action);
    void reset()            {   freeLevel(firstLevel); }

protected:
    struct entry { unsigned value; entry * table; };
    void freeLevel(entry * elems);

protected:
    entry  firstLevel[256];
};

void jlib_decl addActionList(StringMatcher & matcher, const char * text, unsigned action, unsigned * maxElementLength = NULL);

#endif 

