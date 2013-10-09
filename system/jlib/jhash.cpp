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


#include "platform.h"
#include <limits.h>
#include <ctype.h>
#include <stdio.h>

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #undef new
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

#include "jhash.hpp"
#include "jmutex.hpp"

#define PSTRINGDATA INT_MIN

#define mix(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<<8); \
  c -= a; c -= b; c ^= (b>>13); \
  a -= b; a -= c; a ^= (c>>12);  \
  b -= c; b -= a; b ^= (a<<16); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>3);  \
  b -= c; b -= a; b ^= (a<<10); \
  c -= a; c -= b; c ^= (b>>15); \
}

#define GETBYTE0(n) ((unsigned)k[n])
#define GETBYTE1(n) ((unsigned)k[n+1]<<8)
#define GETBYTE2(n) ((unsigned)k[n+2]<<16)
#define GETBYTE3(n) ((unsigned)k[n+3]<<24)
#define GETWORD(k,n) (GETBYTE0(n)+GETBYTE1(n)+GETBYTE2(n)+GETBYTE3(n))

// the above looks inefficient but the compiler optimizes well

// this hash looks slow but is about twice as quick as using our CRC table
// and gives gives better results
// (see paper at http://burtleburtle.net/bob/hash/evahash.html for more info)

// Global atom table

MODULE_INIT(INIT_PRIORITY_JHASH)
{
    return true;
}

MODULE_EXIT()
{
    // do not delete globalAtomTable as it will slow things down.  If you
    // do not want these in your leak checking call releaseAtoms()
}

#define HASHONE(hash, c)        { hash *= 0x01000193; hash ^= c; }      // Fowler/Noll/Vo Hash... seems to work pretty well, and fast
#define USE_FNV

unsigned hashc( const unsigned char *k, unsigned length, unsigned initval)
{
#ifdef USE_FNV
    unsigned hash = initval;        // ^_rotl(length,2)??
    unsigned char c;
    while (length >= 8)
    {
        c = (*k++); HASHONE(hash, c);
        c = (*k++); HASHONE(hash, c);
        c = (*k++); HASHONE(hash, c);
        c = (*k++); HASHONE(hash, c);
        length-=4;
    }
    switch (length)
    {
    case 7: c = (*k++); HASHONE(hash, c);
    case 6: c = (*k++); HASHONE(hash, c);
    case 5: c = (*k++); HASHONE(hash, c);
    case 4: c = (*k++); HASHONE(hash, c);
    case 3: c = (*k++); HASHONE(hash, c);
    case 2: c = (*k++); HASHONE(hash, c);
    case 1: c = (*k++); HASHONE(hash, c);
    }
    return hash;
#else
   register unsigned a,b,c,len;

   /* Set up the internal state */
   len = length;
   a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
   c = initval;         /* the previous hash value */

   /*---------------------------------------- handle most of the key */
   while (len >= 12)
   {
      a += GETWORD(k,0);
      b += GETWORD(k,4);
      c += GETWORD(k,8);
      mix(a,b,c);
      k += 12; len -= 12;
   }

   /*------------------------------------- handle the last 11 bytes */
   c += length;
   switch(len)              /* all the case statements fall through */
   {
   case 11: c+=GETBYTE3(7);
   case 10: c+=GETBYTE2(7);
   case 9 : c+=GETBYTE1(7);
      /* the first byte of c is reserved for the length */
   case 8 : b+=GETBYTE3(4);
   case 7 : b+=GETBYTE2(4);
   case 6 : b+=GETBYTE1(4);
   case 5 : b+=GETBYTE0(4);
   case 4 : a+=GETBYTE3(0);
   case 3 : a+=GETBYTE2(0);
   case 2 : a+=GETBYTE1(0);
   case 1 : a+=GETBYTE0(0);
     /* case 0: nothing left to add */
   }
   mix(a,b,c);
   /*-------------------------------------------- report the result */
   return c;
#endif
}

#define GETWORDNC(k,n) ((GETBYTE0(n)+GETBYTE1(n)+GETBYTE2(n)+GETBYTE3(n))&0xdfdfdfdf)


unsigned hashnc( const unsigned char *k, unsigned length, unsigned initval)
{
#ifdef USE_FNV
    unsigned hash = initval;
    unsigned char c;
    while (length >= 8)
    {
        c = (*k++)&0xdf; HASHONE(hash, c);
        c = (*k++)&0xdf; HASHONE(hash, c);
        c = (*k++)&0xdf; HASHONE(hash, c);
        c = (*k++)&0xdf; HASHONE(hash, c);
        length-=4;
    }
    switch (length)
    {
    case 7: c = (*k++)&0xdf; HASHONE(hash, c);
    case 6: c = (*k++)&0xdf; HASHONE(hash, c);
    case 5: c = (*k++)&0xdf; HASHONE(hash, c);
    case 4: c = (*k++)&0xdf; HASHONE(hash, c);
    case 3: c = (*k++)&0xdf; HASHONE(hash, c);
    case 2: c = (*k++)&0xdf; HASHONE(hash, c);
    case 1: c = (*k++)&0xdf; HASHONE(hash, c);
    }
    return hash;
#else

   register unsigned a,b,c,len;

   /* Set up the internal state */
   len = length;
   a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
   c = initval;         /* the previous hash value */

   /*---------------------------------------- handle most of the key */
   while (len >= 12)
   {
      a += GETWORDNC(k,0);
      b += GETWORDNC(k,4);
      c += GETWORDNC(k,8);
      mix(a,b,c);
      k += 12; len -= 12;
   }

   /*------------------------------------- handle the last 11 bytes */
   c += length;
   switch(len)              /* all the case statements fall through */
   {
   case 11: c+=GETBYTE3(7)&0xdf;
   case 10: c+=GETBYTE2(7)&0xdf;
   case 9 : c+=GETBYTE1(7)&0xdf;
      /* the first byte of c is reserved for the length */
   case 8 : b+=GETBYTE3(4)&0xdf;
   case 7 : b+=GETBYTE2(4)&0xdf;
   case 6 : b+=GETBYTE1(4)&0xdf;
   case 5 : b+=GETBYTE0(4)&0xdf;
   case 4 : a+=GETBYTE3(0)&0xdf;
   case 3 : a+=GETBYTE2(0)&0xdf;
   case 2 : a+=GETBYTE1(0)&0xdf;
   case 1 : a+=GETBYTE0(0)&0xdf;
     /* case 0: nothing left to add */
   }
   mix(a,b,c);
   /*-------------------------------------------- report the result */
   return c;
#endif
}


MappingKey::MappingKey(const void * inKey, int keysize)
{
  int ksm = keysize;
  if (!ksm)
     ksm = (size32_t)strlen((char *) inKey) + 1;
  else if (ksm<0)
  {
     if (ksm==PSTRINGDATA) {
       ksm = (*((unsigned char *)inKey))+1;
     }
     else {
       ksm = -ksm;
       ksm += (size32_t)strlen((char *) inKey + ksm) + 1;
     }
  }

  void * temp  = malloc(ksm);
  memcpy(temp, inKey, ksm);
  key = temp;
}

//-- Default atom implementations --------------------------------------------

const char *IAtom::getAtomNamePtr() const
{
   if (!this) return NULL;
   return getNamePtr();
}

IAtom * IIdAtom::lower() const
{
    if (!this)
        return NULL;
    return queryLower();
}

//-- Mapping ---------------------------------------------------

unsigned MappingBase::getHash() const       { return hash; }
void     MappingBase::setHash(unsigned hval){ hash = hval; }

const void * Mapping::getKey() const        { return key.key; }

//-- HashMapping ---------------------------------------------------

//const void * HashMapping::getKey() const  { return key.key; }

//-- Atom ---------------------------------------------------

unsigned AtomBase::getHash() const              { return hash; }
void     AtomBase::setHash(unsigned hval)       { hash = hval; }

const void * AtomBase::getKey() const           { return key; }

//-- Case Atom ---------------------------------------------------

CaseAtom::CaseAtom(const void * k) : hash(0)
{
    text = strdup((const char *)k);
    lower = createLowerCaseAtom(text);
}

//-- HashTable ---------------------------------------------------

bool HashTable::addOwn(IMapping & donor)
{
    if(add(donor))
    {
        donor.Release();
        return true;
    }
    return false;
}

bool HashTable::replaceOwn(IMapping & donor)
{
    if(replace(donor))
    {
        donor.Release();
        return true;
    }
    return false;
}

IMapping * HashTable::findLink(const void * findParam) const
{
    IMapping * found = SuperHashTableOf<IMapping, const void>::find(findParam);
    if(found) found->Link();
    return found;
}

bool HashTable::onNotify(INotification & notify)
{
    bool ret = true;
    if (notify.getAction() == NotifyOnDispose)
    {
        IMapping * mapping = static_cast<IMapping *>(notify.querySource());
        ret = removeExact(mapping);
        assertex(ret);
    }
    return ret;
}

bool HashTable::keyeq(const void *key1, const void *key2, int ksize) const
{
    if (ksize<=0)
    {
        if (ksize<0)
        {
            if (ksize==PSTRINGDATA)
                return (memcmp(key1,key2,(*(unsigned char*)key1)) == 0);
            unsigned ksm = -ksize;
            if (memcmp(key1,key2,ksm))
                return false;
            key1 = (char *)key1 + ksm;
            key2 = (char *)key2 + ksm;
        }

        if (ignorecase)
        {
            unsigned char *k1 = (unsigned char *) key1;
            unsigned char *k2 = (unsigned char *) key2;
            loop
            {
                unsigned char c1 = toupper(*k1);
                if (c1 != toupper(*k2))
                    return false;
                if (c1 == 0)
                    return true;
                k1++;
                k2++;
            }
        }
        return strcmp((char *)key1, (char *)key2) == 0;
    }
    return memcmp(key1,key2,ksize)==0;
}

unsigned HashTable::hash(const void *key, int ksize) const
{
   unsigned h = 0x811C9DC5;
   unsigned char *bp = (unsigned char *) key;
   if (ksize<=0)
   {
      if (ksize==PSTRINGDATA) {
        ksize = (*(unsigned char*)key)+1;
        goto BlockHash;
      }
      if (ksize<0) {
        h = hashc(bp,-ksize,h);
        bp -= ksize;
      }
      unsigned char* ks = bp;
      while (*bp) bp++;
      if (ignorecase)
        h = hashnc(ks,(size32_t)(bp-ks),h);
      else
        h = hashc(ks,(size32_t)(bp-ks),h);
   }
   else
   {
BlockHash:
      if (ignorecase)
        h = hashnc(bp,ksize,h);
      else
        h = hashc(bp,ksize,h);
   }
#ifdef USE_FNV
      //Strings that only differ by a single character don't generate hashes very far apart without this
   h *= 0x01000193;
   //h = ((h << 7) ^ (h >> 25));
#endif
   return h;
}

IMapping * HashTable::createLink(const void *key)
{
   IMapping * match = findLink(key);
   if (!match)
   {
      match = newMapping(key);
      if (match) add(*match);            // link for hash table
   }
   return match;
}

IMapping *HashTable::newMapping(const void *)
{
   assertex(!"Newmapping must be overridden to use HashTable::Create");
   return NULL;
}

// -- Helper functions...

void IntersectHash(HashTable & h1, HashTable & h2)
{
  HashIterator iter(h1);
  iter.first();
  while (iter.isValid())
  {
    IMapping & cur = iter.query();
    IMapping * matched = h2.find(cur.getKey());
    iter.next();
    if (!matched)
      h1.removeExact(&cur);
  }
}

void UnionHash(HashTable & h1, HashTable & h2)
{
  HashIterator iter(h2);
  iter.first();
  while (iter.isValid())
  {
    IMapping & cur = iter.query();
    IMapping * matched = h1.find(cur.getKey());
    if (!matched)
      h1.add(cur);
    iter.next();
  }
}

void SubtractHash(HashTable & main, HashTable & sub)
{
  HashIterator iter(sub);
  iter.first();
  while (iter.isValid())
  {
    IMapping & cur = iter.query();
    IMapping * matched = main.find(cur.getKey());
    iter.next();
    if (matched)
      main.removeExact(&cur);
  }
}

//===========================================================================

void KeptHashTable::onAdd(void * et)
{
    static_cast<IMapping *>(et)->Link();
}

void KeptHashTable::onRemove(void * et)
{
    static_cast<IMapping *>(et)->Release();
}

IMapping * KeptHashTable::create(const void *key)
{
   IMapping * match = find(key);
   if (!match)
   {
      match = newMapping(key);
      if (match) addOwn(*match);         // already linked for hash table
   }
   return match;
}


void ObservedHashTable::onAdd(void * et)
{
    static_cast<IMapping *>(et)->addObserver(*this);
}

void ObservedHashTable::onRemove(void * et)
{
    static_cast<IMapping *>(et)->removeObserver(*this);
}

//===========================================================================

static CriticalSection atomCrit;
static KeptLowerCaseAtomTable *globalAtomTable = NULL;
inline KeptLowerCaseAtomTable * queryGlobalAtomTable()
{
    if (!globalAtomTable)
    {
        globalAtomTable = new KeptLowerCaseAtomTable;
        globalAtomTable->reinit(2000);
    }
    return globalAtomTable;
}

extern jlib_decl IAtom * createAtom(const char *value)
{
    if (!value) return NULL;
    CriticalBlock crit(atomCrit);
    return queryGlobalAtomTable()->addAtom(value);
}

extern jlib_decl IAtom * createAtom(const char *value, size32_t len)
{
    if (!value || !len)
        return NULL;
    char * nullTerminated = (char *)alloca(len+1);
    memcpy(nullTerminated, value, len);
    nullTerminated[len] = 0;
    CriticalBlock crit(atomCrit);
    return queryGlobalAtomTable()->addAtom(nullTerminated);
}

//===========================================================================

static CriticalSection caseAtomCrit;
static KeptCaseAtomTable *globalCaseAtomTable = NULL;
inline KeptCaseAtomTable * queryGlobalCaseAtomTable()
{
    if (!globalCaseAtomTable)
    {
        globalCaseAtomTable = new KeptCaseAtomTable;
        globalCaseAtomTable->reinit(2000);
    }
    return globalCaseAtomTable;
}

extern jlib_decl IIdAtom * createIdAtom(const char *value)
{
    if (!value) return NULL;
    CriticalBlock crit(caseAtomCrit);
    return queryGlobalCaseAtomTable()->addAtom(value);
}

extern jlib_decl IIdAtom * createIdAtom(const char *value, size32_t len)
{
    if (!value || !len)
        return NULL;
    char * nullTerminated = (char *)alloca(len+1);
    memcpy(nullTerminated, value, len);
    nullTerminated[len] = 0;
    CriticalBlock crit(caseAtomCrit);
    return queryGlobalCaseAtomTable()->addAtom(nullTerminated);
}

#ifdef THE_GLOBAL_HASH_TABLE_BECOMES_CASE_SENSITIVE
extern jlib_decl IAtom * createLowerCaseAtom(const char *value)
{
    if (!value) return NULL;

    unsigned len = strlen(value);
    const byte * src = (const byte *)value;
    char * lower = (char *)alloca(len+1);
    for (unsigned i=0; i < len; i++)
        lower[i] = tolower(src[i]);
    lower[len] = 0;

    CriticalBlock crit(atomCrit);
    return queryGlobalAtomTable()->addAtom(value);
}

extern jlib_decl IAtom * createLowerCaseAtom(const char *value, size32_t len)
{
    if (!value || !len)
        return NULL;

    const byte * src = (const byte *)value;
    char * lower = (char *)alloca(len+1);
    for (unsigned i=0; i < len; i++)
        lower[i] = tolower(src[i]);
    lower[len] = 0;

    CriticalBlock crit(atomCrit);
    return queryGlobalAtomTable()->addAtom(lower);
}
#endif

extern jlib_decl void releaseAtoms()
{
    delete globalCaseAtomTable;
    globalCaseAtomTable = NULL;
    delete globalAtomTable;
    globalAtomTable = NULL;
}
