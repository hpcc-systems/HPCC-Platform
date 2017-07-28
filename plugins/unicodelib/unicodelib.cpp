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

#include "jlib.hpp"
#include "jsem.hpp"

#include <string.h>
#include "unicodelib.hpp"
#include "unicode/usearch.h"
#include "unicode/schriter.h"
#include "unicode/locid.h"
#include "unicode/coll.h"
#include "unicode/stsearch.h"
#include "unicode/translit.h"
#include "unicode/rbbi.h"
#include "../stringlib/wildmatch.tpp"

#define UNICODELIB_VERSION "UNICODELIB 1.1.06"

static UChar32 const u32comma = ',';
static UChar32 const u32space = ' ';
static UChar const u16asterisk = '*';
static UChar const u16query = '?';
static UChar const u16space = ' ';

static const char * EclDefinition =
"export UnicodeLib := SERVICE:fold\n"
"  unicode UnicodeFilterOut(const unicode src, const unicode _within) : c, pure,entrypoint='ulUnicodeFilterOut'; \n"
"  unicode UnicodeFilter(const unicode src, const unicode _within) : c, pure,entrypoint='ulUnicodeFilter'; \n"
"  unicode UnicodeSubstituteOut(const unicode src, const unicode _within, const unicode _newchar) : c, pure,entrypoint='ulUnicodeSubsOut'; \n"
"  unicode UnicodeSubstitute(const unicode src, const unicode _within, const unicode _newchar) : c, pure,entrypoint='ulUnicodeSubs'; \n"
"  unicode UnicodeRepad(const unicode src, unsigned4 size) : c, pure,entrypoint='ulUnicodeRepad'; \n"
"  unsigned integer4 UnicodeFind(const unicode src, const unicode tofind, unsigned4 instance) : c, pure,entrypoint='ulUnicodeFind', hole; \n"
"  unsigned integer4 UnicodeLocaleFind(const unicode src, const unicode tofind, unsigned4 instance, const varstring localename) : c, pure,entrypoint='ulUnicodeLocaleFind', hole; \n"
"  unsigned integer4 UnicodeLocaleFindAtStrength(const unicode src, const unicode tofind, unsigned4 instance, const varstring localename, integer1 strength) : c, pure,entrypoint='ulUnicodeLocaleFindAtStrength', hole; \n"
"  unicode UnicodeExtract(const unicode src, unsigned4 instance) : c,pure,entrypoint='ulUnicodeExtract'; \n"
"  unicode50 UnicodeExtract50(const unicode src, unsigned4 instance) : c,pure,entrypoint='ulUnicodeExtract50', hole; \n"
"  unicode UnicodeToLowerCase(const unicode src) : c,pure,entrypoint='ulUnicodeToLowerCase';\n"
"  unicode UnicodeToUpperCase(const unicode src) : c,pure,entrypoint='ulUnicodeToUpperCase';\n"
"  unicode UnicodeToProperCase(const unicode src) : c,pure,entrypoint='ulUnicodeToProperCase';\n"
"  unicode80 UnicodeToLowerCase80(const unicode src) : c,pure,entrypoint='ulUnicodeToLowerCase80', hole;\n"
"  unicode80 UnicodeToUpperCase80(const unicode src) : c,pure,entrypoint='ulUnicodeToUpperCase80', hole;\n"
"  unicode80 UnicodeToProperCase80(const unicode src) : c,pure,entrypoint='ulUnicodeToProperCase80', hole;\n"
"  unicode UnicodeLocaleToLowerCase(const unicode src, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleToLowerCase';\n"
"  unicode UnicodeLocaleToUpperCase(const unicode src, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleToUpperCase';\n"
"  unicode UnicodeLocaleToProperCase(const unicode src, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleToProperCase';\n"
"  unicode80 UnicodeLocaleToLowerCase80(const unicode src, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleToLowerCase80', hole;\n"
"  unicode80 UnicodeLocaleToUpperCase80(const unicode src, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleToUpperCase80', hole;\n"
"  unicode80 UnicodeLocaleToProperCase80(const unicode src, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleToProperCase80', hole;\n"
"  integer4 UnicodeCompareIgnoreCase(const unicode src1, const unicode src2) : c,pure,entrypoint='ulUnicodeCompareIgnoreCase', hole;\n"
"  integer4 UnicodeCompareAtStrength(const unicode src1, const unicode src2, integer1 strength) : c,pure,entrypoint='ulUnicodeCompareAtStrength', hole;\n"
"  integer4 UnicodeLocaleCompareIgnoreCase(const unicode src1, const unicode src2, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleCompareIgnoreCase', hole;\n"
"  integer4 UnicodeLocaleCompareAtStrength(const unicode src1, const unicode src2, const varstring localename, integer1 strength) : c,pure,entrypoint='ulUnicodeLocaleCompareAtStrength', hole;\n"
"  unicode UnicodeReverse(const unicode src) : c,pure,entrypoint='ulUnicodeReverse';\n"
"  unicode UnicodeFindReplace(const unicode src, const unicode stok, const unicode rtok) : c,pure,entrypoint='ulUnicodeFindReplace';\n"
"  unicode UnicodeLocaleFindReplace(const unicode src, const unicode stok, const unicode rtok, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleFindReplace';\n"
"  unicode UnicodeLocaleFindAtStrengthReplace(const unicode src, const unicode stok, const unicode rtok, const varstring localename, integer1 strength) : c,pure,entrypoint='ulUnicodeLocaleFindAtStrengthReplace';\n"
"  unicode80 UnicodeFindReplace80(const unicode src, const unicode stok, const unicode rtok) : c,pure,entrypoint='ulUnicodeFindReplace80', hole;\n"
"  unicode80 UnicodeLocaleFindReplace80(const unicode src, const unicode stok, const unicode rtok, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleFindReplace80',hole;\n"
"  unicode80 UnicodeLocaleFindAtStrengthReplace80(const unicode src, const unicode stok, const unicode rtok, const varstring localename, integer1 strength) : c,pure,entrypoint='ulUnicodeLocaleFindAtStrengthReplace80',hole;\n"
"  unicode UnicodeCleanAccents(const unicode src) : c,pure,entrypoint='ulUnicodeCleanAccents'; \n"
"  unicode UnicodeCleanSpaces(const unicode src) : c,pure,entrypoint='ulUnicodeCleanSpaces'; \n"
"  unicode25 UnicodeCleanSpaces25(const unicode src) : c,pure,entrypoint='ulUnicodeCleanSpaces25', hole; \n"
"  unicode80 UnicodeCleanSpaces80(const unicode src) : c,pure,entrypoint='ulUnicodeCleanSpaces80', hole; \n"
"  boolean UnicodeWildMatch(const unicode src, const unicode _pattern, boolean _noCase) : c, pure,entrypoint='ulUnicodeWildMatch', hole; \n"
"  boolean UnicodeContains(const unicode src, const unicode _pattern, boolean _noCase) : c, pure,entrypoint='ulUnicodeContains', hole; \n"
"  unsigned4 UnicodeLocaleEditDistance(const unicode left, const unicode right, const varstring localename) : c,time,pure,entrypoint='ulUnicodeLocaleEditDistance', hole; \n"
"  boolean UnicodeLocaleEditDistanceWithinRadius(const unicode left, const unicode right, unsigned4 radius,  const varstring localename) : c,time,pure,entrypoint='ulUnicodeLocaleEditDistanceWithinRadius', hole; \n"
"  unsigned4 UnicodeLocaleWordCount(const unicode text, const varstring localename) : c, pure,entrypoint='ulUnicodeLocaleWordCount', hole; \n"
"  unicode UnicodeLocaleGetNthWord(const unicode text, unsigned4 n, const varstring localename) : c,pure,entrypoint='ulUnicodeLocaleGetNthWord';\n"
"  unicode UnicodeLocaleExcludeNthWord(const unicode text, unsigned4 n, const varstring localename) :c,pure,entrypoint='ulUnicodeLocaleExcludeNthWord';\n"
"  unicode UnicodeLocaleTranslate(const unicode text, unicode sear, unicode repl, varstring localename) :c,pure,entrypoint='ulUnicodeLocaleTranslate';\n"
"END;\n";

static const char * compatibleVersions[] = {
    "UNICODELIB 1.1.01 [64d78857c1cecae15bd238cd7767b3c1]", 
    "UNICODELIB 1.1.01 [e8790fe30d9627997749c3c4839b5957]", 
    "UNICODELIB 1.1.02", 
    "UNICODELIB 1.1.03", 
    "UNICODELIB 1.1.04", 
    "UNICODELIB 1.1.05",
    NULL };

UNICODELIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb) 
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = UNICODELIB_VERSION;
    pb->moduleName = "lib_unicodelib";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE | PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "UnicodeLib unicode string manipulation library";
    return true;
}


namespace nsUnicodelib {

IPluginContext * parentCtx = NULL;

void doTrimRight(UnicodeString & source)
{
        int32_t oldLength = source.length();
        if (!oldLength)
            return;
        int32_t currentLength = oldLength;
        bool uSpace = true;
        do {
            UChar32 c = source[--currentLength];
            if(!(c == 0x20 || u_isWhitespace(c))) {
                currentLength++;
                uSpace = false;
            }
        } while (uSpace && currentLength>0);
        if (currentLength < oldLength) {
            source.truncate(currentLength);
        }
}


void forceLength(UnicodeString & str, int32_t len)
{
    if(str.length()>len)
        str.truncate(len);
    else if(str.length()<len)
        str.padTrailing(len);
}

void doModifySearchStrength(StringSearch & search, char strength, UErrorCode & error)
{
    RuleBasedCollator * coll = search.getCollator();
    switch(strength)
    {
    case 1:
        coll->setStrength(Collator::PRIMARY);
        break;
    case 2:
        coll->setStrength(Collator::SECONDARY);
        break;
    case 3:
        coll->setStrength(Collator::TERTIARY);
        break;
    case 4:
        coll->setStrength(Collator::QUATERNARY);
        break;
    case 5:
    default:
        coll->setStrength(Collator::IDENTICAL);
    }
    search.setCollator(coll, error);
}


bool extract(UnicodeString & out, UnicodeString const & in, unsigned instance)
{
    if(!instance) return false;
    int32_t start = 0;
    while(--instance)
    {
        start = in.indexOf(u32comma, start);
        if(start == -1) return false;
        start++;
    }
    int32_t end = in.indexOf(u32comma, start);
    if(end == -1)
        end = in.length();
    out.append(in, start, end-start);
    return true;
}


int doUnicodeCompareAtStrength(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2, Collator::ECollationStrength strength)
{
    UErrorCode error = U_ZERO_ERROR;
    Collator * coll = Collator::createInstance(error);
    coll->setStrength(strength);
    Collator::EComparisonResult ret = coll->compare(src1, src1Len, src2, src2Len);
    delete coll;
    return ret;
}

int doUnicodeLocaleCompareAtStrength(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2, char const * localename, Collator::ECollationStrength strength)
{
    UErrorCode error = U_ZERO_ERROR;
    Locale locale(localename);
    Collator * coll = Collator::createInstance(locale, error);
    coll->setStrength(strength);
    Collator::EComparisonResult ret = coll->compare(src1, src1Len, src2, src2Len);
    delete coll;
    return ret;
}


void doUnicodeLocaleFindReplace(UnicodeString & source, UnicodeString const & pattern, UnicodeString const & replace, char const * localename)
{
    UErrorCode error = U_ZERO_ERROR;
    Locale locale(localename);
    StringSearch search(pattern, source, locale, 0, error);
    int32_t pos = search.first(error);
    while(pos != USEARCH_DONE)
    {
        source.replace(pos, search.getMatchedLength(), replace);
        search.setText(source, error);
        search.setOffset(pos+replace.length(), error);
        pos = search.next(error);
    }
}

void doUnicodeLocaleFindAtStrengthReplace(UnicodeString & source, UnicodeString const & pattern, UnicodeString const & replace, char const * localename, char strength)
{
    UErrorCode error = U_ZERO_ERROR;
    Locale locale(localename);
    StringSearch search(pattern, source, locale, 0, error);
    doModifySearchStrength(search, strength, error);
    int32_t pos = search.first(error);
    while(pos != USEARCH_DONE)
    {
        source.replace(pos, search.getMatchedLength(), replace);
        search.setText(source, error);
        search.setOffset(pos+replace.length(), error);
        pos = search.next(error);
    }
}

void doUnicodeCleanSpaces(UnicodeString & source)
{
    int32_t srclen;
    int32_t pos = source.indexOf(u32space);
    int32_t endpos;
    int32_t spacelen;
    while(pos != -1)
    {
        srclen = source.length();
        for(endpos=pos; endpos<srclen; endpos++)
            if(source.charAt(endpos)!=u32space) break;
        spacelen = endpos-pos;
        if((pos>0) && (endpos<srclen)) spacelen--;
        if(spacelen>0) source.remove(pos, spacelen);
        pos = source.indexOf(u32space, pos+1);
    }
}

/*
  N.B. To do 'real' case-insensitive matching we should use full stringwise casefolding on the source and pattern. The simple char-by-char toupper approach has problems with unicode. For example, some chars uppercase to multiple chars (e.g. the German 'sharp s' uppercases to 'SS'). See http://icu-project.org/userguide/posix.html#case_mappings for more examples. Furthermore, converting as 16-bit code units does not work when code points from U+10000 upwards are involved. Nevertheless, we use the simple char-by-char toupper approach for the UnicodeWildMatch function, because it is intended as a high-speed function. For accurate case-folding, you should either use the UnicodeToUpperCase function explicitly on the arguments or use REGEXFIND.
*/

inline UChar u16toupper(UChar c)
{
    UChar32 o = u_toupper(c);
    return U_IS_SUPPLEMENTARY(o) ? c : (UChar)o;
}

static icu::Transliterator* deAccenter = NULL;
static CriticalSection accenterCrit;

inline unsigned char min3(unsigned char a, unsigned char b, unsigned char c)
{
    unsigned char min = (a<b)? a:b;
    return (min<c)? min:c;
}

// returns the length of Unicode Code Point in 16-bit Code Units
inline int ucpLength(UChar32 c)
{
    return U16_IS_SINGLE(c)?1:2;
}

#define DISTANCE_ON_ERROR 999
class UPCList // User perceived character list
{
private:
    UnicodeString ustring_;
    uint32_t* next_;
    uint32_t  length_;
    uint32_t  capacity_;
    bool invalid_;
    void doCreateUPCList(BreakIterator& cbi) {
        UErrorCode status = U_ZERO_ERROR;
        if (!capacity_) {
             capacity_ = ustring_.length();
         }
        next_ = new uint32_t[capacity_+1]; // the number of characters is always less or equal to the string length
        unsigned index=0;
        cbi.setText(ustring_);
        next_[index] = cbi.first();
        for (int32_t end = cbi.next(); end != BreakIterator::DONE && length_ < capacity_; end = cbi.next())
        {
            length_++;
            next_[++index]=end;
        }
        if (U_FAILURE(status)) { length_ = 0; capacity_ = 0; invalid_ = true; }
    }
    void doCreateUPCList() {
        if (!capacity_) {
             capacity_ = ustring_.length();
         }
        next_ = new uint32_t[capacity_+1]; // the number of characters is always less or equal to the string length
        unsigned index=0;
        next_[index] = 0;
        int32_t end = 0;
        while (end < capacity_)
        {
            end = end+ucpLength(ustring_[end]);
            next_[++index] = end;
        }
        length_ = index;
    }

public:
    UPCList(BreakIterator* cbi, const UnicodeString & source, uint32_t capacity=0)
        : length_(0), capacity_(capacity),ustring_(source), invalid_(false)
    {
        !cbi?doCreateUPCList():doCreateUPCList(*cbi);
    }

    ~UPCList()
    {
        delete[] next_;
    }

    uint32_t charOffset(uint32_t index) const
    {
        return (index < length_ )? next_[index]:0;
    }

    uint32_t charLength(uint32_t index) const
    {
        return (index < length_ )? next_[index+1]-next_[index]:0;
    }

    bool equal(uint32_t index, const UPCList& srcText, uint32_t srcIndex) const
    {
        uint32_t lLen = charLength(index);
        uint32_t rLen = srcText.charLength(srcIndex);
        if ( lLen != rLen )
            return false;
        UChar lChar,rChar;
        for (unsigned i=0; i < lLen; i++)
        {
            lChar = ustring_[charOffset(index)+i];
            rChar = srcText.getString()[srcText.charOffset(srcIndex)+i];
            if (lChar != rChar)
                return false;
        }
        return true;
    }
    const UnicodeString& getString() const {return ustring_;}
    uint32_t length() const { return length_;}
    uint32_t capacity() const {return capacity_;}
    inline bool isInvalid() const { return invalid_; }
};

class CEList
{
private:
    UnicodeString ustring_;
    uint32_t* ces_;
    uint32_t  length_;
    uint32_t  capacity_;
    bool invalid;

    void doCreateCEList(RuleBasedCollator& rbc) {
        UErrorCode status = U_ZERO_ERROR;
        CollationElementIterator*  ceIterator = rbc.createCollationElementIterator( ustring_ );
        if (!capacity_) {
            capacity_ = ustring_.length();
        }
        ces_ = new uint32_t[capacity_]; 
        uint32_t ce = 0; 
        do { 
            ce = ceIterator->next(status); 
            if ((length_ == capacity_) || (ce == CollationElementIterator::NULLORDER)) 
                break;
            ces_[length_++] = ce;
        } while (ce != CollationElementIterator::NULLORDER); 
        delete ceIterator;
        if (U_FAILURE(status)) invalid = true;
    }

public:
    CEList(RuleBasedCollator& rbc, const UnicodeString & source, uint32_t capacity=0)
        : length_(0), capacity_(capacity), ustring_(source), invalid(false)
    {
        doCreateCEList(rbc);
    }

    ~CEList()
    {
        delete[] ces_;
    }
  
    uint32_t operator[](uint32_t offset)
    {
        return (offset < length_ )? ces_[offset]:0xffff; 
    }

    uint32_t length() { return length_;}
    uint32_t capacity() {return capacity_;}
    inline bool isInvalid() const { return invalid; }
};

inline unsigned mask(unsigned x) { return x & 1; }

unsigned unicodeEditDistanceV2(UnicodeString & left, UnicodeString & right, RuleBasedCollator& rbc)
{
    unsigned char i, j;

    doTrimRight(left);
    doTrimRight(right);

    unsigned leftLen = left.length();
    unsigned rightLen = right.length();

    if (leftLen > 255)
        leftLen = 255;

    if (rightLen > 255)
        rightLen = 255;

    if (leftLen == 0)
        return rightLen;

    if (rightLen == 0)
        return leftLen;

    CEList   leftCEs(rbc, left, leftLen);
    CEList   rightCEs(rbc, right, rightLen);
    if (leftCEs.isInvalid() || rightCEs.isInvalid())
        return DISTANCE_ON_ERROR;

    leftLen = leftCEs.length();
    rightLen = rightCEs.length();

    //Optimize the storage requirements by
    //i) Only storing two stripes
    //ii) Calculate, but don't store the row comparing against the null string
    unsigned char da[2][256];
    uint32_t r_0 = rightCEs[0];
    uint32_t l_0 = leftCEs[0];
    bool matched_l0 = false;
    for (j = 0; j < rightLen; j++)
    {
        if (rightCEs[j] == l_0) matched_l0 = true;
        da[0][j] = (matched_l0) ? j : j+1;
    }

    bool matched_r0 = (l_0 == r_0);
    for (i = 1; i < leftLen; i++)
    {
        uint32_t l_i = leftCEs[i];
        if (l_i == r_0)
            matched_r0 = true;

        byte da_i_0 = matched_r0 ? i : i+1;
        da[mask(i)][0] = da_i_0;
        byte da_i_prevj = da_i_0;
        for (j = 1; j < rightLen; j++)
        {
            uint32_t r_j = rightCEs[j];
            unsigned char next = (l_i == r_j) ? da[mask(i-1)][j-1] :
                        min3(da[mask(i-1)][j], da_i_prevj, da[mask(i-1)][j-1]) + 1;
            da[mask(i)][j] = next;
            da_i_prevj = next;
        }
    }

    return da[mask(leftLen-1)][rightLen-1];
}

//This could be further improved in the following ways:
// * Only use 2*radius bytes of temporary storage - I doubt it is worth it.
// * special case edit1 - you could use variables for the 6 interesting array elements, and get
//   rid of the array completely.  You could also unwind the first (and last iterations).
// * I suspect the early exit condition could be improved depending the lengths of the strings.
unsigned unicodeEditDistanceV3(UnicodeString & left, UnicodeString & right, unsigned radius, RuleBasedCollator& rbc)
{
    if (radius >= 255)
        return 255;

    doTrimRight(left);
    doTrimRight(right);

    unsigned leftLen = left.length();
    unsigned rightLen = right.length();

    unsigned minED = (leftLen < rightLen)? rightLen - leftLen: leftLen - rightLen;
    if (minED > radius)
        return minED;

    if (leftLen > 255)
        leftLen = 255;

    if (rightLen > 255)
        rightLen = 255;

    //Checking for leading common substrings actually slows the function down.
    if (leftLen == 0)
        return rightLen;

    if (rightLen == 0)
        return leftLen;

    CEList   leftCEs(rbc, left, leftLen);
    CEList   rightCEs(rbc, right, rightLen);
    leftLen = leftCEs.length();
    rightLen = rightCEs.length();

    /*
    This function applies two optimizations over the function above.
    a) Adding a charcter (next row) can at most decrease the edit distance by 1, so short circuit when
       we there is no possiblity of getting within the distance.
    b) We only need to evaluate the martix da[i-radius..i+radius][j-radius..j+radius]
       not taking into account values outside that range [can use max value to prevent access]
    */

    //Optimize the storage requirements by
    //i) Only storing two stripes
    //ii) Calculate, but don't store the row comparing against the null string
    unsigned char da[2][256];
    uint32_t r_0 = rightCEs[0];
    uint32_t l_0 = leftCEs[0];
    bool matched_l0 = false;
    for (unsigned char j = 0; j < rightLen; j++)
    {
        if (rightCEs[j] == l_0) matched_l0 = true;
        da[0][j] = (matched_l0) ? j : j+1;
    }

    bool matched_r0 = (l_0 == r_0);
    for (unsigned char i = 1; i < leftLen; i++)
    {
        uint32_t l_i = leftCEs[i];
        if (l_i == r_0)
            matched_r0 = true;

        byte da_i_0 = matched_r0 ? i : i+1;
        da[mask(i)][0] = da_i_0;
        byte da_i_prevj = da_i_0;
        unsigned low = i-radius;
        unsigned high = i+radius;
        unsigned first = (i > radius) ? low : 1;
        unsigned last = (high >= rightLen) ? rightLen : high +1;

        for (unsigned j = first; j < last; j++)
        {
            uint32_t r_j = rightCEs[j];
            unsigned char next = da[mask(i-1)][j-1];
            if (l_i != r_j)
            {
                if (j != low)
                {
                    if (next > da_i_prevj)
                        next = da_i_prevj;
                }
                if (j != high)
                {
                    byte da_previ_j = da[mask(i-1)][j];
                    if (next > da_previ_j)
                        next = da_previ_j;
                }
                next++;
            }
            da[mask(i)][j] = next;
            da_i_prevj = next;
        }

        // bail out early if ed can't possibly be <= radius
        // Only considering a strip down the middle of the matrix, so the maximum the score can ever be adjusted is 2xradius
        unsigned max_valid_score = 3*radius;

        // But maximum is also 1 for every difference in string length - comes in to play when close to the end.
        //In 32bit goes slower for radius=1 I suspect because running out of registers.  Retest in 64bit.
        if (radius > 1)
        {
            unsigned max_distance = radius + (leftLen - (i+1)) + (rightLen - last);
            if (max_valid_score > max_distance)
                max_valid_score = max_distance;
        }
        if (da_i_prevj > max_valid_score)
            return da_i_prevj;
    }

    return da[mask(leftLen-1)][rightLen-1];
}

//This function is based on the unicodeEditDistanceV3 to pickup optimizations;
// It replaces RuleBasedCollator with the CharacterIterator
unsigned unicodeEditDistanceV4(UnicodeString & left, UnicodeString & right, unsigned radius, BreakIterator* bi)
{
    if (radius >= 255)
        return 255;

    doTrimRight(left);
    doTrimRight(right);

    unsigned leftLen = left.length();
    unsigned rightLen = right.length();

    if (leftLen > 255)
        leftLen = 255;

    if (rightLen > 255)
        rightLen = 255;

    //Checking for leading common substrings actually slows the function down.
    if (leftLen == 0)
        return rightLen;

    if (rightLen == 0)
        return leftLen;

    UPCList leftCs(bi, left, leftLen);
    UPCList rightCs(bi, right, rightLen);
    if (leftCs.isInvalid() || rightCs.isInvalid())
        return DISTANCE_ON_ERROR;

    // get Unicode character lengths
    leftLen = leftCs.length();
    rightLen = rightCs.length();

    unsigned minED = (leftLen < rightLen)? rightLen - leftLen: leftLen - rightLen;
    if (minED > radius)
        return minED;

    /*
    This function applies two optimizations over the function above.
    a) Adding a character (next row) can at most decrease the edit distance by 1, so short circuit when
       we there is no possibility of getting within the distance.
    b) We only need to evaluate the matrix da[i-radius..i+radius][j-radius..j+radius]
       not taking into account values outside that range [can use max value to prevent access]
    */

    //Optimize the storage requirements by
    //i) Only storing two stripes
    //ii) Calculate, but don't store the row comparing against the null string
    unsigned char da[2][256];
    uint32_t rI_0 = 0;
    uint32_t lI_0 = 0;
    bool matched_l0 = false;
    for (unsigned char j = 0; j < rightLen; j++)
    {
        if (leftCs.equal(lI_0, rightCs, rI_0+j)) matched_l0 = true;
        da[0][j] = (matched_l0) ? j : j+1;
    }

    bool matched_r0 = leftCs.equal(lI_0, rightCs, rI_0);
    for (unsigned char i = 1; i < leftLen; i++)
    {
        uint32_t lI_i = i;
        if (leftCs.equal(lI_i, rightCs, rI_0))
            matched_r0 = true;

        byte da_i_0 = matched_r0 ? i : i+1;
        da[mask(i)][0] = da_i_0;
        byte da_i_prevj = da_i_0;
        unsigned low = i-radius;
        unsigned high = i+radius;
        unsigned first = (i > radius) ? low : 1;
        unsigned last = (high >= rightLen) ? rightLen : high +1;

        for (unsigned j = first; j < last; j++)
        {
            uint32_t rI_j = j;
            unsigned char next = da[mask(i-1)][j-1];
            if (!leftCs.equal(lI_i, rightCs, rI_j))
            {
                if (j != low)
                {
                    if (next > da_i_prevj)
                        next = da_i_prevj;
                }
                if (j != high)
                {
                    byte da_previ_j = da[mask(i-1)][j];
                    if (next > da_previ_j)
                        next = da_previ_j;
                }
                next++;
            }
            da[mask(i)][j] = next;
            da_i_prevj = next;
        }

        // bail out early if ed can't possibly be <= radius
        // Only considering a strip down the middle of the matrix, so the maximum the score can ever be adjusted is 2xradius
        unsigned max_valid_score = 3*radius;

        // But maximum is also 1 for every difference in string length - comes in to play when close to the end.
        //In 32bit goes slower for radius=1 I suspect because running out of registers.  Retest in 64bit.
        if (radius > 1)
        {
            unsigned max_distance = radius + (leftLen - (i+1)) + (rightLen - last);
            if (max_valid_score > max_distance)
                max_valid_score = max_distance;
        }
        if (da_i_prevj > max_valid_score)
            return da_i_prevj;
    }

    return da[mask(leftLen-1)][rightLen-1];
}

void translate(UnicodeString & source, UChar const * sear, unsigned searLen, UChar const * repl, unsigned replLen)
{
    UnicodeString search(sear, searLen);
    UnicodeString replace(repl, replLen);
    if (search.countChar32() != replace.countChar32() || source.isEmpty() || search.isEmpty() || replace.isEmpty())
    {
        return;
    }
    StringCharacterIterator it(source);
    source.remove();
    int32_t Beg = it.setToStart();
    while (Beg != it.endIndex())
    {
        int32_t x = search.lastIndexOf(it.current32());
        if (x == -1)
        {
            source.append(it.current32());
        }
        else
        {
            x = replace.moveIndex32(0, x);
            source.append(replace.char32At(x));
        }
        Beg = it.move32(1, CharacterIterator::kCurrent);
    }
    return;
}

void excludeNthWord(RuleBasedBreakIterator& bi, UnicodeString & source, unsigned n)
{
    bi.setText(source);
    int32_t idx = bi.first();
    int32_t wordidx = 0;
    unsigned wordBeginning = 0;
    while (idx != BreakIterator::DONE)
    {
        int breakType = bi.getRuleStatus();
        if (breakType != UBRK_WORD_NONE)
        {
            // Exclude spaces, punctuation, and the like.
            //   A status value UBRK_WORD_NONE indicates that the boundary does
            //   not start a word or number.
            if (++wordidx == n)
            {
                if (n == 1)
                {
                    wordBeginning = 0;
                }
                unsigned wordEnd;
                do
                {
                    wordEnd = idx;
                    idx = bi.next();
                } while (bi.getRuleStatus() == UBRK_WORD_NONE && idx != BreakIterator::DONE);
                source.removeBetween(wordBeginning, wordEnd);
                return;
            }
        }
        wordBeginning = idx;
        idx = bi.next();
    }
    if (!wordidx)
    {
        source.removeBetween(bi.first(), bi.last());
    }
}

UnicodeString getNthWord(RuleBasedBreakIterator& bi, UnicodeString const & source, unsigned n)
{
    UnicodeString word;
    if (!n) return word;
    bi.setText(source);
    int32_t start = bi.first();
    while (start != BreakIterator::DONE && n)  {
        int breakType = bi.getRuleStatus();
        if (breakType != UBRK_WORD_NONE) {        
            // Exclude spaces, punctuation, and the like. 
            //   A status value UBRK_WORD_NONE indicates that the boundary does
            //   not start a word or number.            
            //    
            n--;
            if (!n) {
                unsigned wordBegining = bi.preceding(start);
                unsigned wordEnd = bi.next();
                source.extractBetween(wordBegining, wordEnd, word);
            }
        } 
        start = bi.next();
    }  
    return word; 
}

unsigned doCountWords(RuleBasedBreakIterator& bi, UnicodeString const & source)
{
    bi.setText(source);
    int32_t start = bi.first();
    int32_t count = 0; 
    while (start != BreakIterator::DONE)  {
        int breakType = bi.getRuleStatus();
        if (breakType != UBRK_WORD_NONE) {        
            // Exclude spaces, punctuation, and the like. 
            //   A status value UBRK_WORD_NONE indicates that the boundary does
            //   not start a word or number.            
            //    
            ++count;
        } 
        start = bi.next();
    }  
    return count; 
}

static BreakIterator * createCharacterBreakIterator(const char * localename)
{
    UErrorCode status = U_ZERO_ERROR;
    Locale locale(localename);
    BreakIterator * cbi = (BreakIterator *)BreakIterator::createCharacterInstance(locale, status);
    if (U_FAILURE(status))
    {
        delete cbi;
        return NULL;
    }
    return cbi;
}
class CBILocale
{
public:
    CBILocale(char const * _locale) : locale(_locale)
    {
        cbi = createCharacterBreakIterator(locale);
    }
    ~CBILocale()
    {
        delete cbi;
    }
    BreakIterator * queryCharacterBreakIterator() const { return cbi; }
private:
    StringAttr locale;
    BreakIterator * cbi;
};

typedef MapStringTo<CBILocale, char const *> MapStrToCBI;
static MapStrToCBI * localeCBiMap;
static CriticalSection localeCBiCrit;

static BreakIterator * queryCharacterBreakIterator(const char * localename)
{
    if (!localename) localename = "";
    CriticalBlock b(localeCBiCrit);
    if (!localeCBiMap)
        localeCBiMap = new MapStrToCBI;
    CBILocale * loc = localeCBiMap->getValue(localename);
    if(!loc)
    {
        const char * normalizedlocale = localename;
        localeCBiMap->setValue(localename, normalizedlocale);
        loc = localeCBiMap->getValue(localename);
    }
    return loc->queryCharacterBreakIterator();
}

static RuleBasedCollator * createRBCollator(const char * localename)
{
    UErrorCode status = U_ZERO_ERROR;
    Locale locale(localename);
    RuleBasedCollator * rbc = (RuleBasedCollator *)RuleBasedCollator::createInstance(locale, status);
    rbc->setAttribute(UCOL_NORMALIZATION_MODE, UCOL_ON, status);
    if (U_FAILURE(status))
    {
        delete rbc;
        return NULL;
    }
    return rbc;
}

class RBCLocale
{
public:
    RBCLocale(char const * _locale) : locale(_locale)
    {
        rbc = createRBCollator(locale);
    }
    ~RBCLocale()
    {
        delete rbc;
    }
    RuleBasedCollator * queryCollator() const { return rbc; }
private:
    StringAttr locale;
    RuleBasedCollator * rbc;
};

typedef MapStringTo<RBCLocale, char const *> MapStrToRBC;
static MapStrToRBC * localeMap;
static CriticalSection localeCrit;

static RuleBasedCollator * queryRBCollator(const char * localename)
{
    if (!localename) localename = "";
    CriticalBlock b(localeCrit);
    if (!localeMap)
        localeMap = new MapStrToRBC;
    RBCLocale * loc = localeMap->getValue(localename);
    if(!loc)
    {
        //MORE: ECLRTL calls rtlGetNormalizedUnicodeLocaleName().  Should this be happening here?
        const char * normalizedlocale = localename;
        localeMap->setValue(localename, normalizedlocale);
        loc = localeMap->getValue(localename);
    }
    return loc->queryCollator();
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    delete localeMap;
    localeMap = NULL;
     delete localeCBiMap;
    localeCBiMap = NULL;
}

}//namespace

using namespace nsUnicodelib;

UNICODELIB_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

UNICODELIB_API void UNICODELIB_CALL ulUnicodeFilterOut(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit)
{
    UnicodeString const in(src, srcLen);
    UnicodeString const filter(hit, hitLen);
    UnicodeString out;
    StringCharacterIterator iter(in);
    for(iter.first32(); iter.hasNext(); iter.next32())
    {
        UChar32 c = iter.current32();
        if(filter.indexOf(c) == -1)
            out.append(c);
    }
    tgtLen = out.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    out.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeFilter(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit)
{
    UnicodeString const in(src, srcLen);
    UnicodeString const filter(hit, hitLen);
    UnicodeString out;
    StringCharacterIterator iter(in);
    for(iter.first32(); iter.hasNext(); iter.next32())
    {
        UChar32 c = iter.current32();
        if(filter.indexOf(c) != -1)
            out.append(c);
    }
    tgtLen = out.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    out.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeSubsOut(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned newCharLen, UChar const * newChar)
{
    UnicodeString out;
    if(newCharLen > 0)
    {
        UnicodeString const in(src, srcLen);
        UnicodeString const filter(hit, hitLen);
        UnicodeString const replaceString(newChar, newCharLen);
        UChar32 replace = replaceString.char32At(0);
        StringCharacterIterator iter(in);
        for(iter.first32(); iter.hasNext(); iter.next32())
        {
            UChar32 c = iter.current32();
            if(filter.indexOf(c) == -1)
                out.append(c);
            else
                out.append(replace);
        }
    }
    else
        out.append(src, srcLen);
    tgtLen = out.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    out.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeSubs(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned newCharLen, UChar const * newChar)
{
    UnicodeString out;
    if(newCharLen > 0)
    {
        UnicodeString const in(src, srcLen);
        UnicodeString const filter(hit, hitLen);
        UnicodeString const replaceString(newChar, newCharLen);
        UChar32 replace = replaceString.char32At(0);
        StringCharacterIterator iter(in);
        for(iter.first32(); iter.hasNext(); iter.next32())
        {
            UChar32 c = iter.current32();
            if(filter.indexOf(c) != -1)
                out.append(c);
            else
                out.append(replace);
        }
    }
    else
        out.append(src, srcLen);
    tgtLen = out.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    out.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeRepad(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned tLen)
{
    UnicodeString out(src, srcLen);
    out.trim();
    forceLength(out, tLen);
    tgtLen = out.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    out.extract(0, tgtLen, tgt);
}

UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeFind(unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned instance)
{
    return ulUnicodeLocaleFind(srcLen, src, hitLen, hit, instance, "");
}

UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeLocaleFind(unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned instance, char const * localename)
{
    UErrorCode error = U_ZERO_ERROR;
    UStringSearch * search = usearch_open(hit, hitLen, src, srcLen, localename, 0, &error);
    int32_t pos;
    for(pos = usearch_first(search, &error); pos != USEARCH_DONE; pos = usearch_next(search, &error))
    {
        if(!--instance)
        {
            usearch_close(search);
            return pos+1;
        }
    }
    usearch_close(search);
    return 0;
}

UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeLocaleFindAtStrength(unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned instance, char const * localename, char strength)
{
    //Very strange behaviour - if source or pattern lengths are 0 search->getCollator() is invalid
    if (srcLen == 0 || hitLen == 0)
        return 0;
    UnicodeString const source(src, srcLen);
    UnicodeString const pattern(hit, hitLen);
    UErrorCode error = U_ZERO_ERROR;
    Locale locale(localename);
    StringSearch search(pattern, source, locale, 0, error);
    doModifySearchStrength(search, strength, error);
    int32_t pos = search.first(error);
    while(pos != USEARCH_DONE)
    {
        if(!--instance)
            return pos+1;
        pos = search.next(error);
    }
    return 0;
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeExtract(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned instance){
    UnicodeString const in(src, srcLen);
    UnicodeString out;
    if(extract(out, in, instance))
    {
        tgtLen = out.length();
        tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
        out.extract(0, tgtLen, tgt);
    }
    else
    {
        tgtLen = 0;
        tgt = 0;
    }
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeExtract50(UChar *tgt, unsigned srcLen, UChar const * src, unsigned instance)
{
    UnicodeString const in(src, srcLen);
    UnicodeString out;
    extract(out, in, instance);
    forceLength(out, 50);
    out.extract(0, 50, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeToLowerCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString unicode(src, srcLen);
    unicode.toLower();
    tgtLen = unicode.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    unicode.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeToUpperCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString unicode(src, srcLen);
    unicode.toUpper();
    tgtLen = unicode.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    unicode.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeToProperCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString unicode(src, srcLen);
    unicode.toTitle(0);
    tgtLen = unicode.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    unicode.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeToLowerCase80(UChar * tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString unicode(src, srcLen);
    unicode.toLower();
    forceLength(unicode, 80);
    unicode.extract(0, 80, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeToUpperCase80(UChar * tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString unicode(src, srcLen);
    unicode.toUpper();
    forceLength(unicode, 80);
    unicode.extract(0, 80, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeToProperCase80(UChar * tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString unicode(src, srcLen);
    unicode.toTitle(0);
    forceLength(unicode, 80);
    unicode.extract(0, 80, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToLowerCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, char const * localename)
{
    UnicodeString unicode(src, srcLen);
    Locale locale(localename);
    unicode.toLower(locale);
    tgtLen = unicode.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    unicode.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToUpperCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, char const * localename)
{
    UnicodeString unicode(src, srcLen);
    Locale locale(localename);
    unicode.toUpper(locale);
    tgtLen = unicode.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    unicode.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToProperCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, char const * localename)
{
    UnicodeString unicode(src, srcLen);
    Locale locale(localename);
    unicode.toTitle(0, locale);
    tgtLen = unicode.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    unicode.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToLowerCase80(UChar * tgt, unsigned srcLen, UChar const * src, char const * localename)
{
    UnicodeString unicode(src, srcLen);
    Locale locale(localename);
    unicode.toLower(locale);
    forceLength(unicode, 80);
    unicode.extract(0, 80, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToUpperCase80(UChar * tgt, unsigned srcLen, UChar const * src, char const * localename)
{
    UnicodeString unicode(src, srcLen);
    Locale locale(localename);
    unicode.toUpper(locale);
    forceLength(unicode, 80);
    unicode.extract(0, 80, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToProperCase80(UChar * tgt, unsigned srcLen, UChar const * src, char const * localename)
{
    UnicodeString unicode(src, srcLen);
    Locale locale(localename);
    unicode.toTitle(0, locale);
    forceLength(unicode, 80);
    unicode.extract(0, 80, tgt);
}
UNICODELIB_API int UNICODELIB_CALL ulUnicodeCompareIgnoreCase(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2)
{
    return doUnicodeCompareAtStrength(src1Len, src1, src2Len, src2, Collator::SECONDARY);
}

UNICODELIB_API int UNICODELIB_CALL ulUnicodeCompareAtStrength(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2, char strength)
{
    switch(strength)
    {
    case 1:
        return doUnicodeCompareAtStrength(src1Len, src1, src2Len, src2, Collator::PRIMARY);
    case 2:
        return doUnicodeCompareAtStrength(src1Len, src1, src2Len, src2, Collator::SECONDARY);
    case 3:
        return doUnicodeCompareAtStrength(src1Len, src1, src2Len, src2, Collator::TERTIARY);
    case 4:
        return doUnicodeCompareAtStrength(src1Len, src1, src2Len, src2, Collator::QUATERNARY);
    case 5:
    default:
        return doUnicodeCompareAtStrength(src1Len, src1, src2Len, src2, Collator::IDENTICAL);
    }
}

UNICODELIB_API int UNICODELIB_CALL ulUnicodeLocaleCompareIgnoreCase(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2, char const * localename)
{
    return doUnicodeLocaleCompareAtStrength(src1Len, src1, src2Len, src2, localename, Collator::SECONDARY);
}

UNICODELIB_API int UNICODELIB_CALL ulUnicodeLocaleCompareAtStrength(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2, char const * localename, char strength)
{
    switch(strength)
    {
    case 1:
        return doUnicodeLocaleCompareAtStrength(src1Len, src1, src2Len, src2, localename, Collator::PRIMARY);
    case 2:
        return doUnicodeLocaleCompareAtStrength(src1Len, src1, src2Len, src2, localename, Collator::SECONDARY);
    case 3:
        return doUnicodeLocaleCompareAtStrength(src1Len, src1, src2Len, src2, localename, Collator::TERTIARY);
    case 4:
        return doUnicodeLocaleCompareAtStrength(src1Len, src1, src2Len, src2, localename, Collator::QUATERNARY);
    case 5:
    default:
        return doUnicodeLocaleCompareAtStrength(src1Len, src1, src2Len, src2, localename, Collator::IDENTICAL);
    }
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeReverse(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString in(src, srcLen);
    UnicodeString out;
    StringCharacterIterator iter(in);
    for(iter.last32(); iter.hasPrevious(); iter.previous32())
        out.append(iter.current32());
    if(srcLen) out.append(iter.current32());
    tgtLen = out.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    out.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeFindReplace(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok)
{
    UnicodeString source(src, srcLen);
    UnicodeString const pattern(stok, stokLen);
    UnicodeString const replace(rtok, rtokLen);
    source.findAndReplace(pattern, replace);
    tgtLen = source.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    source.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleFindReplace(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok, char const * localename)
{
    UnicodeString source(src, srcLen);
    UnicodeString const pattern(stok, stokLen);
    UnicodeString const replace(rtok, rtokLen);
    doUnicodeLocaleFindReplace(source, pattern, replace, localename);
    tgtLen = source.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    source.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleFindAtStrengthReplace(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok, char const * localename, char strength)
{
    UnicodeString source(src, srcLen);

    //Very strange behaviour - if source or pattern lengths are 0 search->getCollator() is invalid
    if (srcLen && stokLen)
    {
        UnicodeString const pattern(stok, stokLen);
        UnicodeString const replace(rtok, rtokLen);
        doUnicodeLocaleFindAtStrengthReplace(source, pattern, replace, localename, strength);
    }

    tgtLen = source.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    source.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeFindReplace80(UChar * tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok)
{
    UnicodeString source(src, srcLen);
    UnicodeString const pattern(stok, stokLen);
    UnicodeString const replace(rtok, rtokLen);
    source.findAndReplace(pattern, replace);
    forceLength(source, 80);
    source.extract(0, 80, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleFindReplace80(UChar * tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok, char const * localename)
{
    UnicodeString source(src, srcLen);
    UnicodeString const pattern(stok, stokLen);
    UnicodeString const replace(rtok, rtokLen);
    doUnicodeLocaleFindReplace(source, pattern, replace, localename);
    forceLength(source, 80);
    source.extract(0, 80, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleFindAtStrengthReplace80(UChar * tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok, char const * localename, char strength)
{
    UnicodeString source(src, srcLen);
    UnicodeString const pattern(stok, stokLen);
    UnicodeString const replace(rtok, rtokLen);
    doUnicodeLocaleFindAtStrengthReplace(source, pattern, replace, localename, strength);
    forceLength(source, 80);
    source.extract(0, 80, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeCleanSpaces(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString source(src, srcLen);
    doUnicodeCleanSpaces(source);
    tgtLen = source.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    source.extract(0, tgtLen, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeCleanSpaces25(UChar * tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString source(src, srcLen);
    doUnicodeCleanSpaces(source);
    forceLength(source, 25);
    source.extract(0, 25, tgt);
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeCleanSpaces80(UChar * tgt, unsigned srcLen, UChar const * src)
{
    UnicodeString source(src, srcLen);
    doUnicodeCleanSpaces(source);
    forceLength(source, 80);
    source.extract(0, 80, tgt);
}


UNICODELIB_API bool UNICODELIB_CALL ulUnicodeWildMatch(unsigned srcLen, UChar const * src, unsigned patLen, UChar const * pat, bool noCase)
{
    return wildTrimMatch<UChar, u16toupper, u16query, u16asterisk, u16space>(src, srcLen, pat, patLen, noCase);
}

UNICODELIB_API bool UNICODELIB_CALL ulUnicodeContains(unsigned srcLen, UChar const * src, unsigned patLen, UChar const * pat, bool noCase)
{
    UnicodeString source(src, srcLen);
    UnicodeString pattern(pat, patLen);
    if(noCase)
    {
        source.foldCase();
        pattern.foldCase();
    }
    StringCharacterIterator iter(pattern);
    for(iter.first32(); iter.hasNext(); iter.next32())
        if(source.indexOf(iter.current32()) == -1)
            return false;
    return true;
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeCleanAccents(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src)
{
    if (!deAccenter)
    {
        CriticalBlock b(accenterCrit);
        if (!deAccenter)
        {
            UErrorCode lStatus = U_ZERO_ERROR;
            deAccenter = icu::Transliterator::createInstance("NFD; [:M:] Remove; NFC;", UTRANS_FORWARD, lStatus);
        }
    }
    UnicodeString source(src, srcLen);
    deAccenter->transliterate(source);
    tgtLen = source.length();
    tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
    source.extract(0, tgtLen, tgt);
}


UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeLocaleEditDistance(unsigned leftLen, UChar const * left, unsigned rightLen, UChar const * right, char const * localename)
{
    BreakIterator* bi = 0;
    if (localename && *localename)
    {
        bi = queryCharacterBreakIterator(localename);
        if (!bi)
            return DISTANCE_ON_ERROR;
    }

    UnicodeString uLeft(false, left, leftLen); // Readonly-aliasing UChar* constructor.
    UnicodeString uRight(false, right, rightLen);

    unsigned distance = nsUnicodelib::unicodeEditDistanceV4(uLeft, uRight, 254, bi);
    return distance;
}


UNICODELIB_API bool UNICODELIB_CALL ulUnicodeLocaleEditDistanceWithinRadius(unsigned leftLen, UChar const * left, unsigned rightLen, UChar const * right, unsigned radius, char const * localename)
{
    BreakIterator* bi = 0;
    if (localename && *localename)
    {
        bi = queryCharacterBreakIterator(localename);
        if (!bi)
            return false;
    }

    UnicodeString uLeft(false, left, leftLen); // Readonly-aliasing UChar* constructor.
    UnicodeString uRight(false, right, rightLen);

    unsigned distance = nsUnicodelib::unicodeEditDistanceV4(uLeft, uRight, radius, bi);
    return distance <= radius;
}

UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeLocaleWordCount(unsigned textLen, UChar const * text,  char const * localename)
{
    UErrorCode status = U_ZERO_ERROR;  
    Locale locale(localename);
    RuleBasedBreakIterator* bi = (RuleBasedBreakIterator*)RuleBasedBreakIterator::createWordInstance(locale, status);
    UnicodeString uText(text, textLen);
    uText.trim();
    unsigned count = doCountWords(*bi, uText);
    delete bi;
    return count;
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleGetNthWord(unsigned & tgtLen, UChar * & tgt, unsigned textLen, UChar const * text, unsigned n, char const * localename)
{
    UErrorCode status = U_ZERO_ERROR;  
    Locale locale(localename);
    RuleBasedBreakIterator* bi = (RuleBasedBreakIterator*)RuleBasedBreakIterator::createWordInstance(locale, status);

    UnicodeString uText(text, textLen);
    uText.trim();
    UnicodeString word = getNthWord(*bi, uText, n);
    delete bi;
    if(word.length()>0)
    {
        tgtLen = word.length();
        tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
        word.extract(0, tgtLen, tgt);
    }
    else
    {
        tgtLen = 0;
        tgt = 0;
    }
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleExcludeNthWord(unsigned & tgtLen, UChar * & tgt, unsigned textLen, UChar const * text, unsigned n, char const * localename)
{
    UErrorCode status = U_ZERO_ERROR;
    Locale locale(localename);
    RuleBasedBreakIterator* bi = (RuleBasedBreakIterator*)RuleBasedBreakIterator::createWordInstance(locale, status);
    UnicodeString processed(text, textLen);
    excludeNthWord(*bi, processed, n);
    delete bi;
    if (processed.length()>0)
    {
        tgtLen = processed.length();
        tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen*2);
        processed.extract(0, tgtLen, tgt);
    }
    else
    {
        tgtLen = 0;
        tgt = 0;
    }
}

UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleTranslate(unsigned & tgtLen, UChar * & tgt, unsigned textLen, UChar const * text, unsigned searLen, UChar const * sear, unsigned replLen, UChar * repl, char const * localename)
{
    UErrorCode status = U_ZERO_ERROR;
    UnicodeString source(text, textLen);
    translate(source, sear, searLen, repl, replLen);
    if (source.length()>0)
    {
        tgtLen = source.length();
        tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen * 2);
        source.extract(0, tgtLen, tgt);
    }
    else
    {
        tgtLen = 0;
        tgt = 0;
    }
}