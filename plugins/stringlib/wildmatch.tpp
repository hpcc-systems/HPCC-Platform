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

template<typename C, C NORMALIZE_FN(C)>
inline bool mismatches(C left, C right, bool doNormalize)
{
    return doNormalize ? (NORMALIZE_FN(left)!=NORMALIZE_FN(right)) : (left!=right);
}


template<typename C, C NORMALIZE_FN(C), C QUERY, C ASTERISK>
bool doWildMatch(const C * src, unsigned srcLen, unsigned srcIdx, const C * pat, unsigned patLen, unsigned patIdx, bool doNormalize)
{
    while (patIdx < patLen)
    {
        C next_char = pat[patIdx++];
        switch(next_char)
        {
        case QUERY:
            if (srcIdx == srcLen)
                return false;
            srcIdx++;
            break;
        case ASTERISK:
            //ensure adjacent *s don't cause exponential search times.
            for (;;)
            {
                if (patIdx == patLen)
                    return true;
                if (pat[patIdx] != ASTERISK)
                    break;
                patIdx++;
            }
            
            //check for non wildcarded trailing text
            for (;;)
            {
                //No need to guard patLen since guaranteed to contain an ASTERISK
                const C tail_char = pat[patLen-1];
                if (tail_char == ASTERISK)
                    break;
                if (srcIdx == srcLen)
                    return false;
                if ((tail_char != QUERY) && mismatches<C, NORMALIZE_FN>(src[srcLen-1], tail_char, doNormalize))
                    return false;
                patLen--;
                srcLen--;
                if (patIdx == patLen)
                    return true;
            }
            //The remaining pattern must match at least one character
            while (srcIdx < srcLen) 
            {
                if (doWildMatch<C, NORMALIZE_FN, QUERY, ASTERISK>(src, srcLen, srcIdx, pat, patLen, patIdx, doNormalize))
                    return true;
                srcIdx++;
            }
            return false;
        default:
            if (srcIdx == srcLen)
                return false;
            if (mismatches<C, NORMALIZE_FN>(src[srcIdx], next_char, doNormalize))
                return false;
            srcIdx++;
            break;
        }
    }
    return (srcIdx == srcLen);
}

template<typename C, C NORMALIZE_FN(C), C QUERY, C ASTERISK, C BLANK>
inline bool wildTrimMatch(const C * src, unsigned srcLen, const C * pat, unsigned patLen, bool doNormalize)
{
    while (srcLen && src[srcLen - 1] == BLANK)
        --srcLen;
    while (patLen && pat[patLen - 1] == BLANK)
        --patLen;

    if (patLen == 0)
        return (srcLen == 0);

    return doWildMatch<C, NORMALIZE_FN, QUERY, ASTERISK>(src, srcLen, 0, pat, patLen, 0, doNormalize);
}

template<typename C, C NORMALIZE_FN(C), C QUERY, C ASTERISK>
inline bool wildMatch(const C * src, unsigned srcLen, const C * pat, unsigned patLen, bool doNormalize)
{
    if (patLen == 0)
        return (srcLen == 0);

    return doWildMatch<C, NORMALIZE_FN, QUERY, ASTERISK>(src, srcLen, 0, pat, patLen, 0, doNormalize);
}
