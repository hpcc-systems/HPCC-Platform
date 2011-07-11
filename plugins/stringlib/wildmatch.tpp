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
