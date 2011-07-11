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

#ifndef __THORRREGEX_HPP_
#define __THORRREGEX_HPP_

#ifdef _WIN32
 #ifdef THORHELPER_EXPORTS
  #define THORHELPER_API __declspec(dllexport)
 #else
  #define THORHELPER_API __declspec(dllimport)
 #endif
#else
 #define THORHELPER_API
#endif

#define RegexSpecialMask                    0xF0000000
#define RegexSpecialCharacterClass          0x10000000
#define RegexSpecialCollationClass          0x20000000
#define RegexSpecialEquivalenceClass        0x30000000

#define RCCalnum                            (RegexSpecialCharacterClass|0)
#define RCCcntrl                            (RegexSpecialCharacterClass|1)
#define RCClower                            (RegexSpecialCharacterClass|2)
#define RCCupper                            (RegexSpecialCharacterClass|3)
#define RCCspace                            (RegexSpecialCharacterClass|4)
#define RCCalpha                            (RegexSpecialCharacterClass|5)
#define RCCdigit                            (RegexSpecialCharacterClass|6)
#define RCCprint                            (RegexSpecialCharacterClass|7)
#define RCCblank                            (RegexSpecialCharacterClass|8)
#define RCCgraph                            (RegexSpecialCharacterClass|9)
#define RCCpunct                            (RegexSpecialCharacterClass|10)
#define RCCxdigit                           (RegexSpecialCharacterClass|11)
#define RCCany                              (RegexSpecialCharacterClass|12)
#define RCCutf8lead                         (RegexSpecialCharacterClass|13)
#define RCCutf8follow                       (RegexSpecialCharacterClass|14)
                
#define PATTERN_UNLIMITED_LENGTH            ((unsigned)-1)


#endif /* __THORREGEX_HPP_ */
