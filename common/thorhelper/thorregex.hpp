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

#ifndef __THORRREGEX_HPP_
#define __THORRREGEX_HPP_

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
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
