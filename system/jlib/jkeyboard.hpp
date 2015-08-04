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



#ifndef JKEYBOARD_HPP
#define JKEYBOARD_HPP

#include "jlib.hpp"
#ifdef _WIN32
#include <conio.h>
#else
#include <termios.h>
#endif


class jlib_decl keyboard
{
public:
    keyboard();
    ~keyboard();
    int kbhit();
    int getch();
private:
#ifndef _WIN32
    struct termios initial_settings, new_settings;
    int peek_character;
#endif
};


#endif //JKEYBOARD_HPP


