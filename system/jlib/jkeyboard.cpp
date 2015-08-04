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


#include "platform.h"
#include "jutil.hpp"
#include "jkeyboard.hpp"


keyboard::keyboard()
{
#ifndef _WIN32
    tcgetattr(0,&initial_settings);
    new_settings = initial_settings;
    new_settings.c_lflag &= ~ICANON;
    new_settings.c_lflag &= ~ECHO;
    new_settings.c_lflag &= ~ISIG;
    new_settings.c_cc[VMIN] = 1;
    new_settings.c_cc[VTIME] = 0;
    tcsetattr(0, TCSANOW, &new_settings);
    peek_character=-1;
#endif
}

keyboard::~keyboard()
{
#ifndef _WIN32
    tcsetattr(0, TCSANOW, &initial_settings);
#endif
}

int keyboard::kbhit()
{
#ifndef _WIN32
    unsigned char ch;
    int nread;

    if (peek_character != -1) return 1;
    new_settings.c_cc[VMIN]=0;
    tcsetattr(0, TCSANOW, &new_settings);
    nread = read(0,&ch,1);
    new_settings.c_cc[VMIN]=1;
    tcsetattr(0, TCSANOW, &new_settings);

    if (nread == 1) 
    {
        peek_character = ch;
        return 1;
    }
    return 0;
#else 
    return ::_kbhit();
#endif
}

int keyboard::getch()
{
#ifndef _WIN32
    char ch;

    if (peek_character != -1) 
    {
        ch = peek_character;
        peek_character = -1;
    } 
    else
    {
        if (read(0,&ch,1) == 0)
            return -1;
    }

    return ch;
#else
    return ::_getch();
#endif
}
