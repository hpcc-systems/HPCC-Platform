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
        read(0,&ch,1);

    return ch;
#else
    return ::_getch();
#endif
}
