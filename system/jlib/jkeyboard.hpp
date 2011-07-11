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



#ifndef JKEYBOARD_HPP
#define JKEYBOARD_HPP

#include "jutil.hpp"
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


