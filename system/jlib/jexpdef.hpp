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


#ifdef _WIN32
 #ifdef JLIB_EXPORTS
  #define jlib_decl __declspec(dllexport)
  #define jlib_thrown_decl __declspec(dllexport)
 #else
  #define jlib_decl __declspec(dllimport)
  #define jlib_thrown_decl __declspec(dllimport)
 #endif
#else
#if __GNUC__ >= 4
  #define jlib_decl  __attribute__ ((visibility("default")))
  #define jlib_thrown_decl __attribute__ ((visibility("default")))
#else
 #define jlib_decl
 #define jlib_thrown_decl 
#endif
#endif
