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

#ifndef GITFILE_HPP
#define GITFILE_HPP

#include "jfile.hpp"

#ifdef GITFILE_EXPORTS
#define GITFILE_API __declspec(dllexport)
#else
#define GITFILE_API __declspec(dllimport)
#endif


/*
 * Direct access to files in git repositories, by revision, without needing to check them out first
 * Installs hooks into createIFile, spotting filenames of the form /my/directory/.git/{revision}/path/within/git
 */

extern "C" {
  extern GITFILE_API void installFileHook();
  extern GITFILE_API void removeFileHook();
};

#endif
