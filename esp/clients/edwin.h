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

#ifndef __EDWIN_H__
#define __EDWIN_H__

typedef int (*PFN_THUNK_CLIENT)(void *data);

#ifndef EDWIN_OFF_

#ifdef _WIN32

void *GetThunkingHandle();

void ThunkToClientThread(void *hThunk, PFN_THUNK_CLIENT fn, void *data);

int GetResourceData(const char *restype, int resid, void *&data, unsigned &len);

#else

inline void *GetThunkingHandle()
{
    return 0;
}

inline void ThunkToClientThread(void *hThunk, PFN_THUNK_CLIENT fn, void *data)
{
    fn(data);
}

inline int GetResourceData(const char *restype, int resid, void *&data, unsigned &len)
{
    data=0;
    len=0;
    return 0;
}

#endif ////WIN32

#endif //EDWIN_OFF_




#endif //__EDWIN_H__
