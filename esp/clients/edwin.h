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
