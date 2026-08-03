/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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
#include "jmisc.hpp"
#include "jfile.hpp"
#include "jprotrace.hpp"

#ifdef _USE_PROTRACE
#include <protrace.h>
#endif

bool protraceResumeRecording()
{
#ifdef _USE_PROTRACE
    protrace::resume();
    return true;
#else
    return false;
#endif
}

bool protraceSuspendRecording()
{
#ifdef _USE_PROTRACE
    protrace::suspend();
    return true;
#else
    return false;
#endif
}

bool protraceClearRecording(bool clearMetadata)
{
#ifdef _USE_PROTRACE
    protrace::clear_events(clearMetadata);
    return true;
#else
    return false;
#endif
}

void protraceSaveRecording(StringBuffer & outputFilename, const char * filename)
{
    outputFilename.clear();

#ifdef _USE_PROTRACE
    if (!isAbsolutePath(filename))
    {
        getTempFilePath(outputFilename, "protrace", nullptr);
        outputFilename.append(PATHSEPCHAR);
    }

    if (!isEmptyString(filename))
        outputFilename.append(filename);
    else
    {
        __uint64 timestamp = getTimeStampNowValue();
        outputFilename.appendf("protrace_%" I64F "u", timestamp);
    }

    if (!pathExtension(outputFilename.str()))
        outputFilename.append(".kstrc");

    recursiveCreateDirectoryForFile(outputFilename.str());
    protrace::save_events(outputFilename.str());
#endif
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
#ifdef _USE_PROTRACE
    protrace::note_thread(protrace::get_tid(), "main thread");
#endif
    return true;
}
