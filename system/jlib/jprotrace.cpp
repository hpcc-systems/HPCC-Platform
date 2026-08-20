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
#include "jprop.hpp"
#include "jprotrace.hpp"
#include "jexcept.hpp"

#ifdef _USE_PROTRACE
#include <protrace.h>
#endif

#ifdef _USE_PROTRACE
static StringBuffer exitTraceFilename;
static StringBuffer protraceComponent;
static bool writeTraceAtExit = false;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    protrace::note_thread(protrace::get_tid(), "main thread");
    return true;
}
MODULE_EXIT()
{
    protraceOnTerminate();
}
#endif

void protraceInitialize(const char * component, IPropertyTree * componentConfig)
{
#ifdef _USE_PROTRACE
    if (!componentConfig)
        return;

    protraceComponent.set(component);

    size_t protraceMemoryMB = 0;
    protrace::trace_options protraceOptions = (protrace::trace_options)0;
    bool startRecording = false;

    const IPropertyTree * protraceConfig = componentConfig->queryPropTree("protrace");
    if (protraceConfig)
    {
        protraceMemoryMB = protraceConfig->getPropInt("@memoryMB", 100);
        if (protraceConfig->getPropBool("@recordFromStart", false))
            startRecording = true;

        //Disable this code for the moment - it does not appear to be safe
#if 0
        if (protraceConfig->getPropBool("@writeTraceAtExit", false))
            writeTraceAtExit = true;
#endif

        // FUTURE: Check for options, and set values in the flags if provided
    }
    else
    {
        // backward compatibility - check for the old-style protrace options
        if (componentConfig->hasProp("@protrace"))
            protraceMemoryMB = 100;

        protraceMemoryMB = componentConfig->getPropInt("@protraceMemoryMB", protraceMemoryMB);
        startRecording = componentConfig->getPropBool("@protraceRecord", startRecording);
    }

    if (protraceMemoryMB == 0)
        return;

    unsigned protraceCpus = 0; // This parameter is to be deleted
    if (!protrace::init_protrace("hpcc", protraceMemoryMB * oneMB, protraceCpus, protraceOptions))
    {
        WARNLOG("Failed to initialize protrace (memory=%zuMB, cpus=%u options=%x)", protraceMemoryMB, protraceCpus, (unsigned)protraceOptions);
        return;
    }

    if (startRecording)
        protrace::resume();

    if (writeTraceAtExit)
        exitTraceFilename.clear().append("protrace_").append(component).append("_").append(GetCurrentProcessId()).append(".ustrc");
#endif
}

void protraceOnTerminate()
{
#ifdef _USE_PROTRACE
    if (writeTraceAtExit && exitTraceFilename.length() > 0)
    {
        writeTraceAtExit = false;
        try
        {
            protrace::save_events(exitTraceFilename.str());
        }
        catch (const std::exception & e)
        {
            OWARNLOG("Failed to save protrace events to %s: %s", exitTraceFilename.str(), e.what());
        }
    }
#endif
}

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
        outputFilename.appendf("protrace_%s_%" I64F "u", protraceComponent.str(), timestamp);
    }

    if (!pathExtension(outputFilename.str()))
        outputFilename.append(".kstrc");

    DBGLOG("Generate save file to %s", outputFilename.str());
    recursiveCreateDirectoryForFile(outputFilename.str());
    try
    {
        protrace::save_events(outputFilename.str());
    }
    catch (const std::exception & e)
    {
        throw makeStringExceptionV(0, "Failed to save protrace events to %s: %s", outputFilename.str(), e.what());
    }
#endif
}
