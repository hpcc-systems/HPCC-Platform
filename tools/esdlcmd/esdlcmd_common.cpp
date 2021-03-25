/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#include <stdio.h>
#include "jlog.hpp"
#include "jfile.hpp"
#include "jargv.hpp"
#include "junicode.hpp"
#include "esdlcmdutils.hpp"

#include "esdlcmd_common.hpp"

void outputMultiExceptions(const IMultiException &me)
{
    fprintf(stderr, "\nException(s):\n");
    aindex_t count = me.ordinality();
    for (aindex_t i=0; i<count; i++)
    {
        IException& e = me.item(i);
        StringBuffer msg;
        fprintf(stderr, "%d: %s\n", e.errorCode(), e.errorMessage(msg).str());
    }
    fprintf(stderr, "\n");
}
static inline void appendFileName(StringBuffer &path, const char *filename)
{
    if (!filename)
        return;
    while (*filename==PATHSEPCHAR)
        filename++;
    if (!*filename)
        return;
    if (path.length() && path.charAt(path.length()-1) != PATHSEPCHAR)
        path.append(PATHSEPCHAR);
    path.append(filename);
}
void saveAsFile(const char * filepath, const char *filename, const char *text, const char *ext)
{
    StringBuffer path(filepath);

    appendFileName(path, filename);
    if(ext && *ext)
        path.append(ext);

    Owned<IFile> file = createIFile(path.str());
    Owned<IFileIO> io = file->open(IFOcreate);

    DBGLOG("Writing to file %s", file->queryFilename());

    if (io.get())
        io->write(0, strlen(text), text);
    else
        DBGLOG("File %s can't be created", file->queryFilename());
}

//=========================================================================================

#define PE_OFFSET_LOCATION_IN_DOS_SECTION 0x3C

esdlCmdOptionMatchIndicator EsdlCmdCommon::matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
{
    bool boolValue;
    if (iter.matchFlag(boolValue, ESDLOPT_HELP))
    {
        usage();
        return EsdlCmdOptionCompletion;
    }
    if (iter.matchFlag(boolValue, ESDLOPT_VERSION))
    {
        fprintf(stdout, "%s\n", hpccBuildTag);
        return EsdlCmdOptionCompletion;
    }

    if (iter.matchFlag(optVerbose, ESDL_OPTION_VERBOSE) || iter.matchFlag(optVerbose, ESDL_OPT_VERBOSE))
    {
        return EsdlCmdOptionMatch;
    }

    StringAttr traceCategories;
    if (iter.matchOption(traceCategories, ESDL_OPTION_TRACE_CATEGORY) || iter.matchOption(traceCategories, ESDL_OPT_TRACE_CATEGORY))
    {
        parseTraceFlags(traceCategories);
        return EsdlCmdOptionMatch;
    }

    StringAttr tempArg;
    if (iter.matchOption(tempArg, "-brk"))
    {
#if defined(_WIN32) && defined(_DEBUG)
        unsigned id = atoi(tempArg.str());
        if (id == 0)
            DebugBreak();
        else
            _CrtSetBreakAlloc(id);
#endif
        return EsdlCmdOptionMatch;
    }
    if (finalAttempt)
    {
        fprintf(stderr, "\n%s option not recognized\n", iter.query());
        usage();
    }
    return EsdlCmdOptionNoMatch;
}

void EsdlCmdCommon::parseTraceFlags(const char* traceCategories)
{
    m_optTraceFlagsGiven = true;
    if (!isEmptyString(traceCategories))
    {
        using Category = std::pair<const char*, IEsdlDefReporter::Flags>;
        using CategoryMap = std::list<Category>;
        static const CategoryMap definedCategories({
            { ESDL_TRACE_CATEGORY_DEVELOPER, IEsdlDefReporter::ReportDeveloperAudience },
            { ESDL_TRACE_CATEGORY_OPERATOR, IEsdlDefReporter::ReportOperatorAudience },
            { ESDL_TRACE_CATEGORY_USER, IEsdlDefReporter::ReportUserAudience },
            { ESDL_TRACE_CATEGORY_ERROR, IEsdlDefReporter::ReportErrorClass },
            { ESDL_TRACE_CATEGORY_WARNING, IEsdlDefReporter::ReportWarningClass },
            { ESDL_TRACE_CATEGORY_PROGRESS, IEsdlDefReporter::ReportProgressClass },
            { ESDL_TRACE_CATEGORY_INFO, IEsdlDefReporter::ReportInfoClass },
            { ESDL_TRACE_CATEGORY_IERROR, IEsdlDefReporter::ReportIError },
            { ESDL_TRACE_CATEGORY_OERROR, IEsdlDefReporter::ReportOError },
            { ESDL_TRACE_CATEGORY_UERROR, IEsdlDefReporter::ReportUError },
            { ESDL_TRACE_CATEGORY_IWARNING, IEsdlDefReporter::ReportIWarning },
            { ESDL_TRACE_CATEGORY_OWARNING, IEsdlDefReporter::ReportOWarning },
            { ESDL_TRACE_CATEGORY_UWARNING, IEsdlDefReporter::ReportUWarning },
            { ESDL_TRACE_CATEGORY_DPROGRESS, IEsdlDefReporter::ReportDProgress },
            { ESDL_TRACE_CATEGORY_OPROGRESS, IEsdlDefReporter::ReportOProgress },
            { ESDL_TRACE_CATEGORY_UPROGRESS, IEsdlDefReporter::ReportUProgress },
            { ESDL_TRACE_CATEGORY_DINFO, IEsdlDefReporter::ReportDInfo },
            { ESDL_TRACE_CATEGORY_OINFO, IEsdlDefReporter::ReportOInfo },
            { ESDL_TRACE_CATEGORY_UINFO, IEsdlDefReporter::ReportUInfo },
        });
        StringArray categories;
        categories.appendList(traceCategories, ",");
        for (aindex_t idx = 0; idx < categories.ordinality(); idx++) // not using ForEachItemIn because ordinality may change
        {
            const char* category = categories.item(idx);
            CategoryMap::const_iterator it = std::find_if(definedCategories.begin(), definedCategories.end(), [category](const Category& entry) {
                return strieq(category, entry.first);
            });

            if (it != definedCategories.end())
            {
                m_actualTraceFlags |= it->second;
            }
        }
    }
}

bool EsdlCmdCommon::finalizeOptions(IProperties *globals)
{
    if (!optVerbose)
    {
        Owned<ILogMsgFilter> filter = getCategoryLogMsgFilter(MSGAUD_user, MSGCLS_error);
        queryLogMsgManager()->changeMonitorFilter(queryStderrLogMsgHandler(), filter);
    }

    return true;
}

bool EsdlConvertCmd::parseCommandLineOptions(ArgvIterator &iter)
{
    if (iter.done())
    {
        usage();
        return false;
    }

    for (; !iter.done(); iter.next())
    {
        if (parseCommandLineOption(iter))
            continue;

        if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
            return false;
    }

    return true;
}

bool EsdlConvertCmd::parseCommandLineOption(ArgvIterator &iter)
{
    if (iter.matchOption(optSource, ESDL_CONVERT_SOURCE))
        return true;
    if (iter.matchOption(optOutDirPath, ESDL_CONVERT_OUTDIR))
        return true;
    StringAttr oneOption;
    if (iter.matchOption(oneOption, ESDLOPT_INCLUDE_PATH) || iter.matchOption(oneOption, ESDLOPT_INCLUDE_PATH_S))
    {
        if(optIncludePath.length() > 0)
            optIncludePath.append(ENVSEPSTR);
        optIncludePath.append(oneOption.get());
        return true;
    }
    return false;
}

esdlCmdOptionMatchIndicator EsdlConvertCmd::matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
{
    if (iter.matchOption(optSource, ESDL_CONVERT_SOURCE))
      return EsdlCmdOptionMatch;
    if (iter.matchOption(optOutDirPath, ESDL_CONVERT_OUTDIR))
      return EsdlCmdOptionMatch;

    return EsdlCmdCommon::matchCommandLineOption(iter, true);
}

bool EsdlConvertCmd::finalizeOptions(IProperties *globals)
{
    extractEsdlCmdOption(optIncludePath, globals, ESDLOPT_INCLUDE_PATH_ENV, ESDLOPT_INCLUDE_PATH_INI, NULL, NULL);

    if (optSource.isEmpty())
    {
        fprintf(stderr, "\nError: Source esdl parameter required\n");
        return false;
    }

    if (optOutDirPath.isEmpty())
    {
        fprintf(stderr, "\nError: Target output directory path parameter required\n");
        return false;
    }

    return EsdlCmdCommon::finalizeOptions(globals);
}
