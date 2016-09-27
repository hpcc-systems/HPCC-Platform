/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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
#ifndef __ESDL2XML_HPP__
#define __ESDL2XML_HPP__

#include "jhash.hpp"
#include "esdlcomp.h"

typedef MapStringTo<bool> AddedHash;

class Esdl2Esxdl : public CInterface
{
public:
    Esdl2Esxdl()
    {
        optRecursive = false;
        optVerbose   = false;
    }

    Esdl2Esxdl(bool recursive, bool verbose)
    {
        optRecursive = recursive;
        optVerbose   = verbose;
    }

    void setRecursive(bool recursive){optRecursive = recursive;};
    bool getRecursive(){return optRecursive;};

    void setVerbose(bool verbose){optVerbose = verbose;};
    bool getVerbose(){return optVerbose;};

    void transform(const char * source, const char * outdir="", StringBuffer * out=NULL, bool outputIncludes=true, bool includedESDL=false)
    {
        if (!added.getValue(source))
        {
            if (optVerbose)
            {
                fprintf(stdout, "Processing ESDL definition: %s\n", source);
                if (out==NULL && (!outdir || !*outdir))
                    fprintf(stdout, "Output directory not specified\n");
            }

            //prevent recursive adding of this same content
            added.setValue(source, true);

            ESDLcompiler hc(source, out==NULL, outdir, outputIncludes, includedESDL);
            hc.Process();

            if (optRecursive && hc.includes)
            {
                StringBuffer subfile;
                StringBuffer srcDir = hc.getSrcDir();

                IncludeInfo * ii;
                for (ii=hc.includes;ii;ii=ii->next)
                {
                   subfile.setf("%s%s%s", srcDir.str(), ii->pathstr.str(), ESDL_FILE_EXTENSION);
                   transform(subfile, outdir, out, outputIncludes, true);
                }
            }

            if (out != NULL) //includes must come before body to handle ESDLDefVersions correctly
                out->append(hc.getEsxdlContent());

            if (optVerbose)
                fprintf(stdout, "Finished processing ESDL definition: %s\n", source);
        }
        else if (optVerbose)
            fprintf(stdout, "ESDL definition: %s has already been loaded!\n", source);
    }

protected:
    bool      optRecursive;
    bool      optVerbose;

    AddedHash added;
};

#endif
