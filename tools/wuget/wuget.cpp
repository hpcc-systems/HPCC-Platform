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

#include "jlib.hpp"
#include "thorplugin.hpp"

void usage(const char *progname)
{
    fprintf(stderr, "%s - extract workunit information from query dll.\nUsage: %s filename\n", progname, progname);
}

int main(int argc, char **argv)
{
    bool doSoapInfo = false;
    bool doWorkunit = false;
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0]=='-')
        {
            if (strcmp(argv[i], "-s")==0)
                doSoapInfo = true;
            else if (strcmp(argv[i], "-w")==0)
                doWorkunit = true;
            else
            {
                usage(argv[0]);
                return 1;
            }
        }
    }
    int filesSeen = 0;
    int errors = 0;
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0]!='-')
        {
            filesSeen++;
            if (doWorkunit || !doSoapInfo)
            {
                StringBuffer xml;
                if (getWorkunitXMLFromFile(argv[i], xml))
                {
                    printf("%s\n", xml.str());
                }
                else
                {
                    fprintf(stderr, "Could not load workunit from %s\n", argv[i]);
                    errors++;
                }
            }
            if (doSoapInfo)
            {
                StringBuffer xml;
                if (getSoapInfoXMLFromFile(argv[i], xml))
                {
                    printf("%s\n", xml.str());
                }
                else
                {
                    fprintf(stderr, "Could not load soapInfo from %s\n", argv[i]);
                    errors++;
                }
            }
        }
    }
    if (!filesSeen)
    {
        fprintf(stderr, "No files specified\n");
        errors++;
    }
    return errors ? 1 : 0;
}
