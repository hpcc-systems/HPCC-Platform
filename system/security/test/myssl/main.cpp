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

#include "jliball.hpp"
#include "securesocket.hpp"
#ifdef _WIN32
#include <conio.h>
#else
#include <unistd.h>
#endif

char *myfgets(char *s, int size, FILE *stream)
{
    fgets(s, size, stream);
    int len = strlen(s);
    if(len > 0)
        s[len - 1] = '\0';

    return s;
}

void inputpassword(const char* prompt, StringBuffer& passwd)
{
    passwd.clear();
#ifdef _WIN32
    printf("%s", prompt);
    char input=0;
    short num_entries=0;
    while (0x0d != (input = (char)getch()))
    {
        if (input == '\b')
        {
            printf("\b \b");
            if (num_entries)
            { 
                num_entries--; 
            }
            continue;
        }
        passwd.append(input);
        num_entries++;
        printf("*");
     }
#else
    const char* pass = getpass(prompt);
    passwd.append(pass);
#endif
}

void getpassword(const char* prompt, StringBuffer& passwd)
{
    passwd.clear();

    StringBuffer passwd1, passwd2;
    int tries = 0;
    while(1)
    {
        if(tries++ >= 3)
        {
            exit(-1);
        }

        inputpassword(prompt, passwd1);
        inputpassword("\nVerifying password, retype: ", passwd2);
        if(passwd1.length() < 4)
        {
            printf("\npassword too short, should be 4 chars or longer\n");
        }
        else if(strcmp(passwd1.str(), passwd2.str()) != 0)
        {
            printf("\npasswords don't match.\n");
        }
        else
            break;
    }

    passwd.append(passwd1.str());

}


void usage()
{
    // For now this tool only generates x509 certificates.
    // New functions can be added.
    printf("usage: myssl [-crt|-csr|-sign] [options]\n");
    printf("-crt: create self-signed certificate and privatekey pair\n");
    printf("-csr: create certificate signing request, using an existing privatekey or generating a new privatekey\n");
    printf("-sign: sign a CSR with your own certificate/privatekey pair\n");
    printf("options:\n");
    printf("    -b : batchmode\n");
    printf("    -c <country>\n");
    printf("    -s <state>\n");
    printf("    -l <locality>\n");
    printf("    -org <organization>\n");
    printf("    -ou <organizational unit>\n");
    printf("    -cn <common name, normally hostname or ip address\n");
    printf("    -e <email>\n");
    printf("    -days <number-of-days-to-be-valid>\n");
    printf("    -ip <input-private-key-file>\n");
    printf("    -ic <input-certificate-file>\n");
    printf("    -ir <input-csr-file>\n");
    printf("    -op <outputfile for privatekey>\n");
    printf("    -oc <outputfile for certificate>\n");
    printf("    -or <outputfile for csr>\n");
    printf("    -p <passphrase>\n");
}

enum MysslAction
{
    CRT=0,
    CSR=1,
    SIGN=2
};

int main(int argc, char* argv[])
{
    InitModuleObjects();

    StringBuffer passwd;
    if(argc < 2)
    {
        usage();
        return -1;
    }
    else if (stricmp(argv[1], "-?") == 0 || stricmp(argv[1], "-h") == 0 || stricmp(argv[1], "-help") == 0)
    {
        usage();
        return 0;
    }

    bool isBatchMode = false;
    MysslAction action = CRT;
    StringBuffer opfname, ocfname, orfname, cbuf, sbuf, lbuf, orgbuf, oubuf, cnbuf, ebuf, daysbuf, pfbuf, cfbuf, rfbuf, pbuf;
    int i;
    for (i=1; i<argc; i++)
    {
        if (stricmp(argv[i], "-crt") == 0)
        {
            action = CRT;
        }
        else if (stricmp(argv[i], "-csr") == 0)
        {
            action = CSR;
        }
        else if (stricmp(argv[i], "-sign") == 0)
        {
            action = SIGN;
        }
        else if (stricmp(argv[i], "-b") == 0)
        {
            isBatchMode = true;
        }
        else if(stricmp(argv[i], "-op") == 0)
        {
            i++;
            opfname.append(argv[i]);
        }
        else if(stricmp(argv[i], "-oc") == 0)
        {
            i++;
            ocfname.append(argv[i]);
        }
        else if(stricmp(argv[i], "-or") == 0)
        {
            i++;
            orfname.append(argv[i]);
        }
        else if(stricmp(argv[i], "-c") == 0)
        {
            i++;
            cbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-s") == 0)
        {
            i++;
            sbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-l") == 0)
        {
            i++;
            lbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-org") == 0)
        {
            i++;
            orgbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-ou") == 0)
        {
            i++;
            oubuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-cn") == 0)
        {
            i++;
            cnbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-e") == 0)
        {
            i++;
            ebuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-days") == 0)
        {
            i++;
            daysbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-ip") == 0)
        {
            i++;
            pfbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-ic") == 0)
        {
            i++;
            cfbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-ir") == 0)
        {
            i++;
            rfbuf.append(argv[i]);
        }
        else if(stricmp(argv[i], "-p") == 0)
        {
            i++;
            pbuf.append(argv[i]);
        }
        else
        {
            printf("unknown option %s\n", argv[i]);
            return -1;
        }
    }

    char buf[128];

    if(!isBatchMode)
    {
        if(action == CSR || action == CRT)
        {
            if(cbuf.length() == 0)
            {
                printf("Country Name (2 letter code): ");
                myfgets(buf,128,stdin);
                if(*buf == '\0')
                    strcpy(buf, "US");
                cbuf.append(buf);
            }

            if(sbuf.length() == 0)
            {
                printf("State (full name): ");
                myfgets(buf,128,stdin);
                sbuf.append(buf);
            }

            if(lbuf.length() == 0)
            {
                printf("Locality Name (eg, city): ");
                myfgets(buf,128,stdin);
                lbuf.append(buf);
            }

            if(orgbuf.length() == 0)
            {
                printf("Organization Name (eg, company): ");
                myfgets(buf,128,stdin);
                orgbuf.append(buf);
            }

            if(oubuf.length() == 0)
            {
                printf("Organizational Unit Name: ");
                myfgets(buf,128,stdin);
                oubuf.append(buf);
            }

            if(ebuf.length() == 0)
            {
                printf("Email: ");
                myfgets(buf, 128, stdin);
                ebuf.append(buf);
            }

            if(cnbuf.length() == 0)
            {
                printf("Common Name (Server's hostname or IP address): ");
                myfgets(buf, 128, stdin);
                cnbuf.append(buf);
            }

            if(action == CRT)
            {
                printf("Number of days for the certificate to be valid: ");
                myfgets(buf, 128, stdin);
                if(strlen(buf) > 0)
                    daysbuf.append(buf);

                printf("Private Key file(leave it blank if you want to generate a private key): ");
                myfgets(buf,128,stdin);
                pfbuf.append(buf);

                getpassword("Enter PEM pass phrase: ", pbuf);
            }
            else if(action == CSR)
            {
                printf("Private Key file(leave it blank if you want to generate a private key): ");
                myfgets(buf,128,stdin);
                pfbuf.append(buf);

                getpassword("Enter PEM pass phrase: ", pbuf);
            }
        }
        else if(action == SIGN)
        {
            printf("csr file: ");
            myfgets(buf,128,stdin);
            rfbuf.append(buf);

            printf("CA certificate file: ");
            myfgets(buf,128,stdin);
            cfbuf.append(buf);

            printf("CA privatekey file: ");
            myfgets(buf,128,stdin);
            pfbuf.append(buf);

            getpassword("CA private key passphrase: ", pbuf);

            printf("\nNumber of days for the certificate to be valid: ");
            myfgets(buf,128,stdin);
            daysbuf.append(buf);
        }
    }

    try
    {

        Owned<IFile> opf;
        Owned<IFileIO> opfio;
        if(opfname.length() > 0)
        {
            opf.setown(createIFile(opfname.str()));
            opfio.setown(opf->open(IFOcreate));
        }

        Owned<IFile> ocf;
        Owned<IFileIO> ocfio;
        if(ocfname.length() > 0)
        {
            ocf.setown(createIFile(ocfname.str()));
            ocfio.setown(ocf->open(IFOcreate));
        }

        Owned<IFile> orf;
        Owned<IFileIO> orfio;
        if(orfname.length() > 0)
        {
            orf.setown(createIFile(orfname.str()));
            orfio.setown(orf->open(IFOcreate));
        }

        if(action == CRT || action == CSR)
        {
            Owned<ICertificate> cc = createCertificate();

            if(cbuf.length() > 0)
                cc->setCountry(cbuf.str());
            if(sbuf.length() > 0)
                cc->setState(sbuf.str());
            if(lbuf.length() > 0)
                cc->setCity(lbuf.str());
            if(orgbuf.length() > 0)
                cc->setOrganization(orgbuf.str());
            if(oubuf.length() > 0)
                cc->setOrganizationalUnit(oubuf.str());
            if(ebuf.length() > 0)
                cc->setEmail(ebuf.str());
            if(cnbuf.length() > 0)
                cc->setDestAddr(cnbuf.str());

            if(action == CSR)
            {
                if(pbuf.length() > 0)
                    cc->setPassphrase(pbuf.str());
                else
                    throw MakeStringException(-1, "passphrase not specified.");

                StringBuffer csrbuf, privkey;
                if(pfbuf.length() == 0)
                {
                    cc->generateCSR(privkey, csrbuf);

                    if(opfio.get() != NULL)
                        opfio->write(0, privkey.length(), privkey.str());
                    else
                        printf("\n%s\n", privkey.str());

                    if(orfio.get() != NULL)
                        orfio->write(0, csrbuf.length(), csrbuf.str());
                    else
                        printf("\n%s\n", csrbuf.str());

                }
                else
                {
                    privkey.loadFile(pfbuf.str());
                    cc->generateCSR(privkey.str(), csrbuf);

                    if(orfio.get() != NULL)
                        orfio->write(0, csrbuf.length(), csrbuf.str());
                    else
                        printf("\n%s\n", csrbuf.str());
                }

            }
            else if(action == CRT)
            {
                if(daysbuf.length() > 0)
                {
                    cc->setDays(atoi(daysbuf.str()));
                }

                if(pbuf.length() > 0)
                    cc->setPassphrase(pbuf.str());
                else
                    throw MakeStringException(-1, "passphrase not specified.");

                StringBuffer certbuf, privkey;
                if(pfbuf.length() == 0)
                {
                    cc->generate(certbuf, privkey);

                    if(opfio.get() != NULL)
                        opfio->write(0, privkey.length(), privkey.str());
                    else
                        printf("\n%s\n", privkey.str());

                    if(ocfio.get() != NULL)
                        ocfio->write(0, certbuf.length(), certbuf.str());
                    else
                        printf("\n%s\n", certbuf.str());

                }
                else
                {
                    privkey.loadFile(pfbuf.str());
                    cc->generate(certbuf, privkey.str());

                    if(ocfio.get() != NULL)
                        ocfio->write(0, certbuf.length(), certbuf.str());
                    else
                        printf("\n%s\n\n", certbuf.str());
                }
            }
        }
        else if(stricmp(argv[1], "-sign") == 0)
        {
            StringBuffer csrbuf, ca_cert, ca_privkey, certbuf;
            
            if(rfbuf.length() == 0 || cfbuf.length() == 0 || pfbuf.length() == 0)
                throw MakeStringException(-1, "You need to specify csr file, certificate file and privatekey file");

            csrbuf.loadFile(rfbuf.str());
            ca_cert.loadFile(cfbuf.str());
            ca_privkey.loadFile(pfbuf.str());

            if(pbuf.length() == 0)
                throw MakeStringException(-1, "passphrase not specified.");

            int days = 365;
            if(daysbuf.length() > 0)
            {
                days = atoi(daysbuf.str());
            }

            signCertificate(csrbuf.str(),ca_cert.str(), ca_privkey.str(), pbuf.str(), days, certbuf);
            if(ocfio.get() != NULL)
                ocfio->write(0, certbuf.length(), certbuf.str());
            else
                printf("\n%s\n", certbuf.str());
        }
        else
        {
            usage();
            return -1;
        }
    }
    catch(IException* e)
    {
        StringBuffer errmsg;
        printf("\nError - %s\n", e->errorMessage(errmsg).str());
        e->Release();
    }
    catch(...)
    {
        printf("\nUnknown error.");
    }

    releaseAtoms();
    return 0;
}

