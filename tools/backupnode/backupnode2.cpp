/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jlib.hpp"
#include "jiface.hpp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jregexp.hpp"
#include "jset.hpp"
#include "jflz.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "rmtfile.hpp"

#define LOGPFX "backupnode: "

#define FLZCOMPRESS

class CFileListWriter
{
public:
    bool abort;
    bool verbose;
    StringArray clustersin;
    StringArray clustersout;
    IGroup *group;
    IpAddress *iphash;
    unsigned *ipnum;
    unsigned iphashsz;
    unsigned numparts;
    unsigned numfiles;
    IArrayOf<IFileIOStream> *outStreams;

    void log(const char * format, ...) __attribute__((format(printf, 2, 3)))
    {
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        PROGLOG(LOGPFX "%s",line.str());
    }

    void error(const char * format, ...) __attribute__((format(printf, 2, 3)))
    {
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        ERRLOG(LOGPFX "%s",line.str());
    }

    void warn(const char * format, ...) __attribute__((format(printf, 2, 3)))
    {
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        WARNLOG(LOGPFX "%s",line.str());
    }

    void addIpHash(const IpAddress &ip,unsigned n)
    {
        unsigned r;
        _cpyrev4(&r,&ip);
        unsigned h = hashc((const byte *)&r,sizeof(r),0)%iphashsz;
        while (!iphash[h].isNull()) 
            if (++h==iphashsz)
                h = 0;
        iphash[h] = ip;
        ipnum[h] = n;
    }

    unsigned checkIpHash(const IpAddress &ip)
    {
        unsigned r;
        _cpyrev4(&r,&ip);
        unsigned h = hashc((const byte *)&r,sizeof(r),0)%iphashsz;
        while (!iphash[h].isNull()) {
            if (iphash[h].ipequals(ip))
                return ipnum[h];
            if (++h==iphashsz)
                h = 0;
        }
        return NotFound;
    }


    CFileListWriter()
    {
        abort = false;
        verbose = false;
        iphash = NULL;
        ipnum = NULL;
        iphashsz = 0;
        numfiles = 0;
        numparts = 0;
    }

    ~CFileListWriter()
    {
        if (iphash) 
            delete [] iphash;
        delete [] ipnum;
    }

    void write(IGroup *_group,IArrayOf<IFileIOStream> &_outStreams)
    {
        if (!_group||abort)
            return;
        group = _group;
        outStreams = &_outStreams;
        delete [] iphash;
        iphash = NULL;
        delete [] ipnum;
        iphashsz = group->ordinality()*2;
        iphash = new IpAddress[iphashsz];
        ipnum = new unsigned[iphashsz];
        bool grphasports = false;
        ForEachNodeInGroup(i,*group) {
            const SocketEndpoint &ep = group->queryNode(i).endpoint();
            if (ep.port!=0) 
                grphasports = true;
            addIpHash(ep,i);
        }   
        if (grphasports)
            ERRLOG(LOGPFX "Group has ports!");


        class cfilescan1 : public CSDSFileScanner
        {

            Owned<IRemoteConnection> conn;
            CFileListWriter &parent;
            bool &abort;

            bool checkFileOk(IPropertyTree &file,const char *filename)
            {
                if (abort)
                    return false;
                StringArray groups;
                getFileGroups(&file,groups);
                if (groups.ordinality()==0) { 
                    parent.error("File has no group defined: %s",filename);
                    return false;
                }
                ForEachItemIn(i,groups) {
                    const char *cluster = groups.item(i);
                    ForEachItemIn(j1,parent.clustersin) {
                        if (strcmp(parent.clustersin.item(j1),cluster)==0) 
                            return true;
                    }
                    bool excluded = false;
                    ForEachItemIn(j2,parent.clustersout) {
                        if (strcmp(parent.clustersout.item(j2),cluster)==0) {
                            excluded = true;
                            break;
                        }
                    }
                    if (excluded)
                        continue;
                    Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
                    if (!group) {
                        parent.error("cannot find cluster %s",cluster);
                        parent.clustersout.append(cluster);
                        continue;
                    }
                    ForEachNodeInGroup(i,*group) {
                        unsigned nn = parent.checkIpHash(group->queryNode(i).endpoint());
                        if (nn!=NotFound) {
                            parent.clustersin.append(cluster);
                            return true;
                        }
                    }
                }
                return false;
            }

            bool checkScopeOk(const char *scopename)
            {
                return !abort;
            }
            

            void processFile(IPropertyTree &file,StringBuffer &name)
            {
                if (abort)
                    return;
                if (parent.verbose)
                    parent.log("Process file %s",name.str());
                Owned<IFileDescriptor> fdesc;
                try {
                    fdesc.setown(deserializeFileDescriptorTree(&file,&queryNamedGroupStore()));
                }
                catch (IException *e) {
                    EXCLOG(e,LOGPFX "processFile");
                    e->Release();
                }
                if (fdesc) {
                    unsigned np = fdesc->numParts();
                    if (np==0) {
                        parent.error("File has no parts %s",name.str());
                        return;
                    }
                    parent.numfiles++;
                    StringBuffer fn;
                    StringBuffer dir;
                    bool incluster = true;          
                    StringBuffer ln;
                    for (unsigned p=0;p<np;p++) {
                        if (abort)
                            return;
                        unsigned matched = 0;
                        unsigned nc = fdesc->numCopies(p);
                        unsigned c;
                        UnsignedArray map;
                        unsigned nf = 0;
                        for (c=0;c<nc;c++) {
                            INode *node = fdesc->queryNode(p,c);
                            unsigned nn = parent.checkIpHash(node->endpoint());
                            map.append(nn);
                            if (nn!=NotFound)
                                nf++;
                        }
                        if (nf>1) {     // 1 not much use
                            parent.numparts++;
                            ForEachItemIn(i,map) {
                                unsigned from = map.item(i);
                                if (from!=NotFound) {
                                    ForEachItemIn(j,map) {
                                        if (i!=j) {
                                            unsigned to = map.item(j);
                                            if (to!=NotFound) {
                                                // right lets go for it
                                                IFileIOStream &out = parent.outStreams->item(from);
                                                RemoteFilename rfn;
                                                fdesc->getFilename(p,i,rfn);
                                                rfn.getLocalPath(ln.clear());
                                                ln.append('|');
                                                fdesc->getFilename(p,j,rfn);
                                                rfn.getRemotePath(ln);
                                                ln.append('\n');
                                                out.write(ln.length(),ln.str());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                else 
                    parent.error("cannot create file descriptor %s",name.str());
            }
        public:

            cfilescan1(CFileListWriter &_parent,bool &_abort)
                : parent(_parent), abort(_abort)
            {
            }

            ~cfilescan1()
            {
            }

            void scan()
            {
                if (abort)
                    return;
                conn.setown(querySDS().connect("/Files", myProcessSession(), 0, 100000));
                if (!conn||abort)
                    return;
                CSDSFileScanner::scan(conn);
            }

        } filescan(*this,abort);

        filescan.scan();
        log("File scan complete, %d files, %d parts",numfiles,numparts);


    }
};

class CFileStreamReader // this ought to be in jlib really
{
    Linked<IFileIOStream> strm;
    MemoryAttr mba;
    size32_t maxlinesize;
    size32_t buffsize;
    char *buf;
    size32_t lbsize;
    char *p;
    bool eof;
public:
    CFileStreamReader(IFileIOStream * _in,size32_t _maxlinesize=8192,size32_t _buffsize=0x10000)
        : strm(_in)
    {
        maxlinesize = _maxlinesize;
        buffsize = _buffsize;
        buf = (char *)mba.allocate(buffsize+maxlinesize+1);
        lbsize = 0;
        p=NULL;
        eof = false;
    }

    char* nextLine(size32_t &lnsize)
    {
        if (lbsize<maxlinesize) {
            if (!eof) {
                if (lbsize&&(p!=buf)) 
                    memmove(buf,p,lbsize);
                p = buf;
                size32_t rd = strm->read(buffsize,buf+lbsize);
                if (rd==0) {
                    eof = true;
                    if (lbsize==0)
                        return NULL;
                    if (buf[lbsize-1]!='\n')
                        buf[lbsize++] = '\n';               // terminate unfinished line
                }
                else 
                    lbsize += rd;
            }
            else if (lbsize==0)
                return NULL;
        }
        size32_t len = 0;
        char *ret = p;
        while ((len<maxlinesize)&&(p[len]!='\n'))
            len++;
        p[len] = 0;
        lnsize = len;
        len++;
        lbsize-=len;
        p+=len;
        return ret;
    }

};

bool outputPartsFiles(const char *daliserver,const char *cluster,const char *outdir, StringBuffer &errstr, bool verbose)
{
    errstr.clear();
    bool dalistarted;
    if (daliserver&&*daliserver) {
        try {
            // connect to dali
            Owned<IGroup> serverGroup = createIGroup(daliserver,DALI_SERVER_PORT);
            initClientProcess(serverGroup, DCR_BackupGen, 0, NULL, NULL, 1000*60*5);
            dalistarted = true;
            CFileListWriter writer;
            writer.verbose = verbose;
            Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
            if (group) {
                IArrayOf<IFileIOStream> outStreams;
                StringBuffer path;
                ForEachNodeInGroup(i,*group) {
                    addPathSepChar(path.clear().append(outdir)).append(i+1).append(".DAT");
                    Owned<IFile> outf = createIFile(path.str());
                    Owned<IFileIO> outio = outf?outf->open(IFOcreate):NULL;
#ifdef FLZCOMPRESS
                    Owned<IFileIOStream> out = outio?createFastLZStreamWrite(outio):NULL;
#else
                    Owned<IFileIOStream> out = outio?createBufferedIOStream(outio):NULL;
#endif
                    if (!out) {
                        errstr.appendf(LOGPFX "cannot create file %s",path.str());
                        closedownClientProcess();
                        return false;
                    }
                    outStreams.append(*out.getClear());
                }   
                writer.write(group,outStreams);
                closedownClientProcess();
                return true;
            }
            else
                errstr.appendf(LOGPFX "cannot find cluster %s",cluster);
        }
        catch (IException *e) {
            errstr.append(LOGPFX "outPartsFile : ");
            e->errorMessage(errstr);
            e->Release();
        }
    }
    else 
        errstr.append(LOGPFX "no dali server specified");
    if (dalistarted)
        closedownClientProcess();
    return errstr.length()==0;
}

void applyPartsFile(IFileIO *in,void (* sync)(const char *,const char *))
{
#ifdef FLZCOMPRESS
    Owned<IFileIOStream> strm = createFastLZStreamRead(in);
#else
    Owned<IFileIOStream> strm = createBufferedIOStream(in);
#endif
    CFileStreamReader reader(strm);
    loop {
        size32_t sz;
        char *line = reader.nextLine(sz);
        if (!line)
            break;
        char *split = strchr(line,'|');
        if (split) {
            *(split++) = 0;
            sync(line,split);
        }
    }
}

