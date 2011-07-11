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

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jfile.hpp"
#include "mpcomm.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "dautils.hpp"

// CRC checker

//#define NOCRC_TEST


struct CNodeStats
{
    __int64 ctotal; 
    __int64 dtotal;
    __int64 ctotalt; // 1/100s
    __int64 dtotalt;
    __int64 ctotals; // k
    __int64 dtotals;
    unsigned cnumparts;
    unsigned dnumparts;
    unsigned cerr;
    unsigned derr;
    unsigned cexc;
    unsigned dexc;
};

Owned<IFileIO> fulldump;
__int64 fulldumpofs;

void checkClusterCRCs(const char *cluster,bool fix,bool verbose,const char *csvfile,const char *singlefilename=NULL)
{

    class cfilescan1: public CSDSFileScanner
    {
        StringArray badfiles;
        IRemoteConnection *conn;
        StringArray grps;
        Owned<IGroup> grp;
        CNodeStats *stats;
        unsigned numprocessed;
        bool verbose;
        bool logfixing;
        bool fix;
        void processSuperFile(IPropertyTree &file,StringBuffer &name) {}

        virtual bool checkFileOk(IPropertyTree &file,const char *filename)
        {
            StringArray groups;
            if (getFileGroups(&file,groups)==0) {
                if (verbose)
                    PROGLOG("ERROR: File %s hasn't got group defined",filename);
                return false;
            }
            ForEachItemIn(i,groups) {
                ForEachItemIn(j,grps) { 
                    if (strcmp(groups.item(i),grps.item(j))==0)
                        return true;
                }
            }
            return false;
        }
        void processFile(IPropertyTree &file,StringBuffer &name)
        {
            unsigned n = file.getPropInt("@numparts");
            if (!n) {
                if (verbose)
                    PROGLOG("ERROR: File %s hasn't got numparts defined",name.str());
                return;
            }
            const char *dir = file.queryProp("@directory");
            if (!dir||!*dir) {
                if (verbose)
                    PROGLOG("ERROR: File %s hasn't got directory defined",name.str());
                return;
            }
            const char *mask = file.queryProp("@partmask");
            if (!mask||!*mask) {
                // check for single part file
                if (file.getPropInt("@numparts",0)==1) 
                    mask = file.queryProp("Part[@num=\"1\"]/@name");
            }
            if (!mask||!*mask) {
                if (verbose)
                    PROGLOG("ERROR: File %s hasn't got part mask defined",name.str());
                return;
            }
            Owned<IPropertyTree> fileattr = file.getPropTree("Attr");
            if (!fileattr)
                fileattr.setown(createPTree("Attr",false));
            bool compressedfile = isCompressed(*fileattr);
            IPropertyTree **parts = (IPropertyTree **)calloc(n,sizeof(IPropertyTree *));
            Owned<IPropertyTreeIterator> it;




            MemoryBuffer mb;
            if (file.getPropBin("Parts",mb)) 
                it.setown(deserializePartAttrIterator(mb));
            else
                it.setown(file.getElements("Part"));
            unsigned f=0;
            ForEach(*it) {
                IPropertyTree &part = it->get();
                unsigned pn = part.getPropInt("@num");
                if (pn&&(pn<=n)&&(parts[pn-1]==NULL)) {
                    parts[pn-1] = &part;
                    f++;
                }
                else
                    ::Release(&part);
            }
            if (f!=n) {
                if (verbose)
                    PROGLOG("ERROR: File %s hasn't got full part information defined",name.str());
                for (unsigned i=0;i<n;i++) 
                    ::Release(parts[i]);
                free(parts);
                return;
            }
            bool fixed = false;
            class casyncfor: public CAsyncFor
            {
                CriticalSection crit;
                IPropertyTree *fileattr;
                IPropertyTree **parts;
                const char *dir;
                const char *mask;
                IGroup *grp;
                unsigned n;
                CNodeStats *stats;
                bool verbose;
                bool logfixing;
                bool fix;
                bool &fixed;
                bool compressedfile;
                Semaphore *primdone;    // bit of a hack to save sockets
            public:
                bool ok;
                casyncfor(IPropertyTree *_fileattr,
                          IPropertyTree **_parts, 
                          const char *_dir,
                          const char *_mask,
                          IGroup *_grp,
                          unsigned _n,
                          CNodeStats *_stats,
                          bool _fix,
                          bool &_fixed,
                          bool _verbose,
                          bool _logfixing,
                          bool _compressedfile
                          )
                    : fixed(_fixed)
                {
                    fileattr = _fileattr;
                    parts = _parts;
                    dir = _dir;
                    mask = _mask;
                    grp = _grp;
                    n = _n;
                    stats = _stats;
                    verbose = _verbose;
                    logfixing = _logfixing;
                    fix = _fix;
                    compressedfile = _compressedfile;
                    ok = true;
                    unsigned w = grp->ordinality();
                    primdone = new Semaphore[w];
                    for (unsigned i=0;i<w;i++)
                        primdone[i].signal();
                }
                ~casyncfor()
                {
                    delete [] primdone;
                }
                void Do(unsigned pi)
                {
                    CriticalBlock block(crit);
                    if (!parts[pi]) {
                        if (verbose)
                            PROGLOG("ERROR: no part %d",pi+1);
                        return;
                    }
                    IPropertyTree &part = *parts[pi];
                    unsigned w = grp->ordinality();
                    unsigned i = pi%w; 
                    unsigned r = (pi+1)%w; 
                    unsigned crc;
                    bool crcgot = getCrcFromPartProps(*fileattr,part, crc);
                    __int64 sz = part.getPropInt("@compressedSize",-1);
                    if (sz==-1)
                        sz = part.getPropInt64("@size",-1);
                    StringBuffer path(dir);
                    addPathSepChar(path);
                    expandMask(path,mask,pi,n);
                    RemoteFilename rfnc;
                    rfnc.setPath(grp->queryNode(i).endpoint(),path.str());
                    path.clear().append(dir);
                    setPathDrive(path,1);
                    addPathSepChar(path);
                    expandMask(path,mask,pi,n);     
                    RemoteFilename rfnd;
                    rfnd.setPath(grp->queryNode(r).endpoint(),path.str());// replicate offset TBD
                    unsigned cfcrc;
                    __int64 cfsz=-1;
                    unsigned dfcrc;
                    __int64 dfsz=-1;
                    unsigned cut=0;
                    unsigned dut=0;
                    Owned<IException> cexc;
                    Owned<IException> dexc;
                    {
                        IFile *testf = NULL;
                        CriticalUnblock unblock(crit);
                        if (!primdone[i].wait(1000*60*60))  // kludge to save handles
                            ERRLOG("Timing out on semaphore(1)");
                        try {
                            Owned<IFile> f = createIFile(rfnc);
                            if (f) {
                                if (f->exists()) {
                                    cfsz = f->size();
                                    unsigned start = msTick();
#ifndef NOCRC_TEST
                                    cfcrc = cfsz?f->getCRC():0;
#else
                                    cfcrc = 0;
#endif
                                    cut = msTick()-start;
                                    if ((int)cut<0)
                                        cut = 0; // not sure why this needed
                                }
                                else if (verbose&&sz) {
                                    StringBuffer tmp;
                                    PROGLOG("ERROR: missing part %s",rfnc.getRemotePath(tmp).str());
                                    ok = false;
                                }
                            }
                        }
                        catch (IException *e) {
                            cfsz = -1;
                            cexc.setown(e);
                        }
                        primdone[i].signal();
                        if (!primdone[r].wait(1000*60*60))  // kludge to save handles
                            ERRLOG("Timing out on semaphore(2)");
                        try {
                            Owned<IFile> f = createIFile(rfnd);
                            if (f) {
                                if (f->exists()) {
                                    dfsz = f->size();
                                    unsigned start = msTick();
#ifndef NOCRC_TEST
                                    dfcrc = dfsz?f->getCRC():0;
#else
                                    dfcrc = 0;
#endif
                                    dut = msTick()-start;
                                    if ((int)dut<0)
                                        dut = 0; // not sure why this needed
                                }
                                else if (verbose&&sz) {
                                    StringBuffer tmp;
                                    PROGLOG("ERROR: missing part %s",rfnd.getRemotePath(tmp).str());
                                    ok = false;
                                }
                            }
                        }
                        catch (IException *e) {
                            dfsz = -1;
                            dexc.setown(e);
                        }
                        primdone[r].signal();   // kludge to save handles
                    }
                    if (cfsz>=0)
                        stats[i].cnumparts++;
                    if (dfsz>=0)
                        stats[r].dnumparts++;
                    StringBuffer errmsg;
                    if (cexc.get()) {
                        errmsg.clear().append("EXCEPTION:(");
                        rfnc.getRemotePath(errmsg);
                        errmsg.append("):");
                        cexc->errorMessage(errmsg);
                        //PROGLOG("%s",errmsg.str());
                        LOG(MCoperatorError, unknownJob, cexc, errmsg.str());
                        ok = false;
                        stats[i].cexc++;
                    }
                    if (dexc.get()) {
                        errmsg.clear().append("EXCEPTION:(");
                        rfnd.getRemotePath(errmsg);
                        errmsg.append("):");
                        dexc->errorMessage(errmsg);
                        //PROGLOG("%s",errmsg.str());
                        LOG(MCoperatorError, unknownJob, dexc, errmsg.str());
                        ok = false;
                        stats[r].dexc++;
                    }
                    bool mismatchdone = false;
                    if ((cfsz>=0)&&(dfsz>=0)) { // both files exist
                        if ((cfsz!=dfsz)&&(cfsz!=sz)&&(dfsz!=sz)) {
                            errmsg.clear().append("SIZE MISMATCH:(");
                            rfnc.getRemotePath(errmsg);
                            errmsg.append(", ");
                            rfnd.getRemotePath(errmsg);
                            errmsg.appendf("): %"I64F"d, %"I64F"d",cfsz,dfsz);
                            PROGLOG("%s",errmsg.str());
                            mismatchdone = true;
                            stats[i].cerr++;    
                            stats[r].derr++;    
                            ok = false;
                        }
#ifndef NOCRC_TEST
                        else if ((cfcrc!=dfcrc)&&(!crcgot||((cfcrc!=crc)&&(dfcrc!=crc)))) {
                            errmsg.clear().append("CRC MISMATCH:(");
                            rfnc.getRemotePath(errmsg);
                            errmsg.append(", ");
                            rfnd.getRemotePath(errmsg);
                            errmsg.appendf("): %x, %x",cfcrc,dfcrc);
                            PROGLOG("%s",errmsg.str());
                            mismatchdone = true;
                            stats[i].cerr++;    
                            stats[r].derr++;    
                            ok = false;
                        }
#endif
                    }
                    if (!mismatchdone&&((cfsz>=0)||(dfsz>=0))) { // at least one file exists
                        unsigned fcrc = (cfsz>=0)?cfcrc:dfcrc;
                        __int64 fsz = (cfsz>=0)?cfsz:dfsz;
                        if (fix) {
                            if ((sz<0)||(compressedfile&&!part.hasProp("@compressedSize"))) {
                                if (logfixing) {
                                    errmsg.clear().append("FIX DALI SIZE :(");
                                    rfnc.getRemotePath(errmsg);
                                    errmsg.appendf("): %"I64F"d",fsz);
                                    PROGLOG("%s",errmsg.str());
                                }
#ifndef NOCRC_TEST
                                if (compressedfile)
                                    part.setPropInt64("@compressedSize",fsz);
                                else
                                    part.setPropInt64("@size",fsz);
#endif

                                sz = fsz;
                                fixed = true;
                            }
#ifndef NOCRC_TEST
                            if (!crcgot||
                                ((crc!=fcrc)&&
                                 ((crc==(unsigned)-1)||
                                  (compressedfile&&(fcrc==(unsigned)-1))||
                                   !part.hasProp("@fileCrc")
                                 )
                                )
                               ) {
                                if (logfixing) {
                                    errmsg.clear().append("FIX DALI CRC :(");
                                    rfnc.getRemotePath(errmsg);
                                    errmsg.appendf("): %x",fcrc);
                                    PROGLOG("%s",errmsg.str());
                                }
                                part.setPropInt("@fileCrc",fcrc);
                                part.removeProp("@crc");
                                crcgot = true;
                                crc = fcrc;
                                fixed = true;
                            }
#endif
                        }
                    }
                    if (!mismatchdone) {
                        if (cfsz>=0) {
                            if ((sz>=0)&&(cfsz!=sz)) {
                                errmsg.clear().append("DALI SIZE MISMATCH:(");
                                rfnc.getRemotePath(errmsg);
                                errmsg.appendf("): %"I64F"d, %"I64F"d",cfsz,sz);
                                PROGLOG("%s",errmsg.str());
                                stats[i].cerr++;    
                                ok = false;
                            }
#ifndef NOCRC_TEST
                            else if (crcgot&&(cfcrc!=crc)) {
                                const char * kind = part.queryProp("@kind");
                                if (verbose&&(!kind||(strcmp(kind,"topLevelKey")!=0))) { // ignore TLK crcs 
                                    errmsg.clear().append("DALI CRC MISMATCH:(");
                                    rfnc.getRemotePath(errmsg);
                                    errmsg.appendf("): %x, %x",cfcrc,crc);
                                    PROGLOG("%s",errmsg.str());
                                    stats[i].cerr++;    
                                    ok = false;
                                }
                            }
#endif
                        }
                        if (dfsz>=0) {
                            if ((sz>=0)&&(dfsz!=sz)) {
                                errmsg.clear().append("DALI SIZE MISMATCH:(");
                                rfnd.getRemotePath(errmsg);
                                errmsg.appendf("): %"I64F"d, %"I64F"d",dfsz,sz);
                                PROGLOG("%s",errmsg.str());
                                stats[r].derr++;    
                                ok = false;
                            }
#ifndef NOCRC_TEST
                            else if (crcgot&&(dfcrc!=crc)) {
                                const char * kind = part.queryProp("@kind");
                                if (verbose&&(!kind||(strcmp(kind,"topLevelKey")!=0))) { // ignore TLK crcs 
                                    errmsg.clear().append("DALI CRC MISMATCH:(");
                                    rfnd.getRemotePath(errmsg);
                                    errmsg.appendf("): %x, %x",dfcrc,crc);
                                    PROGLOG("%s",errmsg.str());
                                    stats[r].derr++;    
                                    ok = false;
                                }
                            }
#endif
                        }
                    }
                    if ((cexc.get()==NULL)&&(dexc.get()==NULL)) {
                        if (fulldump) {
                            StringBuffer out;
                            out.append(i).append(',');
                            if ((cut>100)&&(cfsz>0x1000000)) { // ignore small
                                if ((cut&&(cfsz/(__int64)cut<10000))||(dut&&(dfsz/(__int64)dut<10000))) {
                                    rfnc.getRemotePath(out).append(',');
                                    out.append(cut).append(',').append(cfsz).append('\n');
                                    out.append(i).append(',');
                                    rfnd.getRemotePath(out).append(',');
                                    out.append(dut).append(',').append(dfsz).append('\n');
                                    fulldump->write(fulldumpofs,out.length(),out.str());
                                    fulldumpofs += out.length();
                                }
                            }
                        }
                        if ((cut>100)&&(cfsz>0x1000000)) { // ignore small
                            stats[i].ctotalt+=cut/10;
                            stats[i].ctotals+=(cfsz+512)/1024;
                        }
                        stats[i].ctotal+=cfsz;
                        if ((dut>100)&&(dfsz>0x1000000)) { // ignore small
                            stats[r].dtotalt+=dut/10;
                            stats[r].dtotals+=(dfsz+512)/1024;
                        }
                        stats[r].dtotal+=dfsz;
                    }
                }
            } afor(fileattr,parts,dir,mask,grp,n,stats,fix,fixed,verbose,logfixing,compressedfile);
            afor.For(n,n<401?n:401);
            if (!afor.ok)
                badfiles.append(name.str());
            if (fixed)
                conn->commit();
            for (unsigned i=0;i<n;i++) 
                ::Release(parts[i]);
            free(parts);
            numprocessed++;
            if (numprocessed%100==0)
                PROGLOG("%d files processed",numprocessed);
        } 

    public:

        void scan(IRemoteConnection *_conn,const char *cluster,bool _fix,bool _verbose,const char *csvname,const char *singlefilename)
        {
            conn = _conn;
            numprocessed = 0;
            CDfsLogicalFileName dlfn;
            if (singlefilename) {
                dlfn.set(singlefilename);
                if (!cluster) {
                    StringBuffer query;
                    dlfn.makeFullnameQuery(query,DXB_File,false);
                    IPropertyTree *file = conn->queryRoot()->queryPropTree(query.str());
                    if (!file) {
                        ERRLOG("File %s not found",singlefilename);
                        return;
                    }
                    cluster = file->queryProp("@group");
                    if (!cluster||!*cluster) {
                        ERRLOG("Cluster not found for %s",singlefilename);
                        return;
                    }
                }
            }
            fix = _fix;
            verbose = _verbose;
            logfixing = verbose&&!singlefilename;
            grp.setown(queryNamedGroupStore().lookup(cluster));
            if (!grp) {
                ERRLOG("Cluster %s not found",cluster);
                return;
            }
            PROGLOG("Scanning cluster %s",cluster);
            if (singlefilename) {
                grps.append(cluster);
            }
            else {
                Owned<INamedGroupIterator> iter = queryNamedGroupStore().getIterator();
                StringBuffer name;
                ForEach(*iter) {
                    iter->get(name.clear());
                    Owned<IGroup> lgrp = queryNamedGroupStore().lookup(name.str());
                    GroupRelation cmp = grp->compare(lgrp);
                    if (cmp==GRidentical) {
                        grps.append(name.str());
                        if (verbose&&(stricmp(name.str(),cluster)!=0)) 
                            PROGLOG("Scanning matching cluster %s",name.str());
                    }
                    else if (verbose &&(cmp!=GRdisjoint))
                        PROGLOG("NOT scanning overlapping cluster %s",name.str());
                }
            }
            StringBuffer out;
            unsigned w = grp->ordinality();
            stats = (CNodeStats *)calloc(w,sizeof(CNodeStats));
            unsigned start = msTick();
            if (singlefilename) {
                CSDSFileScanner::singlefile(conn,dlfn);
            }
            else
                CSDSFileScanner::scan(conn,true,false);
            if (badfiles.ordinality()&&!singlefilename) {
                PROGLOG("Files affected:");
                ForEachItemIn(i,badfiles)
                    PROGLOG("  %s",badfiles.item(i));
            }
            unsigned t = (msTick()-start)/1000;
            if (t>=3600) {
                t /= 60;
                PROGLOG("Done. Time taken = %dh %dm",t/60,t%60);
            }
            else
                PROGLOG("Done. Time taken = %dm %ds",t/60,t%60);
            if (!singlefilename) {
                Owned<IFile> file = createIFile(csvname);
                Owned<IFileIO> fileio = file->open(IFOcreate);
                if (fileio) {
                    out.clear().append("\"Node\",\"c errors\",\"d errors\",\"c bad crc\",\"d bad crc\",\"c KB/s\",\"d KB/s\",\"c parts\",\"d parts\",\"c read\",\"d read\"\n");
                    for (unsigned i=0;i<w;i++) {
                        CNodeStats &stat = stats[i];
                        grp->queryNode(i).endpoint().getUrlStr(out);
                        out.append(',').append(stat.cexc);
                        out.append(',').append(stat.dexc);
                        out.append(',').append(stat.cerr);
                        out.append(',').append(stat.derr);
                        unsigned crate = stat.ctotalt?((unsigned)((stat.ctotals*100)/stat.ctotalt)):0;
                        unsigned drate = stat.dtotalt?((unsigned)((stat.dtotals*100)/stat.dtotalt)):0;
                        out.append(',').append(crate);
                        out.append(',').append(drate);
                        out.append(',').append(stat.cnumparts);
                        out.append(',').append(stat.dnumparts);
                        out.append(',').append(stat.ctotal);
                        out.append(',').append(stat.dtotal);
                        out.append('\n');
                    }
                    if (badfiles.ordinality()) {
                        out.append("Files affected:\n");
                        ForEachItemIn(i,badfiles)
                            out.append(badfiles.item(i)).append('\n');
                    }
                    fileio->write(0,out.length(),out.str());
                }
                else
                    ERRLOG("Couldn't create %s",csvname);
            }
            free(stats);
        }


    } filescan;
 
//  Owned<IFile> fdfile = createIFile("fulldump.csv");
//  fulldump.setown(fdfile->open(IFOcreate));
    fulldumpofs = 0;
    
    
    Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(), 0, 100000);
    if (!conn) {
        ERRLOG("cannot connect to /Files");
        return;
    }
    StringBuffer outfile;
    filescan.scan(conn,cluster,fix,verbose,csvfile?csvfile:addFileTimestamp(outfile.append(cluster?cluster:"sdsfix")).append(".csv").str(),singlefilename); 
    
}


void listWorkUnitAssociatedFiles()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/", myProcessSession(), 0, 5*60*1000);  
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("WorkUnits/*");
    ForEach(*iter) {
        IPropertyTree &wu=iter->query();
        Owned<IPropertyTreeIterator> iter2 = wu.getElements("Query/Associated/File[@type=\"dll\"]");
        ForEach(*iter2) {
            IPropertyTree &fi=iter2->query();
            const char *name = fi.queryProp("@filename");
            unsigned version = fi.getPropInt("@crc");
            StringBuffer path;
            path.append("GeneratedDlls/GeneratedDll[@uid=\"").append(name).append('$').append(version).append("\"]");
            Owned<IPropertyTreeIterator> iter3 = conn->queryRoot()->getElements(path.str());
            bool found=false;
            ForEach(*iter3) {
                IPropertyTree &di=iter3->query();
                const char *typ = di.queryProp("@kind");
                if (typ&&(stricmp(typ,"Workunit DLL")==0)) {
                    Owned<IPropertyTreeIterator> iter4 = di.getElements("location");
                    ForEach(*iter4) {
                        IPropertyTree &dil=iter4->query();
                        found = true;
                        printf("%s,%s\n",dil.queryProp("@ip"),dil.queryProp("@dll"));
                    }
                }
            }
            if (!found)
                printf("NOTFOUND,%s\n",name);
        }
    }
}
