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

#include <platform.h>


#include "jlib.hpp"
#include "jiface.hpp"
#include "jlog.hpp"
#include "jprop.hpp"
#include "jfile.hpp"
#include "jlzw.hpp"
#include "jexcept.hpp"
#include "jmutex.hpp"
#include "jthread.hpp"
#include "rmtfile.hpp"
#include "daclient.hpp"
#include "dautils.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "mplog.hpp"

#define FUSE_USE_VERSION 26
#include <fuse.h>

#define FUSE_UNIMPLEMENTED(f) assertex(!"Unimplemented function: " f);
#define FUSE_USE(f) ops.f = __##f;

#define MAXPARAM 256

class CFuseBase // base NB singleton
{
    static CriticalSection crit;
    static CFuseBase *self;

protected:

    static int __getattr(const char *path, struct stat *stbuf)
    {
        CriticalBlock block(crit);
        return self->fuse_getattr(path,stbuf);
    }

    static int __readlink (const char *path, char *out, size_t sz)
    {
        CriticalBlock block(crit);
        return self->fuse_readlink(path,out,sz);
    }

    static int __mkdir (const char *path, mode_t mode)
    {
        CriticalBlock block(crit);
        return self->fuse_mkdir(path,mode);
    }

    static int __unlink (const char *path)
    {
        CriticalBlock block(crit);
        return self->fuse_unlink(path);
    }

    static int __rmdir (const char *path)
    {
        CriticalBlock block(crit);
        return self->fuse_rmdir(path);
    }

    static int __rename (const char *from, const char *to)
    {
        CriticalBlock block(crit);
        return self->fuse_rename(from,to);
    }

    static int __truncate (const char *path, off_t pos)
    {
        CriticalBlock block(crit);
        return self->fuse_truncate(path,pos);
    }

    static int __open (const char *path, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_open(path,info);
    }

    static int __read (const char *path, char *dst, size_t sz, off_t pos, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_read(path,dst,sz,pos,info);
    }

    static int __write (const char *path, const char *src, size_t sz, off_t pos, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_write(path,src,sz,pos,info);
    }

    static int __statfs (const char *path, struct statvfs *stbuf)
    {
        CriticalBlock block(crit);
        return self->fuse_statfs(path,stbuf);
    }

    static int __flush (const char *path, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_flush(path,info);
    }

    static int __release (const char *path, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_release(path,info);
    }

    static int __fsync (const char *path, int m, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_fsync(path,m,info);
    }

    static int __opendir (const char *path, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_opendir(path,info);
    }

    static int __readdir (const char *path, void *buf, fuse_fill_dir_t filler, off_t pos, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_readdir(path,buf,filler,pos,info);
    }

    static int __releasedir (const char *path, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_releasedir(path,info);
    }

    static void *__init (struct fuse_conn_info *conn)
    {
        CriticalBlock block(crit);
        return self->fuse_init(conn);
    }

    static void __destroy (void *p)
    {
        CriticalBlock block(crit);
        self->fuse_destroy(p);
    }

    static int __access (const char *path, int m)
    {
        CriticalBlock block(crit);
        return self->fuse_access(path,m);
    }

    static int __create (const char *path, mode_t mode, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_create(path,mode,info);
    }

    static int __ftruncate (const char *path, off_t pos, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_ftruncate(path,pos,info);
    }

    static int __fgetattr (const char *path, struct stat *stbuf, struct fuse_file_info *info)
    {
        CriticalBlock block(crit);
        return self->fuse_fgetattr(path,stbuf,info);
    }

    static int __utimens (const char *path, const struct timespec tv[2])
    {
        CriticalBlock block(crit);
        return self->fuse_utimens(path,tv);
    }

    fuse_operations ops;


public:

    CFuseBase()
    {
        assertex(!self); // can only be one
        self = this;
        memset(&ops,0,sizeof(ops));
        ops.getattr = __getattr;
        ops.readdir = __readdir;
        ops.open = __open;

    }

    ~CFuseBase()
    {
        assertex(self); // can only be one
        self = NULL;
    }

    int main(int argc, char *argv[])
    {
#ifdef _WIN32
        fprintf(stderr,"FUSE not supported on Windows\n");
        return 0;
#else
        return fuse_main(argc, argv, &ops, NULL);
#endif
    }


    // required
    virtual int fuse_getattr(const char *path, struct stat *stbuf) = 0;  
    virtual int fuse_readdir (const char *path, void *buf, fuse_fill_dir_t filler, off_t pos, struct fuse_file_info *info) = 0;
    virtual int fuse_open (const char *path, struct fuse_file_info *info) = 0;

    // optional
    virtual int fuse_readlink (const char *path, char *out, size_t sz)
    {
        FUSE_UNIMPLEMENTED("fuse_readlink");
        return -1;
    }
    virtual int fuse_mkdir (const char *path, mode_t mode)
    {
        FUSE_UNIMPLEMENTED("fuse_mkdir");
        return -1;
    }
    virtual int fuse_unlink (const char *path)
    {
        FUSE_UNIMPLEMENTED("fuse_unlink");
        return -1;
    }
    virtual int fuse_rmdir (const char *path)
    {
        FUSE_UNIMPLEMENTED("fuse_rmdir");
        return -1;
    }
    virtual int fuse_rename (const char *from, const char *to)
    {
        FUSE_UNIMPLEMENTED("fuse_rename");
        return -1;
    }
    virtual int fuse_truncate (const char *path, off_t pos)
    {
        FUSE_UNIMPLEMENTED("fuse_truncate");
        return -1;
    }
    virtual int fuse_read (const char *path, char *dst, size_t sz, off_t pos, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_read");
        return -1;
    }
    virtual int fuse_write (const char *path, const char *src, size_t sz, off_t pos, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_write");
        return -1;
    }
    virtual int fuse_statfs (const char *path, struct statvfs *s)
    {
        FUSE_UNIMPLEMENTED("fuse_statfs");
        return -1;
    }
    virtual int fuse_flush (const char *path, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_flush");
        return -1;
    }
    virtual int fuse_release (const char *path, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_release");
        return -1;
    }
    virtual int fuse_fsync (const char *path, int m, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_fsync");
        return -1;
    }
    virtual int fuse_opendir (const char *path, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_opendir");
        return -1;
    }
    virtual int fuse_releasedir (const char *path, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_releasedir");
        return -1;
    }
    virtual void *fuse_init (struct fuse_conn_info *conn)
    {
        FUSE_UNIMPLEMENTED("fuse_init");
        return NULL;
    }
    virtual void fuse_destroy (void *p)
    {
        FUSE_UNIMPLEMENTED("fuse_destroy");
    }
    virtual int fuse_access (const char *path, int m)
    {
        FUSE_UNIMPLEMENTED("fuse_access");
        return -1;
    }
    virtual int fuse_create (const char *path, mode_t mode, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_create");
        return -1;
    }
    virtual int fuse_ftruncate (const char *path, off_t pos, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_ftruncate");
        return -1;
    }
    virtual int fuse_fgetattr (const char *path, struct stat *s, struct fuse_file_info *info)
    {
        FUSE_UNIMPLEMENTED("fuse_fgetattr");
        return -1;
    }
    virtual int fuse_utimens (const char *path, const struct timespec tv[2])
    {
        FUSE_UNIMPLEMENTED("fuse_utimens");
        return -1;
    }
};

CriticalSection CFuseBase::crit;
CFuseBase *CFuseBase::self=NULL;

#define FH_SALT 100
#define DIRCACHE_MAX 100   // TBD configurable
#define DIRCACHE_TIMEOUT  1  // min

class CDirCacheItem: public CInterface
{
    int err;
public:
    CDateTime dt;
    StringAttr path;
    StringArray scopes;
    StringArray supers;
    StringArray files;
    bool donefiles;

    CDirCacheItem()
    {
        donefiles = false;
        err = -ENOENT;
    }
        
    int init(const char *caller,const char *_path, bool allscopes,bool allfiles)
    {
        if (_path==NULL)
            path.set("");
        else
            path.set(_path);
        err = 0;
        try {
            if (!queryDistributedFileDirectory().loadScopeContents(_path,allscopes?&scopes:NULL,allfiles?&supers:NULL,allfiles?&files:NULL,true))
                err = -ENOENT;
            else
                if (allfiles)
                    donefiles = true;
        }
        catch (IException *e) {
            EXCLOG(e,caller);
            err = -EFAULT;
        }   
        return err;
    }

    const char *get(unsigned idx,bool &isscope)
    {
        if (idx<scopes.ordinality()) {
            isscope = true;
            return scopes.item(idx);
        }
        idx -= scopes.ordinality();
        isscope = false;
        if (idx<supers.ordinality()) 
            return supers.item(idx);
        idx -= supers.ordinality();
        if (idx<files.ordinality()) 
            return files.item(idx);
        return NULL;
    }
};

class CFuseDaliDFS: public CFuseBase
{
    IUserDescriptor *user;
    StringAttr daliServer;
    PointerArray openfiles;
    CIArrayOf<CDirCacheItem> dircache;

    bool checkUser()
    {
        fuse_context *context = fuse_get_context();

    }

    class DaFuseCmdThread: public Thread
    {
    public:
        DaFuseCmdThread()
            : Thread("DaFuseCmdThread")
        {
        }
    } *DaFuseCmdThread;

    CDirCacheItem *findDirCache(const char *path)
    {
        if (path==NULL)
            path = "";
        CDateTime cutoff;
        cutoff.setNow();
        cutoff.adjustTime(-DIRCACHE_TIMEOUT);
        ForEachItemInRev(i,dircache) {
            CDirCacheItem &item = dircache.item(i);
            if (item.dt.compare(cutoff)<0)
                dircache.remove(i);
            else if (strcmp(path,item.path)==0)
                return &item;
        }
        return NULL;
    }


    void addDirCache(CDirCacheItem *val)
    {
        CDateTime now;
        now.setNow();
        CDateTime cutoff(now);
        cutoff.adjustTime(-DIRCACHE_TIMEOUT);
        ForEachItemInRev(i,dircache) {
            CDirCacheItem &item = dircache.item(i);
            if (item.dt.compare(cutoff)<0)
                dircache.remove(i);
        }
        if (val) {
            if (dircache.ordinality()>=DIRCACHE_MAX)
                dircache.remove(0);
            dircache.append(*val);
            val->dt = now;
        }
    }



    bool pathToLFN(const char *path,CDfsLogicalFileName &lfn)
    {
        StringBuffer fn;
        StringArray dirs;
        dirs.appendList(path, "/");
        ForEachItemIn(i,dirs) {
            const char *dir = dirs.item(i);
            if (dir&&*dir) {
                if (fn.length())
                    fn.append("::");
                fn.append(dir);
            }
        }
        if (!fn.length())
            fn.append('.');
        return lfn.setValidate(fn.str());
    }


    int fuse_getattr(const char *path, struct stat *stbuf)
    {
        PROGLOG("fuse_getattr(%s,)",path);
        int res = 0;
        memset(stbuf, 0, sizeof(struct stat));
        if (!path||!*path)
            return -ENOENT;
        CDfsLogicalFileName lfn;
        const char *lfnstr = NULL;
        if (strcmp(path,"/")!=0) {
            if (!pathToLFN(path,lfn))
                return -ENOENT;
            lfnstr = lfn.get();
        }

        // this is a bit extreme (scanning all) but needed to count sub entries

        try {
            CDirCacheItem *dci = findDirCache(lfnstr);
            if (!dci) {
                dci = new CDirCacheItem();
                int err = dci->init("fuse_getattr",lfnstr,true,false);
                if (err) {
                    dci->Release();
                    dci = NULL;
                    if (err!=-ENOENT)
                        return err;
                }
                else
                    addDirCache(dci);
            }
            if (dci) {
                stbuf->st_mode = S_IFDIR | 0755;
                stbuf->st_nlink = dci->scopes.ordinality()+2;
            }
            else {
                Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn,user,false,false,false,nullptr,defaultPrivilegedUser);
                if (file) {
                    stbuf->st_mode = S_IFREG | 0444;
                    stbuf->st_nlink = 1;
#ifdef _WIN32
                    stbuf->st_size = (_off_t)file->getFileSize(true,false);
#else
                    stbuf->st_size = file->getFileSize(true,false);
#endif
                    CDateTime dt;
                    if (file->getModificationTime(dt)) {
                        stbuf->st_ctime = timetFromIDateTime(&dt);
                        stbuf->st_mtime = stbuf->st_ctime;
                        stbuf->st_atime = stbuf->st_ctime;
                    }
                    if (file->getAccessedTime(dt)) {
                        stbuf->st_atime = timetFromIDateTime(&dt);
                    }
                }
                else
                    res = -ENOENT;
            }
        }
        catch (IException *e) {
            EXCLOG(e,"fuse_getattr");
            res = -EFAULT;
        }   
        return res;
    }

    int fuse_readdir (const char *path, void *buf, fuse_fill_dir_t filler, off_t pos, struct fuse_file_info *info)
    {
        PROGLOG("fuse_readdir(%s,)",path);
        CDfsLogicalFileName lfn;
        const char *lfnstr = NULL;
        if (strcmp(path,"/")!=0) {
            if (!pathToLFN(path,lfn))
                return -ENOENT;
            lfnstr = lfn.get();
        }
        CDirCacheItem *dci = findDirCache(lfnstr);
        if (!dci) {
            dci = new CDirCacheItem();
            int err = dci->init("fuse_readdir",lfnstr,true,true);
            if (err) {
                dci->Release();
                dci = NULL;
                if (err!=-ENOENT)
                    return err;
            }
            else
                addDirCache(dci);
        }
        else if (!dci->donefiles) {
            int err = dci->init("fuse_readdir",lfnstr,false,true); // scopes already done
            if (err) {
                dircache.zap(*dci);
                return err; 
            }
        }

        struct stat s;
        memset(&s,0,sizeof(s));
        s.st_mode = (S_IFDIR | 0755);
        if (pos==0)
            if (filler(buf, ".", &s, ++pos))
                return 0;
        if (pos==1)
            if (filler(buf, "..", &s, ++pos))
                return 0;
        for (;;) {
            bool isscope;
            const char *fn = dci->get((unsigned)pos-2,isscope);
            if (!fn)
                break;
            s.st_mode = isscope?(S_IFDIR | 0755):(S_IFREG | 0444);
            PROGLOG("filler('%s',%d,%s)",fn,(unsigned)pos,isscope?"scope":"file");
            if (!filler(buf,fn,&s,++pos))
                break;
            
        }
        PROGLOG("fuse_readdir(%s,) exit",path);

        return 0;
    }


    int fuse_open (const char *path, struct fuse_file_info *info)
    {
        PROGLOG("fuse_open(%s,) flags=%d",path,info?info->flags:-1);
        CDfsLogicalFileName lfn;
        if (!pathToLFN(path,lfn))
            return -ENOENT;
        try {
            Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn,user,false,false,false,nullptr,defaultPrivilegedUser);
            if (!file)
                return -ENOENT;
            if((info->flags & 3) != O_RDONLY ) 
                return -EACCES;
            ForEachItemIn(i,openfiles) {
                if (openfiles.item(i)==NULL) {
                    openfiles.replace(file.getClear(),i);
                    info->fh = i+FH_SALT;
                    return 0;
                }
            }
            info->fh = openfiles.ordinality()+FH_SALT;
            openfiles.append(file.getClear());
        }
        catch (IException *e) {
            EXCLOG(e,"fuse_open");
            return -EFAULT;
        }   
        return 0;
    }

    int fuse_read (const char *path, char *dst, size_t _sz, off_t _pos, struct fuse_file_info *info)
    {
        offset_t pos = _pos;
        size32_t sz = _sz;
        PROGLOG("fuse_read(%s,,%d,%" I64F "d,)",path,sz,pos);
        int ret = 0;
        try {
            unsigned h = (unsigned)info->fh;
            Owned<IDistributedFile> file;
            if ((h>=FH_SALT)&&(h<FH_SALT+openfiles.ordinality())) 
                file.set((IDistributedFile *)openfiles.item(h-FH_SALT));
            else { // don't think this should happen but...
                CDfsLogicalFileName lfn;
                if (!pathToLFN(path,lfn))
                    return -ENOENT;
                file.setown(queryDistributedFileDirectory().lookup(lfn,user,false,false,false,nullptr,defaultPrivilegedUser));
                if (!file)
                    return -ENOENT;
            }
            offset_t base;
            unsigned pn = file->getPositionPart(pos,base);
            for (;;) {
                if (pn==NotFound)
                    break;
                IDistributedFilePart &part = file->queryPart(pn);
                offset_t psz = part.getFileSize(true,false);
                size32_t toread = (psz-pos-base<(offset_t)sz)?((size32_t)(psz-pos-base)):sz;
                PROGLOG("reading %d from part %d size %" I64F "d pos = %" I64F "d base=%" I64F "d",toread,pn,psz,pos,base);
                RemoteFilename rfn;
                part.getFilename(rfn,0);
                Owned<IFile> partfile = createIFile(rfn);
                for (unsigned copy=0;;) {
                    try {
                        if (partfile->exists())
                            break;
                    }
                    catch (IException *e) {
                        StringBuffer tmp;
                        IERRLOG("fuse_open %s",e->errorMessage(tmp).str());
                        partfile.clear();
                    }
                    copy++;
                    if (copy>=part.numCopies()) {
                        StringBuffer tmp;
                        part.getFilename(rfn,0);
                        rfn.getRemotePath(tmp);
                        if (copy>1) {
                            tmp.append(" or ");
                            part.getFilename(rfn,1);
                            rfn.getRemotePath(tmp);
                        }
                        OERRLOG("%s part %d not found at %s",path,pn+1,tmp.str());
                        return -ENOENT;
                    }
                    part.getFilename(rfn,copy);
                    partfile.setown(createIFile(rfn));
                }
                Owned<IFileIO> srcio = partfile->open(IFOread); // Need to cache all this! TBD
                if (!srcio) {
                    StringBuffer tmp;
                    rfn.getRemotePath(tmp);
                    OERRLOG("could not open '%s' for read",tmp.str());
                    return -EACCES;
                }
                bool blocked;
                bool compressed = file->isCompressed(&blocked);
                if (compressed&&blocked) {
                    Owned<ICompressedFileIO> cmpio = createCompressedFileReader(srcio);
                    if (cmpio.get())
                        srcio.setown(cmpio.getClear());
                }
                size32_t szrd = srcio->read(pos-base,toread,dst);
                PROGLOG("Read %s offset %" I64F "d len %d returned %d",partfile->queryFilename(),pos-base,toread,szrd);
                sz -= szrd;
                dst += szrd;
                ret += szrd;
                if (sz==0)
                    break;
                pn++;
                if (pn>=file->numParts())
                    break;
                base += psz;
                pos += szrd;
            }

        }
        catch (IException *e) {
            EXCLOG(e,"fuse_open");
            return -EFAULT;
        }   
        return ret;
    }

    int fuse_release (const char *path, struct fuse_file_info *info)
    {
        PROGLOG("fuse_release(%s)",path);
        try {
            unsigned h = (unsigned)info->fh;
            Owned<IDistributedFile> file;
            if ((h>=FH_SALT)&&(h<FH_SALT+openfiles.ordinality())) 
                openfiles.replace(NULL,h-FH_SALT);
        }
        catch (IException *e) {
            EXCLOG(e,"fuse_release");
            return -EFAULT;
        }   
        return 0;
    }

    void *fuse_init (struct fuse_conn_info *conn)
    {
        PROGLOG("fuse_init");
        try {
            setDaliServixSocketCaching(true);
            Owned<IGroup> serverGroup = createIGroup(daliServer,DALI_SERVER_PORT);
            initClientProcess(serverGroup, DCR_Dfu, 0, NULL, NULL, MP_WAIT_FOREVER);                // so that 
            startLogMsgParentReceiver();    // for auditing
            connectLogMsgManagerToDali();
        }
        catch (IException *e) {
            EXCLOG(e,"fuse_init");
        }   
        PROGLOG("fuse_init done");
        return this;

    }
    void fuse_destroy (void *p)
    {
        PROGLOG("fuse_destroy");
        try {
            openfiles.kill();
            dircache.kill();
            closedownClientProcess();
        }
        catch (IException *e) {
            EXCLOG(e,"fuse_destroy");
        }   
        PROGLOG("fuse_destroy done");
    }

/*

    int fuse_readlink (const char *path, char *out, size_t sz)
    {
        // TBD
        return -1;
    }

    int fuse_mkdir (const char *path, mode_t mode)
    {
        // TBD
        return -1;
    }

    int fuse_unlink (const char *path)
    {
        // TBD
        return -1;
    }

    int fuse_rmdir (const char *path)
    {
        // TBD
        return -1;
    }

    int fuse_rename (const char *from, const char *to)
    {
        // TBD
        return -1;
    }

    int fuse_truncate (const char *path, off_t pos)
    {
        // TBD
        return -1;
    }


    int fuse_write (const char *path, const char *src, size_t sz, off_t pos, struct fuse_file_info *info)
    {
        // TBD
        return -1;
    }

    int fuse_statfs (const char *path, struct statvfs *stbuf)
    {
        // TBD
        return -1;
    }

    int fuse_flush (const char *path, struct fuse_file_info *info)
    {
        // TBD
        return -1;
    }


    int fuse_fsync (const char *path, int m, struct fuse_file_info *info)
    {
        // TBD
        return -1;
    }

    int fuse_opendir (const char *path, struct fuse_file_info *info)
    {
        // TBD
        return -1;
    }

    int fuse_releasedir (const char *path, struct fuse_file_info *info)
    {
        // TBD
        return -1;
    }

    int fuse_access (const char *path, int m)
    {
        // TBD
        return -1;
    }

    int fuse_create (const char *path, mode_t mode, struct fuse_file_info *info)
    {
        // TBD
        return -1;
    }

    int fuse_ftruncate (const char *path, off_t pos, struct fuse_file_info *info)
    {
        // TBD
        return -1;
    }

    int fuse_fgetattr (const char *path, struct stat *stbuf, struct fuse_file_info *info)
    {
        // TBD
        return -1;
    }

    int fuse_utimens (const char *path, const struct timespec tv[2])
    {
        // TBD
        return -1;
    }
*/

public:
    CFuseDaliDFS(const char *_daliServer,IProperties *prop)
        : daliServer(_daliServer)
    {
        FUSE_USE(getattr);
        FUSE_USE(readdir);
        FUSE_USE(open);
        FUSE_USE(read);
        FUSE_USE(init);
        FUSE_USE(destroy);
        FUSE_USE(release);
        user = NULL; // TBD
    }


};

static void usage()
{
    printf("Usage:\n");
    printf("  dafuse mount <mount-dir> <options>\n");
    printf("  dafuse unmount <mount-dir> \n");
    printf("  dafuse mapuser <mount-dir> <ldap-user> [ <ldap-password> ]\n");
    printf("  dafuse sprayopt <mount-dir> <dstfilemask> <cluster> <create-options>\n");
    printf("  dafuse list <mount-dir>     -- lists spray options set and user mappings\n");
    printf("The following options can be passed on the command line or in ./dafuse.ini\n\n");
    printf("  DALISERVER=<dali-ip>\n");
    printf("TBD:\n");
    printf("  DIR=<mount-dir>                 -- (not needed if specified on command line)\n");
    printf("  DEFAULTLDAPUSER=<LDAP-user-name>  -- default LDAP user\n");
    printf("  PASSWORD=<LDAP-password>        -- if not specified will prompt for\n");
    printf("  LOCALUSERS=<userlist>           -- allowed non-root users of share (default all)\n");
    printf("  READONLY=0|1                    -- whether can create/delete files\n");
    printf("  ROOTONLY=0|1                    -- only root can access mount\n");
    printf("  NODELETE=0|1                    -- disable deletion or overwrite\n");
    printf("  CACHETIMEOUT=<mins>             -- default 1min \n");
}

static bool isOpt(const char *&s,const char *name)
{
    if (s&&name) {
        size32_t nl = strlen(name);
        if (strlen(s)>nl) {
            if (memicmp(s,name,nl)==0) {
                if (s[nl]=='=') {
                    s += nl+1;
                    return true;
                }
            }
        }
    }
    return false;
}

static bool parseOption(const char *s, IProperties *prop)
{
    if (isOpt(s,"daliserver")||isOpt(s,"daliservers")) {
        prop->setProp("DALISERVER",s);
        return true;
    }
    if (isOpt(s,"user")) {
        prop->setProp("USER",s);
        return true;
    }
    if (isOpt(s,"password")) {
        prop->setProp("PASSWORD",s);
        return true;
    }
    if (isOpt(s,"logfile")) {
        prop->setProp("LOGFILE",s);
        return true;
    }
    return false;
}

static void removeParam(int &argc, char *argv[], int i)
{
    while (++i<argc)
        argv[i-1] = argv[i];
    argc--;
}

inline bool eqs(const char *s1,const char *s2)
{
    return strieq(s1?s1:"",s2?s2:"");
}

static bool parseParams(int &argc, char *argv[],IProperties *prop)
{
    int i = 1;
    bool actiongot = false;
    bool dirgot = false;
    prop->removeProp("ACTION");
    while (i<argc) {
        const char *arg = argv[i];
        if (*arg=='-') {
            // ignore - all passed to fuse
        }
        else if (!actiongot) {
            prop->setProp("ACTION",arg);
            removeParam(argc,argv,i);
            actiongot = true;
            continue;
        }
        else if (!dirgot) {
            prop->setProp("DIR",arg);
            // don't remove
            dirgot = true;
            continue;
        }
        else if (parseOption(arg,prop)) {
            removeParam(argc,argv,i);
            actiongot = true;
            continue;
        }
        i++;
    }
    // validate props
    const char *action= prop->queryProp("ACTION");
    if (eqs(action,"mount")) {
        if (prop->hasProp("DIR"))
            return true;
    }
    if (eqs(action,"login")) {
        return true;
    }
    if (eqs(action,"unmount")||eqs(action,"umount")) {
        prop->setProp("ACTION","unmount");
        if (prop->hasProp("DIR"))
            return true;
    }
    if (eqs(action,"creatopt")) {
        return true;
    }
    usage();
    return false;
}


int main(int argc, char *_argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    // setDefaultUser("TBD","TBD");
    Thread::setDefaultStackSize(0x20000);

    SocketEndpoint ep;
    ep.setLocalHost(0);
    char **argv = (char **)calloc(MAXPARAM,sizeof(char *));
    for (int i1=0;i1<argc;i1++)
        argv[i1] = strdup(_argv[i1]);       

    Owned<IProperties> prop = createProperties("dafuse.ini",true);
    if (!parseParams(argc,argv,prop))
        return 1;

    {
        StringBuffer logName;
        if (!prop->getProp("LOGFILE", logName))
            logName.set(dafuse);

        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(logName.str());
        lf->setMsgFields(MSGFIELD_STANDARD);
        lf->setAppend(false);
        ls->setMaxDetail(TopDetail);
        lf->beginLogging();
    }

    StringBuffer daliServer;
    if (!prop->getProp("DALISERVER", daliServer)) {
        OERRLOG("DALISERVER setting not found in dafuse.ini");
        return 1;
    }
    int res;
    try {
        const char *action= prop->queryProp("ACTION");
        if (eqs(action,"mount")) {
            printf("mounting %s",prop->queryProp("DIR"));
            CFuseDaliDFS hw(daliServer.str(),prop);
            res = hw.main(argc,argv);
        }
        else if (eqs(action,"unmount")) {
            StringBuffer s("umount ");
            s.append(prop->queryProp("DIR"));
            printf("unmounting %s",prop->queryProp("DIR"));
            system(s.str());
        }
    }
    catch (IException *e) {
        EXCLOG(e,"dafuse");
        res = 2;
    }   
/*  only needed for leak checking
    for (unsigned i2=0;i2<MAXPARAM;i2++)
        free(argv[i2]);     
    free(argv);
    releaseAtoms();
*/
    return res;
}

/* fuse opts

general options:
    -o opt,[opt...]        mount options
    -h   --help            print help
    -V   --version         print version

FUSE options:
    -d   -o debug          enable debug output (implies -f)
    -f                     foreground operation
    -s                     disable multi-threaded operation

    -o allow_other         allow access to other users
    -o allow_root          allow access to root
    -o nonempty            allow mounts over non-empty file/dir
    -o default_permissions enable permission checking by kernel
    -o fsname=NAME         set filesystem name
    -o subtype=NAME        set filesystem type
    -o large_read          issue large read requests (2.4 only)
    -o max_read=N          set maximum size of read requests

    -o hard_remove         immediate removal (don't hide files)
    -o use_ino             let filesystem set inode numbers
    -o readdir_ino         try to fill in d_ino in readdir
    -o direct_io           use direct I/O
    -o kernel_cache        cache files in kernel
    -o [no]auto_cache      enable caching based on modification times (off)
    -o umask=M             set file permissions (octal)
    -o uid=N               set file owner
    -o gid=N               set file group
    -o entry_timeout=T     cache timeout for names (1.0s)
    -o negative_timeout=T  cache timeout for deleted names (0.0s)
    -o attr_timeout=T      cache timeout for attributes (1.0s)
    -o ac_attr_timeout=T   auto cache timeout for attributes (attr_timeout)
    -o intr                allow requests to be interrupted
    -o intr_signal=NUM     signal to send on interrupt (10)
    -o modules=M1[:M2...]  names of modules to push onto filesystem stack

    -o max_write=N         set maximum size of write requests
    -o max_readahead=N     set maximum readahead
    -o async_read          perform reads asynchronously (default)
    -o sync_read           perform reads synchronously
    -o atomic_o_trunc      enable atomic open+truncate support
    -o big_writes          enable larger than 4kB writes
    -o no_remote_lock      disable remote file locking

Module options:

[subdir]
    -o subdir=DIR           prepend this directory to all paths (mandatory)
    -o [no]rellinks         transform absolute symlinks to relative

[iconv]
    -o from_code=CHARSET   original encoding of file names (default: UTF-8)
    -o to_code=CHARSET      new encoding of the file names (default: UTF-8)

*/
