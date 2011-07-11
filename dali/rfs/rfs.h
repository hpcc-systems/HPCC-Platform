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

#ifndef RFS_H
#define RFS_H


#include <stdio.h>

#ifndef byte
#define byte    unsigned char
#endif

struct RFS_context;

#define RFS_OPEN_MODE_CREATE        0x00
#define RFS_OPEN_MODE_READ          0x01
#define RFS_OPEN_MODE_WRITE     0x02
#define RFS_OPEN_MODE_READWRITE 0x03
#define RFS_OPEN_MODE_CREATERW      0x04
#define RFS_OPEN_MODE_MASK          0x07

#define RFS_SHARING_NONE            0x00
#define RFS_SHARING_READ            0x01
#define RFS_SHARING_WRITE           0x02
#define RFS_SHARING_EXEC            0x03
#define RFS_SHARING_ALL         0x04


#ifdef _WIN32
typedef unsigned __int64 rfs_fpos_t;
#else
typedef unsigned long long rfs_fpos_t;
#endif

class RFS_ConnectionBase
{
public:
    RFS_ConnectionBase() {}
    virtual ~RFS_ConnectionBase() {}

    virtual void read(rfs_fpos_t pos, size_t len, size_t &outlen, void *out) = 0;
    virtual rfs_fpos_t size() = 0;
    virtual void write(rfs_fpos_t pos, size_t len, const void *in) = 0;
    virtual void close() = 0;
};

class RFS_ServerBase
{
public:
    virtual RFS_ConnectionBase *open(const char *name, byte mode, byte share) = 0;  // return NULL if not found

    virtual void existFile(const char *filename, bool &existsout) = 0;
    virtual void removeFile(const char *filename) = 0;
    virtual void renameFile(const char *fromname,const char *toname) = 0;
    virtual void getFileTime(const char *filename, time_t &outaccessedtime, time_t &outcreatedtime, time_t &outmodifiedtime) = 0;
    virtual void setFileTime(const char *filename, time_t *inaccessedtime, time_t *increatedtime, time_t *inmodifiedtime) = 0; // params NULL if not to be set

    virtual void isFile(const char *filename, bool &outisfile) = 0;
    virtual void isDir(const char *filename, bool &outisdir) = 0;
    virtual void isReadOnly(const char *filename, bool &outisreadonly) = 0;
    virtual void setReadOnly(const char *filename, bool readonly) = 0;
    virtual void createDir(const char *dirname,bool &createdout) = 0;
    virtual void openDir(const char *dirname, const char *mask, bool recursive, bool includedir, void * &outhandle) = 0;
    virtual void nextDirEntry(void *handle, size_t maxfilenamesize, char *outfilename, bool &isdir, rfs_fpos_t &filesizeout, time_t &outmodifiedtime) = 0;  // empty return for filename marks end
    virtual void closeDir(void *handle) = 0;

    virtual void getVersion(size_t programnamemax, char *programname, short &version) = 0;
    virtual unsigned short getDefaultPort();        // use if --port not specified (if overridden)

    virtual void poll() {}                          // called approx every second when idle

    virtual void getServiceName(size_t maxname, char *outname, // only called for windows services
                                size_t maxdisplayname, char *outdisplayname);
    virtual int serviceInit(int argc, const char **argv,
                            bool &outmulti);            // only called for windows services (return 0 if OK)

    RFS_ServerBase() { context = NULL; }
    virtual ~RFS_ServerBase();

    bool init(int &argc, const char **argv);    // must be called from main, will remove parameters it uses.
                                                // If returns false must exit returning 1


    int run(bool multi=false,                   // called to enter server run loop (multi not yet supported)
            const char *logname="");            // NULL is no log, "" is default

    void stop();                                // can be called async to stop server (e.g. from poll)

    void setLogFilename(const char *filename);  // set to NULL for no logfile (default is <exename>_<datetime>.log in cur dir)
    virtual void log(const char *format, ...);
    void throwError(int err, const char *errstr, bool fatal=false);     // does not return, if fatal will stop process
    const char *logFilename();

    unsigned myNode();               // not yet supported
    unsigned clusterSize();          // not yet supported

    RFS_context *queryContext();    // internal
    int debugLevel();               // value of --debug

private:
    RFS_context *context;

};

class RFS_SimpleString
{
    size_t max;
    size_t inc;
    char *base;
    char *end;
public:

    RFS_SimpleString(const char *inits=NULL,size_t initsz=0x1000);
    ~RFS_SimpleString()
    {
        free(base);
    }

    void appendc(char c);
    void appends(const char *s);

    inline const char *str() { *end = 0; return base; }
    inline char *data() { return base; }
    inline char *dup() { return _strdup(str()); }
    inline void clear() { end = base; }
    inline size_t length() { return end-base; }
    inline void setLength(size_t sz) { end = base+sz; }
    inline char lastChar() { if (end==base) return 0; return *(end-1); }
    inline void decLength(size_t sz=1) { if ((size_t)(end-base)<=sz) end = base; else end-=sz; }
    void trim();
};


class RFS_CSVwriter // for serializing rows (handles escaping etc)
{
    RFS_SimpleString out;
public:
    RFS_CSVwriter();
    void rewrite();
    void putField(size_t fldsize,void *data);
    void putRow();
    void consume(size_t sz);
    inline const void *base()  { return out.str(); }
    inline size_t length() { return out.length(); }
};

class RFS_CSVreader // for serializing rows (handles escaping etc)
{
    RFS_SimpleString fld;
    char * str;
    char * end;
public:
    RFS_CSVreader();
    void reset(size_t sz,const void *data);
    bool nextRow();
    const char * getField(size_t &fldsize);
};


#endif
