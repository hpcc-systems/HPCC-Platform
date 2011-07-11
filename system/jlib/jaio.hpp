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



#ifndef JAIO_INCL
#define JAIO_INCL

#include "jlib.hpp"
#include "jmisc.hpp"

#if defined (__linux__)

class AsyncRequest;
class jlib_decl AsyncBlockReader
{
public:

  AsyncBlockReader();
  ~AsyncBlockReader();

  void init(int file, offset_t start,size32_t blocksize,void *buf1,void *buf2);
  void *readnext(size32_t &ret);
  void getinfo(offset_t &of,offset_t &p,offset_t &sz);
private:
  void waitblk();
  void enqueue(AsyncRequest *req);
  void finish();
  
  
  AsyncRequest *cur;
  AsyncRequest *next;
  size32_t blksize;
  offset_t offset;
  offset_t pos;
  offset_t start;
  offset_t insize;
  int infile;
  int eof;
};

#endif  



#ifdef _WIN32

class CW32AsyncRequest;                 // forward reference


class jlib_decl CW32AsyncBlockReader
{

private:
    OVERLAPPED overlapped;              // the file io overlap control structure
    HANDLE hfile;
    CW32AsyncRequest * currentRequest, * nextRequest;
    bool eof, pending;                  // pending is true when an IO operation has been 'requested' but no yet completed
    C64bitComposite offset, insize;
    offset_t start;                     // offset is contained also in overlapped as hi and lo DWORDs
    size32_t blockSize;

    void enqueue();                     // calls for next record
    DWORD waitblk();                    // waits for next record, returns bytes read
    void finish();

public:
    CW32AsyncBlockReader();
    ~CW32AsyncBlockReader();

    void init(HANDLE file, offset_t start, size32_t blockSize, void * buffer1, void * buffer2); 
    void * readnext(size32_t &readLength);
    void reset();
    void getinfo(offset_t &of, offset_t &p, offset_t &sz);
};

#endif  // _WIN32




#endif

