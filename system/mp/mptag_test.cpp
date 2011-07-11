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

#include "mptag.hpp"
#include "mptag.ipp"
//#include "mptag.cpp"
#include <stdio.h>
#include <jexcept.hpp>

/******************************
 ** TEST CASES
 ******************************/

// 1. populate to and beyond maximum limit, free everything, 
void test1()
{
    int i; const size = 1000; //TAG_DYNAMIC_MAX - TAG_DYNAMIC
    mptag_t a[size];
    for( i=0;i<(size);i++)  {   
        a[i]=allocMPtag("hello",123);
        printf("alloc: %d \n" ,a[i]);   
    }
    printf("alloc: %d \n" , allocMPtag("hello",123));
    for( i=0;i<size;i++)    {       //free marked 10
        printf("free: %d \n",a[i]);
        freeMPtag(a[i]);    
    }

}

// 2. populate table, remove a few randomly, populate some more
void test2()
{
    mptag_t a[10]; int i; //IMessageTraceFormatter f;
    printf("**********************ENTER 100 \n"); 
    for( i=1;i<101;i++) {       //enter 100
        printf("alloc: %d \n" ,allocMPtag("hello",123));    
    }
    printf("**********************BELOW MARKED FOR REMOVAL \n"); 
    for( i=0;i<10;i++)  {       //enter another 10 (marked for removal)
        printf("alloc: %d \n" ,a[i] = allocMPtag("hello",123));
    }
    printf("**********************ENTER ANOTHER 100 \n"); 
    for( i=1;i<101;i++) {       //enter 100
        printf("alloc: %d \n" ,allocMPtag("hello",123));    
    }
    printf("**********************RETRIVE FORMATTER, CALL FORMAT ON MARKED 10\n"); 
    MemoryBuffer* m = new MemoryBuffer(); m->append("sdasda");  StringBuffer* s = new StringBuffer("random string");
    for( i=0;i<10;i++)  {       //free marked 10
        CMessageTraceFormatterDefault* f = (CMessageTraceFormatterDefault*) queryFormatter(a[i]);
        printf("format: %s \n",f->format((mptag_t)999, *m, *s));
    }   
    printf("**********************FREEING MARKED FOR REMOVAL \n"); 
    for( i=0;i<10;i++)  {       //free marked 10
        printf("free: %d \n",a[i]);
        freeMPtag(a[i]);    
    }
    printf("**********************ENTER 100 \n"); 
    for( i=1;i<101;i++) {       //enter 100
        printf("alloc: %d \n" ,allocMPtag("hello",123));    
    }
    printf("**********************ENDGAME \n");
    a[0] =  allocMPtag("hello",123); //alloc,free,alloc
    printf("alloc: %d \n" , a[0]);
    freeMPtag(a[0]);
    printf("free: %d \n" , a[0]);
    a[0] =  allocMPtag("hello",123);
    printf("alloc: %d \n" , a[0]);

    delete m; delete s;
}

// 3. create formatter and pass in; create new formatter and associate
void test3(){
    IMessageTraceFormatter* f = new CMessageTraceFormatterDefault("tracename ",101);
    int i; const size = 10; //size must be even
    mptag_t a[size];
    for( i=0;i<(size);i++)  {   
        a[i]=allocMPtag(*f);
        printf("allocF: %d \n" ,a[i]);  
    }
    f->Release();

    IMessageTraceFormatter* f2 = new CMessageTraceFormatterDefault("tracename ",101);
    for( i=0;i<((size/2)-1);i++)    {   
        associateMPtag(a[i],*f2);
        printf("a$$ocF: %d \n" ,a[i]);  
    }
    f2->Release();

    for( i=((size/2));i<size;i++)   {       //free marked 10
        printf("freeF: %d \n",a[i]);
        freeMPtag(a[i]);
    }
}

// 4. free non-existant tag, get fromatter table for non-existant tag
void test4()
{   StringBuffer msg("ERROR: ");    StringBuffer msg2("ERROR: ");
    printf("***Freeing Tag that does not exist****\n");
    try{
        freeMPtag((mptag_t)999);
    }catch(IException *e){printf("%s\n", e->errorMessage(msg).toCharArray());e->Release();}
    try{
        printf("***Getting Formater for Tag that does not exist***\n");
        queryFormatter((mptag_t)999);
    }catch(IException *e){printf("%s\n", e->errorMessage(msg2).toCharArray());e->Release();}

}

int main(int argc, char* argv[])
{
    printf("\n\n");
    test1();
    printf("\n\n");
    test2();    
    printf("\n\n");
    test3();    
    printf("\n\n");
    test4();    

    releaseAtoms();

    return 1;
}
