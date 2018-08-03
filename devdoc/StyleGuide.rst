..  ################################################################################
    #    HPCC SYSTEMS software Copyright (C) 2012-2018 HPCC Systems®.
    #
    #    Licensed under the Apache License, Version 2.0 (the "License");
    #    you may not use this file except in compliance with the License.
    #    You may obtain a copy of the License at
    #
    #       http://www.apache.org/licenses/LICENSE-2.0
    #
    #    Unless required by applicable law or agreed to in writing, software
    #    distributed under the License is distributed on an "AS IS" BASIS,
    #    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    #    See the License for the specific language governing permissions and
    #    limitations under the License.
    ################################################################################

==================
Coding conventions
==================

***********************
Why coding conventions?
***********************

Everyone has their own ideas of what the best code formatting style is, but most
would agree that code in a mixture of styles is the worst of all worlds. A
consistent coding style makes unfamiliar code easier to understand and navigate.

In an ideal world, the HPCC sources would adhere to the coding standards described
perfectly. In reality, there are many places that do not. These are being cleaned up
as and when we find time.

**********************
C++ coding conventions
**********************
Unlike most software projects around, HPCC has some very specific
constraints that makes most basic design decisions difficult, and often
the results are odd to developers getting acquainted with its code base.
For example, when HPCC was initially developed, most common-place
libraries we have today (like STL and Boost) weren't available or stable
enough at the time.

Also, at the beginning, both C++ and Java were being considered as
the language of choice, but development started with C++. So a C++
library that copied most behaviour of the Java standard library (At the
time, Java 1.4) was created (see jlib below) to make the transition, if
ever taken, easier. The transition never happened, but the decisions
were taken and the whole platform is designed on those terms.

Most importantly, the performance constraints in HPCC can make
no-brainer decisions look impossible in HPCC. One example is the use of
traditional smart pointers implementations (such as boost::shared_ptr or
C++'s auto_ptr), that can lead to up to 20% performance hit if used
instead of our internal shared pointer implementation.

The last important point to consider is that some
libraries/systems were designed to replace older ones but haven't got
replaced yet. There is a slow movement to deprecate old systems in
favour of consolidating a few ones as the elected official ways to use
HPCC (Thor, Roxie) but old systems still could be used for years in
tests or legacy sub-systems.

In a nutshell, expect re-implementation of well-known containers
and algorithms, expect duplicated functionality of sub-systems and
expect to be required to use less-friendly libraries for the sake of
performance, stability and longevity.

For the most part out coding style conventions match those
described at http://geosoft.no/development/cppstyle.html, with a few
exceptions or extensions as noted below.

Source files
============

We use the extension .cpp for C++ source files, and .h or .hpp for header files.
Header files with the .hpp extension should be used for headers that are internal
to a single library, while header files with the .h extension should be used for
the interface that the library exposes. There will typically be one .h file per
library, and one .hpp file per cpp file.

Source file names within a single shared library should share a common prefix to aid
in identifying where they belong.

Header files with extension .ipp (i for internal) and .tpp (t for template) will
be phased out in favour of the scheme described above.

Java-style
==========
We adopted a Java-like inheritance model, with macro
substitution for the basic Java keywords. This changes nothing on the
code, but make it clearer for the reader on what's the recipient of
the inheritance doing with it's base.

* **interface** (struct): declares an interface (pure virtual class)

* **extends** (public): One interface extending another, both are pure virtual

* **implements** (public): Concrete class implementing an interface

There is no semantic check, which makes it difficult to enforce
such scheme, which has led to code not using it intermixed with code
using it. You should use it when possible, most importantly on code
that already uses it.

We also tend to write methods inline, which matches well with
C++ Templates requirements. We, however, do not enforce the
one-class-per-file rule.

See the `Interfaces`_ section for more information on our implementation of
interfaces.

Identifiers
===========
Class and interface names are in CamelCase with a leading
capital letter. Interface names should be prefixed capital I followed
by another capital. Class names may be prefixed with a C if there is a
corresponding I-prefixed interface name, e.g. when the interface is primarily used to create an opaque type, but
need not be otherwise.

Variables, function and method names, and parameters use camelCase starting with a lower case letter. Parameters may
be prefixed with underscore when the parameter is used to initialize a member variable of the same name.  Common cases
are constructors and setter methods.

Example::

   class MySQLSuperClass
   {
        bool haslocalcopy = false;
        void mySQLFunctionIsCool(int _haslocalcopy, bool enablewrite)
        {
            if (enablewrite)
                haslocalcopy = _haslocalcopy;
        }
    };

Pointers
========
Use real pointers when you can, and smart pointers when you have
to. Take extra care on understanding the needs of your pointers and
their scope. Most programs can afford a few dangling pointers, but a
high-performance clustering platform cannot.

Most importantly, use common sense and a lot of thought. Here are a few guidelines:

* Use real pointers for return values, parameter passing.

* For local variables use real pointers if their lifetime is
  guaranteed to be longer than the function (and no exception
  is thrown from functions you call), shared pointers otherwise.

* Use Shared pointers for member variables - unless there is
  a strong guarantee the object has a longer lifetime.

* Create Shared<X> with either:

  - Owned<X>: if your new pointer will take ownership of the pointer

  - Linked<X>: if you are sharing the ownership (shared)

Warning: Direct manipulation of the ownership might
cause Shared<> pointers to lose the pointers, so subsequent
calls to it (like o2->doIt() after o3 gets ownership) **will** cause
segmentation faults.

Refer to `Reference counted objects` for more information on our smart pointer
implementation, Shared<>.

Methods that return pointers to link counted objects, or that use them,
should use a common naming standard:

* Foo * queryFoo()
  Does not return a linked pointer since lifetime is guaranteed for a set period. Caller should link if it
  needs to retain it for longer.

* Foo * getFoo()
  Returned value is linked and should be assigned to an owned, or returned directly.

* void setFoo(Foo * x)
  Generally parameters to functions are assumed to be owned by the caller, the callee needs to link them if they
  are retained.

* void setFoo(Foo * ownedX)
  Some calls do transfer ownership of parameters - the parameter should be named to indicate this.  If the function
  only has a single signficant parameter then sometimes the name of the function indicates the ownership.

Indentation
===========
We use 4 spaces to indent each level. TAB characters should not be used.

The { that starts a new scope and the corresponding } to close it are placed on a
new line by themselves, and are not indented. This is sometimes known as the Allman
or ANSI style.

Comments
========
We generally believe in the philosophy that well written code is self-documenting.  Comments are also
encouraged to describe *why* something is done, rather than how - which should be clear from the code.

javadoc-formatted comments for classes and interfaces are being added.

Classes
========
The virtual keyword should be included on the declaration of all virtual functions - including those in derived
classes, and the override keyword should be used on all virtual functions in derived classes.

Namespaces
==========
MORE: Update!!!

We do not use namespaces. We probably should, following the Google style guide's
guidelines - see http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml#Namespaces

Other
=====
We often pretend we are coding in Java and write all our class members inline.

C++11
=====


************************
Other coding conventions
************************

ECL code
========
The ECL style guide is published separately.

Javascript, XML, XSL etc
========================
We use the commonly accepted conventions for formatting these files.

===============
Design Patterns
===============

********************
Why Design Patterns?
********************
Consistent use of design patterns helps make the code easy to understand.

Interfaces
==========
While C++ does not have explicit support for interfaces (in the java sense), an
abstract class with no data members and all functions pure virtual can be used
in the same way.

Interfaces are pure virtual classes. They are similar concepts to
Java's interfaces and should be used on public APIs. If you need common
code, use policies (see below).

An interface's name must start with an 'I' and the base class for
its concrete implementations should start with a 'C' and have the same
name, ex::

    CFoo : implements IFoo { };

When an interface has multiple implementations, try to stay as
close as possible to this rule. Ex::

    CFooCool : implements IFoo { };
    CFooWarm : implements IFoo { };
    CFooALot : implements IFoo { };

Or, for partial implementation, use something like this::

    CFoo : implements IFoo { };
    CFooCool : public CFoo { };
    CFooWarm : public CFoo { };

Extend current interfaces only on a 'is-a' approach, not to
aggregate functionality. Avoid pollution of public interfaces by having
only the public methods on the most-base interface in the header, and
internal implementation in the source file. Prefer pImpl idiom
(pointer-to-implementation) for functionality-only requirements and
policy based design for interface requirements.

Example 1: You want to decouple part of the implementation from
your class, and this part does not implements the interface your
contract requires.::

    interface IFoo
    {
        virtual void foo()=0;
    };
    // Following is implemented in a separate private file...
    class CFoo : implements IFoo
    {
        MyImpl *pImpl;
    public:
        virtual void foo() override { pImpl->doSomething(); }
    };

Example2: You want to implement the common part of one (or more)
interface(s) in a range of sub-classes.::

    interface ICommon
    {
        virtual void common()=0;
    };
    interface IFoo : extends ICommon
    {
        virtual void foo()=0;
    };
    interface IBar : extends ICommon
    {
        virtual void bar()=0;
    };

    template <class IFACE>
    class Base : implements IFACE
    {
        virtual void common() override { ... };
    }; // Still virtual

    class CFoo : public Base<IFoo>
    {
        void foo() override { 1+1; };
    };
    class CBar : public Base<IBar>
    {
        void bar() override { 2+2; };
    };

NOTE: Interfaces deliberately do not contain virtual destructors.  This is to help ensure that they are never
destroyed by calling delete directly.

Reference counted objects
=========================
Shared<> is an in-house intrusive smart pointer implementation. It is
close to boost's intrusive_ptr. It has two derived implementations:
Linked and Owned, which are used to control whether the pointer is
linked when a shared pointer is created from a real pointer or not,
respectively. Ex::

    Owned<Foo> myFoo = new Foo; // Take owenership of the pointers
    Linked<Foo> anotherFoo = = myFoo; // Shared ownership

Shared<> is thread-safe and uses atomic reference count
handled by each object (rather than by the smart pointer itself, like
boost's shared_ptr).

This means that, to use Shared<>, your class must implement the Link() and Release() methods - most commonly by
extending the CInterfaceOf<> class, or the CInterface class (and using the IMPLEMENT_IINTERFACE macro in the public
section of your class declaration).

This interface controls how you Link() and Release() the pointer.
This is necessary because in some inner parts of HPCC, the use of a
"really smart" smart pointer would add too many links and releases (on
temporaries, local variables, members, etc) that could add to a
significant performance hit.

The CInterface implementation also include a virtual function beforeDispose() which is called before the object is
deleted.  This allows resources to be cleanly freed up, with the full class hierarchy (including virtual functions)
available even when freeing items in base classes.  It is often used for caches that do not cause the objects to be
retained.

STL
===
MORE: This needs documenting

=================================
Structure of the HPCC source tree
=================================

MORE!

Requiring more work:
* namespaces
* STL
* c++11
* Review all documentation
* Better examples for shared