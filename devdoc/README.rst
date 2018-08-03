=======================
Developer Documentation
=======================

This directory contains the documentation specifically targeted at developers of the HPCC system.  Information
is also include in the wiki at https://github.com/hpcc-systems/HPCC-Platform/wiki.

General documentation
=====================

* `Development guide`_: Building the system and development guide.

* `C++ style guide`_: Style guide for c++ code.

* `ECL style guide`_: Style guide for ECL code.

Implementation details for different parts of the system
========================================================

* `Workunit Workflow`_: An explanation of workunits, and a walk-through of the steps in executing a query.

* `Code Generator Documentation`_: Details of the internals of eclcc.

* `Memory Manager`_: Details of the memory manager (roxiemem) used by the query engines.


Other documentation
===================
The ECL language is documented in the ecl language reference manual (generated as ECLLanguageReference-<version>.pdf).

.. _Development guide: Development.rst
.. _Code Generator Documentation: CodeGenerator.rst
.. _Workunit Workflow: WorkUnits.rst
.. _Memory Manager: MemoryManager.rst
.. _C++ style guide: StyleGuide.rst
.. _ECL style guide: ../ecllibrary/StyleGuide.html
