import{_ as a,a as i,o as t,ag as n}from"./chunks/framework.Do1Zayaf.js";const f=JSON.parse('{"title":"CMake files structure and usage","description":"","frontmatter":{},"headers":[],"relativePath":"cmake_modules/DOCUMENTATION.md","filePath":"cmake_modules/DOCUMENTATION.md","lastUpdated":1755191786000}'),o={name:"cmake_modules/DOCUMENTATION.md"};function r(s,e,l,c,d,m){return t(),i("div",null,e[0]||(e[0]=[n(`<h1 id="cmake-files-structure-and-usage" tabindex="-1">CMake files structure and usage <a class="header-anchor" href="#cmake-files-structure-and-usage" aria-label="Permalink to &quot;CMake files structure and usage&quot;">​</a></h1><h2 id="directory-structure-of-cmake-files" tabindex="-1">Directory structure of CMake files <a class="header-anchor" href="#directory-structure-of-cmake-files" aria-label="Permalink to &quot;Directory structure of CMake files&quot;">​</a></h2><p>- /</p><p>: - CMakeLists.txt - Root CMake file - version.cmake - common cmake file where version variables are set - build-config.h.cmake - cmake generation template for build-config.h</p><pre><code>\\- cmake\\_modules/ - Directory storing modules and configurations for CMake

:   -   FindXXXXX.cmake - CMake find files used to locate libraries,
        headers, and binaries
    -   commonSetup.cmake - common configuration settings for the
        entire project (contains configure time options)
    -   docMacros.cmake - common documentation macros used for
        generating fop and pdf files
    -   optionDefaults.cmake - contains common variables for the
        platform build
    -   distrocheck.sh - script that determines if the OS uses DEB
        or RPM
    -   getpackagerevisionarch.sh - script that returns OS version
        and arch in format used for packaging

    \\- dependencies/ - Directory storing dependency files used for package dependencies

    :   -   \\&lt;OS\\&gt;.cmake - File containing either DEB or RPM
            dependencies for the given OS

\\- build-utils/ - Directory for build related utilities

:   -   cleanDeb.sh - script that unpacks a deb file and rebuilds
        with fakeroot to clean up lintain errors/warnings
</code></pre><h3 id="common-macros" tabindex="-1">Common Macros <a class="header-anchor" href="#common-macros" aria-label="Permalink to &quot;Common Macros&quot;">​</a></h3><ul><li>MACRO_ENSURE_OUT_OF_SOURCE_BUILD - prevents building from with in source tree</li><li>HPCC_ADD_EXECUTABLE - simple wrapper around add_executable</li><li>HPCC_ADD_LIBRARY - simple wrapper around add_library</li><li>PARSE_ARGUMENTS - macro that can be used by other macros and functions for arg list parsing</li><li>HPCC_ADD_SUBDIRECTORY - argument controlled add subdirectory wrapper</li><li>HPCC_ADD_SUBDIRECTORY(test t1 t2 t3) - Will add the subdirectory test if t1,t2, or t3 are set to any value other then False/OFF/0</li><li>LOG_PLUGIN - used to log any code based plugin for the platform</li><li>ADD_PLUGIN - adds a plugin with optional build dependencies to the build if dependencies are found</li></ul><h3 id="documentation-macros" tabindex="-1">Documentation Macros <a class="header-anchor" href="#documentation-macros" aria-label="Permalink to &quot;Documentation Macros&quot;">​</a></h3><ul><li>RUN_XSLTPROC - Runs xsltproc using given args</li><li>RUN_FOP - Runs fop using given args</li><li>CLEAN_REL_BOOK - Uses a custom xsl and xsltproc to clean relative book paths from given xml file</li><li>CLEAN_REL_SET - Uses a custom xsl and xsltproc to clean relative set paths from given xml file</li><li>DOCBOOK_TO_PDF - Master macro used to generate pdf, uses above macros</li></ul><h3 id="initfiles-macro" tabindex="-1">Initfiles macro <a class="header-anchor" href="#initfiles-macro" aria-label="Permalink to &quot;Initfiles macro&quot;">​</a></h3><ul><li>GENERATE_BASH - used to run processor program on given files to replace ###&lt;REPLACE&gt;### with given variables FindXXXXX.cmake</li></ul><h2 id="some-standard-techniques-used-in-cmake-project-files" tabindex="-1">Some standard techniques used in Cmake project files <a class="header-anchor" href="#some-standard-techniques-used-in-cmake-project-files" aria-label="Permalink to &quot;Some standard techniques used in Cmake project files&quot;">​</a></h2><h3 id="common-looping" tabindex="-1">Common looping <a class="header-anchor" href="#common-looping" aria-label="Permalink to &quot;Common looping&quot;">​</a></h3><p>Use FOREACH:</p><pre><code>FOREACH( oITEMS
  item1
  item2
  item3
  item4
  item5
)
  Actions on each item here.
ENDFOREACH ( oITEMS )
</code></pre><h3 id="common-installs-over-just-install" tabindex="-1">Common installs over just install <a class="header-anchor" href="#common-installs-over-just-install" aria-label="Permalink to &quot;Common installs over just install&quot;">​</a></h3><ul><li>install ( FILES ... ) - installs item with 664 permissions</li><li>install ( PROGRAMS ... ) - installs runable item with 755 permissions</li><li>install ( TARGETS ... ) - installs built target with 755 permissions</li><li>install ( DIRECTORY ... ) - installs directory with 777 permissions</li></ul><h3 id="common-settings-for-generated-source-files" tabindex="-1">Common settings for generated source files <a class="header-anchor" href="#common-settings-for-generated-source-files" aria-label="Permalink to &quot;Common settings for generated source files&quot;">​</a></h3><ul><li>set_source_files_properties(&lt;file&gt; PROPERTIES GENERATED TRUE) - Must be set on generated source files or dependency generation fails and increases build time.</li></ul><h3 id="using-custom-commands-between-multiple-cmake-files" tabindex="-1">Using custom commands between multiple cmake files <a class="header-anchor" href="#using-custom-commands-between-multiple-cmake-files" aria-label="Permalink to &quot;Using custom commands between multiple cmake files&quot;">​</a></h3><ul><li>GET_TARGET_PROPERTY(&lt;VAR from other cmake file&gt; &lt;var for this file&gt; LOCATION)</li><li>GET_TARGET_PROPERTY(ESDL_EXE esdl LOCATION) - will get from the top level cache the ESDL_EXE value and set it in esdl for your current cmake file</li></ul><p>USE add_custom_command only when 100% needed.</p><p>All directories in a cmake project should have a CMakeLists.txt file and be called from the upper level project with an add_subdirectory or HPCC_ADD_SUBDIRECTORY</p><p>When you have a property that will be shared between cmake projects use define_property to set it in the top level cache.</p><ul><li>define_property(GLOBAL PROPERTY TEST_TARGET BRIEF_DOCS &quot;test doc&quot; FULL_DOCS &quot;Full test doc&quot;)</li><li>mark_as_advanced(TEST_TARGET) - this is required to force the property into the top level cache.CMake Layout:</li></ul><h2 id="findxxxxx-cmake-format" tabindex="-1">FindXXXXX.cmake format <a class="header-anchor" href="#findxxxxx-cmake-format" aria-label="Permalink to &quot;FindXXXXX.cmake format&quot;">​</a></h2><p>All of our Find scripts use the following format:</p><pre><code>NOT XXXXX_FOUND
  Externals set
    define needed vars for finding external based libraries/headers

    Use Native set
      use FIND_PATH to locate headers
      use FIND_LIBRARY to find libs

Include Cmake macros file for package handling
define package handling args for find return  (This will set XXXXX_FOUND)

XXXXX_FOUND
  perform any modifications you feel is needed for the find

Mark defined variables used in package handling args as advanced for return
</code></pre><p>Will define when done:</p><pre><code>XXXXX_FOUND
XXXXX_INCLUDE_DIR
XXXXX_LIBRARIES
</code></pre><p>(more can be defined, but must be at min the previous unless looking for only a binary)</p><p>For an example, see FindAPR.cmake</p>`,32)]))}const h=a(o,[["render",r]]);export{f as __pageData,h as default};
