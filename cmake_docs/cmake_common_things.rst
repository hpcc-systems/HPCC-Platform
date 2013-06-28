Common looping:

FOREACH( oITEMS
    item1
    item2
    item3
    item4
    item5
)
    Actions on each item here.
ENDFOREACH ( oITEMS )

Common installs over just install:

    install ( FILES ... ) - installs item with 664 permissions
    install ( PROGRAMS ... ) - installs runable item with 755 permissions
    install ( TARGETS ... ) - installs built target with 755 permissions
    install ( DIRECTORY ... ) - installs directory with 777 permissions

Common settings for generated source files:
    set_source_files_properties(<file> PROPERTIES GENERATED TRUE) - Must be set on generated source files or dependency generation fails and increases build time.

When using custom commands between multiple cmake files use:
    GET_TARGET_PROPERTY(<VAR from other cmake file> <var for this file> LOCATION)

    GET_TARGET_PROPERTY(ESDL_EXE esdl LOCATION) - will get from the top level cache the ESDL_EXE value and set it in esdl for your current cmake file


USE add_custom_command only when 100% needed.

All directories in a cmake project should have a CMakeLists.txt file and be called to the upper level project with an add_subdirectory or HPCC_ADD_SUBDIRECTORY

When you have a property that will be shared between cmake projects use define_property to set it in the top level cache.
    define_property(GLOBAL PROPERTY TEST_TARGET BRIEF_DOCS "test doc" FULL_DOCS "Full test doc")
    mark_as_advanced(TEST_TARGET)  - this is required to force the property into the top level cache.