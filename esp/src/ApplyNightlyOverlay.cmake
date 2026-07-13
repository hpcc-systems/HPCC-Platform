# Apply nightly eclwatch overlay after the main build output is generated
# Invoked by the `eclwatch` custom target command after its DEPENDS are satisfied
if(NOT DEFINED ECLWATCH_BINARY_DIR)
    message(FATAL_ERROR "ECLWATCH_BINARY_DIR must be provided when running ApplyNightlyOverlay.cmake")
endif()

set(ECLWATCH_BUILD_DIR "${ECLWATCH_BINARY_DIR}/build")
set(ECLWATCH_NIGHTLY_OVERLAY_DIR "${ECLWATCH_BINARY_DIR}/nightly-overlay")

if(IS_DIRECTORY "${ECLWATCH_NIGHTLY_OVERLAY_DIR}")
    file(MAKE_DIRECTORY "${ECLWATCH_BUILD_DIR}")
    message(STATUS "Applying ECL Watch nightly overlay from ${ECLWATCH_NIGHTLY_OVERLAY_DIR} to ${ECLWATCH_BUILD_DIR}")
    file(COPY "${ECLWATCH_NIGHTLY_OVERLAY_DIR}/" DESTINATION "${ECLWATCH_BUILD_DIR}")
endif()
