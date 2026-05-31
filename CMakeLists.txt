cmake_minimum_required(VERSION 3.30)

project(assert)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(cmake/CPM.cmake)

option(ASRT_BUILD_TESTS "Build ASRT tests" ${PROJECT_IS_TOP_LEVEL})

set(_asrt_cxx_libraries asrtlpp asrtcpp asrtrpp asrtio)
set(_asrt_all_libraries asrtl asrtc asrtr ${_asrt_cxx_libraries})

set(ASRT_LIBRARIES
    "${_asrt_all_libraries}"
    CACHE
      STRING
      "Semicolon-separated list of asrt sub-libraries to build (default: all).")

# Only pull in ecor when at least one C++ wrapper library is requested.
foreach(_lib IN LISTS _asrt_cxx_libraries)
  if(_lib IN_LIST ASRT_LIBRARIES)
    cpmaddpackage("gh:koniarik/ecor@0.3.0")
    break()
  endif()
endforeach()

foreach(_lib IN LISTS _asrt_all_libraries)
  if(_lib IN_LIST ASRT_LIBRARIES)
    add_subdirectory(${_lib})
  endif()
endforeach()

if(ASRT_BUILD_TESTS)
  include(CTest)

  if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(test)
  endif()
endif()
# --- install / package config ---

# C libraries (asrtl, asrtc, asrtr) — no external dependencies

configure_package_config_file(
  cmake/asrtConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/asrtConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/asrt)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/asrtConfig.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/asrt)

install(
  EXPORT asrtCTargets
  NAMESPACE asrt::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/asrt)

# C++ wrappers (asrtlpp, asrtcpp, asrtrpp) — require ecor

configure_package_config_file(
  cmake/asrtCxxConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/asrtCxxConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/asrtCxx)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/asrtCxxConfig.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/asrtCxx)

foreach(_lib IN LISTS _asrt_cxx_libraries)
  if(_lib IN_LIST ASRT_LIBRARIES)
    install(
      EXPORT asrtCxxTargets
      NAMESPACE asrt::
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/asrtCxx)
    break()
  endif()
endforeach()
