cmake_minimum_required(VERSION 3.10)

project(asrtio VERSION 0.0)

cpmaddpackage("gh:CLIUtils/CLI11@2.5.0")
cpmaddpackage("gh:libuv/libuv@1.51.0")
cpmaddpackage("gh:nlohmann/json@3.11.3")

add_library(asrtio_lib util.cpp param_config.cpp transport.cpp
                       cntr_stream_sys.cpp log_sink.cpp run_session.cpp)
target_compile_features(asrtio_lib PUBLIC cxx_std_20)
target_include_directories(asrtio_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(asrtio_lib PUBLIC asrtcpp asrtrpp uv ecor
                                        nlohmann_json::nlohmann_json)
find_package(Threads REQUIRED)
target_link_libraries(asrtio_lib PUBLIC Threads::Threads)
if(NOT WIN32)
  target_sources(asrtio_lib PRIVATE serial_posix.cpp)
endif()
if(WIN32)
  # <windows.h> must not define min/max macros, nor pull in the old winsock.h
  # that clashes with the winsock2.h libuv includes.
  target_compile_definitions(asrtio_lib PUBLIC NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

add_executable(asrtio main.cpp)
target_link_libraries(asrtio PUBLIC asrtio_lib CLI11::CLI11)
