cmake_minimum_required(VERSION 3.10)

project(asrtio VERSION 0.0)

cpmaddpackage("gh:CLIUtils/CLI11@2.5.0")
cpmaddpackage("gh:libuv/libuv@1.51.0")
cpmaddpackage("gh:nlohmann/json@3.11.3")

add_library(asrtio_lib util.cpp param_config.cpp transport.cpp
                       cntr_stream_sys.cpp log_sink.cpp)
target_compile_features(asrtio_lib PUBLIC cxx_std_20)
target_link_libraries(asrtio_lib PUBLIC asrtcpp asrtrpp uv ecor
                                        nlohmann_json::nlohmann_json)

add_executable(asrtio main.cpp run_session.cpp)
target_link_libraries(asrtio PUBLIC asrtio_lib CLI11::CLI11)
