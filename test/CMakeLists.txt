cpmaddpackage("gh:doctest/doctest@2.5.0")

add_executable(asrtc_test asrtc_test.cpp)
target_link_libraries(asrtc_test PRIVATE doctest::doctest asrtc asrtr)
target_compile_features(asrtc_test PUBLIC cxx_std_20)
add_test(NAME asrtc_test COMMAND asrtc_test)
set_tests_properties(asrtc_test PROPERTIES TIMEOUT 30)

add_executable(asrtr_test asrtr_test.cpp)
target_link_libraries(asrtr_test PRIVATE doctest::doctest asrtr)
target_compile_features(asrtr_test PUBLIC cxx_std_20)
add_test(NAME asrtr_test COMMAND asrtr_test)
set_tests_properties(asrtr_test PROPERTIES TIMEOUT 30)

add_executable(asrtl_test asrtl_test.cpp)
target_link_libraries(asrtl_test PRIVATE doctest::doctest asrtl)
target_compile_features(asrtl_test PUBLIC cxx_std_20)
add_test(NAME asrtl_test COMMAND asrtl_test)
set_tests_properties(asrtl_test PROPERTIES TIMEOUT 30)

add_executable(asrtcpp_test asrtcpp_test.cpp)
target_link_libraries(asrtcpp_test PRIVATE doctest::doctest asrtcpp asrtrpp)
target_compile_features(asrtcpp_test PUBLIC cxx_std_20)
add_test(NAME asrtcpp_test COMMAND asrtcpp_test)
set_tests_properties(asrtcpp_test PROPERTIES TIMEOUT 30)

add_executable(asrtrpp_test asrtrpp_test.cpp)
target_link_libraries(asrtrpp_test PRIVATE doctest::doctest asrtrpp asrtcpp)
target_compile_features(asrtrpp_test PUBLIC cxx_std_20)
add_test(NAME asrtrpp_test COMMAND asrtrpp_test)
set_tests_properties(asrtrpp_test PROPERTIES TIMEOUT 30)

add_executable(asrtio_test asrtio_test.cpp)
target_link_libraries(asrtio_test PRIVATE doctest::doctest asrtio_lib)
target_compile_features(asrtio_test PUBLIC cxx_std_20)
add_test(NAME asrtio_test COMMAND asrtio_test)
set_tests_properties(asrtio_test PROPERTIES TIMEOUT 60)

add_test(NAME asrtio_rsim COMMAND asrtio rsim)
set_tests_properties(asrtio_rsim PROPERTIES TIMEOUT 30)

# Interactive progress-bar demo — not a CTest test, just a visual sanity check
# Run manually:  ./_build/debug/test/pbar_demo [--fast] [--total 40]
add_executable(pbar_demo pbar_demo.cpp)
target_compile_features(pbar_demo PUBLIC cxx_std_20)
target_link_libraries(pbar_demo PRIVATE CLI11::CLI11)

if(ASSERT_ENABLE_COVERAGE)
  set(GCOV_EXECUTABLE
      "gcov"
      CACHE STRING "gcov binary used by the coverage test")
  add_test(
    NAME coverage
    COMMAND
      gcovr --gcov-executable ${GCOV_EXECUTABLE} --cobertura-pretty --cobertura
      cobertura.xml -r ${CMAKE_SOURCE_DIR}/ --filter ${CMAKE_SOURCE_DIR}/asrtl/
      --filter ${CMAKE_SOURCE_DIR}/asrtc/ --filter ${CMAKE_SOURCE_DIR}/asrtr/
      --filter ${CMAKE_SOURCE_DIR}/asrtlpp/ --filter
      ${CMAKE_SOURCE_DIR}/asrtcpp/ --filter ${CMAKE_SOURCE_DIR}/asrtrpp/
      --filter ${CMAKE_SOURCE_DIR}/asrtio/
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
endif()
