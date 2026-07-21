cmake_minimum_required(VERSION 3.20)
project(DeviceSimulator VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ── 依赖 ──────────────────────────────────────────────
# ASIO (standalone, header-only)
find_path(ASIO_INCLUDE_DIR "asio.hpp"
    HINTS /usr/local/include /usr/include $ENV{ASIO_ROOT}/include
          ${CMAKE_SOURCE_DIR}/third_party/asio/asio/include
)
if(NOT ASIO_INCLUDE_DIR)
    message(FATAL_ERROR "ASIO not found. Install asio or set ASIO_ROOT.")
endif()

# nlohmann/json (header-only)
find_path(NLOHMANN_JSON_INCLUDE_DIR "nlohmann/json.hpp"
    HINTS /usr/local/include /usr/include $ENV{NLOHMANN_JSON_ROOT}/include
          ${CMAKE_SOURCE_DIR}/third_party/nlohmann/json/include
)
if(NOT NLOHMANN_JSON_INCLUDE_DIR)
    message(FATAL_ERROR "nlohmann/json not found. Install or set NLOHMANN_JSON_ROOT.")
endif()

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${ASIO_INCLUDE_DIR}
    ${NLOHMANN_JSON_INCLUDE_DIR}
)

# ── 源文件 ────────────────────────────────────────────
set(SOURCES
    src/main.cpp
    src/core/Config.cpp
    src/core/DeviceModel.cpp
    src/core/DeviceConnection.cpp
    src/core/Simulator.cpp
    src/protocol/FrameProtocol.cpp
    src/devices/MonitorModel.cpp
    src/devices/TempSensorModel.cpp
    src/anomaly/AnomalyInjector.cpp
)

set(HEADERS
    src/core/Config.h
    src/core/DeviceModel.h
    src/core/DeviceConnection.h
    src/core/Simulator.h
    src/protocol/FrameProtocol.h
    src/devices/MonitorModel.h
    src/devices/TempSensorModel.h
    src/anomaly/AnomalyInjector.h
)

# ── 目标 ──────────────────────────────────────────────
add_executable(simulator ${SOURCES} ${HEADERS})

target_link_libraries(simulator PRIVATE
    ${CMAKE_DL_LIBS}
)

# ── 测试（可选） ──────────────────────────────────────
option(BUILD_TESTS "Build unit tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    find_package(GTest REQUIRED)
    add_executable(simulator_test
        tests/test_frame_protocol.cpp
        tests/test_device_model.cpp
        src/protocol/FrameProtocol.cpp
        src/devices/MonitorModel.cpp
        src/devices/TempSensorModel.cpp
        src/core/DeviceModel.cpp
        src/anomaly/AnomalyInjector.cpp
    )
    target_link_libraries(simulator_test PRIVATE GTest::GTest GTest::Main)
    include_directories(simulator_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
    gtest_discover_tests(simulator_test)
endif()

# ── 安装 ──────────────────────────────────────────────
install(TARGETS simulator RUNTIME DESTINATION bin)
