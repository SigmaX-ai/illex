cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project(illex VERSION 0.0.1 LANGUAGES CXX)

find_package(Arrow 1.0 CONFIG REQUIRED)
find_library(zmqpp REQUIRED)
find_library(zmq REQUIRED)

include(FetchContent)

FetchContent_Declare(cmake-modules
  GIT_REPOSITORY  https://github.com/abs-tudelft/cmake-modules.git
  GIT_TAG         master
)
FetchContent_MakeAvailable(cmake-modules)
include(CompileUnits)

# rapidjson
FetchContent_Declare(rapidjson
  GIT_REPOSITORY  https://github.com/Tencent/rapidjson.git
  GIT_TAG         v1.1.0
)
FetchContent_GetProperties(rapidjson)
if (NOT rapidjson_POPULATED)
  FetchContent_Populate(rapidjson)
  include_directories("${rapidjson_SOURCE_DIR}/include")
endif ()

# CLI11
FetchContent_Declare(CLI11
  GIT_REPOSITORY  https://github.com/CLIUtils/CLI11.git
  GIT_TAG         v1.9.1
)
FetchContent_MakeAvailable(CLI11)

# spdlog
FetchContent_Declare(spdlog
  GIT_REPOSITORY  https://github.com/gabime/spdlog
  GIT_TAG         v1.7.0
)
FetchContent_MakeAvailable(spdlog)

# concurrentqueue
FetchContent_Declare(concurrentqueue
  GIT_REPOSITORY  https://github.com/cameron314/concurrentqueue.git
  GIT_TAG         v1.0.1
)
FetchContent_GetProperties(concurrentqueue)
if (NOT concurrentqueue_POPULATED)
  FetchContent_Populate(concurrentqueue)
endif ()
include_directories("${concurrentqueue_SOURCE_DIR}")

# kissnet
FetchContent_Declare(kissnet
  GIT_REPOSITORY  https://github.com/johanpel/kissnet
  GIT_TAG         master
)
FetchContent_MakeAvailable(kissnet)

# putong
FetchContent_Declare(putong
  GIT_REPOSITORY  https://github.com/abs-tudelft/putong.git
  GIT_TAG         master
)
FetchContent_MakeAvailable(putong)

add_compile_unit(
  NAME illex::static
  TYPE OBJECT
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/illex/zmq_client.cpp
    src/illex/raw_client.cpp
    src/illex/document.cpp
    src/illex/arrow.cpp
    src/illex/value.cpp
  DEPS
    kissnet
    putong
    spdlog::spdlog
    zmqpp
    zmq
)

add_compile_unit(
  NAME illex::obj
  TYPE OBJECT
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/illex/cli.cpp
    src/illex/file.cpp
    src/illex/producer.cpp
    src/illex/raw_server.cpp
    src/illex/stream.cpp
    src/illex/zmq_server.cpp
  TSTS
    test/illex/test_arrow.cpp
    test/illex/test_gen.cpp
  DEPS
    kissnet
    illex::static
    arrow_shared
    CLI11::CLI11
    spdlog::spdlog
    zmqpp
    zmq
    putong
)

add_compile_unit(
  NAME illex
  TYPE EXECUTABLE
  PRPS
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
  SRCS
    src/illex/main.cpp
  DEPS
    illex::obj
)

compile_units()
