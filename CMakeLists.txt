cmake_minimum_required(VERSION 3.16)
project(nano_mooncake LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(NANO_USE_CUDA "Enable CUDA buffer path" ON)

add_library(nano_mooncake_core
    src/engine.cpp
)

target_include_directories(nano_mooncake_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(NANO_USE_CUDA)
    target_compile_definitions(nano_mooncake_core PUBLIC NANO_USE_CUDA=1)
endif()

# Python binding is optional at configure time.
option(NANO_BUILD_PYTHON "Build pybind module" OFF)
if(NANO_BUILD_PYTHON)
    find_package(pybind11 REQUIRED)
    pybind11_add_module(_nano_mooncake src/python/bindings.cpp)
    target_link_libraries(_nano_mooncake PRIVATE nano_mooncake_core)
    target_include_directories(_nano_mooncake PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
endif()
