cmake_minimum_required(VERSION 3.16)
project(nano_mooncake LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
enable_testing()

option(NANO_USE_CUDA "Enable CUDA buffer path" ON)
option(NANO_ENABLE_RDMA "Enable RDMA transport when libibverbs is available" ON)

set(NANO_MOONCAKE_CORE_SOURCES
    csrc/nano_mooncake/engine.cpp
    csrc/nano_mooncake/master_client.cpp
    csrc/nano_mooncake/master_protocol.cpp
    csrc/nano_mooncake/master_server.cpp
    csrc/nano_mooncake/mooncake_master.cpp
    csrc/nano_mooncake/observability.cpp
    csrc/nano_mooncake/transport.cpp
    csrc/nano_mooncake/tcp_transport.cpp
)

set(NANO_RDMA_INCLUDE_DIR "")
set(NANO_RDMA_LIBRARY "")
set(NANO_CUDA_INCLUDE_DIR "")
set(NANO_CUDA_DRIVER_LIBRARY "")

if(NANO_USE_CUDA)
    find_path(NANO_CUDA_INCLUDE_DIR cuda.h
        HINTS /usr/local/cuda/include /usr/include
    )
    find_library(NANO_CUDA_DRIVER_LIBRARY cuda
        HINTS /usr/local/cuda/lib64 /usr/lib64 /usr/lib/x86_64-linux-gnu
    )
    if(NANO_CUDA_INCLUDE_DIR AND NANO_CUDA_DRIVER_LIBRARY)
        message(STATUS "nano-MoonCake CUDA driver support enabled")
    else()
        message(STATUS "nano-MoonCake CUDA driver support disabled: cuda.h or libcuda not found")
    endif()
endif()

if(NANO_ENABLE_RDMA)
    find_path(NANO_RDMA_INCLUDE_DIR infiniband/verbs.h)
    find_library(NANO_RDMA_LIBRARY ibverbs)
    if(NANO_RDMA_INCLUDE_DIR AND NANO_RDMA_LIBRARY)
        list(APPEND NANO_MOONCAKE_CORE_SOURCES
            csrc/nano_mooncake/rdma_transport.cpp
        )
        message(STATUS "nano-MoonCake RDMA backend enabled")
    else()
        message(STATUS "nano-MoonCake RDMA backend disabled: libibverbs not found")
    endif()
endif()

add_library(nano_mooncake_core
    ${NANO_MOONCAKE_CORE_SOURCES}
)

target_include_directories(nano_mooncake_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(NANO_USE_CUDA)
    target_compile_definitions(nano_mooncake_core PUBLIC NANO_USE_CUDA=1)
endif()

if(NANO_CUDA_INCLUDE_DIR AND NANO_CUDA_DRIVER_LIBRARY)
    target_compile_definitions(nano_mooncake_core PUBLIC NANO_HAS_CUDA=1)
    target_include_directories(nano_mooncake_core PRIVATE ${NANO_CUDA_INCLUDE_DIR})
    target_link_libraries(nano_mooncake_core PRIVATE ${NANO_CUDA_DRIVER_LIBRARY})
endif()

if(NANO_RDMA_INCLUDE_DIR AND NANO_RDMA_LIBRARY)
    target_compile_definitions(nano_mooncake_core PUBLIC NANO_HAS_RDMA=1)
    target_include_directories(nano_mooncake_core PRIVATE ${NANO_RDMA_INCLUDE_DIR})
    target_link_libraries(nano_mooncake_core PRIVATE ${NANO_RDMA_LIBRARY})
endif()

# Python binding is optional at configure time.
option(NANO_BUILD_PYTHON "Build pybind module" OFF)
if(NANO_BUILD_PYTHON)
    find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)

    # If pybind11 is installed by pip, this makes CMake find pybind11Config.cmake
    # without requiring callers to manually pass -Dpybind11_DIR.
    execute_process(
        COMMAND "${Python_EXECUTABLE}" -m pybind11 --cmakedir
        OUTPUT_VARIABLE _pybind11_dir
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(_pybind11_dir)
        list(APPEND CMAKE_PREFIX_PATH "${_pybind11_dir}")
    endif()
    find_package(pybind11 REQUIRED)
    pybind11_add_module(_nano_mooncake csrc/python/bindings.cpp)
    target_link_libraries(_nano_mooncake PRIVATE nano_mooncake_core)
    target_include_directories(_nano_mooncake PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    install(TARGETS _nano_mooncake LIBRARY DESTINATION nano_mooncake)
endif()

add_executable(mooncake_master csrc/nano_mooncake/master_main.cpp)
target_link_libraries(mooncake_master PRIVATE nano_mooncake_core)
target_include_directories(mooncake_master PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(nano_mooncake_master_test tests/mooncake_master_test.cpp)
target_link_libraries(nano_mooncake_master_test PRIVATE nano_mooncake_core)
target_include_directories(nano_mooncake_master_test
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
add_test(NAME nano_mooncake_master_test COMMAND nano_mooncake_master_test)
