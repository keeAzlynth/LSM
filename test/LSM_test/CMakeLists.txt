cmake_minimum_required(VERSION 4.0)
project(LSM_Test)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(EXISTS ${CMAKE_SOURCE_DIR}/../LSMTestOptions.cmake)
	include(${CMAKE_SOURCE_DIR}/../LSMTestOptions.cmake)
endif()

option(ENABLE_TSAN "启用 ThreadSanitizer" OFF)
option(ENABLE_VALGRIND "启用 Valgrind 支持" OFF)
option(ENABLE_ASAN "启用 AddressSanitizer" OFF)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
	add_compile_options(-g -O0)
	add_link_options(-g)
endif()

if(ENABLE_TSAN)
 set(TSAN_OPTIONS "detect_deadlocks=1 halt_on_error=1 abort_on_error=1")
    add_custom_target(tsan-check
        COMMAND ${CMAKE_COMMAND} -E env TSAN_OPTIONS=${TSAN_OPTIONS} ./lsm_test
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "运行 ThreadSanitizer 检测"
    )
	add_compile_options(-fsanitize=thread -g -O1)
	add_link_options(-fsanitize=thread)
endif()

if(ENABLE_ASAN)
	add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
	add_link_options(-fsanitize=address)
endif()

if(ENABLE_VALGRIND)
	add_compile_options(-g -O0)
endif()

if(ENABLE_TSAN AND ENABLE_VALGRIND)
	message(FATAL_ERROR "ThreadSanitizer 和 Valgrind 不能同时启用")
endif()

if(ENABLE_ASAN AND ENABLE_VALGRIND)
	message(FATAL_ERROR "AddressSanitizer 和 Valgrind 不能同时启用")
endif()

if(ENABLE_TSAN AND ENABLE_ASAN)
	message(FATAL_ERROR "ThreadSanitizer 和 AddressSanitizer 不能同时启用")
endif()

if(NOT DEFINED ENABLE_LSM_TEST)
	set(ENABLE_LSM_TEST ON)
endif()

if(NOT ENABLE_LSM_TEST)
	message(STATUS "LSM_test disabled (ENABLE_LSM_TEST=OFF)")
	return()
endif()

find_package(GTest REQUIRED)

set(SOURCE_FILES
	../../src/LSM.cpp
	../../src/core/memtable.cpp
	../../src/core/Skiplist.cpp
	../../src/storage/Sstable.cpp
	../../src/iterator/SstableIterator.cpp
	../../src/storage/Block.cpp
	../../src/iterator/BlockIterator.cpp
	../../src/storage/Blockcache.cpp
	../../src/storage/BlockMeta.cpp
	../../src/storage/BloomFilter.cpp
	../../src/iterator/Baselterator.cpp
	../../src/storage/std_file.cpp
	../../src/storage/mmap.cpp
	../../src/storage/file.cpp
	../../src/core/Global.cpp
	../../src/utils/Loger.cpp
	../../src/core/record.cpp
	../../src/storage/wal.cpp
	../../src/compaction/Manifest.cpp
)

add_executable(lsm_test
	LSMtest.cpp
	${SOURCE_FILES}
)

target_include_directories(lsm_test PRIVATE ../../include)
target_link_libraries(lsm_test PRIVATE GTest::gtest GTest::gtest_main pthread)

enable_testing()
add_test(NAME LSMTest COMMAND lsm_test)

# Valgrind target (optional)
if(ENABLE_VALGRIND)
	find_program(VALGRIND_EXECUTABLE valgrind)
	if(VALGRIND_EXECUTABLE)
		add_custom_target(valgrind-check
			COMMAND ${VALGRIND_EXECUTABLE}
				--leak-check=full
				--show-leak-kinds=all
				--error-exitcode=1
				./lsm_test
			WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
			COMMENT "运行 Valgrind 内存检查"
		)
	endif()
endif()
