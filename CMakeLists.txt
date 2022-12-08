
cmake_minimum_required(VERSION 3.20)
set(PROJECT_NAME vector)
set(PROEJCT_VERSION "0.3.2")
project(${PROJECT_NAME} VERSION ${PROEJCT_VERSION} LANGUAGES C)

include(GNUInstallDirs)
set(CMAKE_MACOSX_RPATH 1)


# Search paths for Postgres binaries
if (WIN32)
    find_path(PG_PATH
            bin/postgres
            HINTS
            "C:/PostgreSQL"
            "C:/Program Files/PostgreSQL"
            PATH_SUFFIXES
            bin
            10/bin
            96/bin
            pg96/bin
            DOC
            "The path to a PostgreSQL installation")
endif (WIN32)

if (UNIX)
    find_path(PG_PATH
            bin/postgres
            HINTS
            $ENV{HOME}
            /opt/local/pgsql
            /usr/local/pgsql
            /usr/lib/postgresql
            PATH_SUFFIXES
            bin
            10/bin
            9.6/bin
            96/bin
            pg96/bin
            DOC
            "The path to a PostgreSQL installation")
endif (UNIX)

find_program(PG_CONFIG pg_config
        HINTS
        ${PG_PATH}
        PATH_SUFFIXES
        bin
        DOC
        "The path to the pg_config of the PostgreSQL version to compile against"
        REQUIRED)

if (NOT PG_CONFIG)
    message(FATAL_ERROR "Unable to find 'pg_config'")
endif ()


# Check PostgreSQL version
execute_process(
        COMMAND ${PG_CONFIG} --version
        OUTPUT_VARIABLE PG_VERSION_STRING
        OUTPUT_STRIP_TRAILING_WHITESPACE)

if (NOT ${PG_VERSION_STRING} MATCHES "^PostgreSQL[ ]+([0-9]+)\\.([0-9]+)(\\.([0-9]+))*$")
    message(FATAL_ERROR "Could not parse PostgreSQL version ${PG_VERSION}")
endif ()

set(PG_VERSION_MAJOR ${CMAKE_MATCH_1})
set(PG_VERSION_MINOR ${CMAKE_MATCH_2})
set(PG_VERSION_PATCH ${CMAKE_MATCH_4})

if (NOT ${PG_VERSION_PATCH} OR ${PG_VERSION_PATCH} EQUAL "")
    set(PG_VERSION "${PG_VERSION_MAJOR}.${PG_VERSION_MINOR}")
else ()
    set(PG_VERSION "${PG_VERSION_MAJOR}.${PG_VERSION_MINOR}.${PG_VERSION_PATCH}")
endif ()

message(STATUS "Compiling against PostgreSQL version ${PG_VERSION}")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(PostgreSQL  REQUIRED)
if (MSVC)

    #set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
    add_compile_options(-O3)

    else()
    add_compile_options(-ftree-vectorize -fassociative-math -fno-signed-zeros -fno-trapping-math)
endif (MSVC)

# Get PostgreSQL configuration from pg_config
execute_process(
        COMMAND ${PG_CONFIG} --sharedir
        OUTPUT_VARIABLE PG_SHAREDIR
        OUTPUT_STRIP_TRAILING_WHITESPACE)


add_subdirectory(sql)
add_subdirectory(src)

link_directories(${PostgreSQL_LIBRARY_DIRS})

set(EXT_CONTROL_FILE ${PROJECT_NAME}.control)
configure_file(${EXT_CONTROL_FILE} ${EXT_CONTROL_FILE})
install(
        FILES ${CMAKE_CURRENT_BINARY_DIR}/${EXT_CONTROL_FILE}
        DESTINATION "${PG_SHAREDIR}/extension")