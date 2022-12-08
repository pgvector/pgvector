

add_library(vector
MODULE
ivfbuild.c 
ivfflat.c 
ivfinsert.c 
ivfkmeans.c 
ivfscan.c 
ivfutils.c 
ivfvacuum.c 
vector.c
)
target_include_directories(vector PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
                         $<INSTALL_INTERFACE:include>)
target_include_directories(vector PRIVATE ${PostgreSQL_INCLUDE_DIRS})
if (MSVC)
    target_include_directories(vector PRIVATE ${CMAKE_PREFIX_PATH}/include/server/port/win32)
    target_include_directories(vector PRIVATE ${CMAKE_PREFIX_PATH}/include/server/port/win32_msvc)
    target_link_directories(vector PRIVATE ${PostgreSQL_LIBRARY_DIRS})
    target_link_libraries(vector postgres)
endif(MSVC)

set_target_properties(vector PROPERTIES PREFIX "")

install(TARGETS vector
LIBRARY DESTINATION lib
ARCHIVE DESTINATION lib
RUNTIME DESTINATION bin
)

