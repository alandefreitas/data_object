#######################################################
### CMAKE SETTINGS                                  ###
#######################################################
cmake_minimum_required(VERSION 3.9)
project(dataobject)
set(CMAKE_CXX_STANDARD 14)

#######################################################
### EXTERNAL COMPONENTS                             ###
#######################################################
include(findpackages.cmake)

#######################################################
### INCLUDES                                        ###
#######################################################
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

#######################################################
### EXECUTABLES                                     ###
#######################################################
add_executable(data_object_example example.cpp)
target_link_libraries(data_object_example ${Boost_LIBRARIES} ${POSTGRES_LIBRARIES} ${SQLITE3_LIBRARIES})
