# Set local modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
include(FindPackageHandleStandardArgs)

#######################################################
###              BOOST LIBRARIES                    ###
#######################################################
find_package(Boost QUIET)
include_directories(${Boost_INCLUDE_DIR})
set(ALL_LIBRARIES ${ALL_LIBRARIES} ${Boost_LIBRARIES})

if (NOT Boost_FOUND)
    MESSAGE(SEND_ERROR  "You need to install Boost to run this project")
    if (${APPLE})
        MESSAGE(FATAL_ERROR "Install http://brewformulas.org/ and then run `brew install boost`")
    endif()
    if (${UNIX})
        MESSAGE(FATAL_ERROR "Run `sudo apt-get install libboost-all-dev`")
    endif()
    if (${WIN32})
        MESSAGE(FATAL_ERROR "Go to http://www.boost.org/")
    endif()
endif ()

#######################################################
###             POSTGRESQL LIBRARIES                ###
#######################################################
find_package(POSTGRES)
include_directories(${POSTGRES_INCLUDE_DIRS})
set(ALL_LIBRARIES ${ALL_LIBRARIES} ${POSTGRES_LIBRARIES})

#######################################################
###                 SQLITE LIBRARIES                ###
#######################################################
find_package(SQLITE3)
include_directories(${SQLITE3_INCLUDE_DIRS})
set(ALL_LIBRARIES ${ALL_LIBRARIES} ${SQLITE3_LIBRARIES})

if ((NOT POSTGRES_FOUND) AND (NOT SQLITE3_FOUND))
    MESSAGE(FATAL_ERROR "You need to install at least one database driver")
endif ()

