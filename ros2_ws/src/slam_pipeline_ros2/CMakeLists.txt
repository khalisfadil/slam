cmake_minimum_required(VERSION 3.16)
project(slam_pipeline_ros2 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -fPIC -Wno-unused-but-set-variable")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -ffast-math -march=native")
# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG")

# Silence deprecated policy warning from Open3D
if(POLICY CMP0072)
    cmake_policy(SET CMP0072 NEW)
endif()

# Find ROS2 dependencies
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(nav_msgs REQUIRED)
find_package(tf2_ros REQUIRED)

# Find other dependencies
set(Open3D_DIR "/usr/local/lib/cmake/Open3D/" CACHE PATH "Path to Open3D CMake config")
find_package(Eigen3 REQUIRED NO_MODULE)
find_package(nlohmann_json REQUIRED)
find_package(Boost REQUIRED COMPONENTS system thread)
find_package(Open3D REQUIRED)
find_package(OpenMP REQUIRED)
find_package(lgmath REQUIRED)
find_package(steam REQUIRED)

# Set OpenMP flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")

# Find cpuid library
find_library(CPUID_LIBRARY NAMES cpuid libcpuid)
if(CPUID_LIBRARY)
    message(STATUS "Found cpuid library: ${CPUID_LIBRARY}")
else()
    message(STATUS "cpuid library not found; runtime CPU detection disabled")
    add_compile_definitions(NO_CPUID)
endif()

# Check for AVX2 and FMA support
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mavx2" COMPILER_SUPPORTS_AVX2)
check_cxx_compiler_flag("-mfma" COMPILER_SUPPORTS_FMA)
if(COMPILER_SUPPORTS_AVX2 AND COMPILER_SUPPORTS_FMA)
    message(STATUS "AVX2 and FMA are supported by the compiler")
    add_compile_options(-mavx2 -mfma)
else()
    message(WARNING "AVX2 or FMA not supported; using scalar implementation")
endif()

# Set default build type to Debug if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Choose the type of build." FORCE)
endif()

# Define include directories
set(SLAM_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/include")
set(STATEESTIMATION_INCLUDE_DIRS
    "${CMAKE_SOURCE_DIR}/extern/state_estimation/include"
    "${CMAKE_SOURCE_DIR}/extern/state_estimation/include/odometry"
    "${CMAKE_SOURCE_DIR}/extern/state_estimation/include/utils"
)
set(GNSSKOMPASS_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/extern/GNSSKompass/include")
set(LIDARDECODE_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/extern/LidarDecode/include")
set(ROBIN_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/3rdparty/robin-map/include/tsl")

# Combine include directories
set(PROJECT_INCLUDE_DIRS
    ${SLAM_INCLUDE_DIRS}
    ${STATEESTIMATION_INCLUDE_DIRS}
    ${GNSSKOMPASS_INCLUDE_DIRS}
    ${LIDARDECODE_INCLUDE_DIRS}
    ${ROBIN_INCLUDE_DIRS}
    ${lgmath_INCLUDE_DIRS}
    ${steam_INCLUDE_DIRS}
)

# Source file groups
set(SOURCE_GROUPS
    LIDARDECODE
    GNSSKOMPASS
    STATEESTIMATION
    SLAM
)

# Define source files for each group
set(LIDARDECODE_SOURCES
    extern/LidarDecode/src/OusterLidarCallback.cpp
)
set(GNSSKOMPASS_SOURCES
    extern/GNSSKompass/src/callback_gnssComp.cpp
    extern/GNSSKompass/src/callback_navMsg.cpp
    extern/GNSSKompass/src/navMath.cpp
)
set(STATEESTIMATION_SOURCES
    extern/state_estimation/src/odometry/steam_lo.cpp
)
set(SLAM_SOURCES
    src/rclcpppipeline.cpp
)

# Combine all sources
set(ALL_SOURCES)
foreach(GROUP ${SOURCE_GROUPS})
    list(APPEND ALL_SOURCES ${${GROUP}_SOURCES})
endforeach()

# Create core library
add_library(slam_core STATIC ${ALL_SOURCES})
target_compile_options(slam_core PRIVATE -fPIC)
target_include_directories(slam_core PUBLIC ${PROJECT_INCLUDE_DIRS})
ament_target_dependencies(slam_core PUBLIC rclcpp sensor_msgs nav_msgs tf2_ros)
target_link_libraries(slam_core
    PUBLIC
        Eigen3::Eigen
        nlohmann_json::nlohmann_json
        OpenMP::OpenMP_CXX
        Boost::system
        Boost::thread
        Open3D::Open3D
        Threads::Threads
        lgmath
        steam
    PRIVATE
        $<$<BOOL:${CPUID_LIBRARY}>:${CPUID_LIBRARY}>
)

# Create ROS2 executable
add_executable(ros2_slam_node src/rclcpp.cpp)
target_include_directories(ros2_slam_node PUBLIC ${PROJECT_INCLUDE_DIRS})
ament_target_dependencies(ros2_slam_node PUBLIC rclcpp sensor_msgs nav_msgs tf2_ros)
target_link_libraries(ros2_slam_node
    PUBLIC
        slam_core
        Eigen3::Eigen
        nlohmann_json::nlohmann_json
        OpenMP::OpenMP_CXX
        Boost::system
        Boost::thread
        Open3D::Open3D
        Threads::Threads
        lgmath
        steam
    PRIVATE
        $<$<BOOL:${CPUID_LIBRARY}>:${CPUID_LIBRARY}>
)

# Install targets
install(TARGETS ros2_slam_node
    DESTINATION lib/${PROJECT_NAME}
)

# Generate compile commands for IDEs
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ROS2 package setup
ament_package()