cmake_minimum_required(VERSION 2.8)
project(kafka_fill)

include_directories(/usr/local/include src)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")

set(SOURCE_FILES src/main.cpp)

add_executable(kafka_fill ${SOURCE_FILES})

target_link_libraries(kafka_fill
   :libjemalloc.a
   :libfolly.a
   pthread
   :librdkafka++.a
   :librdkafka.a
   rt
   z
)

