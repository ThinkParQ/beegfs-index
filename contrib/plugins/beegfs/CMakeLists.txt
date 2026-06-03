set(BEEGFS_PLUGIN_COMMON
  beegfs_shared.c
)

set(BEEGFS_PLUGINS
  beegfs_indexing
  beegfs_querying
)

foreach(BEEGFS_PLUGIN ${BEEGFS_PLUGINS})
  add_library(${BEEGFS_PLUGIN} SHARED ${BEEGFS_PLUGIN}.c ${BEEGFS_PLUGIN_COMMON})
  add_dependencies(${BEEGFS_PLUGIN} install_dependencies)
  install(TARGETS ${BEEGFS_PLUGIN} DESTINATION "${SERVER_LIB}" COMPONENT Server)
endforeach()

if (APPLE OR DARWIN)
  foreach(BEEGFS_PLUGIN ${BEEGFS_PLUGINS})
    # Resolve symbols at runtime when dlopen() loads the plugin.
    target_link_libraries(${BEEGFS_PLUGIN} -Wl,-undefined -Wl,dynamic_lookup)
  endforeach()
elseif(CYGWIN)
  # Cygwin requires importing symbols from the executable's import libraries.
  target_link_libraries(beegfs_indexing "${CMAKE_BINARY_DIR}/src/libgufi_dir2index.dll.a")
  target_link_libraries(beegfs_querying "${CMAKE_BINARY_DIR}/src/libgufi_query.dll.a")
else()
  foreach(BEEGFS_PLUGIN ${BEEGFS_PLUGINS})
    target_link_libraries(${BEEGFS_PLUGIN} "${DEP_INSTALL_PREFIX}/sqlite3/lib/libsqlite3.${EXT}")
    set_target_properties(${BEEGFS_PLUGIN} PROPERTIES
      BUILD_WITH_INSTALL_RPATH FALSE
      INSTALL_RPATH "$ORIGIN"
      BUILD_RPATH "${DEP_INSTALL_PREFIX}/sqlite3/lib")
  endforeach()
endif()
