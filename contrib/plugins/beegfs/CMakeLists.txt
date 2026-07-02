# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 ThinkParQ GmbH

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

foreach(BEEGFS_PLUGIN ${BEEGFS_PLUGINS})
  target_link_libraries(${BEEGFS_PLUGIN} "${DEP_INSTALL_PREFIX}/sqlite3/lib/libsqlite3.${EXT}")
  set_target_properties(${BEEGFS_PLUGIN} PROPERTIES
    BUILD_WITH_INSTALL_RPATH FALSE
    INSTALL_RPATH "$ORIGIN"
    BUILD_RPATH "${DEP_INSTALL_PREFIX}/sqlite3/lib")
endforeach()
