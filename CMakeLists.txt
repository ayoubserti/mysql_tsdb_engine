SET(TSDB_ENGINE_PLUGIN_DYNAMIC "ha_tsdb_engine")

SET(TSDB_ENGINE_SOURCES ha_tsdb_engine.cc private_func.cc)

#ADD_LIBRARY(${TSDB_ENGINE_PLUGIN_DYNAMIC} SHARED ${TSDB_ENGINE_SOURCES})
IF(WITH_TSDB_ENGINE_STORAGE_ENGINE AND NOT WITHOUT_TSDB_ENGINE_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(TSDB_ENGINE ${TSDB_ENGINE_SOURCES} STORAGE_ENGINE DEFAULT LINK_LIBRARIES tsdb)
ELSEIF(NOT WITHOUT_TSDB_ENGINE_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(TSDB_ENGINE ${TSDB_ENGINE_SOURCES} STORAGE_ENGINE MODULE_ONLY LINK_LIBRARIES  tsdb)
ENDIF()
#link_directories(/usr/local/lib)
#TARGET_LINK_LIBRARIES(${TSDB_ENGINE_PLUGIN_DYNAMIC} tsdb hdf5)
