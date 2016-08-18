package org.rabix.executor.pathmapper.ftp;

import java.util.Map;

import org.rabix.bindings.filemapper.FileMapper;
import org.rabix.bindings.filemapper.FileMappingException;
import org.rabix.executor.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class LocalToFTPPathMapper implements FileMapper {

  private final static Logger logger = LoggerFactory.getLogger(LocalToFTPPathMapper.class);
  
  private StorageConfiguration storageConfig;
  
  @Inject
  public LocalToFTPPathMapper(StorageConfiguration storageConfig) {
    this.storageConfig = storageConfig;
  }
  
  @Override
  public String map(String path, Map<String, Object> config) throws FileMappingException {
    logger.info("Map absolute physical path {} to relative physical path.", path);
    return path.substring(storageConfig.getLocalExecutionDirectory().getAbsolutePath().length() + 1);
  }

}
