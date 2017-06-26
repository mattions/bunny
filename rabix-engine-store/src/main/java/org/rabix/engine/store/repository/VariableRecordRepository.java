package org.rabix.engine.store.repository;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.engine.store.cache.CachableRepository;
import org.rabix.engine.store.model.JobRecord;
import org.rabix.engine.store.model.VariableRecord;
import org.rabix.engine.store.cache.Cachable;

public abstract class VariableRecordRepository implements CachableRepository {

  @Override
  public int insertCachable(Cachable record) {
    return insert((VariableRecord) record);
  }
  
  @Override
  public int updateCachable(Cachable record) {
    return update((VariableRecord) record);
  }
  
  @Override
  public void insertCachables(List<Cachable> cachables) {
    List<VariableRecord> records = new ArrayList<>();
    for (Cachable cachable : cachables) {
      records.add((VariableRecord) cachable);
    }
    insertBatch(records.iterator());
  }
  
  @Override
  public void updateCachables(List<Cachable> cachables) {
    List<VariableRecord> records = new ArrayList<>();
    for (Cachable cachable : cachables) {
      records.add((VariableRecord) cachable);
    }
    updateBatch(records.iterator());
  }

  public abstract void insertBatch(Iterator<VariableRecord> records);
  
  public abstract void updateBatch(Iterator<VariableRecord> records);
  
  public abstract int insert(VariableRecord jobRecord);
  
  public abstract int update(VariableRecord jobRecord);
  
  public abstract void delete(Set<JobRecord.JobIdRootIdPair> externalIDs);
  
  public abstract VariableRecord get(String jobId, String portId, LinkPortType type, UUID rootId);
 
  public abstract List<VariableRecord> getByType(String jobId, LinkPortType type, UUID rootId);
  
  public abstract List<VariableRecord> getByPort(String jobId, String portId, UUID rootId);
 
}
