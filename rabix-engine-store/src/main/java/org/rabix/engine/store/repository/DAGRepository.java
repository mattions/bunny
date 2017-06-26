package org.rabix.engine.store.repository;

import java.util.UUID;

import org.rabix.bindings.model.dag.DAGNode;

public interface DAGRepository {

  void insert(UUID rootId, DAGNode dag);
  
  DAGNode get(String id, UUID rootId);
}
