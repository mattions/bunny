package org.rabix.executor.service;

import java.util.List;
import java.util.UUID;

import org.rabix.backend.api.engine.EngineStub;
import org.rabix.executor.model.JobData;
import org.rabix.executor.model.JobData.JobDataStatus;

public interface JobDataService {

  void initialize(EngineStub<?,?,?> engineStub);
  
  void save(JobData data);

  JobData save(JobData jobData, String message, JobDataStatus status);
  
  public JobData find(UUID id, UUID contextId);

  public List<JobData> find(JobDataStatus... statuses);

}
