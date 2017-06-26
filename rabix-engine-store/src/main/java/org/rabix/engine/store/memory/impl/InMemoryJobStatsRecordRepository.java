package org.rabix.engine.store.memory.impl;

import com.google.inject.Inject;
import org.rabix.engine.store.model.JobStatsRecord;
import org.rabix.engine.store.repository.JobStatsRecordRepository;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryJobStatsRecordRepository implements JobStatsRecordRepository {

  Map<UUID, JobStatsRecord> jobStatsRecordRepository;

  @Inject
  public InMemoryJobStatsRecordRepository() {
    this.jobStatsRecordRepository = new ConcurrentHashMap<UUID, JobStatsRecord>();
  }

  @Override
  public synchronized int insert(JobStatsRecord jobStatsRecord) {
    jobStatsRecordRepository.put(jobStatsRecord.getRootId(), jobStatsRecord);
    return 1;
  }

  @Override
  public synchronized int update(JobStatsRecord jobStatsRecord) {
    jobStatsRecordRepository.put(jobStatsRecord.getRootId(), jobStatsRecord);
    return 1;
  }

  @Override
  public synchronized JobStatsRecord get(UUID id) {
    return jobStatsRecordRepository.get(id);
  }

  @Override
  public synchronized int delete(UUID id) {
    jobStatsRecordRepository.remove(id);
    return 1;
  }

}
