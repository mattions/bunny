package org.rabix.engine;

import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.engine.service.impl.JobRecordServiceImpl.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobHelper {

  private static Logger logger = LoggerFactory.getLogger(JobHelper.class);
    
  public static JobStatus transformStatus(JobState state) {
    switch (state) {
    case COMPLETED:
      return JobStatus.COMPLETED;
    case FAILED:
      return JobStatus.FAILED;
    case RUNNING:
      return JobStatus.RUNNING;
    case READY:
      return JobStatus.READY;
    case PENDING:
      return JobStatus.PENDING;
    default:
      break;
    }
    return null;
  }
  
  public static JobState transformStatus(JobStatus status) {
    switch (status) {
    case COMPLETED:
      return JobState.COMPLETED;
    case FAILED:
      return JobState.FAILED;
    case RUNNING:
      return JobState.RUNNING;
    case READY:
      return JobState.READY;
    case PENDING:
      return JobState.PENDING;
    case ABORTED:
      return JobState.ABORTED;
    default:
      break;
    }
    return null;
  }
}
