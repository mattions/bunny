package org.rabix.executor.execution.command;

import java.util.UUID;

import javax.inject.Inject;

import org.rabix.backend.api.callback.WorkerStatusCallback;
import org.rabix.bindings.BindingException;
import org.rabix.executor.ExecutorException;
import org.rabix.executor.execution.JobHandlerCommand;
import org.rabix.executor.handler.JobHandler;
import org.rabix.executor.model.JobData;
import org.rabix.executor.model.JobData.JobDataStatus;
import org.rabix.executor.service.JobDataService;
import org.rabix.executor.service.JobFitter;

/**
 * Command that stops {@link JobHandler} 
 */
public class StopCommand extends JobHandlerCommand {

  private JobFitter jobFitter;
  
  @Inject
  public StopCommand(JobDataService jobDataService, WorkerStatusCallback statusCallback, JobFitter jobFitter) {
    super(jobDataService, statusCallback);
    this.jobFitter = jobFitter;
  }

  @Override
  public Result run(JobData jobData, JobHandler handler, UUID rootId) {
    UUID jobId = jobData.getJob().getId();
    try {
      handler.removeContainer();
      handler.stop();
      String message = String.format("Job %s aborted successfully.", jobId);
      jobData = jobDataService.save(jobData, message, JobDataStatus.ABORTED);
      stopped(jobData, message, handler.getEngineStub());
      jobFitter.free(jobData.getJob());
    } catch (ExecutorException | BindingException e) {
      String message = String.format("Failed to stop %s. %s", jobId, e.toString());
      logger.error(message, e);
      jobData = jobDataService.save(jobData, message, JobDataStatus.FAILED);
    }
    return new Result(true);
  }

  @Override
  public JobHandlerCommandType getType() {
    return JobHandlerCommandType.STOP;
  }

}
