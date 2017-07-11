package org.rabix.engine.processor.handler.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.rabix.bindings.BindingException;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.common.helper.CloneHelper;
import org.rabix.common.helper.InternalSchemaHelper;
import org.rabix.engine.JobHelper;
import org.rabix.engine.db.AppDB;
import org.rabix.engine.db.DAGNodeDB;
import org.rabix.engine.event.Event;
import org.rabix.engine.event.impl.InputUpdateEvent;
import org.rabix.engine.event.impl.JobStatusEvent;
import org.rabix.engine.event.impl.OutputUpdateEvent;
import org.rabix.engine.model.JobRecord;
import org.rabix.engine.model.JobRecord.PortCounter;
import org.rabix.engine.model.JobStatsRecord;
import org.rabix.engine.model.LinkRecord;
import org.rabix.engine.model.VariableRecord;
import org.rabix.engine.model.scatter.ScatterStrategy;
import org.rabix.engine.processor.EventProcessor;
import org.rabix.engine.processor.handler.EventHandler;
import org.rabix.engine.processor.handler.EventHandlerException;
import org.rabix.engine.service.ContextRecordService;
import org.rabix.engine.service.JobRecordService;
import org.rabix.engine.service.JobService;
import org.rabix.engine.service.JobStatsRecordService;
import org.rabix.engine.service.LinkRecordService;
import org.rabix.engine.service.VariableRecordService;
import org.rabix.engine.service.impl.JobRecordServiceImpl.JobState;

import com.google.inject.Inject;

/**
 * Handles {@link OutputUpdateEvent} events.
 */
public class OutputEventHandler implements EventHandler<OutputUpdateEvent> {

  private JobRecordService jobRecordService;
  private LinkRecordService linkService;
  private VariableRecordService variableService;
  private ContextRecordService contextService;
  private JobStatsRecordService jobStatsRecordService;
  private final EventProcessor eventProcessor;

  private DAGNodeDB dagNodeDB;
  private AppDB appDB;
  private JobService jobService;

  @Inject
  public OutputEventHandler(EventProcessor eventProcessor, JobRecordService jobRecordService, VariableRecordService variableService,
      LinkRecordService linkService, ContextRecordService contextService, DAGNodeDB dagNodeDB, AppDB appDB, JobService jobService,
      JobStatsRecordService jobStatsRecordService) {
    this.dagNodeDB = dagNodeDB;
    this.appDB = appDB;
    this.jobRecordService = jobRecordService;
    this.linkService = linkService;
    this.contextService = contextService;
    this.variableService = variableService;
    this.eventProcessor = eventProcessor;
    this.jobService = jobService;
    this.jobStatsRecordService = jobStatsRecordService;
  }

  public void handle(final OutputUpdateEvent event) throws EventHandlerException {
    JobRecord sourceJob = jobRecordService.find(event.getJobId(), event.getContextId());
    if (sourceJob.getState().equals(JobState.COMPLETED)) {
      return;
    }
    if (sourceJob.isScatterWrapper()) {
      jobRecordService.resetOutputPortCounter(sourceJob, event.getNumberOfScattered(), event.getPortId());
    }
    VariableRecord sourceVariable = variableService.find(event.getJobId(), event.getPortId(), LinkPortType.OUTPUT, event.getContextId());
    jobRecordService.decrementPortCounter(sourceJob, event.getPortId(), LinkPortType.OUTPUT);
    variableService.addValue(sourceVariable, event.getValue(), event.getPosition(), sourceJob.isScatterWrapper() && !sourceJob.getScatterStrategy().isEmptyListDetected());
    variableService.update(sourceVariable); // TODO wha?
    jobRecordService.update(sourceJob);

    Boolean isScatterWrapper = sourceJob.isScatterWrapper();

    if (sourceJob.isCompleted()) {
      if (sourceJob.getOutputCounter(sourceVariable.getPortId()) != null) {
        if ((sourceJob.isContainer() || isScatterWrapper) && sourceJob.getParentId() != null && sourceJob.getParentId().equals(sourceJob.getRootId())) {
          JobStatsRecord jobStatsRecord = jobStatsRecordService.findOrCreate(sourceJob.getRootId());
          jobStatsRecord.increaseCompleted();
          jobStatsRecord.increaseRunning();
          jobStatsRecordService.update(jobStatsRecord);
        }

        if (sourceJob.isRoot()) {
          if (sourceJob.isContainer()) {  
        	  Job rootJob = createRootJob(sourceJob, JobHelper.transformStatus(sourceJob.getState()));
              jobService.handleJobRootPartiallyCompleted(rootJob.getId(), rootJob.getOutputs(), event.getProducedByNode());
            eventProcessor.send(new JobStatusEvent(sourceJob.getId(), event.getContextId(), JobState.COMPLETED, rootJob.getOutputs(), event.getEventGroupId(),
                event.getProducedByNode()));
          }
          return;
        } else {
          try {
            Job completedJob = JobHelper.createCompletedJob(sourceJob, JobStatus.COMPLETED, jobRecordService, variableService, linkService, contextService,
                dagNodeDB, appDB);
            jobService.handleJobCompleted(completedJob);
            if(sourceJob.isScatterWrapper())
            eventProcessor.addToQueue(new JobStatusEvent(sourceJob.getId(), event.getContextId(), JobState.COMPLETED, completedJob.getOutputs(), event.getEventGroupId(), sourceJob.getId()));
          } catch (BindingException e) {
          }
        }
      }
    }

    Object value = variableService.getValue(sourceVariable);
    PortCounter outputCounter = sourceJob.getOutputCounter(event.getPortId());
    Integer numberOfScattered = outputCounter == null ? 0 : outputCounter.getGlobalCounter();
    
    if (isScatterWrapper) {
      numberOfScattered = sourceJob.getNumberOfGlobalOutputs();
      ScatterStrategy scatterStrategy = sourceJob.getScatterStrategy();
      if (scatterStrategy.isBlocking()) {
        if (sourceJob.isOutputPortReady(event.getPortId())) {
          value = scatterStrategy.values(variableService, sourceJob.getId(), event.getPortId(), event.getContextId());
        } else {
          return;
        }
      }
    }

    List<LinkRecord> links = linkService.findBySourceAndSourceType(sourceVariable.getJobId(), sourceVariable.getPortId(), LinkPortType.OUTPUT,
        event.getContextId());
    List<String> terminals = new ArrayList<>();
    for (LinkRecord link : links) {
      Object tempValue = value;
      Event newEvent = null;
      switch (link.getDestinationVarType()) {
        case INPUT:
          boolean lookAhead = false;
          JobRecord destinationJob = jobRecordService.find(link.getDestinationJobId(), link.getRootId());
          int position = link.getPosition();
          if (isScatterWrapper) {
            if (destinationJob.isScatterPort(link.getDestinationJobPort()) && !destinationJob.isBlocking() && !(destinationJob.getInputPortIncoming(event.getPortId()) > 1)) {
              tempValue = event.getValue();
              position = event.getPosition();
              lookAhead = true;
            } else {
              if (!sourceJob.isOutputPortReady(event.getPortId())) {
                break;
              }
            }
          }
          newEvent = new InputUpdateEvent(event.getContextId(), link.getDestinationJobId(), link.getDestinationJobPort(), tempValue, lookAhead, numberOfScattered, position, event.getEventGroupId(), event.getProducedByNode());
          break;
        case OUTPUT:
          boolean destinationRoot = link.getDestinationJobId().equals(InternalSchemaHelper.ROOT_NAME);
		  if(sourceJob.isScattered() && destinationRoot) 
        	break;
          if (sourceJob.isOutputPortReady(event.getPortId()) || sourceJob.isScattered()) {
            newEvent = new OutputUpdateEvent(event.getContextId(), link.getDestinationJobId(), link.getDestinationJobPort(), value, numberOfScattered, link.getPosition(), event.getEventGroupId(), event.getProducedByNode());
          }
          break;
      }
      if (newEvent != null)
        eventProcessor.send(newEvent);
    }
  }

  private Job createRootJob(JobRecord jobRecord, JobStatus status) {
    Map<String, Object> outputs = new HashMap<>();
    List<VariableRecord> outputVariables = variableService.find(jobRecord.getId(), LinkPortType.OUTPUT, jobRecord.getRootId());
    for (VariableRecord outputVariable : outputVariables) {
      Object value = CloneHelper.deepCopy(variableService.getValue(outputVariable));
      outputs.put(outputVariable.getPortId(), value);
    }
    return JobHelper.createRootJob(jobRecord, status, jobRecordService, variableService, linkService, contextService, dagNodeDB, appDB, outputs);
  }

}
