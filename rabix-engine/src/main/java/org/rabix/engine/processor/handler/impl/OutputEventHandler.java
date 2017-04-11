package org.rabix.engine.processor.handler.impl;

import java.util.List;
import java.util.Map;

import org.rabix.bindings.BindingException;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.common.helper.InternalSchemaHelper;
import org.rabix.engine.JobHelper;
import org.rabix.engine.event.Event;
import org.rabix.engine.event.impl.InputUpdateEvent;
import org.rabix.engine.event.impl.JobStatusEvent;
import org.rabix.engine.event.impl.OutputUpdateEvent;
import org.rabix.engine.model.JobRecord;
import org.rabix.engine.model.JobStatsRecord;
import org.rabix.engine.model.LinkRecord;
import org.rabix.engine.model.VariableRecord;
import org.rabix.engine.model.scatter.ScatterStrategy;
import org.rabix.engine.processor.handler.EventHandler;
import org.rabix.engine.processor.handler.EventHandlerException;
import org.rabix.engine.service.impl.JobRecordServiceImpl.JobState;

/**
 * Handles {@link OutputUpdateEvent} events.
 */
public class OutputEventHandler extends AbstractHandler implements EventHandler<OutputUpdateEvent> {
  
  public void handle(final OutputUpdateEvent event) throws EventHandlerException {
    JobRecord sourceJob = jobRecordService.find(event.getJobId(), event.getContextId());
    if (sourceJob.getState().equals(JobState.COMPLETED)) {
      return;
    }
    if (event.isFromScatter()) {
      jobRecordService.resetOutputPortCounter(sourceJob, event.getNumberOfScattered(), event.getPortId());
    }
    VariableRecord sourceVariable = variableService.find(event.getJobId(), event.getPortId(), LinkPortType.OUTPUT, event.getContextId());
    jobRecordService.decrementPortCounter(sourceJob, event.getPortId(), LinkPortType.OUTPUT);
    variableService.addValue(sourceVariable, event.getValue(), event.getPosition(), sourceJob.isScatterWrapper() || event.isFromScatter());
    variableService.update(sourceVariable); // TODO wha?
    jobRecordService.update(sourceJob);
    
    if (sourceJob.isCompleted()) {
      if(sourceJob.getOutputCounter(sourceVariable.getPortId()) != null) {
        if ((sourceJob.isContainer() || sourceJob.isScatterWrapper()) &&
            sourceJob.getParentId() != null && sourceJob.getParentId().equals(sourceJob.getRootId())) {
          JobStatsRecord jobStatsRecord = jobStatsRecordService.findOrCreate(sourceJob.getRootId());
          jobStatsRecord.increaseCompleted();
          jobStatsRecord.increaseRunning();
          jobStatsRecordService.update(jobStatsRecord);
        }

        if (sourceJob.isRoot()) {
          Map<String, Object> outputs = getOutputs(sourceJob);
          Job rootJob = createRootJob(sourceJob, JobHelper.transformStatus(sourceJob.getState()), outputs);
          jobService.handleJobRootPartiallyCompleted(rootJob, event.getProducedByNode());

          if(sourceJob.isRoot() && sourceJob.isContainer()) {
            // if root job is CommandLineTool OutputUpdateEvents are created from JobStatusEvent
            eventProcessor.send(new JobStatusEvent(sourceJob.getId(), event.getContextId(), JobState.COMPLETED, outputs, event.getEventGroupId(), event.getProducedByNode()));
            jobService.handleJobCompleted(rootJob);
          }
          return;
        }
        else {
          try {
            Job completedJob = createCompletedJob(sourceJob);
            jobService.handleJobCompleted(completedJob);
          } catch (BindingException e) {
          }
        }
      }
    }
    
    if (sourceJob.isRoot()) {
      jobService.handleJobRootPartiallyCompleted(createRootJob(sourceJob, JobHelper.transformStatus(sourceJob.getState()), getOutputs(sourceJob)), event.getProducedByNode());
    }
    
    Object value = null;
    
    if (sourceJob.isScatterWrapper()) {
      ScatterStrategy scatterStrategy = sourceJob.getScatterStrategy();
      
      boolean isValueFromScatterStrategy = false;
      if (scatterStrategy.isBlocking()) {
        if (sourceJob.isOutputPortReady(event.getPortId())) {
          isValueFromScatterStrategy = true;
          value = scatterStrategy.values(variableService, sourceJob.getId(), event.getPortId(), event.getContextId());
        } else {
          return;
        }
      }
      
      List<LinkRecord> links = linkService.findBySourceAndSourceType(sourceVariable.getJobId(), sourceVariable.getPortId(), LinkPortType.OUTPUT, event.getContextId());
      for (LinkRecord link : links) {
        if (!isValueFromScatterStrategy) {
          value = null; // reset
        }
        VariableRecord destinationVariable = variableService.find(link.getDestinationJobId(), link.getDestinationJobPort(), link.getDestinationVarType(), event.getContextId());

        JobRecord destinationJob = null;
        boolean isDestinationPortScatterable = false;
        switch (destinationVariable.getType()) {
        case INPUT:
          destinationJob = jobRecordService.find(destinationVariable.getJobId(), destinationVariable.getRootId());
          isDestinationPortScatterable = destinationJob.isScatterPort(destinationVariable.getPortId());
          if (isDestinationPortScatterable && !destinationJob.isBlocking() && !(destinationJob.getInputPortIncoming(event.getPortId()) > 1)) {
            value = value != null ? value : event.getValue();
            int numberOfScattered = sourceJob.getNumberOfGlobalOutputs();
            Event updateInputEvent = new InputUpdateEvent(event.getContextId(), destinationVariable.getJobId(), destinationVariable.getPortId(), value, true, numberOfScattered, event.getPosition(), event.getEventGroupId(), event.getProducedByNode());
            eventProcessor.send(updateInputEvent);
          } else {
            if (sourceJob.isOutputPortReady(event.getPortId())) {
              value = value != null ? value : variableService.getValue(sourceVariable);
              Event updateInputEvent = new InputUpdateEvent(event.getContextId(), destinationVariable.getJobId(), destinationVariable.getPortId(), value, link.getPosition(), event.getEventGroupId(), event.getProducedByNode());
              eventProcessor.send(updateInputEvent);
            }
          }
          break;
        case OUTPUT:
          destinationJob = jobRecordService.find(destinationVariable.getJobId(), destinationVariable.getRootId());
          if (destinationJob.getOutputPortIncoming(event.getPortId()) > 1) {
            if (sourceJob.isOutputPortReady(event.getPortId())) {
              value = value != null ? value : variableService.getValue(sourceVariable);
              Event updateInputEvent = new OutputUpdateEvent(event.getContextId(), destinationVariable.getJobId(), destinationVariable.getPortId(), value, link.getPosition(), event.getEventGroupId(), event.getProducedByNode());
              eventProcessor.send(updateInputEvent);
            }
          } else {
            value = value != null ? value : event.getValue();
            if (isValueFromScatterStrategy) {
              Event updateOutputEvent = new OutputUpdateEvent(event.getContextId(), destinationVariable.getJobId(), destinationVariable.getPortId(), value, false, 1, 1, event.getEventGroupId(), event.getProducedByNode());
              eventProcessor.send(updateOutputEvent);
            } else {
              int numberOfScattered = sourceJob.getNumberOfGlobalOutputs();
              Event updateOutputEvent = new OutputUpdateEvent(event.getContextId(), destinationVariable.getJobId(), destinationVariable.getPortId(), value, true, numberOfScattered, event.getPosition(), event.getEventGroupId(), event.getProducedByNode());
              eventProcessor.send(updateOutputEvent);
            }
          }
          break;
        }
      }
      return;
    }
    
    if (sourceJob.isOutputPortReady(event.getPortId())) {
      List<LinkRecord> links = linkService.findBySourceAndSourceType(event.getJobId(), event.getPortId(),
          LinkPortType.OUTPUT, event.getContextId());
      for (LinkRecord link : links) {
        VariableRecord destinationVariable = variableService.find(link.getDestinationJobId(), link.getDestinationJobPort(), link.getDestinationVarType(), event.getContextId());
        value = variableService.getValue(sourceVariable);
        switch (destinationVariable.getType()) {
        case INPUT:
          Event updateInputEvent = new InputUpdateEvent(event.getContextId(), destinationVariable.getJobId(), destinationVariable.getPortId(), value, link.getPosition(), event.getEventGroupId(), event.getProducedByNode());
          eventProcessor.send(updateInputEvent);
          break;
        case OUTPUT:
          if (sourceJob.isScattered()) {
            int numberOfScattered = sourceJob.getNumberOfGlobalOutputs();
            int position = InternalSchemaHelper.getScatteredNumber(sourceJob.getId());
            Event updateOutputEvent = new OutputUpdateEvent(event.getContextId(), destinationVariable.getJobId(), destinationVariable.getPortId(), value, true, numberOfScattered, position, event.getEventGroupId(), event.getProducedByNode());
            eventProcessor.send(updateOutputEvent);
          } else if (InternalSchemaHelper.getParentId(sourceJob.getId()).equals(destinationVariable.getJobId())) {
            Event updateOutputEvent = new OutputUpdateEvent(event.getContextId(), destinationVariable.getJobId(), destinationVariable.getPortId(), value, link.getPosition(), event.getEventGroupId(), event.getProducedByNode());
            eventProcessor.send(updateOutputEvent);
          }
          break;
        }
      }
    }
  }
}