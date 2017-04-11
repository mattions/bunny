package org.rabix.engine.processor.handler.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.helper.URIHelper;
import org.rabix.bindings.model.ApplicationPort;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.Job.JobStatus;
import org.rabix.bindings.model.dag.DAGLinkPort;
import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.bindings.model.dag.DAGNode;
import org.rabix.common.helper.CloneHelper;
import org.rabix.common.helper.InternalSchemaHelper;
import org.rabix.common.logging.DebugAppender;
import org.rabix.engine.db.AppDB;
import org.rabix.engine.db.DAGNodeDB;
import org.rabix.engine.model.ContextRecord;
import org.rabix.engine.model.JobRecord;
import org.rabix.engine.model.VariableRecord;
import org.rabix.engine.processor.EventProcessor;
import org.rabix.engine.service.ContextRecordService;
import org.rabix.engine.service.JobRecordService;
import org.rabix.engine.service.JobService;
import org.rabix.engine.service.JobStatsRecordService;
import org.rabix.engine.service.LinkRecordService;
import org.rabix.engine.service.VariableRecordService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public abstract class AbstractHandler {
  @Inject
  JobRecordService jobRecordService;
  @Inject
  LinkRecordService linkService;
  @Inject
  VariableRecordService variableService;
  @Inject
  ContextRecordService contextService;
  @Inject
  JobStatsRecordService jobStatsRecordService;
  @Inject
  EventProcessor eventProcessor;
  @Inject
  DAGNodeDB dagNodeDB;
  @Inject
  AppDB appDB;
  @Inject
  JobService jobService;

  private Logger logger = LoggerFactory.getLogger(getClass());


  public Job createCompletedJob(JobRecord job) throws BindingException {
    List<VariableRecord> outputVariables = variableService.find(job.getId(), LinkPortType.OUTPUT, job.getRootId());
    Map<String, Object> outputs = new HashMap<>();
    for (VariableRecord outputVariable : outputVariables) {
      outputs.put(outputVariable.getPortId(), variableService.getValue(outputVariable));
    }
    return createJob(job, JobStatus.COMPLETED, outputs);
  }

  public Job createFailedJob(JobRecord job, String message) throws BindingException {
    return createJob(job, JobStatus.FAILED, null);
  }

  public Job createRootJob(JobRecord job, JobStatus status, Map<String, Object> outputs) {
    return createRootJob(job, status, outputs, null);
  }

  public Job createRootJob(JobRecord job, JobStatus status, Map<String, Object> outputs, String message) {
    DAGNode node = dagNodeDB.get(InternalSchemaHelper.normalizeId(job.getId()), job.getRootId(), job.getDagHash());

    Map<String, Object> inputs = new HashMap<>();
    List<VariableRecord> inputVariables = variableService.find(job.getId(), LinkPortType.INPUT, job.getRootId());
    for (VariableRecord inputVariable : inputVariables) {
      Object value = CloneHelper.deepCopy(variableService.getValue(inputVariable));
      inputs.put(inputVariable.getPortId(), value);
    }

    ContextRecord contextRecord = contextService.find(job.getRootId());
    String encodedApp = URIHelper.createDataURI(appDB.get(node.getAppHash()).serialize());
    return new Job(job.getExternalId(), job.getParentId(), job.getRootId(), job.getId(), encodedApp, status, message, inputs, outputs,
        contextRecord.getConfig(), null);
  }

  public Job createReadyJob(JobRecord job) throws BindingException {
    return createJob(job, JobStatus.READY, null);
  }

  public Job createJob(JobRecord job, JobStatus status, Map<String, Object> outputs) throws BindingException {
    DAGNode node = dagNodeDB.get(InternalSchemaHelper.normalizeId(job.getId()), job.getRootId(), job.getDagHash());

    boolean autoBoxingEnabled = false; // get from configuration

    DebugAppender inputsLogBuilder = new DebugAppender(logger);

    inputsLogBuilder.append("\n ---- JobRecord ", job.getId(), "\n");

    Map<String, Object> inputs = new HashMap<>();

    List<VariableRecord> inputVariables = variableService.find(job.getId(), LinkPortType.INPUT, job.getRootId());

    Map<String, Object> preprocesedInputs = new HashMap<>();
    for (VariableRecord inputVariable : inputVariables) {
      Object value = variableService.getValue(inputVariable);
      preprocesedInputs.put(inputVariable.getPortId(), value);
    }

    ContextRecord contextRecord = contextService.find(job.getRootId());
    String encodedApp = URIHelper.createDataURI(appDB.get(node.getAppHash()).serialize());

    Job newJob = new Job(job.getExternalId(), job.getParentId(), job.getRootId(), job.getId(), encodedApp, status, null, preprocesedInputs, null,
        contextRecord.getConfig(), null);
    try {
      if (!job.isContainer() && !job.isScatterWrapper()) {
        Bindings bindings = null;
        if (node.getProtocolType() != null) {
          bindings = BindingsFactory.create(node.getProtocolType());
        } else {
          bindings = BindingsFactory.create(encodedApp);
        }

        for (VariableRecord inputVariable : inputVariables) {
          Object value = CloneHelper.deepCopy(variableService.getValue(inputVariable));
          ApplicationPort port = appDB.get(node.getAppHash()).getInput(inputVariable.getPortId());
          if (port == null) {
            continue;
          }
          for (DAGLinkPort p : node.getInputPorts()) {
            if (p.getId().equals(inputVariable.getPortId())) {
              if (p.getTransform() != null) {
                Object transform = p.getTransform();
                if (transform != null) {
                  value = bindings.transformInputs(value, newJob, transform);
                }
              }
            }
          }
          if (port != null && autoBoxingEnabled) {
            if (port.isList() && !(value instanceof List)) {
              List<Object> transformed = new ArrayList<>();
              transformed.add(value);
              value = transformed;
            }
          }
          inputsLogBuilder.append(" ---- Input ", inputVariable.getPortId(), ", value ", value, "\n");
          inputs.put(inputVariable.getPortId(), value);
        }
      } else {
        inputs = preprocesedInputs;
      }
    } catch (BindingException e) {
      throw new BindingException("Failed to transform inputs", e);
    }

    logger.debug(inputsLogBuilder.toString());
    return new Job(job.getExternalId(), job.getParentId(), job.getRootId(), job.getId(), encodedApp, status, null, inputs, outputs, contextRecord.getConfig(),
        null);
  }

  public Map<String, Object> getOutputs(JobRecord jobRecord) {
    List<VariableRecord> outputVariables = variableService.find(jobRecord.getId(), LinkPortType.OUTPUT, jobRecord.getRootId());
    Map<String, Object> outputs = new HashMap<>();
    for (VariableRecord outputVariable : outputVariables) {
      outputs.put(outputVariable.getPortId(), CloneHelper.deepCopy(variableService.getValue(outputVariable)));//TODO why?
    }
    return outputs;
  }

}
