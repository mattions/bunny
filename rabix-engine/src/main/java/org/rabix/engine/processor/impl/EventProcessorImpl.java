package org.rabix.engine.processor.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.dag.DAGLinkPort.LinkPortType;
import org.rabix.common.helper.InternalSchemaHelper;
import org.rabix.engine.event.Event;
import org.rabix.engine.event.Event.EventStatus;
import org.rabix.engine.event.Event.EventType;
import org.rabix.engine.event.Event.PersistentEventType;
import org.rabix.engine.event.impl.ContextStatusEvent;
import org.rabix.engine.model.ContextRecord;
import org.rabix.engine.model.ContextRecord.ContextStatus;
import org.rabix.engine.model.VariableRecord;
import org.rabix.engine.processor.EventProcessor;
import org.rabix.engine.processor.handler.EventHandlerException;
import org.rabix.engine.processor.handler.HandlerFactory;
import org.rabix.engine.repository.EventRepository;
import org.rabix.engine.repository.JobRepository;
import org.rabix.engine.repository.TransactionHelper;
import org.rabix.engine.repository.TransactionHelper.TransactionException;
import org.rabix.engine.service.CacheService;
import org.rabix.engine.service.ContextRecordService;
import org.rabix.engine.service.JobService;
import org.rabix.engine.service.VariableRecordService;
import org.rabix.engine.status.EngineStatusCallback;
import org.rabix.engine.status.EngineStatusCallbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

/**
 * Event processor implementation
 */
public class EventProcessorImpl implements EventProcessor {

  private static final Logger logger = LoggerFactory.getLogger(EventProcessorImpl.class);
  
  public final static long SLEEP = 100;
  
  private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();
  private final BlockingQueue<Event> externalEvents = new LinkedBlockingQueue<>();
  
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);

  private final HandlerFactory handlerFactory;
  
  private final ContextRecordService contextRecordService;
  
  private final TransactionHelper transactionHelper;
  private final CacheService cacheService;
  
  private final JobRepository jobRepository;
  private final EventRepository eventRepository;
  private final JobService jobService;
  private final VariableRecordService variableService;

  private EngineStatusCallback engineStatusCallback;
  
  @Inject
  public EventProcessorImpl(HandlerFactory handlerFactory, ContextRecordService contextRecordService,
      TransactionHelper transactionHelper, CacheService cacheService, EventRepository eventRepository,
      JobRepository jobRepository, JobService jobService, VariableRecordService variableService, EngineStatusCallback engineStatusCallback) {
    this.handlerFactory = handlerFactory;
    this.contextRecordService = contextRecordService;
    this.transactionHelper = transactionHelper;
    this.cacheService = cacheService;
    this.eventRepository = eventRepository;
    this.jobRepository = jobRepository;
    this.jobService = jobService;
    this.variableService = variableService;
    this.engineStatusCallback = engineStatusCallback;
  }

  public void start() {
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        final AtomicReference<Event> eventReference = new AtomicReference<Event>(null);
        while (!stop.get()) {
          try {
            eventReference.set(externalEvents.poll());
            if (eventReference.get() == null) {
              running.set(false);
              Thread.sleep(SLEEP);
              continue;
            }
            running.set(true);
            transactionHelper.doInTransaction(new TransactionHelper.TransactionCallback<Void>() {
              @Override
              public Void call() throws TransactionException {
                
            	List<VariableRecord> variables = null;
            	if(eventReference.get().getPersistentType().equals(PersistentEventType.JOB_STATUS_UPDATE_COMPLETED)){
            		variables = getVariables(eventReference.get().getContextId());
            	}
                if (!handle(eventReference.get())) {
                  eventRepository.delete(eventReference.get().getEventGroupId());
                  return null;
                }
                cacheService.flush(eventReference.get().getContextId());
                
                if (checkForReadyJobs(eventReference.get())) {
                  Set<Job> readyJobs = jobRepository.getReadyJobsByGroupId(eventReference.get().getEventGroupId());
                  jobService.handleJobsReady(readyJobs, eventReference.get().getContextId(), eventReference.get().getProducedByNode());  
                }
                eventRepository.delete(eventReference.get().getEventGroupId());
				if (variables != null) {
					Map<String, Object> fresh = getVariableMap(eventReference.get().getContextId());
					fresh.keySet().removeAll(
							variables.stream().map(p -> p.getPortId()).collect(Collectors.toSet()));
					try {
						engineStatusCallback.onJobRootPartiallyCompleted(
								eventReference.get().getContextId(), fresh,
								eventReference.get().getProducedByNode());
					} catch (EngineStatusCallbackException e) {
						e.printStackTrace();
					}
				}
                return null;
              }
            });
          } catch (Exception e) {
            logger.error("EventProcessor failed to process event {}.", eventReference.get(), e);
            try {
              Job job = jobRepository.get(eventReference.get().getContextId());
              job = Job.cloneWithMessage(job, "EventProcessor failed to process event:\n" + eventReference.get().toString());
              jobRepository.update(job);
              jobService.handleJobRootFailed(job);
            } catch (Exception ex) {
              logger.error("Failed to call jobFailed handler for job after event {} failed.", e, ex);
            }
            try {
              cacheService.clear(eventReference.get().getContextId());
              eventRepository.update(eventReference.get().getEventGroupId(), eventReference.get().getPersistentType(), Event.EventStatus.FAILED);
              invalidateContext(eventReference.get().getContextId());
            } catch (Exception ehe) {
              logger.error("Failed to invalidate Context {}.", eventReference.get().getContextId(), ehe);
            }
          }
        }
      }
    });
  }
  
  private List<VariableRecord> getVariables(UUID rootId){
    List<VariableRecord> vars = variableService.find(InternalSchemaHelper.ROOT_NAME, LinkPortType.OUTPUT, rootId);
    vars = vars.stream().filter(p->p.getNumberOfTimesUpdated()>0 && p.getNumberOfTimesUpdated()>=p.getNumberOfGlobals())
    .collect(Collectors.toList());
    return vars;
  }
  
  private Map<String, Object> getVariableMap(UUID rootId){
    List<VariableRecord> vars = getVariables(rootId);
    Map<String,Object> outs = new HashMap<>();

	vars.stream().forEach(p -> {
		outs.put(p.getPortId(), p.getValue());
	});
    return outs;
  }

  private boolean checkForReadyJobs(Event event) {
    switch (event.getType()) {
    case INIT:
      return true;
    case JOB_STATUS_UPDATE:
      if (PersistentEventType.JOB_STATUS_UPDATE_COMPLETED.equals(event.getPersistentType())) {
        return true;
      }
      return false;
    default:
      break;
    }
    return false;
  }
  
  private boolean handle(Event event) throws TransactionException {
    while (event != null) {
      try {
        ContextRecord context = contextRecordService.find(event.getContextId());
        if (context != null && (context.getStatus().equals(ContextStatus.FAILED) || context.getStatus().equals(ContextStatus.ABORTED))) {
          logger.info("Skip event {}. Context {} has been invalidated.", event, context.getId());
          return false;
        }
        handlerFactory.get(event.getType()).handle(event);
      } catch (EventHandlerException e) {
        throw new TransactionException(e);
      }
      event = events.poll();
    }
    return true;
  }
  
  /**
   * Invalidates context 
   */
  private void invalidateContext(UUID contextId) throws EventHandlerException {
    handlerFactory.get(Event.EventType.CONTEXT_STATUS_UPDATE).handle(new ContextStatusEvent(contextId, ContextStatus.FAILED));
  }
  
  @Override
  public void stop() {
    stop.set(true);
    running.set(false);
  }

  public boolean isRunning() {
    return running.get();
  }

  public void send(Event event) throws EventHandlerException {
    if (stop.get()) {
      return;
    }
    if (event.getType().equals(EventType.INIT)) {
      addToQueue(event);
      return;
    }
    handlerFactory.get(event.getType()).handle(event);
  }

  public void addToQueue(Event event) {
    if (stop.get()) {
      return;
    }
    this.events.add(event);
  }
  
  @Override
  public void persist(Event event) {
    if (stop.get()) {
      return;
    }
    eventRepository.insert(event.getEventGroupId(), event.getPersistentType(), event, EventStatus.UNPROCESSED);    
  }
  
  public void addToExternalQueue(Event event) {
    if (stop.get()) {
      return;
    }
    this.externalEvents.add(event);
  }

}
