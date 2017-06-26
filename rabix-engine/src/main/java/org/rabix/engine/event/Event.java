package org.rabix.engine.event;

import java.util.UUID;

import org.rabix.engine.event.impl.ContextStatusEvent;
import org.rabix.engine.event.impl.InitEvent;
import org.rabix.engine.event.impl.InputUpdateEvent;
import org.rabix.engine.event.impl.JobStatusEvent;
import org.rabix.engine.event.impl.OutputUpdateEvent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.rabix.engine.store.model.EventRecord;

/**
 * Describes event interface used in the algorithm 
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ 
    @Type(value = InitEvent.class, name = "INIT"),
    @Type(value = InputUpdateEvent.class, name = "INPUT_UPDATE"),
    @Type(value = OutputUpdateEvent.class, name = "OUTPUT_UPDATE"),
    @Type(value = JobStatusEvent.class, name = "JOB_STATUS_UPDATE"),
    @Type(value = ContextStatusEvent.class, name = "CONTEXT_STATUS_UPDATE")})
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface Event {

  public enum EventType {
    INIT,
    INPUT_UPDATE,
    OUTPUT_UPDATE,
    JOB_STATUS_UPDATE,
    CONTEXT_STATUS_UPDATE
  }

  /**
   * Gets type of the event 
   */
  EventType getType();
  
  EventRecord.PersistentType getPersistentType();
  
  UUID getContextId();
  
  UUID getEventGroupId();

  String getProducedByNode();
  
}
