package org.rabix.bindings.cwl1.processor;

import org.rabix.bindings.model.ApplicationPort;

public interface Draft3PortProcessorCallback {

  Draft3PortProcessorResult process(Object value, ApplicationPort port) throws Exception;
  
}
