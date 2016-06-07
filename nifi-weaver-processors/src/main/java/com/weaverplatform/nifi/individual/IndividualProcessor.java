package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.WeaverProcessor;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.UUID;

/**
 * @author Bastiaan Bijl
 */
public abstract class IndividualProcessor extends WeaverProcessor {

  public static final PropertyDescriptor INDIVIDUAL_ATTRIBUTE = new PropertyDescriptor
      .Builder().name("Individual Attribute")
      .description("Look for a FlowFile attribute.")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();

  public static final PropertyDescriptor INDIVIDUAL_STATIC = new PropertyDescriptor
      .Builder().name("Individual Static")
      .description("If there is no FlowFile attribute, use static value.")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();


  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context);

    descriptors.add(INDIVIDUAL_ATTRIBUTE);
    descriptors.add(INDIVIDUAL_STATIC);

  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    
    super.onTrigger(context, session);

  }         
  
  
  public String idFromOptions(ProcessContext context, FlowFile flowFile, boolean createRandomFallback) throws ProcessException {
    String id = null;
    String individualAttributeValue = context.getProperty(INDIVIDUAL_ATTRIBUTE).getValue();
    String individualStaticValue    = context.getProperty(INDIVIDUAL_STATIC).getValue();

    if(individualAttributeValue != null){
      id = flowFile.getAttribute(individualAttributeValue);
    }
    else if(individualStaticValue != null){
      id = individualStaticValue;
    }

    // Fallback generate random ID
    if(id == null) {
      if(!createRandomFallback) {
        throw new ProcessException("No ID could be found.");
      }
      id = UUID.randomUUID().toString();
    }
    return id;
  }


}
