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
public abstract class EntityProcessor extends WeaverProcessor {

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
    
    String fallback = null;
    if(createRandomFallback) {
      fallback = UUID.randomUUID().toString();
    }
    
    return valueFromOptions(context, flowFile, INDIVIDUAL_ATTRIBUTE, INDIVIDUAL_STATIC, fallback);
  }
  
  public String valueFromOptions(ProcessContext context, FlowFile flowFile, PropertyDescriptor attributeValue, PropertyDescriptor staticValue, String fallback) throws ProcessException {
    
    String resultAttributeValue = context.getProperty(attributeValue).getValue();
    String resultStaticValue    = context.getProperty(staticValue).getValue();
    if(resultAttributeValue != null) {
      return flowFile.getAttribute(resultAttributeValue);
    } else if(resultStaticValue != null) {
      return resultStaticValue;
    }

    if(fallback != null) {
      return fallback;
    }
    throw new ProcessException("No attribute value could be found for "+attributeValue+" or "+staticValue);
  }
}