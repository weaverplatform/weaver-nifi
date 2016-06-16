package com.weaverplatform.nifi.view;

import com.weaverplatform.nifi.individual.DatasetProcessor;
import com.weaverplatform.sdk.Entity;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, create, view, filter, condition"})
@CapabilityDescription("Create a View Filter Condition object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateFilterCondition extends DatasetProcessor {

  public static final PropertyDescriptor FILTER_ID_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Filter ID Attribute")
    .description("Look for a Filter ID attribute to link this condition to the filter.")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
  
  public static final PropertyDescriptor CONDITION_TYPE_STATIC = new PropertyDescriptor
    .Builder().name("Condition type Static")
    .description("Static Condition type (individual, string, etc).")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
  
  public static final PropertyDescriptor OPERATION_STATIC = new PropertyDescriptor
    .Builder().name("Operation Static")
    .description("Static operation such as any-individual or this-individual.")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
  
  public static final PropertyDescriptor INDIVIDUAL_STATIC = new PropertyDescriptor
    .Builder().name("Individual Static")
    .description("Individual to which this condition should point to.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor ATTRIBUTE_NAME_FOR_CONDITION_ID = new PropertyDescriptor
    .Builder().name("Attribute Name For Condition Id")
    .description("Expose the id of the created Condition as FlowFile attribute using this attribute name.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    super.init(context); 
    
    descriptors.add(FILTER_ID_ATTRIBUTE);
    descriptors.add(CONDITION_TYPE_STATIC);
    descriptors.add(OPERATION_STATIC);
    descriptors.add(INDIVIDUAL_STATIC);
    descriptors.add(ATTRIBUTE_NAME_FOR_CONDITION_ID);
    
    this.properties = Collections.unmodifiableList(descriptors);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    super.onTrigger(context, session);

    FlowFile flowFile = this.getFlowFile();
    
    // Get the Filter entity by ID
    if(!context.getProperty(FILTER_ID_ATTRIBUTE).isSet()) {
      throw new ProcessException("No attribute value could be found for the ID");
    }
    String filterId = flowFile.getAttribute(context.getProperty(FILTER_ID_ATTRIBUTE).getValue());
    Entity filter = weaver.get(filterId);

    // Prepare condition attributes
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("conditiontype", context.getProperty(CONDITION_TYPE_STATIC).getValue());
    attributes.put("operation",     context.getProperty(OPERATION_STATIC).getValue());

    // Create condition
    Entity condition = weaver.add(attributes, "$CONDITION");
    
    // Link to the individual
    Entity individual = weaver.get(context.getProperty(INDIVIDUAL_STATIC).getValue());
    condition.linkEntity("individual", individual);
    
    // Attach to filter conditions
    Entity conditions = weaver.get(filter.getRelations().get("conditions").getId());
    conditions.linkEntity(condition.getId(), condition);
    
    // Close connection
//    weaver.close();

    // Pass ID of this condition as attribute in flowfile
    if(context.getProperty(ATTRIBUTE_NAME_FOR_CONDITION_ID).isSet()) {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_CONDITION_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, condition.getId());
    }
    
    session.transfer(flowFile, ORIGINAL);
  }
}