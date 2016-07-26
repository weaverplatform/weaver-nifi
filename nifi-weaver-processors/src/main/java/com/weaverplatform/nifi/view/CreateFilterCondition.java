package com.weaverplatform.nifi.view;

import com.weaverplatform.nifi.individual.FlowFileProcessor;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.json.request.ReadPayload;
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
public class CreateFilterCondition extends FlowFileProcessor {

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
  
  public static final PropertyDescriptor CONDITION_PATTERN = new PropertyDescriptor
    .Builder().name("Condition Pattern")
    .description("Depending on the condition type this value is used to test the condition.")
    .required(true)
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
    descriptors.add(CONDITION_PATTERN);
    descriptors.add(ATTRIBUTE_NAME_FOR_CONDITION_ID);
    
    this.properties = Collections.unmodifiableList(descriptors);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    
    Weaver weaver = getWeaver();

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      throw new RuntimeException("FlowFile is null");
    }
    
    // Get the Filter entity by ID
    if(!context.getProperty(FILTER_ID_ATTRIBUTE).isSet()) {
      throw new ProcessException("No attribute value could be found for the ID");
    }
    String filterId = flowFile.getAttribute(context.getProperty(FILTER_ID_ATTRIBUTE).getValue());
    Entity filter = weaver.get(filterId, new ReadPayload.Opts(1));
    
    String conditionType = context.getProperty(CONDITION_TYPE_STATIC).getValue();

    // Prepare condition attributes
    Map<String, String> attributes = new HashMap<>();
    attributes.put("conditiontype", conditionType);
    attributes.put("operation", context.getProperty(OPERATION_STATIC).getValue());

    
    
    if("string".equals(conditionType)) {
      // Link to the string   
      attributes.put("value", context.getProperty(CONDITION_PATTERN).getValue());
    }
    
    else if("individual".equals(conditionType)) {
      // Link to the individual   
      attributes.put("individual", context.getProperty(CONDITION_PATTERN).getValue());
    }
    
    else if("view".equals(conditionType)) {
      // Link to the view   
      attributes.put("view", context.getProperty(CONDITION_PATTERN).getValue());
    }
    else {
      throw new ProcessException("No supported conditiontype set (string, individual or view)!");
    }
    
    // Create condition
    Entity condition = weaver.add(attributes, "$CONDITION");
    
    // Attach to filter conditions
    Entity conditions = weaver.get(filter.getRelations().get("conditions").getId(), new ReadPayload.Opts(1));
    conditions.linkEntity(condition.getId(), condition.toShallowEntity());

    // Pass ID of this condition as attribute in flowfile
    if(context.getProperty(ATTRIBUTE_NAME_FOR_CONDITION_ID).isSet()) {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_CONDITION_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, condition.getId());
    }
    
    session.transfer(flowFile, ORIGINAL);
  }
}