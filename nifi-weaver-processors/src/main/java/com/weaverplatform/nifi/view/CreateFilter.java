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
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, create, view, filter"})
@CapabilityDescription("Create a View Filter object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateFilter extends DatasetProcessor {

  public static final PropertyDescriptor VIEW_ID_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("View ID Attribute")
    .description("Look for a View ID attribute to link this filter to the view.")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
  
  public static final PropertyDescriptor CELLTYPE_STATIC = new PropertyDescriptor
    .Builder().name("Celltype Static")
    .description("Static celltype (individual, string, etc).")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
  
  public static final PropertyDescriptor LABEL_STATIC = new PropertyDescriptor
    .Builder().name("Label Static")
    .description("Static label.")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
  
  public static final PropertyDescriptor PREDICATE_STATIC = new PropertyDescriptor
    .Builder().name("Predicate Static")
    .description("Static predicate.")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor ATTRIBUTE_NAME_FOR_FILTER_ID = new PropertyDescriptor
    .Builder().name("Attribute Name For Filter Id")
    .description("Expose the id of the created Filter as FlowFile attribute using this attribute name.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    super.init(context); 
    
    descriptors.add(VIEW_ID_ATTRIBUTE);
    descriptors.add(CELLTYPE_STATIC);
    descriptors.add(LABEL_STATIC);
    descriptors.add(PREDICATE_STATIC);
    descriptors.add(ATTRIBUTE_NAME_FOR_FILTER_ID);
    
    this.properties = Collections.unmodifiableList(descriptors);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    super.onTrigger(context, session);

    ProcessorLog log  = this.getLogger();
    FlowFile flowFile = this.getFlowFile();
    
    // Get the View entity by ID
    if(!context.getProperty(VIEW_ID_ATTRIBUTE).isSet()) {
      throw new ProcessException("No attribute value could be found for the ID");
    }
    String viewId = flowFile.getAttribute(context.getProperty(VIEW_ID_ATTRIBUTE).getValue());
    Entity view = weaver.get(viewId);

    // Prepare filter attributes
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("label",     context.getProperty(LABEL_STATIC).getValue());
    attributes.put("celltype",  context.getProperty(CELLTYPE_STATIC).getValue());
    attributes.put("predicate", context.getProperty(PREDICATE_STATIC).getValue());

    // Create filter
    Entity filter = weaver.add(attributes, "$FILTER");
    
    // Give it the minimal collections it needs to be qualified as a valid filter
    filter.linkEntity("conditions", weaver.collection());
    
    // Attach to view
    Entity filters = weaver.get(view.getRelations().get("filters").getId());
    filters.linkEntity(filter.getId(), filter);
    
    // Close connection
    weaver.close();

    // Pass ID of this filter as attribute in flowfile
    if(context.getProperty(ATTRIBUTE_NAME_FOR_FILTER_ID).isSet()) {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_FILTER_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, filter.getId());
    }
    
    session.transfer(flowFile, ORIGINAL);
  }
}