package com.weaverplatform.nifi.view;

import com.weaverplatform.nifi.individual.FlowFileProcessor;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.Weaver;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, create, view"})
@CapabilityDescription("Create a View object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateView extends FlowFileProcessor {

  Entity datasetViews;
  
  public static final PropertyDescriptor NAME_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Name Attribute")
    .description("Look for a FlowFile attribute to set the name.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor NAME_STATIC = new PropertyDescriptor
    .Builder().name("Name Static")
    .description("Look for a FlowFile attribute to set the name.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    super.init(context);
    
    descriptors.add(NAME_ATTRIBUTE);
    descriptors.add(NAME_STATIC);
    
    this.properties = Collections.unmodifiableList(descriptors);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    
    Weaver weaver = getWeaver();

    datasetViews = getDatasetViews();

    ProcessorLog log  = this.getLogger();
    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }
    
    String id = idFromOptions(context, flowFile, true);

    // Prepare view attributes with name
    ConcurrentMap<String, String> attributes = new ConcurrentHashMap<>();
    String name = valueFromOptions(context, flowFile, NAME_ATTRIBUTE, NAME_STATIC, "Unnamed");
    attributes.put("name", name);

    // Create view
    Entity view = weaver.add(attributes, EntityType.VIEW, id);

    // Attach to dataset
    datasetViews.linkEntity(id, view.toShallowEntity());

    // Give it the minimal collections it needs to be qualified as a valid view
    view.linkEntity("filters", weaver.collection().toShallowEntity());
    view.linkEntity("objects", weaver.collection().toShallowEntity());

    // Pass ID of this view as attribute in flowfile
    if(context.getProperty(ATTRIBUTE_NAME_FOR_ID).isSet()) {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, id);
    }
    
    session.transfer(flowFile, ORIGINAL);
  }
}