package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.RelationKeys;
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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, create, individual"})
@CapabilityDescription("Create individual object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateIndividual extends IndividualProcessor {

  String datasetId;
  Entity dataset;
  Entity datasetObjects;

  public static final PropertyDescriptor DATASET = new PropertyDescriptor
      .Builder().name("Dataset ID")
      .description("Dataset ID to add individuals to.")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();
  
  public static final PropertyDescriptor NAME_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Name Attribute")
    .description("Look for a FlowFile attribute to set the name.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final Relationship ORIGINAL = new Relationship.Builder()
    .name("Original Relationship")
    .description("Original relationship to transfer content to.")
    .build();
  
  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context); 
    
    descriptors.add(DATASET);
    descriptors.add(NAME_ATTRIBUTE);
    this.properties = Collections.unmodifiableList(descriptors);

    relationshipSet.add(ORIGINAL);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    
    super.onTrigger(context, session);

    // Dataset
    if(context.getProperty(DATASET).getValue() != null) {
      datasetId = context.getProperty(DATASET).getValue();
    } else {
      datasetId = NiFiProperties.getInstance().get(WeaverProperties.DATASET).toString();
    }

    dataset = weaver.get(datasetId);
    datasetObjects = weaver.get(dataset.getRelations().get("objects").getId());

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }
    
    
    String id = idFromOptions(context, flowFile, true);

    // Create entity by user attribute
    Map<String, Object> attributes = new HashMap<>();
    String name = null;
    String nameAttribute = context.getProperty(NAME_ATTRIBUTE).getValue();
    if(nameAttribute != null) {
      name = flowFile.getAttribute(nameAttribute);
    }
    if(name == null) {
      name = "Unnamed";
    }
    attributes.put("name", name);
    
    Entity individual = weaver.add(attributes, EntityType.INDIVIDUAL, id);
    
    // Attach to dataset
    datasetObjects.linkEntity(id, individual);

    Entity collection = weaver.collection();
    individual.linkEntity(RelationKeys.PROPERTIES, collection);


    weaver.close();
    
    session.transfer(flowFile, ORIGINAL);
  }
}
