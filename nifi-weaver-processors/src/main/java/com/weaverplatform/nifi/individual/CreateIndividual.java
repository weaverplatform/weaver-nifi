package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
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

@Tags({"weaver, create, individual"})
@CapabilityDescription("Create individual object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateIndividual extends DatasetProcessor {
  
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

  public static final PropertyDescriptor IS_ADDIFYING = new PropertyDescriptor
      .Builder().name("Is Addifying?")
      .description("If this attribute is set, the object or subject entity will be " +
          "created if it does not already exist. (leave this field empty to disallow " +
          "this behaviour)")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();

  private Entity individual;
  private Entity entityProperties;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context); 
    
    
    descriptors.add(NAME_ATTRIBUTE);
    descriptors.add(NAME_STATIC);
    descriptors.add(IS_ADDIFYING);
    this.properties = Collections.unmodifiableList(descriptors);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ProcessorLog log = this.getLogger();
    
    super.onTrigger(context, session);
    Weaver weaver = getWeaver();

    String id = idFromOptions(context, flowFile, true);
    String source = getSource(context, flowFile);

    // Should we be prepared for the possibility that this entity has already been created.
    boolean isAddifying = context.getProperty(IS_ADDIFYING).isSet();

    // Create without checking for entities prior existence
    if(!isAddifying) {

      Map<String, Object> attributes = new HashMap<>();
      attributes.put("source", source);

      createIndividual(id, attributes);

      // Attach to dataset
      datasetObjects.linkEntity(id, individual);

    // Check to see whether it exists before creation
    } else {
      individual = weaver.get(id);

      // It already exists, check it's attributes.
      if(!EntityType.INDIVIDUAL.equals(individual.getType())) {
        
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("source", source);

        createIndividual(id, attributes);

        // Attach to dataset
        datasetObjects.linkEntity(id, individual);

      }

    }

    session.transfer(flowFile, ORIGINAL);
  }

  private void createIndividual(String id, Map<String, Object> attributes) {
    Weaver weaver = getWeaver();

    individual = weaver.add(attributes, EntityType.INDIVIDUAL, id);

    entityProperties = weaver.collection();
    individual.linkEntity(RelationKeys.PROPERTIES, entityProperties);
  }
}
