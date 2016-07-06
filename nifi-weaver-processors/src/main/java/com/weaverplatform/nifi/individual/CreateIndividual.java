package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.*;
import com.weaverplatform.sdk.json.request.UpdateEntityAttribute;
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
public class CreateIndividual extends FlowFileProcessor {
  
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

  public static final PropertyDescriptor NAME_PREFIX = new PropertyDescriptor
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
  private Entity propertiesCollection;

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


    String datasetId = NiFiProperties.getInstance().get(WeaverProperties.DATASET).toString();

    Entity dataset = weaver.get(datasetId);
    Entity datasetObjects = weaver.get(dataset.getRelations().get("objects").getId());

    String id = idFromOptions(context, flowFile, true);
    String name = getName(context);
    String source = getSource(context, flowFile);

    // Should we be prepared for the possibility that this entity has already been created.
    boolean isAddifying = context.getProperty(IS_ADDIFYING).isSet();

    // Create without checking for entities prior existence
    if(!isAddifying) {

      Map<String, String> attributes = new HashMap<>();
      attributes.put("name", name);
      attributes.put("source", source);

      createIndividual(id, attributes);

      // Attach to dataset
      datasetObjects.linkEntity(id, individual);

    // Check to see whether it exists before creation
    } else {
      individual = weaver.get(id);

      // It doesn't exist yet
      if (!EntityType.INDIVIDUAL.equals(individual.getType())) {

        Map<String, String> attributes = new HashMap<>();
        attributes.put("name", name);
        attributes.put("source", source);

        createIndividual(id, attributes);

        // Attach to dataset
        datasetObjects.linkEntity(id, individual);

      // It exists, see what to update
      } else {

        if (!"".equals(name)) {

          // Check if name attribute is set
          if (!individual.getAttributes().containsKey("name") || !name.equals(individual.getAttributes().get("name"))) {

            weaver.updateEntityAttribute(new UpdateEntityAttribute(new ShallowEntity(individual.getId(), individual.getType()), "name", new ShallowValue(name, "")));
            weaver.updateEntityAttribute(new UpdateEntityAttribute(new ShallowEntity(individual.getId(), individual.getType()), "source", new ShallowValue(source, "")));

          }
        }
      }
    }
    if(context.getProperty(ATTRIBUTE_NAME_FOR_ID).isSet()) {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, id);
    }
    session.transfer(flowFile, ORIGINAL);
  }

  private void createIndividual(String id, Map<String, String> attributes) {
    Weaver weaver = getWeaver();

    individual = weaver.add(attributes, EntityType.INDIVIDUAL, id);
    propertiesCollection = weaver.collection();
    individual.linkEntity(RelationKeys.PROPERTIES, propertiesCollection);
  }

  private String getName(ProcessContext context) {
    String name = valueFromOptions(context, flowFile, NAME_ATTRIBUTE, NAME_STATIC, "Unnamed");

    // Check for prefix
    if(context.getProperty(NAME_PREFIX).isSet()) {
      name = context.getProperty(NAME_PREFIX).getValue() + name;
    }
    return name;
  }
}
