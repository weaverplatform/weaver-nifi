package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.util.LockRegistry;
import com.weaverplatform.sdk.*;
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
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"create, valueproperty, weaver"})
@CapabilityDescription("Creates a valueproperty object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateValueProperty extends PropertyProcessor {
  
  public static final PropertyDescriptor SUBJECT_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Subject Attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor SUBJECT_STATIC = new PropertyDescriptor
    .Builder().name("Subject Static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Predicate Attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_STATIC = new PropertyDescriptor
    .Builder().name("Predicate Static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor OBJECT_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Object Attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor OBJECT_STATIC = new PropertyDescriptor
    .Builder().name("Object Static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor IS_UPDATING = new PropertyDescriptor
      .Builder().name("Updating")
      .description("Optional, default true. If is true it will check if the " +
          "property already exists, and only update the value if this new " +
          "value is new. If is false, create new property regardless.")
      .required(false)
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .build();

  public static final PropertyDescriptor PREVENT_DUPLICATION = new PropertyDescriptor
      .Builder().name("Prevent duplication")
      .description("Optional, default true. Do not create a property if it is " +
          "already existing.")
      .required(false)
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .build();


  @Override
  protected void init(final ProcessorInitializationContext context) {

    super.init(context);

    descriptors.add(SUBJECT_ATTRIBUTE);
    descriptors.add(SUBJECT_STATIC);
    descriptors.add(PREDICATE_ATTRIBUTE);
    descriptors.add(PREDICATE_STATIC);
    descriptors.add(OBJECT_ATTRIBUTE);
    descriptors.add(OBJECT_STATIC);
    descriptors.add(IS_UPDATING);
    descriptors.add(PREVENT_DUPLICATION);
    this.properties = Collections.unmodifiableList(descriptors);
    

    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    final ProcessorLog log = this.getLogger();

    Weaver weaver = getWeaver();

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }

    String id = idFromOptions(context, flowFile, true);
    String source = getSource(context, flowFile);

    String subject = valueFromOptions(context, flowFile, SUBJECT_ATTRIBUTE, SUBJECT_STATIC, null);
    String predicate = valueFromOptions(context, flowFile, PREDICATE_ATTRIBUTE, PREDICATE_STATIC, null);
    String object = valueFromOptions(context, flowFile, OBJECT_ATTRIBUTE, OBJECT_STATIC, null);

    if(subject.equals("") || object.equals("") || predicate.equals("")
        || subject == null || object == null || predicate == null
        || subject.contains(" ") || predicate.contains(" ")) {
      //Write flowfile to error heap, and send it through the flow without any other processing

      new FlowErrorCatcher(context, session, this.getIdentifier()).dump(flowFile);

      session.transfer(flowFile, ORIGINAL);
      //log.warn("Subject ("+(subject.equals("") ? 'X' : "") + "), object ("+(object.equals("") ? 'X' : "") + "), or predicate ("+(predicate.equals("") ? 'X' : "") + ") was empty");
      return;
    }

    Entity individual;
    try {
      individual = weaver.get(subject, new ReadPayload.Opts(1));
    }
    catch (EntityNotFoundException ex){
      throw new ProcessException("CreateValueProperty could not find subject ID " + subject);

    }


    boolean isUpdating = !context.getProperty(IS_UPDATING).isSet() || context.getProperty(IS_UPDATING).asBoolean();
    boolean preventDuplication =  !context.getProperty(PREVENT_DUPLICATION).isSet() || context.getProperty(PREVENT_DUPLICATION).asBoolean();
    
    if(preventDuplication || isUpdating) {
      
      Map<String,Entity> existingProperties = getProperty(weaver, individual, predicate);
      
      if(existingProperties != null) {

        boolean exactSameObject = existingProperties.containsKey(object);
        
        if(!exactSameObject) {
          createNewProperty(weaver,individual, id, predicate, object, source);
        }
      }
      else {
        String propertyHash = individual.getId()+predicate+object+source;
        try {
          if(!LockRegistry.request("created", propertyHash)) {
            createNewProperty(weaver, individual, id, predicate, object, source);
            LockRegistry.release("created", propertyHash);
          }
        } catch (InterruptedException e) {
          throw new ProcessException(e);
        }
      }
    }
    else {
      createNewProperty(weaver,individual, id, predicate, object, source);
    }
    

    if(context.getProperty(ATTRIBUTE_NAME_FOR_ID).isSet()) {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, id);
    }
    session.transfer(flowFile, ORIGINAL);
  }

  
  private void createNewProperty(Weaver weaver, Entity individual, String id, String predicate, String object, String source) {
    ConcurrentMap<String, ShallowEntity> relations = new ConcurrentHashMap<>();
    relations.put("subject", individual.toShallowEntity());
    relations.put("predicate", new ShallowEntity(predicate, "$PREDICATE"));

    ConcurrentMap<String, String> entityAttributes = new ConcurrentHashMap<>();
    entityAttributes.put("object", object);
    entityAttributes.put("source", source);

    Entity valueProperty = weaver.add(entityAttributes, EntityType.VALUE_PROPERTY, id, relations);
    ShallowEntity properties = individual.getRelations().get("properties");

    Entity propertiesEntity = weaver.get(properties.getId());
    propertiesEntity.linkEntity(valueProperty.getId(), valueProperty.toShallowEntity());
  }
}