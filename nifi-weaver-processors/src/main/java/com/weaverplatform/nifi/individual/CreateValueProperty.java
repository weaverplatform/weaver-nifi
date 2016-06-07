/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.RelationKeys;
import com.weaverplatform.sdk.ShallowEntity;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"create, valueproperty, weaver"})
@CapabilityDescription("Creates a valueproperty object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateValueProperty extends IndividualProcessor {
  
  public static final PropertyDescriptor SUBJECT_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Subject attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor SUBJECT_STATIC = new PropertyDescriptor
    .Builder().name("Subject static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Predicate attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_STATIC = new PropertyDescriptor
    .Builder().name("Predicate static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor OBJECT_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Object attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor OBJECT_STATIC = new PropertyDescriptor
    .Builder().name("Object static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final Relationship ORIGINAL = new Relationship.Builder()
    .name("Original")
    .description("Original relationship to transfer content to.")
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
    this.properties = Collections.unmodifiableList(descriptors);
    
    relationshipSet.add(ORIGINAL);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    super.onTrigger(context, session);

    FlowFile flowFile = session.get();
    if ( flowFile == null ) {
      return;
    }

    String subject = null;
    String subjectAttributeValue = context.getProperty(SUBJECT_ATTRIBUTE).getValue();
    String subjectStaticValue    = context.getProperty(SUBJECT_STATIC).getValue();
    if(subjectAttributeValue != null) {
      subject = flowFile.getAttribute(subjectAttributeValue);
    } else if(subjectStaticValue != null) {
      subject = subjectStaticValue;
    }
    if(subject == null) {
      throw new ProcessException("No subject found for this Value Property.");
    }

    String predicate = null;
    String predicateAttributeValue = context.getProperty(PREDICATE_ATTRIBUTE).getValue();
    String predicateStaticValue    = context.getProperty(PREDICATE_STATIC).getValue();
    if(predicateAttributeValue != null) {
      predicate = flowFile.getAttribute(predicateAttributeValue);
    } else if(predicateStaticValue != null) {
      predicate = predicateStaticValue;
    }
    if(predicate == null) {
      throw new ProcessException("No predicate found for this Value Property.");
    }

    String object = null;
    String objectAttributeValue = context.getProperty(OBJECT_ATTRIBUTE).getValue();
    String objectStaticValue    = context.getProperty(OBJECT_STATIC).getValue();
    if(objectAttributeValue != null) {
      object = flowFile.getAttribute(objectAttributeValue);
    } else if(objectStaticValue != null) {
      object = objectStaticValue;
    }
    if(object == null) {
      throw new ProcessException("No object found for this Value Property.");
    }



    try {

      Entity parent = weaver.get(subject);


      Map<String, Object> entityAttributes = new HashMap<>();
      entityAttributes.put("predicate", predicate);
      entityAttributes.put("object", object);

      
      String id = idFromOptions(context, flowFile, true);
      Entity valueProperty = weaver.add(entityAttributes, EntityType.VALUE_PROPERTY, id);
      valueProperty.linkEntity(RelationKeys.SUBJECT, parent);
      ShallowEntity shallowCollection = parent.getRelations().get(RelationKeys.PROPERTIES);

      Entity aCollection = weaver.get(shallowCollection.getId());
      aCollection.linkEntity(valueProperty.getId(), valueProperty);


      
    } catch (IndexOutOfBoundsException e) {
      throw new ProcessException(e);
    } catch (NullPointerException e) {
      throw new ProcessException(e);
    }

    weaver.close();

    session.transfer(flowFile, ORIGINAL);

  }



}
