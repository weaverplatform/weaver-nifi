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
package com.weaverplatform.nifi;

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.RelationKeys;
import com.weaverplatform.sdk.Weaver;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"create, valueproperty, weaver"})
@CapabilityDescription("Creates a valueproperty object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateValueProperty extends AbstractProcessor{

  public static final PropertyDescriptor WEAVER = new PropertyDescriptor
          .Builder().name("weaver_url")
          .description("weaver connection url i.e. weaver.connect(weaver_url)")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  public static final PropertyDescriptor SUBJECT_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("subject_attribute")
    .description("look for the flowfile attribute")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor SUBJECT_STATIC = new PropertyDescriptor
    .Builder().name("subject_static")
    .description("if there is no flowfile attribute, use static value")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("predicate_attribute")
    .description("look for the flowfile attribute")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_STATIC = new PropertyDescriptor
    .Builder().name("predicate_static")
    .description("if there is no flowfile attribute, use static value")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor OBJECT_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("object_attribute")
    .description("look for the flowfile attribute")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor OBJECT_STATIC = new PropertyDescriptor
    .Builder().name("object_static")
    .description("if there is no flowfile attribute, use static value")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final Relationship ORIGINAL = new Relationship.Builder()
    .name("original")
    .description("Original relationship to transfer content to")
    .build();

  private List<PropertyDescriptor> properties;

  private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();

  @Override
  protected void init(final ProcessorInitializationContext context) {
      final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
      descriptors.add(WEAVER);
      descriptors.add(SUBJECT_ATTRIBUTE);
      descriptors.add(SUBJECT_STATIC);
      descriptors.add(PREDICATE_ATTRIBUTE);
      descriptors.add(PREDICATE_STATIC);
      descriptors.add(OBJECT_ATTRIBUTE);
      descriptors.add(OBJECT_STATIC);
      this.properties = Collections.unmodifiableList(descriptors);

      final Set<Relationship> set = new HashSet<Relationship>();
      set.add(ORIGINAL);
      this.relationships = new AtomicReference<>(set);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    FlowFile flowFile = session.get();

    if ( flowFile == null ) {
      return;
    }

    String subject = get(context, flowFile, SUBJECT_ATTRIBUTE, SUBJECT_STATIC);
    String predicate = get(context, flowFile, PREDICATE_ATTRIBUTE, PREDICATE_STATIC);
    String object = get(context, flowFile, OBJECT_ATTRIBUTE, OBJECT_STATIC);

    Weaver weaver = new Weaver();
    String weaverUrl = context.getProperty(WEAVER).getValue();
    weaver.connect(weaverUrl);

    try {

      Entity parent = weaver.get(subject);

      //value of object
      //create an entity attributes-list
      Map<String, Object> entityAttributes = new HashMap<>();
      entityAttributes.put("predicate", predicate);
      entityAttributes.put("object", object);

      //System.out.println("....attributes { " + firstAttributeKey + " : " + firstAttributeValue + ", " + predicateKey + " : " + predicateValue + "}");

      //object
      Entity child = weaver.add(entityAttributes, EntityType.VALUE_PROPERTY, weaver.createRandomUUID());
      //we link the parents as subject
      child.linkEntity(RelationKeys.SUBJECT, parent);

      //get the collection object from parent
      Entity aCollection = parent.getRelations().get(RelationKeys.PROPERTIES);

      //predicate
      aCollection.linkEntity(child.getId(), child);

    }catch (IndexOutOfBoundsException e) {
      System.out.println("de node waar naar gezocht moet worden is niet gevonden!");
    }catch (NullPointerException e) {
      System.out.println("connection error and/or failed to retrieve parent object");
    }

    session.transfer(flowFile, ORIGINAL);

  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships.get();
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }

  public boolean isEmpty(PropertyValue propertyValue){
    String value = propertyValue.getValue();
    if(value == null || value.length() == 0){
      return true;
    }
    return false;
  }

  public String get (PropertyValue propertyValue){
    return propertyValue.getValue();
  }

  public PropertyValue get (final ProcessContext c, PropertyDescriptor p){
    return c.getProperty(p);
  }

  public String get (final ProcessContext c, FlowFile f, PropertyDescriptor a, PropertyDescriptor b){

    boolean useAttribute = !isEmpty(get(c, a));
    boolean useStatic = !isEmpty(get(c, b));

    if( (useAttribute && useStatic) || (!useAttribute && !useStatic) ){
      throw new RuntimeException("only " + a.getName() +" or "+b.getName()+" must be set");
    }

    if(useAttribute){
      return f.getAttribute(get(get(c, a)));
    }else{
      return get(get(c, b));
    }
  }

}
