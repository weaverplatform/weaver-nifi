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
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.websocket.WeaverSocket;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, create, individual"})
@CapabilityDescription("Create individual object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateIndividual extends IndividualProcessor {





  public static final PropertyDescriptor INDIVIDUAL_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("individual_attribute")
    .description("look for a flowfile attribute")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
  
  public static final PropertyDescriptor NAME_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("name_attribute")
    .description("look for a flowfile attribute to set the name")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor INDIVIDUAL_STATIC = new PropertyDescriptor
    .Builder().name("individual_static")
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
    
    //position 0
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(WEAVER);
    descriptors.add(DATASET);
    descriptors.add(INDIVIDUAL_ATTRIBUTE);
    descriptors.add(INDIVIDUAL_STATIC);
    descriptors.add(NAME_ATTRIBUTE);
    this.properties = Collections.unmodifiableList(descriptors);

    final Set<Relationship> set = new HashSet<>();
    set.add(ORIGINAL);
    this.relationships = new AtomicReference<>(set);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    
    super.onTrigger(context, session);

    FlowFile flowFile = session.get();

    if ( flowFile == null ) {
          return;
    }
        
    
    String individual_id = get(context, flowFile, INDIVIDUAL_ATTRIBUTE, INDIVIDUAL_STATIC);

    Weaver weaver = new Weaver();
    try {
      weaver.connect(new WeaverSocket(new URI(weaverUrl)));
    } catch (URISyntaxException e) {
      System.out.println(e.getMessage());
    }
    
    // Get Dataset Entity
    Entity dataset = weaver.get(datasetId);
    
    // create entity by user attribute
    // individual_id = UUID.randomUUID().toString();
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("name", "Unnamed");

    // Check for name
    String nameAttribute = context.getProperty(NAME_ATTRIBUTE).getValue();
    if(nameAttribute != null) {
      String name = flowFile.getAttribute(nameAttribute);
      if (name != null) {
        attributes.put("name", name);
      }
    }
    
    Entity parentObject = weaver.add(attributes, EntityType.INDIVIDUAL, individual_id);
    
    // Attach to dataset
    Entity objects = weaver.get(dataset.getRelations().get("objects").getId());
    objects.linkEntity(individual_id, parentObject);
    
    //object
    Entity aCollection = weaver.add(new HashMap<String, Object>(), EntityType.COLLECTION, weaver.createRandomUUID());

    //predicate
    parentObject.linkEntity(RelationKeys.PROPERTIES, aCollection);

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
