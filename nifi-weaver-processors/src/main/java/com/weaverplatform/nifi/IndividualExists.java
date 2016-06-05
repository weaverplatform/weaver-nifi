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

import com.weaverplatform.nifi.util.WeaverProperties;
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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, individual, exists"})
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class IndividualExists extends AbstractProcessor {
  
  public static final PropertyDescriptor WEAVER = new PropertyDescriptor
    .Builder().name("weaver_url")
    .description("weaver connection url i.e. weaver.connect(weaver_url)")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("id_attribute")
    .description("Look for the ID in a flowfile attribute")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final Relationship EXISTS = new Relationship.Builder()
          .name("exists")
          .description("Original flowfile if individual exists")
          .build();
  
  public static final Relationship NOT_EXISTS = new Relationship.Builder()
          .name("not exists")
          .description("Original flowfile if individual does not exists")
          .build();

  private List<PropertyDescriptor> properties;

  private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();

  private String weaverUrl;
  
  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(WEAVER);
    descriptors.add(ID_ATTRIBUTE);
    this.properties = Collections.unmodifiableList(descriptors);

    final Set<Relationship> set = new HashSet<>();
    set.add(EXISTS);
    set.add(NOT_EXISTS);
    this.relationships = new AtomicReference<>(set);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    FlowFile flowFile = session.get();

    if (flowFile == null) {
      return;
    }

    if(context.getProperty(WEAVER) != null) {
      weaverUrl = context.getProperty(WEAVER).getValue();
    }
    else {
      weaverUrl = NiFiProperties.getInstance().get(WeaverProperties.URL).toString(); 
    }
    
    String individual_id = context.getProperty(ID_ATTRIBUTE).getValue();
    
    getLogger().info("Individual ID is: " + individual_id);
    
    Weaver weaver = new Weaver();
    try {
      weaver.connect(new WeaverSocket(new URI(weaverUrl)));
    } catch (URISyntaxException e) {
      System.out.println(e.getMessage());
    }

    session.transfer(flowFile, EXISTS);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return relationships.get();
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }
}