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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"getweaverid"})
@CapabilityDescription("Looks for a flowfile attribute and fetches a weaver id. Route on Attribute with the new weaver id. Original content untouched.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetWeaverId extends AbstractProcessor {

  public static final PropertyDescriptor ATTRIBUTE = new PropertyDescriptor
          .Builder().name("Flow file attribute")
          .description("The name of the flowfile attribute to look for.")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  public static final Relationship ORIGINAL = new Relationship.Builder()
          .name("original")
          .description("This relationship is used to transfer the result to.")
          .build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {
      final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
      descriptors.add(ATTRIBUTE);
      this.descriptors = Collections.unmodifiableList(descriptors);

      final Set<Relationship> relationships = new HashSet<Relationship>();
      relationships.add(ORIGINAL);
      this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override
  public Set<Relationship> getRelationships() {
      return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
      return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {

  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    FlowFile flowFile = session.get();

    if ( flowFile == null ) {
      return;
    }

    String a = context.getProperty(ATTRIBUTE).getValue();
    String b = flowFile.getAttribute(a);

    if (b != null){
      if(b.equals("80")){
        flowFile = session.putAttribute(flowFile, "weaver_id", "90");
      }
    }

    //do some weaver stuff here

    session.transfer(flowFile, ORIGINAL);

  }


}
