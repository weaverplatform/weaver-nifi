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

import com.jcabi.xml.XML;
import com.jcabi.xml.XMLDocument;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.websocket.WeaverSocket;
import org.apache.commons.io.IOUtils;
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
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

@Tags({"weaver, xmiImporter"})
@CapabilityDescription("an xmi importer which communicates with the weaver-sdk-java.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class XmiImporter extends IndividualProcessor {

  public static final Relationship ORIGINAL = new Relationship.Builder()
    .name("original")
    .description("This relationship is used to transfer the result to.")
    .build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {

    this.descriptors = Collections.unmodifiableList(descriptors);


    relationshipSet.add(ORIGINAL);
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

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    super.onTrigger(context, session);
    FlowFile flowFile = session.get();

    if ( flowFile == null ) {
      return;
    }


    session.read(flowFile, new InputStreamCallback() {

      @Override
      public void process(InputStream isIn) throws IOException {

        try {

          String contents = IOUtils.toString(isIn);
          contents = contents.replaceAll("UML:", "UML.");

          XML xmlNode = new XMLDocument(contents);


          List<XML> list = xmlNode.nodes("//XMI.content/UML.Model/UML.Namespace.ownedElement/UML.Package/UML.Namespace.ownedElement/UML.Package/UML.Namespace.ownedElement/UML.Class");

          System.out.println(list.size()); //47 classes found

          Weaver weaver = new Weaver();
          String weaverURI = context.getProperty(WEAVER).getValue();
          weaver.connect(new WeaverSocket(new URI(weaverURI)));

          for (XML item : list){
            org.w3c.dom.Node xmiIdNode = item.node().getAttributes().getNamedItem("xmi.id");
            String xmiID = xmiIdNode.getTextContent();
            xmiID = xmiID.replace("EAID_", "");
            xmiID = xmiID.replace("_", "-");
            xmiID = xmiID.toLowerCase();
            System.out.println(xmiID);

            //make weaver things
            //Entity entity = weaver.add(new HashMap<>(), EntityType.INDIVIDUAL, xmiID);
          }

        } catch(IOException e){
          System.out.println("w00t");// + e.getMessage());
        }catch(IllegalArgumentException e){
          System.out.println("is xml niet geldig 1?");
          System.out.println(e.getCause());
          System.out.println(e.getMessage());
        }catch(IndexOutOfBoundsException e){
          System.out.println("bah! de node waar gezocht naar moet worden is niet gevonden!");
        } catch (URISyntaxException e) {
          System.out.println(e.getMessage());
        }

      }
    });

    session.transfer(flowFile, ORIGINAL);

  }


}

