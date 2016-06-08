package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, xmiImporter"})
@CapabilityDescription("An XMI importer which communicates with the weaver-sdk-java.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class XmiImporter extends IndividualProcessor {

  String datasetId;
  Entity dataset;
  Entity datasetObjects;

  public static final PropertyDescriptor DATASET = new PropertyDescriptor
    .Builder().name("Dataset ID")
    .description("Dataset ID to add individuals to.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final Relationship ORIGINAL = new Relationship.Builder()
    .name("Original Content")
    .description("Relationship to send original content to to.")
    .build();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context);

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


    session.read(flowFile, new InputStreamCallback() {

      @Override
      public void process(InputStream inputStream) throws IOException {

        // TODO: call xmi importer

      }
    });
    
    weaver.close();

    session.transfer(flowFile, ORIGINAL);
  }
}