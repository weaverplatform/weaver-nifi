package com.weaverplatform.nifi.individual;

import com.weaverplatform.importer.xmi.ImportXmi;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, xmiImporter"})
@CapabilityDescription("An XMI importer which communicates with the weaver-sdk-java.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class XmiImporter extends DatasetProcessor {

  @Override
  protected void init(final ProcessorInitializationContext context) {

    super.init(context);

    this.properties = Collections.unmodifiableList(descriptors);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    super.onTrigger(context, session);

      session.read(flowFile, new InputStreamCallback() {

      @Override
      public void process(InputStream inputStream) throws IOException {

        ImportXmi importXmi = new ImportXmi(weaverUrl, datasetId);
        importXmi.readFromInputStream(inputStream);
        importXmi.run();

      }
    });

    weaver.close();

    session.transfer(flowFile, ORIGINAL);
  }
}