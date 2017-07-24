package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.WeaverProcessor;
import com.weaverplatform.sdk.Weaver;
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, native, query, sparql, virtuoso"})
@CapabilityDescription("A native querying processor for Virtuoso through Weaver.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class NativeQuery extends WeaverProcessor {

  public static final Relationship RESULT_ROW = new Relationship.Builder()
          .name("result row")
          .description("All the found result rows are sent over this link (one by one).")
          .build();

  public static final Relationship ORIGINAL = new Relationship.Builder()
          .name("original")
          .description("Input for this processor will be transferred to this relationship.")
          .build();

  public static final PropertyDescriptor SELECT = new PropertyDescriptor
          .Builder().name("select")
          .description("Comma separated select vars.")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  public static final PropertyDescriptor QUERY = new PropertyDescriptor
          .Builder().name("query")
          .description("The SPARQL query (nifi expressions supported).")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(true)
          .build();


  @Override
  protected void init(final ProcessorInitializationContext context) {
    super.init(context);

    descriptors.add(SELECT);
    descriptors.add(QUERY);
    this.properties = Collections.unmodifiableList(descriptors);

    relationshipSet.add(RESULT_ROW);
    relationshipSet.add(ORIGINAL);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    Weaver weaver = getWeaver();

    FlowFile flowFile = session.get();

    // Compose query
    String query = "";

    // Add prefixed
    query += "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n";
    query += "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n";
    query += "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";
    query += "PREFIX  lib:  <http://ccy.information-backbone.org/library#>\n";
    query += "PREFIX  ins:  <http://ccy.information-backbone.org/instance#>\n\n";

    // Add attributes to query
    if (flowFile != null) {
      query += context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
    } else {
      query += context.getProperty(QUERY).evaluateAttributeExpressions().getValue();
    }

    // Get selects
    String select = context.getProperty(SELECT).getValue();
    ArrayList<String> selectVars = new ArrayList<>();
    Collections.addAll(selectVars, select.split(","));

    // Separator between every column in the row
    String separator = ";";

    // Execute query
    ArrayList<ArrayList<String>> result = weaver.channel.nativeQuery(new com.weaverplatform.sdk.json.request.NativeQuery(query, selectVars));

    for(ArrayList<String> row : result) {

      String resultRow = "";
      for(String value : row) {
        resultRow += value + separator;
      }
      resultRow = resultRow.substring(0, resultRow.length() - 1); // remove last separator

      InputStream in = new ByteArrayInputStream(resultRow.getBytes(StandardCharsets.UTF_8));
      FlowFile newFlowFile;
      if(flowFile != null) {
        newFlowFile = session.create(flowFile);
      } else {
        newFlowFile = session.create();
      }

      newFlowFile = session.importFrom(in, newFlowFile);
      session.transfer(newFlowFile, RESULT_ROW);
    }

    // Transfer original
    if(flowFile != null) {
      session.transfer(flowFile, ORIGINAL);
    }
  }
}