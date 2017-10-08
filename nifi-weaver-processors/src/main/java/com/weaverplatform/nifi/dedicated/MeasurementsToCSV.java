package com.weaverplatform.nifi.dedicated;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.common.primitives.Chars;
import com.weaverplatform.nifi.WeaverProcessor;
import com.weaverplatform.sdk.Weaver;
import org.apache.commons.io.FileUtils;
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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, virtuoso, measurements, csv"})
@CapabilityDescription("A native querying processor for Virtuoso through Weaver.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MeasurementsToCSV extends WeaverProcessor {

    public static final Relationship CSV = new Relationship.Builder()
            .name("CSV")
            .description("Result go over here by CSV.")
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Input for this processor will be transferred to this relationship.")
            .build();

    public static final PropertyDescriptor MEASUREMENT_ID = new PropertyDescriptor
            .Builder().name("MeasurementID")
            .description("The ID of the measurement to create the CSV for")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();


    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        descriptors.add(MEASUREMENT_ID);
        this.properties = Collections.unmodifiableList(descriptors);

        relationshipSet.add(CSV);
        relationshipSet.add(ORIGINAL);
        this.relationships = new AtomicReference<>(relationshipSet);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Weaver weaver = getWeaver();

        FlowFile flowFile = session.get();

        // Compose query
        String query = "";
        try {
            query = Resources.toString(Resources.getResource("measurement-query.spq"), Charsets.UTF_8);
        } catch (IOException e) {
            throw new ProcessException(e);
        }

        // Get measurementId
        String measurementId = "";
        if (flowFile != null) {
            measurementId = context.getProperty(MEASUREMENT_ID).evaluateAttributeExpressions(flowFile).getValue();
        } else {
            measurementId = context.getProperty(MEASUREMENT_ID).evaluateAttributeExpressions().getValue();
        }

        // Fill in measurementId
        query = query.replace("#{MEASUREMENT_ID}", measurementId);

        // Save measurements here
        Map<String, Map<String, String>> measurements = new LinkedHashMap<>();

        // Save all used keys here as headers
        Set<String> headers = new LinkedHashSet<>();
        headers.add("Mslink");
        headers.add("SubTrace");
        headers.add("TypeOnderdeel");

        // Execute query
        ArrayList<ArrayList<String>> result = weaver.channel.nativeQuery(new com.weaverplatform.sdk.json.request.NativeQuery(query, new ArrayList<>()));

        // Process results
        for(ArrayList<String> row : result) {

            String mslink        = row.get(0);
            String subtrace      = row.get(1);
            String typeonderdeel = row.get(2);
            String label         = row.get(3);
            String value         = row.get(4);

            if(!measurements.containsKey(mslink)){
                measurements.put(mslink, new LinkedHashMap<>());
            }

            Map m = measurements.get(mslink);
            m.put("Mslink", mslink);
            m.put("SubTrace", subtrace);
            m.put("TypeOnderdeel", typeonderdeel);

            String propertyKey = label.replace("lib:","");
            m.put(propertyKey, value);

            // Save key for future reference to fill empty cells
            headers.add(propertyKey);
        }


        // Convert to array
        List<Map<String, String>> measurementsList = new ArrayList<>(measurements.values());

        // Write CSV headers
        String csv = "";
        for (String header : headers) {
            csv += header + ";";
        }

        csv = csv.substring(0, csv.length() - 1); // remove last separator
        csv += "\n";

        // Write CSV content
        for (Map measurement : measurementsList) {

            for (String header : headers) {
                if (measurement.containsKey(header)){

                    // Remove blank
                    String value = measurement.get(header).toString().replace("BLANK", "");
                    csv += value + ";";
                }
                else {
                    csv += ";";
                }
            }

            csv = csv.substring(0, csv.length() - 1); // remove last separator
            csv += "\n";
        }

        InputStream in = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
        FlowFile newFlowFile;
        if(flowFile != null) {
            newFlowFile = session.create(flowFile);
        } else {
            newFlowFile = session.create();
        }

        newFlowFile = session.importFrom(in, newFlowFile);
        session.transfer(newFlowFile, CSV);

        // Transfer original
        if(flowFile != null) {
            session.transfer(flowFile, ORIGINAL);
        }
    }
}