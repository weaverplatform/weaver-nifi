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

        // Execute query
        ArrayList<ArrayList<String>> result = weaver.channel.nativeQuery(new com.weaverplatform.sdk.json.request.NativeQuery(query, new ArrayList<>()));

        // Process results
        for(ArrayList<String> row : result) {

            String mslink = row.get(0);
            String label  = row.get(1);
            String value  = row.get(2);

            if(!measurements.containsKey(mslink)){
                measurements.put(mslink, new LinkedHashMap<>());
            }

            Map m = measurements.get(mslink);
            m.put("Mslink", mslink);

            String propertyKey = label.replace("lib:","");
            m.put(propertyKey, value);
        }

        // Header name mapping and order
        Map<String, String> mappedHeaders = new LinkedHashMap<>();
        mappedHeaders.put("Mslink","Mslink");
        mappedHeaders.put("Subtracé","Subtrace");
        mappedHeaders.put("Vaknummer","NummerHMV");
        mappedHeaders.put("Afstandvan","Afstandvan");
        mappedHeaders.put("Afstandtot","Afstandtot");
        mappedHeaders.put("Omschrijvingvan","Omschrijvingvan");
        mappedHeaders.put("Omschrijvingtot","Omschrijvingtot");
        mappedHeaders.put("Type onderdeel","TypeOnderdeel");
        mappedHeaders.put("Verharding","TypeVerharding");
        mappedHeaders.put("Situering","SitueringBPS");
        mappedHeaders.put("Lengte","LengteHMV");
        mappedHeaders.put("Oppervlakte","OppervlakteHMV");
        mappedHeaders.put("Datum spoorvorming","DatumMetingSpoorvorming");
        mappedHeaders.put("Spoorvorming links","ResultaatSpoorvormingLinks");
        mappedHeaders.put("Spoorvorming rechts","ResultaatSpoorvormingRechts");
        mappedHeaders.put("Datum langsonvlakheid","DatumMetingLangsonvlakheid");
        mappedHeaders.put("Langsonvlakheid","ResultaatLangsonvlakheid");
        mappedHeaders.put("Datum stroefheid","DatumMetingStroefheid");
        mappedHeaders.put("Stroefheid","ResultaatStroefheid");
        mappedHeaders.put("Stroefheid_deklaagtype","DeklaagTypeStroefheid");
        mappedHeaders.put("Stroefheid_meetsnelheid","MeetsnelheidStroefheid");
        mappedHeaders.put("Datum dwarshelling","DatumMetingDwarshelling");
        mappedHeaders.put("Dwarshelling","ResultaatDwarshelling");
        mappedHeaders.put("Datum langsonvlakheid overgang","DatumMetingLangsonvlakheid_overgang");
        mappedHeaders.put("Langsonvlakheid overgang","ResultaatLangsonvlakheid_overgang");
        mappedHeaders.put("Inspectiedatum","DatumInspectie");
        mappedHeaders.put("Langscheuren/Craquelé","BeoordelingLangsscheuren");
        mappedHeaders.put("Dwarsscheuren","BeoordelingDwarsscheuren");
        mappedHeaders.put("Rafeling","BeoordelingRafeling");

        // Convert to array
        List<Map<String, String>> measurementsList = new ArrayList<>(measurements.values());

        // Write CSV headers
        String csv = "";
        for (String header : mappedHeaders.keySet()) {
            csv += header + ";";
        }

        csv = csv.substring(0, csv.length() - 1); // remove last separator
        csv += "\n";

        // Write CSV content
        for (Map measurement : measurementsList) {

            for (String header : mappedHeaders.keySet()) {
                if (measurement.containsKey(mappedHeaders.get(header))){

                    // Remove blank
                    String value = measurement.get(mappedHeaders.get(header)).toString().replace("BLANK", "");
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