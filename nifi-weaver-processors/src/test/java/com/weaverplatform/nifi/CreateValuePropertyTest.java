package com.weaverplatform.nifi;

import com.weaverplatform.nifi.individual.CreateValueProperty;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class CreateValuePropertyTest {

  private TestRunner testRunner;

  @Before
  public void init() {
    testRunner = TestRunners.newTestRunner(CreateValueProperty.class);
  }

  @Test
  public void testOnTrigger() {
    
    try {

      //random info and simulate flowfile (with attributes) passed through to this processor in early state
      String file = "line.txt";
      byte[] contents = FileUtils.readFileToByteArray(new File(getClass().getClassLoader().getResource(file).getFile()));
      InputStream in = new ByteArrayInputStream(contents);
      InputStream cont = new ByteArrayInputStream(IOUtils.toByteArray(in));
      ProcessSession session = testRunner.getProcessSessionFactory().createSession();
      FlowFile f = session.create();
      f = session.importFrom(cont, f);
      f = session.putAttribute(f, "id", "816ee370-4274-e211-a3a8-b8ac6f902f00");
      f = session.putAttribute(f, "name", "(AB CT1-N-06) Snelle doorsteek A10");


      //from nifi-envi the user specifies this dynamic attribute, which to look for on the flowfile later
      // Add properties (required)
      testRunner.setProperty(CreateValueProperty.WEAVER, "http://localhost:9487");
      testRunner.setProperty(CreateValueProperty.SUBJECT_ATTRIBUTE, "id");
      testRunner.setProperty(CreateValueProperty.PREDICATE_STATIC, "rdf:label");
      testRunner.setProperty(CreateValueProperty.OBJECT_ATTRIBUTE, "name");

      // Add the flowfile to the runner
      testRunner.enqueue(f);

      // Run the enqueued content, it also takes an int = number of contents queued
      testRunner.run();

      //get original flowfile contents
      List<MockFlowFile> results = testRunner.getFlowFilesForRelationship("original");
      MockFlowFile result = results.get(0);
      String resultValue = new String(testRunner.getContentAsByteArray(result));
      System.out.println(resultValue);

    } catch (IOException e) {
      System.out.println("FOUT!!");
      System.out.println(e.getStackTrace());
    }
  }
}