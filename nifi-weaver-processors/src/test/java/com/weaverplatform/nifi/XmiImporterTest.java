package com.weaverplatform.nifi;

import com.weaverplatform.nifi.individual.XmiImporter;
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

public class XmiImporterTest {

  private TestRunner testRunner;

  @Before
  public void init() {
    testRunner = TestRunners.newTestRunner(XmiImporter.class);
  }

  @Test
  public void testProcessor() {

    try {
      String file = "InformatieBackboneModel.xml";
      byte[] contents = FileUtils.readFileToByteArray(new File(getClass().getClassLoader().getResource(file).getFile()));
      InputStream in = new ByteArrayInputStream(contents);
      InputStream cont = new ByteArrayInputStream(IOUtils.toByteArray(in));

      ProcessSession session = testRunner.getProcessSessionFactory().createSession();
      FlowFile f = session.create();
      f = session.importFrom(in, f);

      testRunner.setProperty(XmiImporter.WEAVER, "http://localhost:9487");
      testRunner.enqueue(cont);
      testRunner.run();

      List<MockFlowFile> listFlowFiles = testRunner.getFlowFilesForRelationship("original");
      MockFlowFile result = listFlowFiles.get(0);

//      result.assertAttributeExists("weaver_id");
//      String resultValue = result.getAttribute("weaver_id");
//
//      System.out.println(resultValue);

      //assertEquals(resultValue, "90");
    } catch(IOException e) {
      System.out.println("cannot open!!");
    }
  }
}