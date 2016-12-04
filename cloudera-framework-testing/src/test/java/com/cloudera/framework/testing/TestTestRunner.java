package com.cloudera.framework.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.atomic.AtomicInteger;

import com.cloudera.framework.testing.server.CdhServer;
import com.cloudera.framework.testing.server.DfsServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(TestRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class TestTestRunner implements TestConstants {

  public static final TestMetaData testMetaData1 = TestMetaData.getInstance().parameters(ImmutableMap.of("metadata", "1"));
  public static final TestMetaData testMetaData2 = TestMetaData.getInstance().parameters(ImmutableMap.of("metadata", "2"));
  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static Logger LOG = LoggerFactory.getLogger(DfsServer.class);
  @ClassRule
  public static CdhServer cdhServerClass = new CdhServer(null) {

    @Override
    public int getIndex() {
      return 100;
    }

    @Override
    public void start() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Start server class");
      }
      assertEquals(1, COUNTER.incrementAndGet());
    }

    @Override
    public void clean() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Clean server class");
      }
      assertEquals(4, COUNTER.incrementAndGet());
    }

    @Override
    public void state() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("State server class");
      }
      assertEquals(4, COUNTER.decrementAndGet());
    }

    @Override
    public void stop() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stop server class");
      }
      assertEquals(0, COUNTER.decrementAndGet());
    }

  };
  @Rule
  public CdhServer cdhServer = new CdhServer(null) {

    @Override
    public int getIndex() {
      return 10;
    }

    @Override
    public void start() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Start server method");
      }
      assertEquals(3, COUNTER.incrementAndGet());
    }

    @Override
    public void clean() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Clean server method");
      }
      assertEquals(5, COUNTER.incrementAndGet());
    }

    @Override
    public void state() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("State server method");
      }
      assertEquals(3, COUNTER.decrementAndGet());
    }

    @Override
    public void stop() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stop server method");
      }
      assertEquals(2, COUNTER.decrementAndGet());
    }

  };

  @BeforeClass
  public static void beforeClass() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Before class");
    }
    assertEquals(2, COUNTER.incrementAndGet());
  }

  @AfterClass
  public static void afterClass() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("After class");
    }
    assertEquals(1, COUNTER.decrementAndGet());
  }

  @Before
  public void before() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Before method");
    }
    assertEquals(6, COUNTER.incrementAndGet());
  }

  @TestWith({"testMetaData1", "testMetaData2"})
  public void testCdhMetaData1(TestMetaData testMetaData) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test " + testMetaData.getParameters()[0]);
    }
    assertNotNull(testMetaData);
    assertEquals(6, COUNTER.get());
  }

  @TestWith({"testMetaData1", "testMetaData2"})
  public void testCdhMetaData2(TestMetaData testMetaData) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test " + testMetaData.getParameters()[0]);
    }
    assertNotNull(testMetaData);
    assertEquals(6, COUNTER.get());
  }

  @Coercion
  public TestMetaData toCdhMetaData(String field) {
    return TestRunner.toCdhMetaData(this, field);
  }

  @After
  public void after() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("After method");
    }
    assertEquals(5, COUNTER.decrementAndGet());
  }

}
