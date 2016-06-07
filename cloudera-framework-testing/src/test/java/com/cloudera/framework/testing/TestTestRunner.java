package com.cloudera.framework.testing;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.framework.testing.server.CdhServer;
import com.cloudera.framework.testing.server.DfsServer;
import com.google.common.collect.ImmutableMap;
import com.googlecode.zohhak.api.Coercion;
import com.googlecode.zohhak.api.TestWith;

@RunWith(TestRunner.class)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestTestRunner implements TestConstants {

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
      Assert.assertEquals(1, COUNTER.incrementAndGet());
    }

    @Override
    public void clean() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Clean server class");
      }
      Assert.assertEquals(4, COUNTER.incrementAndGet());
    }

    @Override
    public void state() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("State server class");
      }
      Assert.assertEquals(4, COUNTER.decrementAndGet());
    }

    @Override
    public void stop() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stop server class");
      }
      Assert.assertEquals(0, COUNTER.decrementAndGet());
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
      Assert.assertEquals(3, COUNTER.incrementAndGet());
    }

    @Override
    public void clean() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Clean server method");
      }
      Assert.assertEquals(5, COUNTER.incrementAndGet());
    }

    @Override
    public void state() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("State server method");
      }
      Assert.assertEquals(3, COUNTER.decrementAndGet());
    }

    @Override
    public void stop() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stop server method");
      }
      Assert.assertEquals(2, COUNTER.decrementAndGet());
    }

  };

  @BeforeClass
  public static void beforeClass() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Before class");
    }
    Assert.assertEquals(2, COUNTER.incrementAndGet());
  }

  @Before
  public void before() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Before method");
    }
    Assert.assertEquals(6, COUNTER.incrementAndGet());
  }

  public static final TestMetaData testMetaData1 = TestMetaData.getInstance()
      .parameters(ImmutableMap.of("metadata", "1"));
  public static final TestMetaData testMetaData2 = TestMetaData.getInstance()
      .parameters(ImmutableMap.of("metadata", "2"));

  @TestWith({ "testMetaData1", "testMetaData2" })
  public void testCdhMetaData1(TestMetaData testMetaData) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test " + testMetaData.getParameters()[0]);
    }
    Assert.assertNotNull(testMetaData);
    Assert.assertEquals(6, COUNTER.get());
  }

  @TestWith({ "testMetaData1", "testMetaData2" })
  public void testCdhMetaData2(TestMetaData testMetaData) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Test " + testMetaData.getParameters()[0]);
    }
    Assert.assertNotNull(testMetaData);
    Assert.assertEquals(6, COUNTER.get());
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
    Assert.assertEquals(5, COUNTER.decrementAndGet());
  }

  @AfterClass
  public static void afterClass() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("After class");
    }
    Assert.assertEquals(1, COUNTER.decrementAndGet());
  }

  private static final AtomicInteger COUNTER = new AtomicInteger();

}
