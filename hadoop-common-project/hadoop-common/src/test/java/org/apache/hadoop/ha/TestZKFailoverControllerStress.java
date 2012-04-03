/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ha;

import java.io.File;
import java.util.Random;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.JMXEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Stress test for ZKFailoverController.
 * Starts multiple ZKFCs for dummy services, and then performs many automatic
 * failovers. While doing so, ensures that a fake "shared resource"
 * (simulating the shared edits dir) is only owned by one service at a time. 
 */
public class TestZKFailoverControllerStress extends ClientBase {
  
  private static final int STRESS_RUNTIME_SECS = 30;
  private static final int EXTRA_TIMEOUT_SECS = 10;
  
  private Configuration conf;
  private MiniZKFCCluster cluster;

  @Override
  public void setUp() throws Exception {
    // build.test.dir is used by zookeeper
    new File(System.getProperty("build.test.dir", "build")).mkdirs();
    super.setUp();
  }
  
  @Before
  public void setupConfAndServices() throws Exception {
    conf = new Configuration();
    conf.set(ZKFailoverController.ZK_QUORUM_KEY, hostPort);
    this.cluster = new MiniZKFCCluster(conf, getServer(serverFactory));
    cluster.start();
  }
  
  @After
  public void stopCluster() throws Exception {
    cluster.stop();
  }

  /**
   * ZK seems to have a bug when we muck with its sessions
   * behind its back, causing disconnects, etc. This bug
   * ends up leaving JMX beans around at the end of the test,
   * and ClientBase's teardown method will throw an exception
   * if it finds JMX beans leaked. So, clear them out there
   * to workaround the ZK bug. See ZOOKEEPER-1438.
   */
  @After
  public void clearZKJMX() throws Exception {
    Set<ObjectName> names = JMXEnv.ensureAll();
    for (ObjectName n : names) {
      JMXEnv.conn().unregisterMBean(n);
    }
  }

  /**
   * Simply fail back and forth between two services for the
   * configured amount of time, via expiring their ZK sessions.
   */
  @Test(timeout=(STRESS_RUNTIME_SECS + EXTRA_TIMEOUT_SECS) * 1000)
  public void testExpireBackAndForth() throws Exception {
    long st = System.currentTimeMillis();
    long runFor = STRESS_RUNTIME_SECS * 1000;

    int i = 0;
    while (System.currentTimeMillis() - st < runFor) {
      // flip flop the services back and forth
      int from = i % 2;
      int to = (i + 1) % 2;

      // Expire one service, it should fail over to the other
      LOG.info("Failing over via expiration from " + from + " to " + to);
      cluster.expireAndVerifyFailover(from, to);

      i++;
    }
  }
  
  /**
   * Randomly expire the ZK sessions of the two ZKFCs. This differs
   * from the above test in that it is not a controlled failover -
   * we just do random expirations and expect neither one to ever
   * generate fatal exceptions.
   */
  @Test(timeout=(STRESS_RUNTIME_SECS + EXTRA_TIMEOUT_SECS) * 1000)
  public void testRandomExpirations() throws Exception {
    long st = System.currentTimeMillis();
    long runFor = STRESS_RUNTIME_SECS * 1000;

    Random r = new Random();
    while (System.currentTimeMillis() - st < runFor) {
      cluster.getTestContext().checkException();
      int targetIdx = r.nextInt(2);
      ActiveStandbyElector target = cluster.getElector(targetIdx);
      long sessId = target.getZKSessionIdForTests();
      if (sessId != -1) {
        LOG.info(String.format("Expiring session %x for svc %d",
            sessId, targetIdx));
        getServer(serverFactory).closeSession(sessId);
      }
      Thread.sleep(r.nextInt(300));
    }
  }
  
  /**
   * Have the services fail their health checks half the time,
   * causing the master role to bounce back and forth in the
   * cluster. Meanwhile, causes ZK to disconnect clients every
   * 50ms, to trigger the retry code and failures to become active.
   */
  @Test(timeout=(STRESS_RUNTIME_SECS + EXTRA_TIMEOUT_SECS) * 1000)
  public void testRandomHealthAndDisconnects() throws Exception {
    long runFor = STRESS_RUNTIME_SECS * 1000;
    Mockito.doAnswer(new RandomlyThrow(0))
        .when(cluster.getService(0).proxy).monitorHealth();
    Mockito.doAnswer(new RandomlyThrow(1))
        .when(cluster.getService(1).proxy).monitorHealth();
    ActiveStandbyElector.NUM_RETRIES = 100;
    
    long st = System.currentTimeMillis();
    while (System.currentTimeMillis() - st < runFor) {
      cluster.getTestContext().checkException();
      serverFactory.closeAll();
      Thread.sleep(50);
    }
  }
  
  
  /**
   * Randomly throw an exception half the time the method is called
   */
  @SuppressWarnings("rawtypes")
  private static class RandomlyThrow implements Answer {
    private Random r = new Random();
    private final int svcIdx;
    public RandomlyThrow(int svcIdx) {
      this.svcIdx = svcIdx;
    }
    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      if (r.nextBoolean()) {
        LOG.info("Throwing an exception for svc " + svcIdx);
        throw new HealthCheckFailedException("random failure");
      }
      return invocation.callRealMethod();
    }
  }
}