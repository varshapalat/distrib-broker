package org.dist.learningkafka

import org.dist.common.ZookeeperTestHarness
import org.dist.simplekafka.util.ZkUtils.Broker

class MyZookeeperClientTest extends ZookeeperTestHarness {

  test("should register brokers with zookeeper") {
    val myZookeeperClient = new MyZookeeperClient(zkClient);
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8000))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.12", 8000))

    assert(3 == myZookeeperClient.getAllBrokers().size)
    assertResult(Broker(0, "10.10.10.10", 8000))( myZookeeperClient.getBrokerInfo(0))
  }
}
