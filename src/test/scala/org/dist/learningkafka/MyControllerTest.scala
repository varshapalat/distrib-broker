package org.dist.learningkafka

import org.I0Itec.zkclient.ZkClient
import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZKStringSerializer
import org.dist.simplekafka.util.ZkUtils.Broker

class MyControllerTest extends ZookeeperTestHarness {
  test("Should elect first server as controller") {
    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient1 = new MyZookeeperClient(zkClient1)
    val broker1 = new Broker(0, "10.10.10.10", 8000)
    myZookeeperClient1.registerBroker(broker1)

    val myController1 = new MyController(myZookeeperClient1, broker1.id)
    myController1.startup()

    TestUtils.waitUntilTrue(() => {
      myController1.currentLeader == broker1.id
    }, "Waiting for first broker to get elected")

    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient2 = new MyZookeeperClient(zkClient2)
    val broker2 = new Broker(1, "10.10.10.11", 8000)
    myZookeeperClient2.registerBroker(broker2)

    val myController2 = new MyController(myZookeeperClient2, broker2.id)
    myController2.startup()

    TestUtils.waitUntilTrue(() => {
      myController2.currentLeader == broker1.id
    }, "Waiting for first broker is still elected")

    zkClient1.close()

    TestUtils.waitUntilTrue(() => {
      myController2.currentLeader == broker2.id
    }, "Waiting for second broker to get elected")
  }
}
