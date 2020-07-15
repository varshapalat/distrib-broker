package org.dist.learningkafka

import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.dist.simplekafka.common.Logging
import org.dist.simplekafka.util.ZkUtils.Broker

class MyControllerChangeListener(zkClient:ZkClient, controller: MyController) extends IZkDataListener with Logging {
  var liveBrokers: Set[Broker] = Set()

  override def handleDataChange(dataPath: String, data: Any): Unit = {
    val existingControllerId:String = zkClient.readData(dataPath)
    controller.setCurrent(existingControllerId.toInt)
  }

  override def handleDataDeleted(dataPath: String): Unit = {
    controller.elect()
  }
}