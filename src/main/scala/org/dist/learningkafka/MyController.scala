package org.dist.learningkafka

import org.dist.simplekafka.ControllerExistsException

class MyController(val myZookeeperClient: MyZookeeperClient, val brokerId: Int) {
  var currentLeader = -1

  def setCurrent(existingControllerId: Int): Unit = {
    this.currentLeader = existingControllerId
  }

  def startup(): Unit = {
    myZookeeperClient.subscribeControllerChangeListener(this)
    elect()
  }

  def elect(): Unit = {
    val leaderId = s"${brokerId}"
    try {
      myZookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
//      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }
  def onBecomingLeader() = {

  }
}
