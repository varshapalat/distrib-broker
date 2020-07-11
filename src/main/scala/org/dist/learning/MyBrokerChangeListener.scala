package org.dist.learning

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.learning.common.Logging
import org.dist.learning.util.ZkUtils.Broker

class MyBrokerChangeListener(zookeeperClient:MyZookeeperClient) extends IZkChildListener with Logging {
  var liveBrokers: Set[Broker] = Set()
  import scala.jdk.CollectionConverters._

  def removeDeadBrokers(deadBrokersIds: Set[Int]) :Unit = {
    val deadBrokers = liveBrokers.filter(b => deadBrokersIds.contains(b.id))
    liveBrokers = liveBrokers -- deadBrokers
  }

  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.asScala.mkString(",")))
    try {

      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- liveBrokerIds
      val deadBrokerIds = liveBrokerIds -- curBrokerIds
      val newBrokers = newBrokerIds.map(b => zookeeperClient.getBrokerInfo(b))

      info(s"${newBrokerIds} are newly created")
      newBrokers.foreach(b => liveBrokers += b)

      info(s"${deadBrokerIds} are dead")
      removeDeadBrokers(deadBrokerIds)

    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }

  private def liveBrokerIds = {
    liveBrokers.map(broker => broker.id)
  }
}