package org.dist.learning

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.dist.learning.common.JsonSerDes
import org.dist.learning.util.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class MyZookeeperClient(zkClient:ZkClient) {
  def subscribeBrokerChangeListener(listener: MyBrokerChangeListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  val BrokerTopicsPath = "/brokers/topics"
  val BrokerIdsPath = "/brokers/ids"

  def registerBroker(broker:Broker) = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(brokerId => {
      val data: String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    val data: String = zkClient.readData(getBrokerPath(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  private def getBrokerPath(id: Int) = {
    BrokerIdsPath + "/" + id
  }

  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }
}
