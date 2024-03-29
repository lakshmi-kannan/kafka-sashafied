/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import collection._
import collection.immutable.Set
import com.yammer.metrics.core.Gauge
import java.lang.{IllegalStateException, Object}
import java.util.concurrent.TimeUnit
import kafka.admin.PreferredReplicaLeaderElectionCommand
import kafka.api._
import kafka.cluster.Broker
import kafka.common._
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import kafka.server.{ZookeeperLeaderElector, KafkaConfig}
import kafka.utils.ZkUtils._
import kafka.utils.{Json, Utils, ZkUtils, Logging}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{IZkDataListener, IZkStateListener, ZkClient}
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException}
import java.util.concurrent.atomic.AtomicInteger
import scala.Some
import kafka.common.TopicAndPartition
import org.apache.log4j.Logger

class ControllerContext(val zkClient: ZkClient,
                        val zkSessionTimeout: Int,
                        var controllerChannelManager: ControllerChannelManager = null,
                        val controllerLock: Object = new Object,
                        var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty,
                        val brokerShutdownLock: Object = new Object,
                        var epoch: Int = KafkaController.InitialControllerEpoch - 1,
                        var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1,
                        val correlationId: AtomicInteger = new AtomicInteger(0),
                        var allTopics: Set[String] = Set.empty,
                        var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty,
                        var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty,
                        var partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] =
                          new mutable.HashMap,
                        var partitionsUndergoingPreferredReplicaElection: mutable.Set[TopicAndPartition] =
                          new mutable.HashSet) {

  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying.filter(brokerId => !shuttingDownBrokerIds.contains(brokerId))

  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying
}

trait KafkaControllerMBean {
  def shutdownBroker(id: Int): Set[TopicAndPartition]
}

object KafkaController extends Logging {
  val MBeanName = "kafka.controller:type=KafkaController,name=ControllerOps"
  val stateChangeLogger = "state.change.logger"
  val InitialControllerEpoch = 1
  val InitialControllerEpochZkVersion = 1

  def parseControllerId(controllerInfoString: String): Int = {
    try {
      Json.parseFull(controllerInfoString) match {
        case Some(m) =>
          val controllerInfo = m.asInstanceOf[Map[String, Any]]
          return controllerInfo.get("brokerid").get.asInstanceOf[Int]
        case None => throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString))
      }
    } catch {
      case t: Throwable =>
        // It may be due to an incompatible controller register version
        warn("Failed to parse the controller info as json. "
          + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString))
        try {
          return controllerInfoString.toInt
        } catch {
          case t: Throwable => throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t)
        }
    }
  }
}

class KafkaController(val config : KafkaConfig, zkClient: ZkClient) extends Logging with KafkaMetricsGroup with KafkaControllerMBean {
  this.logIdent = "[Controller " + config.brokerId + "]: "
  private var isRunning = true
  private val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)
  val controllerContext = new ControllerContext(zkClient, config.zkSessionTimeoutMs)
  private val partitionStateMachine = new PartitionStateMachine(this)
  private val replicaStateMachine = new ReplicaStateMachine(this)
  private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
    config.brokerId)
  val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext)
  private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
  private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)
  private val controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controllerContext, sendRequest, this.config.brokerId, this.clientId)
  registerControllerChangedListener()

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value() = if (isActive) 1 else 0
    }
  )

  newGauge(
    "OfflinePartitionsCount",
    new Gauge[Int] {
      def value(): Int = {
        controllerContext.controllerLock synchronized {
          if (!isActive())
            0
          else
            controllerContext.partitionLeadershipInfo.count(p => !controllerContext.liveBrokerIds.contains(p._2.leaderAndIsr.leader))
        }
      }
    }
  )

  def epoch = controllerContext.epoch

  def clientId = "id_%d-host_%s-port_%d".format(config.brokerId, config.hostName, config.port)

  /**
   * On clean shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @return The number of partitions that the broker still leads.
   */
  def shutdownBroker(id: Int) : Set[TopicAndPartition] = {

    if (!isActive()) {
      throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
    }

    controllerContext.brokerShutdownLock synchronized {
      info("Shutting down broker " + id)

      controllerContext.controllerLock synchronized {
        if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
          throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

        controllerContext.shuttingDownBrokerIds.add(id)

        debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
        debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))
      }

      val allPartitionsAndReplicationFactorOnBroker = controllerContext.controllerLock synchronized {
        getPartitionsAssignedToBroker(zkClient, controllerContext.allTopics.toSeq, id).map {
          case(topic, partition) =>
            val topicAndPartition = TopicAndPartition(topic, partition)
            (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size)
        }
      }

      allPartitionsAndReplicationFactorOnBroker.foreach {
        case(topicAndPartition, replicationFactor) =>
        // Move leadership serially to relinquish lock.
        controllerContext.controllerLock synchronized {
          controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
            if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
              // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
              // notifies all affected brokers
              partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
                controlledShutdownPartitionLeaderSelector)
            }
            else {
              // Stop the replica first. The state change below initiates ZK changes which should take some time
              // before which the stop replica request should be completed (in most cases)
              brokerRequestBatch.newBatch()
              brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic, topicAndPartition.partition, deletePartition = false)
              brokerRequestBatch.sendRequestsToBrokers(epoch, controllerContext.correlationId.getAndIncrement)

              // If the broker is a follower, updates the isr in ZK and notifies the current leader
              replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
                topicAndPartition.partition, id)), OfflineReplica)
            }
          }
        }
      }

      def replicatedPartitionsBrokerLeads() = controllerContext.controllerLock.synchronized {
        trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","))
        controllerContext.partitionLeadershipInfo.filter {
          case (topicAndPartition, leaderIsrAndControllerEpoch) =>
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1
        }.map(_._1)
      }
      replicatedPartitionsBrokerLeads().toSet
    }
  }

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Register controller epoch changed listener
   * 2. Increments the controller epoch
   * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 4. Starts the controller's channel manager
   * 5. Starts the replica state machine
   * 6. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
  def onControllerFailover() {
    if(isRunning) {
      info("Broker %d starting become controller state transition".format(config.brokerId))
      // increment the controller epoch
      incrementControllerEpoch(zkClient)
      // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
      registerReassignedPartitionsListener()
      registerPreferredReplicaElectionListener()
      partitionStateMachine.registerListeners()
      replicaStateMachine.registerListeners()
      initializeControllerContext()
      replicaStateMachine.startup()
      partitionStateMachine.startup()
      // register the partition change listeners for all existing topics on failover
      controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
      Utils.registerMBean(this, KafkaController.MBeanName)
      info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))
      initializeAndMaybeTriggerPartitionReassignment()
      initializeAndMaybeTriggerPreferredReplicaElection()
      /* send partition leadership info to all live brokers */
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
    else
      info("Controller has been shut down, aborting startup/failover")
  }

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive(): Boolean = {
    controllerContext.controllerLock synchronized {
      controllerContext.controllerChannelManager != null
    }
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Triggers the OnlinePartition state change for all new/offline partitions
   * 2. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))

    val newBrokersSet = newBrokers.toSet
    // send update metadata request for all partitions to the newly restarted brokers. In cases of controlled shutdown
    // leaders will not be elected when a new broker comes up. So at least in the common controlled shutdown case, the
    // metadata will reach the new brokers faster
    sendUpdateMetadataRequest(newBrokers)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    replicaStateMachine.handleStateChanges(getAllReplicasOnBroker(zkClient, controllerContext.allTopics.toSeq, newBrokers), OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter{
      case (topicAndPartition, reassignmentContext) =>
        reassignmentContext.newReplicas.exists(newBrokersSet.contains(_))
    }
    partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Mark partitions with dead leaders as offline
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
   * the partition state machine will refresh our cache for us when performing leader election for all new/offline
   * partitions coming online.
   */
  def onBrokerFailure(deadBrokers: Seq[Int]) {
    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))

    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))

    val deadBrokersSet = deadBrokers.toSet
    // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader)).keySet
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // handle dead replicas
    replicaStateMachine.handleStateChanges(getAllReplicasOnBroker(zkClient, controllerContext.allTopics.toSeq, deadBrokers), OfflineReplica)
  }

  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   */
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
    onNewPartitionCreation(newPartitions)
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
    replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
    replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few stages -
   * RAR = Reassigned replicas
   * AR = Original list of replicas for partition
   * 1. Write new AR = AR + RAR
   * 2. Start new replicas RAR - AR.
   * 3. Wait until new replicas are in sync with the leader
   * 4. If the leader is not in RAR, elect a new leader from RAR
   * 5. Stop old replicas AR - RAR
   * 6. Write new AR = RAR
   * 7. Remove partition from the /admin/reassign_partitions path
   */
  def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas) match {
      case true =>
        val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
        // mark the new replicas as online
        reassignedReplicas.foreach { replica =>
          replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
            replica)), OnlineReplica)
        }
        // check if current leader is in the new replicas list. If not, controller needs to trigger leader election
        moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
        // stop older replicas
        stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas)
        // write the new list of replicas for this partition in zookeeper
        updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas)
        // update the /admin/reassign_partitions path to remove this partition
        removePartitionFromReassignedPartitions(topicAndPartition)
        info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
        // after electing leader, the replicas and isr information changes, so resend the update metadata request
        sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
      case false =>
        info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
          "reassigned not yet caught up with the leader")
        val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet
        val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet
        // write the expanded list of replicas to zookeeper
        updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq)
        // update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
        updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
          newAndOldReplicas.toSeq)
        // start new replicas
        startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
        info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
          "reassigned to catch up with the leader")
    }
  }

  private def watchIsrChangesForReassignedPartition(topic: String,
                                                    partition: Int,
                                                    reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
      reassignedReplicas.toSet)
    reassignedPartitionContext.isrChangeListener = isrChangeListener
    // register listener on the leader and isr path to wait until they catch up with the current leader
    zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
  }

  def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                        reassignedPartitionContext: ReassignedPartitionsContext) {
    val newReplicas = reassignedPartitionContext.newReplicas
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    val aliveNewReplicas = newReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
    try {
      val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition)
      assignedReplicasOpt match {
        case Some(assignedReplicas) =>
          if(assignedReplicas == newReplicas) {
            throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
              " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
          } else {
            if(aliveNewReplicas == newReplicas) {
              info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
              // first register ISR change listener
              watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
              controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)
              onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
            } else {
              // some replica in RAR is not alive. Fail partition reassignment
              throw new KafkaException("Only %s replicas out of the new set of replicas".format(aliveNewReplicas.mkString(",")) +
                " %s for partition %s to be reassigned are alive. ".format(newReplicas.mkString(","), topicAndPartition) +
                "Failing partition reassignment")
            }
          }
        case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
          .format(topicAndPartition))
      }
    } catch {
      case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
      // remove the partition from the admin path to unblock the admin client
      removePartitionFromReassignedPartitions(topicAndPartition)
    }
  }

  def onPreferredReplicaElection(partitions: Set[TopicAndPartition]) {
    info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
    try {
      controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitions
      partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
    } catch {
      case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
    } finally {
      removePartitionsFromPreferredReplicaElection(partitions)
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    controllerContext.controllerLock synchronized {
      info("Controller starting up");
      registerSessionExpirationListener()
      isRunning = true
      controllerElector.startup
      info("Controller startup complete")
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    controllerContext.controllerLock synchronized {
      isRunning = false
      partitionStateMachine.shutdown()
      replicaStateMachine.shutdown()
      if(controllerContext.controllerChannelManager != null) {
        controllerContext.controllerChannelManager.shutdown()
        controllerContext.controllerChannelManager = null
        info("Controller shutdown complete")
      }
    }
  }

  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  def incrementControllerEpoch(zkClient: ZkClient) = {
    try {
      var newControllerEpoch = controllerContext.epoch + 1
      val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPathIfExists(zkClient,
        ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
      if(!updateSucceeded)
        throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
      else {
        controllerContext.epochZkVersion = newVersion
        controllerContext.epoch = newControllerEpoch
      }
    } catch {
      case nne: ZkNoNodeException =>
        // if path doesn't exist, this is the first controller whose epoch should be 1
        // the following call can still fail if another controller gets elected between checking if the path exists and
        // trying to create the controller epoch path
        try {
          zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString)
          controllerContext.epoch = KafkaController.InitialControllerEpoch
          controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
        } catch {
          case e: ZkNodeExistsException => throw new ControllerMovedException("Controller moved to another broker. " +
            "Aborting controller startup procedure")
          case oe: Throwable => error("Error while incrementing controller epoch", oe)
        }
      case oe: Throwable => error("Error while incrementing controller epoch", oe)

    }
    info("Controller %d incremented epoch to %d".format(config.brokerId, controllerContext.epoch))
  }

  private def registerSessionExpirationListener() = {
    zkClient.subscribeStateChanges(new SessionExpirationListener())
  }

  private def initializeControllerContext() {
    controllerContext.liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet
    controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).toSet
    controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, controllerContext.allTopics.toSeq)
    controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    startChannelManager()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
  }

  private def initializeAndMaybeTriggerPartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)
    // check if they are already completed
    val reassignedPartitions = partitionsBeingReassigned.filter(partition =>
      controllerContext.partitionReplicaAssignment(partition._1) == partition._2.newReplicas).map(_._1)
    reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
    var partitionsToReassign: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
    partitionsToReassign ++= partitionsBeingReassigned
    partitionsToReassign --= reassignedPartitions

    info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()))
    info("Partitions already reassigned: %s".format(reassignedPartitions.toString()))
    info("Resuming reassignment of partitions: %s".format(partitionsToReassign.toString()))

    partitionsToReassign.foreach { topicPartitionToReassign =>
      initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2)
    }
  }

  private def initializeAndMaybeTriggerPreferredReplicaElection() {
    // read the partitions undergoing preferred replica election from zookeeper path
    val partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient)
    // check if they are already completed
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter(partition =>
      controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == controllerContext.partitionReplicaAssignment(partition).head)
    controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitionsUndergoingPreferredReplicaElection
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsThatCompletedPreferredReplicaElection
    info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
    info("Resuming preferred replica election for partitions: %s".format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
    onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.toSet)
  }

  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config)
    controllerContext.controllerChannelManager.startup()
  }

  private def updateLeaderAndIsrCache() {
    val leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet)
    for((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
      controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
  }

  private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
    getLeaderAndIsrForPartition(zkClient, topic, partition) match {
      case Some(leaderAndIsr) =>
        val replicasNotInIsr = replicas.filterNot(r => leaderAndIsr.isr.contains(r))
        replicasNotInIsr.isEmpty
      case None => false
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
    if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
        "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
    } else {
      // check if the leader is alive or not
      controllerContext.liveBrokerIds.contains(currentLeader) match {
        case true =>
          info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
            "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
          // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
          updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas)
        case false =>
          info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
            "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
          partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
      }
    }
  }

  private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext,
                                                   oldReplicas: Set[Int]) {
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    // first move the replica to offline state (the controller removes it from the ISR)
    oldReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topic, partition, replica)), OfflineReplica)
    }
    // send stop replica command to the old replicas
    oldReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topic, partition, replica)), NonExistentReplica)
    }
  }

  private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                 replicas: Seq[Int]) {
    val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
    partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas)
    updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic)
    info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, replicas.mkString(",")))
    // update the assigned replica list after a successful zookeeper write
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas)
  }

  private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(topicAndPartition: TopicAndPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
    brokerRequestBatch.newBatch()
    updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        // send the shrunk assigned replica list to all the replicas, including the leader, so that it no longer
        // allows old replicas to enter ISR
        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
          topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas)
        brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch, controllerContext.correlationId.getAndIncrement)
        stateChangeLogger.trace(("Controller %d epoch %d sent LeaderAndIsr request %s with new assigned replica list %s " +
          "to leader %d for partition being reassigned %s").format(config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
          newAssignedReplicas.mkString(","), updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition))
      case None => // fail the reassignment
        stateChangeLogger.error(("Controller %d epoch %d failed to send LeaderAndIsr request with new assigned replica list %s " +
          "to leader for partition being reassigned %s").format(config.brokerId, controllerContext.epoch,
          newAssignedReplicas.mkString(","), topicAndPartition))
    }
  }

  private def registerReassignedPartitionsListener() = {
    zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, new PartitionsReassignedListener(this))
  }

  private def registerPreferredReplicaElectionListener() {
    zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, new PreferredReplicaElectionListener(this))
  }

  private def registerControllerChangedListener() {
    zkClient.subscribeDataChanges(ZkUtils.ControllerEpochPath, new ControllerEpochListener(this))
  }

  def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
    // stop watching the ISR changes for this partition
    zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
      controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)
    // read the current list of reassigned partitions from zookeeper
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)
    // remove this partition from that list
    val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition
    // write the new list to zookeeper
    ZkUtils.updatePartitionReassignmentData(zkClient, updatedPartitionsBeingReassigned.mapValues(_.newReplicas))
    // update the cache
    controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
  }

  def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                         newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
    try {
      val zkPath = ZkUtils.getTopicPath(topicAndPartition.topic)
      val jsonPartitionMap = ZkUtils.replicaAssignmentZkData(newReplicaAssignmentForTopic.map(e => (e._1.partition.toString -> e._2)))
      ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionMap)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
    } catch {
      case e: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
      case e2: Throwable => throw new KafkaException(e2.toString)
    }
  }

  def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition]) {
    for(partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if(currentLeader == preferredReplica) {
        info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
      } else {
        warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
      }
    }
    ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath)
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsToBeRemoved
  }

  private def getAllReplicasForPartition(partitions: Set[TopicAndPartition]): Set[PartitionAndReplica] = {
    partitions.map { p =>
      val replicas = controllerContext.partitionReplicaAssignment(p)
      replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
    }.flatten
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   * @param brokers The brokers that the update metadata request should be sent to
   * @param partitions The partitions for which the metadata is to be sent
   */
  private def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
    brokerRequestBatch.newBatch()
    brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
    brokerRequestBatch.sendRequestsToBrokers(epoch, controllerContext.correlationId.getAndIncrement)
  }

  /**
   * Removes a given partition replica from the ISR; if it is not the current
   * leader and there are sufficient remaining replicas in ISR.
   * @param topic topic
   * @param partition partition
   * @param replicaId replica Id
   * @return the new leaderAndIsr (with the replica removed if it was present),
   *         or None if leaderAndIsr is empty.
   */
  def removeReplicaFromIsr(topic: String, partition: Int, replicaId: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Removing replica %d from ISR %s for partition %s.".format(replicaId,
      controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          if (leaderAndIsr.isr.contains(replicaId)) {
            // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
            val newLeader = if(replicaId == leaderAndIsr.leader) -1 else leaderAndIsr.leader
            val newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
              leaderAndIsr.isr.filter(b => b != replicaId), leaderAndIsr.zkVersion + 1)
            // update the new leadership decision in zookeeper or retry
            val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(
              zkClient,
              ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
              ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, epoch),
              leaderAndIsr.zkVersion)
            newLeaderAndIsr.zkVersion = newVersion

            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            if (updateSucceeded)
              info("New leader and ISR for partition %s is %s".format(topicAndPartition, newLeaderAndIsr.toString()))
            updateSucceeded
          } else {
            warn("Cannot remove replica %d from ISR of %s. Leader = %d ; ISR = %s"
                 .format(replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr))
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            true
          }
        case None =>
          warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId, topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   * @param topic topic
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Updating leader epoch for partition %s.".format(topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) =>
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
                                                 leaderAndIsr.isr, leaderAndIsr.zkVersion + 1)
          // update the new leadership decision in zookeeper or retry
          val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(
            zkClient,
            ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
            ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, epoch),
            leaderAndIsr.zkVersion)
          newLeaderAndIsr.zkVersion = newVersion
          finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
          if (updateSucceeded)
            info("Updated leader epoch for partition %s to %d".format(topicAndPartition, newLeaderAndIsr.leaderEpoch))
          updateSucceeded
        case None =>
          throw new IllegalStateException(("Cannot update leader epoch for partition %s as leaderAndIsr path is empty. " +
            "This could mean we somehow tried to reassign a partition that doesn't exist").format(topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  class SessionExpirationListener() extends IZkStateListener with Logging {
    this.logIdent = "[SessionExpirationListener on " + config.brokerId + "], "
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      controllerContext.controllerLock synchronized {
        Utils.unregisterMBean(KafkaController.MBeanName)
        partitionStateMachine.shutdown()
        replicaStateMachine.shutdown()
        if(controllerContext.controllerChannelManager != null) {
          controllerContext.controllerChannelManager.shutdown()
          controllerContext.controllerChannelManager = null
        }
        controllerElector.elect
      }
    }
  }
}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed
 * 2. New replicas are the same as existing replicas
 * 3. Any replica in the new set of replicas are dead
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
 * partitions.
 */
class PartitionsReassignedListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[PartitionsReassignedListener on " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions are reassigned by the admin command
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Partitions reassigned listener fired for path %s. Record partitions to be reassigned %s"
      .format(dataPath, data))
    val partitionsReassignmentData = ZkUtils.parsePartitionReassignmentData(data.toString)
    val newPartitions = partitionsReassignmentData.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
    newPartitions.foreach { partitionToBeReassigned =>
      controllerContext.controllerLock synchronized {
        val context = new ReassignedPartitionsContext(partitionToBeReassigned._2)
        controller.initiateReassignReplicasForTopicPartition(partitionToBeReassigned._1, context)
      }
    }
  }

  /**
   * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

class ReassignedPartitionsIsrChangeListener(controller: KafkaController, topic: String, partition: Int,
                                            reassignedReplicas: Set[Int])
  extends IZkDataListener with Logging {
  this.logIdent = "[ReassignedPartitionsIsrChangeListener on controller " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions need to move leader to preferred replica
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    try {
      controllerContext.controllerLock synchronized {
        debug("Reassigned partitions isr change listener fired for path %s with children %s".format(dataPath, data))
        // check if this partition is still being reassigned or not
        val topicAndPartition = TopicAndPartition(topic, partition)
        controllerContext.partitionsBeingReassigned.get(topicAndPartition) match {
          case Some(reassignedPartitionContext) =>
            // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
            val newLeaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition)
            newLeaderAndIsrOpt match {
              case Some(leaderAndIsr) => // check if new replicas have joined ISR
                val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
                if(caughtUpReplicas == reassignedReplicas) {
                  // resume the partition reassignment process
                  info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                    "Resuming partition reassignment")
                  controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
                }
                else {
                  info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                    "Replica(s) %s still need to catch up".format((reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")))
                }
              case None => error("Error handling reassignment of partition %s to replicas %s as it was never created"
                .format(topicAndPartition, reassignedReplicas.mkString(",")))
            }
          case None =>
        }
      }
    }catch {
      case e: Throwable => error("Error while handling partition reassignment", e)
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election -
 */
class PreferredReplicaElectionListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[PreferredReplicaElectionListener on " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions are reassigned by the admin command
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Preferred replica election listener fired for path %s. Record partitions to undergo preferred replica election %s"
            .format(dataPath, data.toString))
    val partitionsForPreferredReplicaElection = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString)

    controllerContext.controllerLock synchronized {
      info("These partitions are already undergoing preferred replica election: %s"
             .format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
      val newPartitions = partitionsForPreferredReplicaElection -- controllerContext.partitionsUndergoingPreferredReplicaElection
      controller.onPreferredReplicaElection(newPartitions)
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

class ControllerEpochListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[ControllerEpochListener on " + controller.config.brokerId + "]: "
  val controllerContext = controller.controllerContext
  readControllerEpochFromZookeeper()

  /**
   * Invoked when a controller updates the epoch value
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Controller epoch listener fired with new epoch " + data.toString)
    controllerContext.controllerLock synchronized {
      // read the epoch path to get the zk version
      readControllerEpochFromZookeeper()
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }

  private def readControllerEpochFromZookeeper() {
    // initialize the controller epoch and zk version by reading from zookeeper
    if(ZkUtils.pathExists(controllerContext.zkClient, ZkUtils.ControllerEpochPath)) {
      val epochData = ZkUtils.readData(controllerContext.zkClient, ZkUtils.ControllerEpochPath)
      controllerContext.epoch = epochData._1.toInt
      controllerContext.epochZkVersion = epochData._2.getVersion
      info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
    }
  }
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: ReassignedPartitionsIsrChangeListener = null)

case class PartitionAndReplica(topic: String, partition: Int, replica: Int)

case class LeaderIsrAndControllerEpoch(val leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString(): String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

object ControllerStats extends KafkaMetricsGroup {
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)
  val leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
