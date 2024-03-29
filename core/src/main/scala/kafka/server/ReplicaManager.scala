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
package kafka.server

import kafka.cluster.{Broker, Partition, Replica}
import collection._
import mutable.HashMap
import org.I0Itec.zkclient.ZkClient
import java.io.{File, IOException}
import java.util.concurrent.atomic.AtomicBoolean
import kafka.utils._
import kafka.log.LogManager
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import java.util.concurrent.TimeUnit
import kafka.common._
import kafka.api.{StopReplicaRequest, PartitionStateInfo, LeaderAndIsrRequest}
import kafka.controller.{LeaderIsrAndControllerEpoch, KafkaController}
import org.apache.log4j.Logger


object ReplicaManager {
  val UnknownLogEndOffset = -1L
  val HighWatermarkFilename = "replication-offset-checkpoint"
}

class ReplicaManager(val config: KafkaConfig, 
                     time: Time, 
                     val zkClient: ZkClient, 
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean ) extends Logging with KafkaMetricsGroup {
  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[(String, Int), Partition]
  private var leaderPartitions = new mutable.HashSet[Partition]()
  private val leaderPartitionsLock = new Object
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = new ReplicaFetcherManager(config, this)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  val highWatermarkCheckpoints = config.logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap
  private var hwThreadInitialized = false
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)

  newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = {
        leaderPartitionsLock synchronized {
          leaderPartitions.size
        }
      }
    }
  )
  newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = {
        leaderPartitionsLock synchronized {
          leaderPartitions.count(_.isUnderReplicated)
        }
      }
    }
  )
  val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS)


  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  /**
   * This function is only used in two places: in Partition.updateISR() and KafkaApis.handleProducerRequest().
   * In the former case, the partition should have been created, in the latter case, return -1 will put the request into purgatory
   */
  def getReplicationFactorForPartition(topic: String, partitionId: Int) = {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case Some(partition) =>
        partition.replicationFactor
      case None =>
        -1
    }
  }

  def startup() {
    // start ISR expiration thread
    scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs, unit = TimeUnit.MILLISECONDS)
  }

  def stopReplica(topic: String, partitionId: Int, deletePartition: Boolean): Short  = {
    stateChangeLogger.trace("Broker %d handling stop replica for partition [%s,%d]".format(localBrokerId, topic, partitionId))
    val errorCode = ErrorMapping.NoError
    getReplica(topic, partitionId) match {
      case Some(replica) =>
        /* TODO: handle deleteLog in a better way */
        //if (deletePartition)
        //  logManager.deleteLog(topic, partition)
        leaderPartitionsLock synchronized {
          leaderPartitions -= replica.partition
        }
        if(deletePartition)
          allPartitions.remove((topic, partitionId))
      case None => //do nothing if replica no longer exists
    }
    stateChangeLogger.trace("Broker %d finished handling stop replica for partition [%s,%d]".format(localBrokerId, topic, partitionId))
    errorCode
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[(String, Int), Short], Short) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[(String, Int), Short]
      if(stopReplicaRequest.controllerEpoch < controllerEpoch) {
        stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d."
          .format(localBrokerId, stopReplicaRequest.controllerEpoch) +
          " Latest known controller epoch is %d " + controllerEpoch)
        (responseMap, ErrorMapping.StaleControllerEpochCode)
      } else {
        controllerEpoch = stopReplicaRequest.controllerEpoch
        val responseMap = new HashMap[(String, Int), Short]
        // First stop fetchers for all partitions, then stop the corresponding replicas
        replicaFetcherManager.removeFetcherForPartitions(stopReplicaRequest.partitions.map {
          case (topic, partition) => TopicAndPartition(topic, partition)
        })
        for((topic, partitionId) <- stopReplicaRequest.partitions){
          val errorCode = stopReplica(topic, partitionId, stopReplicaRequest.deletePartitions)
          responseMap.put((topic, partitionId), errorCode)
        }
        (responseMap, ErrorMapping.NoError)
      }
    }
  }

  def getOrCreatePartition(topic: String, partitionId: Int, replicationFactor: Int): Partition = {
    var partition = allPartitions.get((topic, partitionId))
    if (partition == null) {
      allPartitions.putIfNotExists((topic, partitionId), new Partition(topic, partitionId, replicationFactor, time, this))
      partition = allPartitions.get((topic, partitionId))
    }
    partition
  }

  def getPartition(topic: String, partitionId: Int): Option[Partition] = {
    val partition = allPartitions.get((topic, partitionId))
    if (partition == null)
      None
    else
      Some(partition)
  }

  def getReplicaOrException(topic: String, partition: Int): Replica = {
    val replicaOpt = getReplica(topic, partition)
    if(replicaOpt.isDefined)
      return replicaOpt.get
    else
      throw new ReplicaNotAvailableException("Replica %d is not available for partition [%s,%d]".format(config.brokerId, topic, partition))
  }

  def getLeaderReplicaIfLocal(topic: String, partitionId: Int): Replica =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException("Partition [%s,%d] doesn't exist on %d".format(topic, partitionId, config.brokerId))
      case Some(partition) =>
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
                                                     .format(topic, partitionId, config.brokerId))
        }
    }
  }

  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None => None
      case Some(partition) => partition.getReplica(replicaId)
    }
  }

  def becomeLeaderOrFollower(leaderAndISRRequest: LeaderAndIsrRequest): (collection.Map[(String, Int), Short], Short) = {
    leaderAndISRRequest.partitionStateInfos.foreach{ case ((topic, partition), stateInfo) =>
      stateChangeLogger.trace("Broker %d handling LeaderAndIsr request correlation id %d received from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerId,
                                        leaderAndISRRequest.controllerEpoch, topic, partition))}
    info("Handling LeaderAndIsr request %s".format(leaderAndISRRequest))

    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[(String, Int), Short]
      if(leaderAndISRRequest.controllerEpoch < controllerEpoch) {
        stateChangeLogger.warn("Broker %d received LeaderAndIsr request correlation id %d with an old controller epoch %d. Latest known controller epoch is %d"
          .format(localBrokerId, leaderAndISRRequest.controllerEpoch, leaderAndISRRequest.correlationId, controllerEpoch))
        (responseMap, ErrorMapping.StaleControllerEpochCode)
      } else {
        val controllerId = leaderAndISRRequest.controllerId
        val correlationId = leaderAndISRRequest.correlationId
        controllerEpoch = leaderAndISRRequest.controllerEpoch

        // First check partition's leader epoch
        val partitionState = new HashMap[Partition, PartitionStateInfo]()
        leaderAndISRRequest.partitionStateInfos.foreach{ case ((topic, partitionId), partitionStateInfo) =>
          val partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor)
          val partitionLeaderEpoch = partition.getLeaderEpoch()
          if (partitionLeaderEpoch < partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch) {
            // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
            // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
            partitionState.put(partition, partitionStateInfo)
          } else {
            // Otherwise record the error code in response
            stateChangeLogger.warn(("Broker %d received invalid LeaderAndIsr request with correlation id %d from " +
              "controller %d epoch %d with an older leader epoch %d for partition [%s,%d], current leader epoch is %d")
              .format(localBrokerId, correlationId, controllerId, partitionStateInfo.leaderIsrAndControllerEpoch.controllerEpoch,
              partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch, topic, partition.partitionId, partitionLeaderEpoch))
            responseMap.put((topic, partitionId), ErrorMapping.StaleLeaderEpochCode)
          }
        }

        val partitionsTobeLeader = partitionState
          .filter{ case (partition, partitionStateInfo) => partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader == config.brokerId}
        val partitionsTobeFollower = (partitionState -- partitionsTobeLeader.keys)

        if (!partitionsTobeLeader.isEmpty) makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, leaderAndISRRequest.correlationId, responseMap)
        if (!partitionsTobeFollower.isEmpty) makeFollowers(controllerId, controllerEpoch, partitionsTobeFollower, leaderAndISRRequest.leaders, leaderAndISRRequest.correlationId, responseMap)

        info("Handled leader and isr request %s".format(leaderAndISRRequest))
        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        if (!hwThreadInitialized) {
          startHighWaterMarksCheckPointThread()
          hwThreadInitialized = true
        }
        replicaFetcherManager.shutdownIdleFetcherThreads()
        (responseMap, ErrorMapping.NoError)
      }
    }
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int, epoch: Int,
                          partitionState: Map[Partition, PartitionStateInfo],
                          correlationId: Int, responseMap: mutable.Map[(String, Int), Short]) = {
    stateChangeLogger.trace(("Broker %d received LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "starting the become-leader transition for partitions %s")
      .format(localBrokerId, correlationId, controllerId, epoch,
      partitionState.keySet.map(p => TopicAndPartition(p.topic, p.partitionId)).mkString(",")))

    for (partition <- partitionState.keys)
      responseMap.put((partition.topic, partition.partitionId), ErrorMapping.NoError)

    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(new TopicAndPartition(_)))
      stateChangeLogger.trace("Broker %d stopped fetchers for partitions %s as per becoming-follower request from controller %d epoch %d"
        .format(localBrokerId, partitionState.keySet.map(p => TopicAndPartition(p.topic, p.partitionId)).mkString(","), controllerId, correlationId))

      // Update the partition information to be the leader
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        partition.makeLeader(controllerId, partitionStateInfo, correlationId)}

      // Finally add these partitions to the list of partitions for which the leader is the current broker
      leaderPartitionsLock synchronized {
        leaderPartitions ++= partitionState.keySet
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "for the become-leader transition for partitions %s")
      .format(localBrokerId, correlationId, controllerId, epoch,
      partitionState.keySet.map(p => TopicAndPartition(p.topic, p.partitionId)).mkString(",")))
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Truncate the log and checkpoint offsets for these partitions.
   * 3. If the broker is not shutting down, add the fetcher to the new leaders
   * 4. Update the partition metadata in cache
   * 5. Remove these partitions from the leader partitions set
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it
   *  TODO: the above may need to be fixed later
   */
  private def makeFollowers(controllerId: Int, epoch: Int,
                            partitionState: Map[Partition, PartitionStateInfo],
                            leaders: Set[Broker], correlationId: Int, responseMap: mutable.Map[(String, Int), Short]) {
    stateChangeLogger.trace(("Broker %d received LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "starting the become-follower transition for partitions %s")
      .format(localBrokerId, correlationId, controllerId, epoch,
      partitionState.keySet.map(p => TopicAndPartition(p.topic, p.partitionId)).mkString(",")))

    for (partition <- partitionState.keys)
      responseMap.put((partition.topic, partition.partitionId), ErrorMapping.NoError)

    try {
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(new TopicAndPartition(_)))
      stateChangeLogger.trace("Broker %d stopped fetchers for partitions %s as per becoming-follower request from controller %d epoch %d"
        .format(localBrokerId, partitionState.keySet.map(p => TopicAndPartition(p.topic, p.partitionId)).mkString(","), controllerId, correlationId))

      logManager.truncateTo(partitionState.map{ case(partition, leaderISRAndControllerEpoch) =>
        new TopicAndPartition(partition) -> partition.getOrCreateReplica().highWatermark
      })
      stateChangeLogger.trace("Broker %d truncated logs and checkpoint recovery boundaries for partitions %s as per becoming-follower request from controller %d epoch %d"
        .format(localBrokerId, partitionState.keySet.map(p => TopicAndPartition(p.topic, p.partitionId)).mkString(","), controllerId, correlationId))

      if (!isShuttingDown.get()) {
        replicaFetcherManager.addFetcherForPartitions(partitionState.map{ case(partition, partitionStateInfo) =>
          new TopicAndPartition(partition) ->
            BrokerAndInitialOffset(leaders.find(_.id == partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader).get,
              partition.getReplica().get.logEndOffset)}
        )
      }
      else {
        stateChangeLogger.trace(("Broker %d ignored the become-follower state change with correlation id %d from " +
          "controller %d epoch %d since it is shutting down")
          .format(localBrokerId, correlationId, controllerId, epoch))
      }

      partitionState.foreach{ case (partition, leaderIsrAndControllerEpoch) =>
        partition.makeFollower(controllerId, leaderIsrAndControllerEpoch, leaders, correlationId)}

      leaderPartitionsLock synchronized {
        leaderPartitions --= partitionState.keySet
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "for the become-follower transition for partitions %s")
      .format(localBrokerId, correlationId, controllerId, epoch, partitionState.keySet.map(p => TopicAndPartition(p.topic, p.partitionId)).mkString(",")))
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    var curLeaderPartitions: List[Partition] = null
    leaderPartitionsLock synchronized {
      curLeaderPartitions = leaderPartitions.toList
    }
    curLeaderPartitions.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs, config.replicaLagMaxMessages))
  }

  def recordFollowerPosition(topic: String, partitionId: Int, replicaId: Int, offset: Long) = {
    val partitionOpt = getPartition(topic, partitionId)
    if(partitionOpt.isDefined) {
      partitionOpt.get.updateLeaderHWAndMaybeExpandIsr(replicaId, offset)
    } else {
      warn("While recording the follower position, the partition [%s,%d] hasn't been created, skip updating leader HW".format(topic, partitionId))
    }
  }

  /**
   * Flushes the highwatermark value for all partitions to the highwatermark file
   */
  def checkpointHighWatermarks() {
    val replicas = allPartitions.values.map(_.getReplica(config.brokerId)).collect{case Some(replica) => replica}
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParent)
    for((dir, reps) <- replicasByDir) {
      val hwms = reps.map(r => (new TopicAndPartition(r) -> r.highWatermark)).toMap
      try {
        highWatermarkCheckpoints(dir).write(hwms)
      } catch {
        case e: IOException =>
          fatal("Error writing to highwatermark file: ", e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def shutdown() {
    info("Shut down")
    replicaFetcherManager.shutdown()
    checkpointHighWatermarks()
    info("Shutted down completely")
  }
}
