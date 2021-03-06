package fr.acinq.hc.app

import fr.acinq.eclair.io._
import fr.acinq.hc.app.Worker._
import fr.acinq.hc.app.channel._
import scala.concurrent.duration._
import fr.acinq.eclair.router.{Router, SyncProgress}
import akka.actor.{Actor, ActorRef, Props, Terminated}
import fr.acinq.eclair.wire.internal.channel.version2.HCProtocolCodecs
import scala.concurrent.ExecutionContext.Implicits.global
import fr.acinq.hc.app.db.Blocking.timeout
import fr.acinq.hc.app.db.HostedChannelsDb
import com.google.common.collect.HashBiMap
import fr.acinq.hc.app.network.HostedSync
import fr.acinq.bitcoin.Crypto.PublicKey
import com.google.common.net.HostAndPort
import fr.acinq.bitcoin.ByteVector32
import scala.collection.mutable
import grizzled.slf4j.Logging
import akka.pattern.ask
import fr.acinq.eclair
import scodec.Attempt


object Worker {
  case object HCPeerConnected

  case object HCPeerDisconnected

  case object TickReconnectHosts { val label = "TickReconnectHosts" }

  case object TickClearIpAntiSpam { val label = "TickClearIpAntiSpam" }

  case object TickRemoveIdleChannels { val label = "TickRemoveIdleChannels" }

  val notFound: CMDResFailure = CMDResFailure("HC with remote node is not found")

  val isHidden: CMDResFailure = CMDResFailure("HC with remote node is hidden")
}

class Worker(kit: eclair.Kit, hostedSync: ActorRef, preimageCatcher: ActorRef, channelsDb: HostedChannelsDb, cfg: Config) extends Actor with Logging { me =>
  context.system.scheduler.scheduleWithFixedDelay(60.minutes, 60.minutes, self, Worker.TickClearIpAntiSpam)
  context.system.scheduler.scheduleWithFixedDelay(20.seconds, 20.seconds, self, Worker.TickReconnectHosts)
  context.system.scheduler.scheduleWithFixedDelay(2.days, 2.days, self, TickRemoveIdleChannels)

  context.system.eventStream.subscribe(channel = classOf[UnknownMessageReceived], subscriber = self)
  context.system.eventStream.subscribe(channel = classOf[PeerDisconnected], subscriber = self)
  context.system.eventStream.subscribe(channel = classOf[PeerConnected], subscriber = self)
  context.system.eventStream.subscribe(channel = classOf[SyncProgress], subscriber = self)

  val inMemoryHostedChannels: HashBiMap[PublicKey, ActorRef] = HashBiMap.create[PublicKey, ActorRef]

  val ipAntiSpam: mutable.Map[Array[Byte], Int] = mutable.Map.empty withDefaultValue 0

  var clientChannelRemoteNodeIds: Set[PublicKey] = Set.empty

  override def receive: Receive = {
    case systemMessage: PeerConnected
      if systemMessage.connectionInfo.remoteInit.features.hasPluginFeature(HCFeature.plugin) =>
      HC.remoteNode2Connection += systemMessage.nodeId -> PeerConnectedWrapNormal(info = systemMessage)
      Option(inMemoryHostedChannels get systemMessage.nodeId).foreach(_ |> HCPeerDisconnected |> HCPeerConnected)
      logger.info(s"PLGN PHC, supporting peer connected, peer=${systemMessage.nodeId}")

    case systemMessage: PeerDisconnected =>
      HC.remoteNode2Connection -= systemMessage.nodeId
      Option(inMemoryHostedChannels get systemMessage.nodeId).foreach(_ ! HCPeerDisconnected)
      logger.info(s"PLGN PHC, supporting peer disconnected, peer=${systemMessage.nodeId}")

    case Worker.TickClearIpAntiSpam => ipAntiSpam.clear

    case peerMsg @ UnknownMessageReceived(_, _, message, _) if HC.preimageQueryTags contains message.tag => preimageCatcher ! peerMsg
    case peerMsg @ UnknownMessageReceived(_, _, message, _) if HC.announceTags contains message.tag => hostedSync ! peerMsg

    case UnknownMessageReceived(_, nodeId, message, _) if HC.hostedMessageTags contains message.tag =>
      Tuple3(HCProtocolCodecs decodeHostedMessage message, HC.remoteNode2Connection get nodeId, inMemoryHostedChannels get nodeId) match {
        case (_: Attempt.Failure, _, _) => logger.info(s"PLGN PHC, hosted message decoding failed, messageTag=${message.tag}, peer=$nodeId")
        case (Attempt.Successful(_: ReplyPublicHostedChannelsEnd), Some(wrap), _) => hostedSync ! HostedSync.GotAllSyncFrom(wrap)
        case (Attempt.Successful(_: QueryPublicHostedChannels), Some(wrap), _) => hostedSync ! HostedSync.SendSyncTo(wrap)

        // Special handling for InvokeHostedChannel: if chan exists neither in memory nor in db, then this is a new chan request and anti-spam rules apply
        case (Attempt.Successful(invoke: InvokeHostedChannel), Some(wrap), null) => restore(guardSpawn(nodeId, wrap, invoke), Tools.none, _ |> HCPeerConnected |> invoke)(nodeId)
        case (Attempt.Successful(_: HostedChannelMessage), _, null) => logger.info(s"PLGN PHC, no target for HostedMessage, messageTag=${message.tag}, peer=$nodeId")
        case (Attempt.Successful(hosted: HostedChannelMessage), _, channelRef) => channelRef ! hosted
      }

    case UnknownMessageReceived(_, nodeId, message, _) if HC.chanIdMessageTags contains message.tag =>
      Tuple2(HCProtocolCodecs decodeHasChanIdMessage message, inMemoryHostedChannels get nodeId) match {
        case (_: Attempt.Failure, _) => logger.info(s"PLGN PHC, HasChannelId message decoding fail, messageTag=${message.tag}, peer=$nodeId")
        case (Attempt.Successful(error: eclair.wire.protocol.Error), null) => restore(Tools.none, Tools.none, _ |> HCPeerConnected |> error)(nodeId)
        case (_, null) => logger.info(s"PLGN PHC, no target for HasChannelIdMessage, messageTag=${message.tag}, peer=$nodeId")
        case (Attempt.Successful(msg), channelRef) => channelRef ! msg
      }

    case cmd: HC_CMD_LOCAL_INVOKE =>
      val isConnected = HC.remoteNode2Connection.contains(cmd.remoteNodeId)
      val isInDb = channelsDb.getChannelByRemoteNodeId(cmd.remoteNodeId).nonEmpty
      val isInMemory = Option(inMemoryHostedChannels get cmd.remoteNodeId).nonEmpty
      if (kit.nodeParams.nodeId == cmd.remoteNodeId) sender ! CMDResFailure("HC with itself is prohibited")
      else if (isInMemory || isInDb) sender ! CMDResFailure("HC with remote peer already exists")
      else if (!isConnected) sender ! CMDResFailure("Not yet connected to remote peer")
      else spawnChannel(cmd.remoteNodeId) |> HCPeerConnected |> cmd
      clientChannelRemoteNodeIds += cmd.remoteNodeId

    case cmd: HC_CMD_RESTORE =>
      val isInDb = channelsDb.getChannelByRemoteNodeId(cmd.remoteNodeId).nonEmpty
      val isInMemory = Option(inMemoryHostedChannels get cmd.remoteNodeId).nonEmpty
      if (kit.nodeParams.nodeId == cmd.remoteNodeId) sender ! CMDResFailure("HC with itself is prohibited")
      else if (isInMemory || isInDb) sender ! CMDResFailure("HC with remote peer already exists")
      else spawnChannel(cmd.remoteNodeId) ! cmd

    case cmd: HasRemoteNodeIdHostedCommand =>
      Option(inMemoryHostedChannels get cmd.remoteNodeId) match {
        // Peer may be disconnected when command arrives, channel must be able to handle command in any state
        case None => restore(sender ! notFound, sender ! isHidden, _ |> cmd)(cmd.remoteNodeId)
        case Some(ref) => ref forward cmd
      }

    case Terminated(channelRef) => inMemoryHostedChannels.inverse.remove(channelRef)

    case TickRemoveIdleChannels => inMemoryHostedChannels.values.forEach(_ ! TickRemoveIdleChannels)

    case SyncProgress(1D) if clientChannelRemoteNodeIds.isEmpty =>
      // We need a fully loaded graph to find Host IP addresses and ports
      val clientChannels: Seq[HC_DATA_ESTABLISHED] = channelsDb.listClientChannels
      val clientRemoteNodeIds: Seq[PublicKey] = clientChannels.map(_.commitments.remoteNodeId)
      val nodeIdCheck = clientChannels.forall(_.commitments.localNodeId == kit.nodeParams.nodeId)

      if (!nodeIdCheck) {
        // Our nodeId has changed, this is very bad
        logger.info("PLGN PHC, NODE ID CHECK FAILED")
        System.exit(0)
      } else {
        clientChannelRemoteNodeIds ++= clientRemoteNodeIds
        clientChannels.foreach(spawnPreparedChannel)
        self ! TickReconnectHosts
      }

    case TickReconnectHosts =>
      // Of all remote peers which are Hosts to our HCs, select those which are not connected
      val unconnectedHosts = clientChannelRemoteNodeIds.filterNot(HC.remoteNode2Connection.contains)

      if (unconnectedHosts.nonEmpty) {
        val routerData = (kit.router ? Router.GetRouterData).mapTo[Router.Data]
        logger.info(s"PLGN PHC, unconnected hosts=$unconnectedHosts")

        for {
          data <- routerData
          nodeId <- unconnectedHosts
          nodeAnnouncement <- data.nodes.get(nodeId)
          sockAddress <- nodeAnnouncement.addresses.headOption.map(_.socketAddress)
          hostAndPort = HostAndPort.fromParts(sockAddress.getHostString, sockAddress.getPort)
          _ = logger.info(s"PLGN PHC, trying to reconnect to ${nodeAnnouncement.nodeId}/$hostAndPort")
        } kit.switchboard ! Peer.Connect(address_opt = Some(hostAndPort), nodeId = nodeId)
      }
  }

  def spawnChannel(nodeId: PublicKey): ActorRef = {
    val spawnedChannelProps = Props(classOf[HostedChannel], kit, nodeId, channelsDb, hostedSync, cfg)
    val channelRef = context watch context.actorOf(spawnedChannelProps)
    inMemoryHostedChannels.put(nodeId, channelRef)
    channelRef
  }

  def guardSpawn(nodeId: PublicKey, wrap: PeerConnectedWrap, invoke: InvokeHostedChannel): Unit = {
    // Spawn new HC requested by remote peer if that peer is in our override map (special handling) or if there are not too many such requests from remote IP
    if (cfg.vals.hcOverrideMap.contains(nodeId) || ipAntiSpam(wrap.remoteIp) < cfg.vals.maxNewChansPerIpPerHour) spawnChannel(nodeId) |> HCPeerConnected |> invoke
    else wrap sendHasChannelIdMsg eclair.wire.protocol.Error(ByteVector32.Zeroes, ErrorCodes.ERR_HOSTED_CHANNEL_DENIED)
    // Record this request for anti-spam
    ipAntiSpam(wrap.remoteIp) += 1
  }

  def spawnPreparedChannel(data: HC_DATA_ESTABLISHED): ActorRef = {
    val channel = spawnChannel(data.commitments.remoteNodeId)
    channel ! data
    channel
  }

  def restore(onNotFound: => Unit, onHidden: => Unit, onFound: ActorRef => Unit)(nodeId: PublicKey): Unit =
    channelsDb.getChannelByRemoteNodeId(nodeId) collect {
      case (data, true) => onFound(me spawnPreparedChannel data)
      case (_, false) => onHidden
    } getOrElse onNotFound

  implicit class MultiSender(channel: ActorRef) {
    def |>(message: Any): MultiSender = {
      channel forward message
      this
    }
  }
}
