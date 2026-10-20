/*
sbt 'clouseau/testOnly com.cloudant.ziose.clouseau.IndexManagerServiceSpec'
 */
package com.cloudant.ziose.clouseau

import com.cloudant.ziose.core.Codec.EPid
import com.cloudant.ziose.core.MessageEnvelope.Response
import org.junit.runner.RunWith
import zio._
import zio.test.junit.{JUnitRunnableSpec, ZTestJUnitRunner}

import java.io.File
import com.cloudant.ziose.core._
import com.cloudant.ziose.otp.{OTPEngineWorker, OTPNodeConfig, OTPProcessContext}
import com.cloudant.ziose.scalang.ProcessLike.NodeName
import com.cloudant.ziose.scalang.{Adapter, Pid, Reference, ServiceContext}
import zio.test._
import zio.test.TestAspect
import com.cloudant.ziose.test.helpers.TestRunner
import com.ericsson.otp.erlang.OtpNode
import zio.test.Assertion.{anything, equalTo, forall, hasSameElements, hasSize, isSome, isSubtype}

@RunWith(classOf[ZTestJUnitRunner])
class IndexManagerServiceSpec extends JUnitRunnableSpec {
  val TIMEOUT_SUITE = 5.minutes
  val environment   = Utils.testEnvironment(1, 1, "IndexManager") ++ Utils.logger
  val adapter       = Adapter.mockAdapterWithFactory(ClouseauTypeFactory)

  val indexDir = new File("target", "indexes")

  if (indexDir.exists) {
    for (f <- indexDir.listFiles) {
      f.delete
    }
  }

  val foodir = new File(indexDir, "foo.1234567890")

  if (!foodir.exists) {
    foodir.mkdirs
  }

  val foo2dir = new File(indexDir, "foo.0987654321")

  if (!foo2dir.exists) {
    foo2dir.mkdirs
  }

  val foo2indexDir = new File(new File(indexDir, "foo.0987654321"), "5838a59330e52227a58019dc1b9edd6e")

  if (!foo2indexDir.exists) {
    foo2indexDir.mkdirs
  }

  type TestContext = EngineWorker & Node & ActorFactory & OTPNodeConfig

  def makeIndexManager(node: OtpNode): ZIO[TestContext & Scope, Throwable, IndexManagerService] = {
    for {
      snode <- Utils.clouseauNode
      worker <- ZIO.service[EngineWorker]
      mbox = node.createMbox("test manager mailbox")
      cfg    <- Utils.defaultConfig
      scfg = new ServiceContext[ConfigurationArgs] {
        override def args: ConfigurationArgs = ConfigurationArgs(cfg)
      }
      reg = Metrics.simpleRegistry
      scope <- ZIO.scope
      closeable <- scope.fork
      builder = IndexManagerServiceBuilder.make(snode, scfg)
      ctx <- OTPProcessContext.builder(Symbol("test process name"))
        .withOtpMbox(mbox)
        .withMeterRegistry(reg)
        .withScope(closeable)
        .withWorker(worker.asInstanceOf[OTPEngineWorker])
        .withBuilder(builder)
        .build()
      _ = println(s"Index manager has Address ${ctx.id}")
      _ = println(s"Index manager has self ${ctx.self}")
      adapter = Adapter(ctx, snode, ClouseauTypeFactory)
    } yield new IndexManagerService(scfg)(adapter)
  }

  val startIndexManager: ZIO[TestContext, Throwable, AddressableActor[IndexManagerService, _ <: ProcessContext]] = {
    for {
      node   <- Utils.clouseauNode
      worker <- ZIO.service[EngineWorker]
      cfg    <- Utils.defaultConfig
      val ctx = new ServiceContext[ConfigurationArgs] { val args = ConfigurationArgs(cfg) }
      service <- node.spawnServiceZIO[IndexManagerService, ConfigurationArgs](
        IndexManagerServiceBuilder.make(node, ctx)
      )
    } yield service
  }

  def callIndexManager(manager: AddressableActor[_, _], msg: Any) = {
    for {
      result <- manager
        .doTestCallTimeout(adapter.fromScala(msg), 3.seconds)
        .delay(100.millis)
        .repeatUntil(_.isSuccess)
        .map(result => result.payload.get)
        .timeout(3.seconds)
    } yield result
  }

  def stopIndexManager(manager: AddressableActor[_, _]) = {
    for {
      _ <- callIndexManager(manager, 'close_lru)
      _ <- manager.exit(adapter.fromScala('normal))
    } yield ()
  }

  def openIndex(
    manager: AddressableActor[_, _],
    peer: AddressableActor[_, _],
    path: String,
    options: AnalyzerOptions
  ) = {
    for {
      result <- callIndexManager(manager, ('open, peer.self.pid, path, options.toMap))
    } yield (result match {
      case Some(value) =>
        adapter.toScala(value) match {
          case ('ok, pid) => Some(pid)
          case _          => None
        }
      case _ => None
    })
  }

  def diskSize(path: String) = {
    for {
      manager <- startIndexManager
      result  <- callIndexManager(manager, ('disk_size, path))
      _       <- stopIndexManager(manager)
    } yield (result match {
      case Some(value) =>
        adapter.toScala(value) match {
          case ('ok, sizeInfo) => Some(sizeInfo)
          case _               => None
        }
      case _ => None
    })
  }

  def startPeer(name: String = "dummy") = {
    for {
      node <- Utils.clouseauNode
      peer <- TestService.start(node, name)
    } yield peer.actor
  }

  def stopActor(actor: AddressableActor[_, _]) = {
    for {
      _ <- actor.exit(adapter.fromScala('normal))
    } yield ()
  }

  val analyzerOptions = AnalyzerOptions.fromAnalyzerName("standard")

  val indexManagerSuite: Spec[Any, Throwable] = {
    suite("index manager")(
      /*test("opens an index when asked")(
        for {
          peer    <- startPeer()
          manager <- startIndexManager
          pid     <- openIndex(manager, peer, "foo", analyzerOptions)
          _       <- stopActor(peer)
          _       <- stopIndexManager(manager)
        } yield assertTrue(pid.isDefined)
      ),
      test("opens an index without reply")(
        for {
          peer   <- startPeer()
          node = new OtpNode("test node")
          manager <- ZIO.scoped(makeIndexManager(node))
          newRef  = Codec.ERef(node.createRef())
          resp <- ZIO.succeed(manager.handleCall((peer.self, newRef), OpenIndexMsg(peer.self, "SOMEPATH", AnalyzerOptions(Map("name" -> "standard")))))
          _       <- stopActor(peer)
        } yield assert(resp)(equalTo(Symbol("noreply")))
      ),*/
      test("puts waiters in a queue and notifies them on when index is open")(
        for {
          _ <- ZIO.succeed(())
          // mockIndex <- startPeer("mock index")
          node = new OtpNode("IndexManager@127.0.0.1")
          //receiverPid = node.createPid()
          receiverMBox = node.createMbox("test mailbox")
          receiverPid = receiverMBox.self()
          mockIndexMBox = node.createMbox("mock index")
          _ = println(s"Node name is '${receiverPid.node()}'")
          mockIndexPid = mockIndexMBox.self()
          manager <- ZIO.scoped(makeIndexManager(node))
          engine <- ZIO.service[EngineWorker]
          collector = new MessageCollector(Address.fromPid(Codec.EPid(receiverPid), 1, Symbol(receiverPid.node())))
          _       <- engine.register(collector)
          newRef  = Reference.toScala(Codec.ERef(node.createRef()))
          path = "SOMEPATH"
          openCall = OpenIndexMsg(Codec.EPid(receiverPid), path, AnalyzerOptions(Map("name" -> "standard")))
          _ <- ZIO.succeed(manager.handleCall((Codec.EPid(receiverPid), newRef), openCall))
          resp <- ZIO.succeed(manager.handleCall((Codec.EPid(receiverPid), newRef), openCall))
          indexPid = Codec.EPid(mockIndexPid)
          openResp <- ZIO.succeed(manager.handleInfo((Symbol("open_ok"), path, Pid.e2pid(Codec.EPid(receiverPid)), Pid.e2pid(indexPid))))
          // _       <- stopActor(peer)
        } yield {
          val messages = collector.capturedMessages
          val isOpenOkResponse = CustomAssertion.make[MessageEnvelope] {
            case MessageEnvelope.Response(_, _, _, _, _, Some(payload), _, _) => Right(Codec.toScala(payload))
            case _ => Left("Not a response with a payload")
          }
          assert(resp)(equalTo(Symbol("noreply"))) &&
            assert(openResp)(equalTo(Symbol("noreply"))) &&
            assert(messages)(hasSize(equalTo(2))) &&
            assertTrue(messages.forall(_.is(_.custom(isOpenOkResponse)) == (Symbol("ok"), EPid(mockIndexPid))))
        }
      ),
      /*test("opens an index atomically")(
        for {
          peer   <- startPeer
          node   <- ZIO.service[Node]
          engine <- ZIO.service[EngineWorker]
          _      <- engine.unregister(peer.self)
          collector = new MessageCollector(peer.self)
          _       <- engine.register(collector)
          manager <- startIndexManager
          newRef  <- node.makeRef()
          Some(openCall) = makeCall(
            manager.self,
            ETuple(peer.self.pid, newRef),
            openMessage(peer.self),
            None
          )
          _ <- manager.onMessage(openCall)
          _ <- manager.onMessage(openCall)
          _ <- stopActor(peer)
          _ <- stopIndexManager(manager)
        } yield {
          val messages = collector.capturedMessages
          assertTrue(messages.size == 2) &&
          assert(messages)(forall(isSubtype[MessageEnvelope.Response](anything))) &&
          assert(messages.map(m => parseOpenIndexResponse(m.asInstanceOf[MessageEnvelope.Response])))(forall(isSome)) &&
          assert(messages.tail)(forall(equalTo(messages.head)))
        }
      ),
      test("returns the same index if it's already open")(
        for {
          peer    <- startPeer()
          manager <- startIndexManager
          pid1    <- openIndex(manager, peer, "foo", analyzerOptions)
          pid2    <- openIndex(manager, peer, "foo", analyzerOptions)
          _       <- stopActor(peer)
          _       <- stopIndexManager(manager)
        } yield assertTrue(
          pid1.isDefined,
          pid2.isDefined,
          pid1.get == pid2.get
        )
      )*/
    ).provideLayer(environment) @@ TestAspect.withLiveClock @@ TestAspect.sequential
  }

  val diskSizeSuite: Spec[Any, Throwable] = {
    suite("disk size service")(
      test("return 0 for (db) when index directory is missing")(
        for {
          result <- diskSize("foo.1234567890")
        } yield assertTrue(
          result.isDefined,
          result.get.equals(List(('disk_size, 0)))
        )
      ),
      test("return 0 for (db/index) when index directory is missing")(
        for {
          result <- diskSize("foo.1234567890/5838a59330e52227a58019dc1b9edd6e")
        } yield assertTrue(
          result.isDefined,
          result.get.equals(List(('disk_size, 0)))
        )
      ),
      test("should not return 0 for (db) when index directory is not missing")(
        for {
          result <- diskSize("foo.0987654321")
        } yield assertTrue(
          result.isDefined,
          !result.get.equals(List(('disk_size, 0)))
        )
      ),
      test("return 0 for (db/index) when index directory is not missing but empty")(
        for {
          result <- diskSize("foo.0987654321/5838a59330e52227a58019dc1b9edd6e")
        } yield assertTrue(
          result.isDefined,
          result.get.equals(List(('disk_size, 0)))
        )
      )
    ).provideLayer(environment) @@ TestAspect.withLiveClock @@ TestAspect.sequential
  }

  def spec: Spec[Any, Throwable] = {
    suite("IndexManagerServiceSpec")(
      indexManagerSuite//,
      //diskSizeSuite
    ) @@ TestAspect.timeout(TIMEOUT_SUITE)
  }
}

/**
 * ```shell
 * rm artifacts/clouseau_*.jar ; make jartest
 * java -cp artifacts/clouseau_*_test.jar com.cloudant.ziose.clouseau.IndexManagerServiceSpecMain
 * ```
 */
object IndexManagerServiceSpecMain {
  def main(args: Array[String]): Unit = {
    TestRunner.runSpec("IndexManagerServiceSpec", new IndexManagerServiceSpec().spec)
  }
}
