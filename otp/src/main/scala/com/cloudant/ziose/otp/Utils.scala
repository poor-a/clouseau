package com.cloudant.ziose.otp

import com.cloudant.ziose.core.{ActorFactory, Engine, EngineWorker, Node}
import zio.{&, TaskLayer, ZLayer, durationInt}

object Utils {
  def testEnvironment(
    engineId: Engine.EngineId,
    workerId: Engine.WorkerId,
    nodeName: String = "test",
    domain: String = "127.0.0.1"
  ): TaskLayer[EngineWorker & Node & ActorFactory & OTPNodeConfig] = {
    val nodeCfg = OTPNodeConfig(nodeName, domain, Some("testCookie"), Some(1.second), Some(60.seconds))
    OTPLayers.nodeLayers(engineId, workerId, nodeCfg) ++ ZLayer.succeed(nodeCfg)
  }
}
