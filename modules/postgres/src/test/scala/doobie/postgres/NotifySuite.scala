// Copyright (c) 2013-2020 Rob Norris and Contributors
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package doobie.postgres

import cats.effect.{IO, Sync}
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import org.postgresql.PGNotification
import scala.concurrent.duration._

class NotifySuite extends munit.CatsEffectSuite {
  import FC.{commit, delay}
  import PostgresTestTransactor.xa

  // Listen on the given channel, notify on another connection
  def listen[A](channel: String, notify: ConnectionIO[A]): IO[List[PGNotification]] =
    (PHC.pgListen(channel) *> commit *>
      delay(IO.sleep(50.millis).unsafeRunSync()) *>
      Sync[ConnectionIO].delay(notify.transact(xa).unsafeRunSync()) *>
      delay(IO.sleep(50.millis).unsafeRunSync()) *>
      PHC.pgGetNotifications).transact(xa)

  // This does not works 
  // def listen[A](channel: String, notify: ConnectionIO[A]): IO[List[PGNotification]] =
  //   (for {
  //     _ <- PHC.pgListen(channel)
  //     _ <- FC.commit
  //     _ <- FC.delay(IO.sleep(50.millis).void)
  //     _ <- notify
  //     _ <- FC.delay(IO.sleep(50.millis).void)
  //     notifications <- PHC.pgGetNotifications
  //   } yield notifications).transact(xa)

  test("LISTEN/NOTIFY should allow cross-connection notification") {
    val channel = "cha" + System.nanoTime.toString
    val notify = PHC.pgNotify(channel)
    listen(channel, notify).map(_.length).assertEquals(1)  
  }

  test("LISTEN/NOTIFY should allow cross-connection notification with parameter") {
    val channel = "chb" + System.nanoTime.toString
    val messages = List("foo", "bar", "baz", "qux")
    val notify = messages.traverse(PHC.pgNotify(channel, _))
    listen(channel, notify).map(_.map(_.getParameter)).assertEquals(messages) 
  }

  test("LISTEN/NOTIFY should collapse identical notifications") {
    val channel = "chc" + System.nanoTime.toString
    val messages = List("foo", "bar", "bar", "baz", "qux", "foo")
    val notify = messages.traverse(PHC.pgNotify(channel, _))
    listen(channel, notify).map(_.map(_.getParameter)).assertEquals(messages.distinct) 
  }
}