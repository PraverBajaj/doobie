// Copyright (c) 2013-2020 Rob Norris and Contributors
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package doobie.hi

import cats.Foldable
import cats.data.Ior
import cats.effect.Sync
import cats.effect.syntax.monadCancel.*
import cats.syntax.all.*
import doobie.enumerated.AutoGeneratedKeys
import doobie.enumerated.Holdability
import doobie.enumerated.Nullability
import doobie.enumerated.ResultSetConcurrency
import doobie.enumerated.ResultSetType
import doobie.enumerated.TransactionIsolation
import doobie.util.analysis.Analysis
import doobie.util.analysis.ColumnMeta
import doobie.util.analysis.ParameterMeta
import doobie.util.compat.propertiesToScala
import doobie.util.stream.repeatEvalChunks
import doobie.util.log.{LogEvent, LoggingInfo}
import doobie.util.{Get, Put, Read, Write}
import fs2.Stream
import doobie.hi.preparedstatement as IHPS
import doobie.free.{
  callablestatement as IFCS,
  connection as IFC,
  databasemetadata as IFDMD,
  preparedstatement as IFPS,
  resultset as IFRS,
  statement as IFS
}

import scala.concurrent.duration.{Duration, FiniteDuration, NANOSECONDS}
import java.sql.{PreparedStatement, ResultSet, Savepoint}
import scala.collection.immutable.Map

/** Module of high-level constructors for `ConnectionIO` actions.
  *
  * @group Modules
  */

object connection {

  import implicits.*

  /** @group Lifting */
  def delay[A](a: => A): ConnectionIO[A] =
    IFC.delay(a)

  /** Create and execute a PreparedStatement and then process the ResultSet. This generalized two uses query patterns:
    *
    *   - Execute a normal SELECT query
    *   - Executing an update and using withGeneratedKeys/RETURNING to return some rows
    *
    * In both cases, a ResultSet is returned which need to be processed.
    *
    * Errors at each step are handled and logged, with cleanups (closing the PreparedStatement/ResultSet.)
    *
    * For usage example, see [[doobie.util.query.Query.to]] or [[doobie.util.update.Update.withUniqueGeneratedKeys]]
    * which uses this function.
    *
    * @param create
    *   Create the PreparedStatement, using e.g. `doobie.FC.prepareStatement`
    * @param prep
    *   Prepare steps before execution, such as setting parameters
    * @param exec
    *   How to execute the PreparedStatment. e.g. `doobie.FPS.executeQuery`
    * @param process
    *   Process steps for the ResultSet
    * @param loggingInfo
    *   Information for logging
    */
  def executeWithResultSet[A](
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[ResultSet],
      process: ResultSetIO[A],
      loggingInfo: LoggingInfo
  ): ConnectionIO[A] =
    execImpl(
      create,
      prep,
      Right((exec, process)),
      loggingInfo
    )

  def executeWithResultSet[A](
      prepared: PreparedExecution[A],
      loggingInfo: LoggingInfo
  ): ConnectionIO[A] = executeWithResultSet(
    prepared.create,
    prepared.prep,
    prepared.exec,
    prepared.process,
    loggingInfo
  )

  /** Create and execute a PreparedStatement which immediately returns the result without reading from a ResultSet. The
    * most common case is executing an INSERT/UPDATE and it returning the rows inserted/updated. If the query you're
    * executing returns a ResultSet, use `executeWithResultSet` instead for better logging and resource cleanup.
    *
    * Errors at each step are handled and logged, with the PreparedStatement being closed at the end.
    *
    * For usage examples, see [[doobie.util.update.Update.updateMany]] which is built on this function
    * @param create
    *   Create the PreparedStatement, using e.g. `doobie.FC.prepareStatement`
    * @param prep
    *   Prepare steps before execution, such as setting parameters
    * @param exec
    *   How the PreparedStatement will be executed. e.g. `doobie.FPS.executeQuery`
    * @param loggingInfo
    *   Information for logging
    */
  def executeWithoutResultSet[A](
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[A],
      loggingInfo: LoggingInfo
  ): ConnectionIO[A] =
    execImpl(
      create,
      prep,
      Left(exec),
      loggingInfo
    )

  def executeWithoutResultSet[A](
      prepared: PreparedExecutionWithoutProcessStep[A],
      loggingInfo: LoggingInfo
  ): ConnectionIO[A] =
    executeWithoutResultSet(
      prepared.create,
      prepared.prep,
      prepared.exec,
      loggingInfo
    )

  private def execImpl[A](
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      execAndProcess: Either[PreparedStatementIO[A], (PreparedStatementIO[ResultSet], ResultSetIO[A])],
      loggingInfo: LoggingInfo
  ): ConnectionIO[A] = {
    def execAndProcessLogged: PreparedStatementIO[A] = {
      execAndProcess match {
        case Left(execDirectToResult) =>
          attemptTimed(execDirectToResult)
            .flatMap {
              case (Left(e), execDur) =>
                IFPS.performLogging(LogEvent.execFailure(loggingInfo, execDur, e))
                  .flatMap(_ => IFPS.raiseError(e))
              case (Right(a), execDur) =>
                IFPS.performLogging(LogEvent.success(loggingInfo, execDur, Duration.Zero))
                  .as(a)
            }
        case Right((execIO, rsIO)) =>
          attemptTimed(execIO)
            .flatMap {
              case (Left(e), execDur) =>
                IFPS.performLogging(LogEvent.execFailure(loggingInfo, execDur, e))
                  .flatMap(_ => IFPS.raiseError(e))
              case (Right(resultSet), execDur) =>
                IFPS.pure(resultSet).bracket(
                  IFPS.embed(_, processLogged(rsIO, execDur))
                )(
                  IFPS.embed(_, IFRS.close)
                )
            }
      }
    }

    def processLogged(
        process: ResultSetIO[A],
        execDuration: FiniteDuration
    ): ResultSetIO[A] = {
      attemptTimed(process)
        .flatMap {
          case (Left(e), processDuration) =>
            IFRS.performLogging(LogEvent.processingFailure(loggingInfo, execDuration, processDuration, e))
              .flatMap(_ => IFRS.raiseError[A](e))
          case (Right(a), processDuration) =>
            IFRS.performLogging(LogEvent.success(loggingInfo, execDuration, processDuration))
              .as(a)
        }
    }

    val createLogged = create.onError { case e =>
      IFC.performLogging(LogEvent.execFailure(loggingInfo, Duration.Zero, e))
    }

    val prepLogged = prep.onError { case e =>
      IFPS.performLogging(LogEvent.execFailure(loggingInfo, Duration.Zero, e))
    }

    createLogged
      .bracket(ps =>
        WeakAsyncConnectionIO.cancelable(
          IFC.embed(ps, prepLogged *> execAndProcessLogged),
          IFC.embed(ps, IFPS.close)
        ))(IFC.embed(_, IFPS.close))
  }

  /** Execute a PreparedStatement query and provide rows from the ResultSet in chunks
    *
    * For usage examples, see [[doobie.util.query.Query.streamWithChunkSize]] which is implemented on top of this
    * function.
    * @param create
    *   Create the PreparedStatement, using e.g. `doobie.FC.prepareStatement`
    * @param prep
    *   Prepare steps before execution, such as setting parameters
    * @param exec
    *   How the PreparedStatement will be executed. e.g. `doobie.FPS.executeQuery`
    * @param chunkSize
    *   Fetch size to hint to JDBC driver
    * @param loggingInfo
    *   Logging information
    */
  def stream[A: Read](
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[ResultSet],
      chunkSize: Int,
      loggingInfo: LoggingInfo
  ): Stream[ConnectionIO, A] = {
    val execLogged: PreparedStatementIO[ResultSet] =
      attemptTimed(exec)
        .flatMap {
          case (Left(e), execDur) =>
            IFPS.performLogging(LogEvent.execFailure(loggingInfo, execDur, e))
              .flatMap(_ => IFPS.raiseError(e))
          case (Right(resultSet), execDur) =>
            IFPS.performLogging(LogEvent.success(loggingInfo, execDur, Duration.Zero))
              .as(resultSet)
        }

    for {
      ps <- Stream.bracket(runPreExecWithLogging(create, loggingInfo))(IFC.embed(_, IFPS.close))
      _ <- Stream.eval(runPreExecWithLogging(IFC.embed(ps, IFPS.setFetchSize(chunkSize) *> prep), loggingInfo))
      resultSet <- Stream.bracketFull[ConnectionIO, ResultSet](poll =>
        poll(WeakAsyncConnectionIO.cancelable(
          IFC.embed(ps, execLogged),
          IFC.embed(ps, IFPS.close)
        )))((rs, _) => IFC.embed(rs, IFRS.close))
      ele <- repeatEvalChunks(IFC.embed(resultSet, resultset.getNextChunk[A](chunkSize)))
    } yield ele
  }

  // Old implementation, used by deprecated methods
  private def liftStream[A: Read](
      chunkSize: Int,
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[ResultSet]
  ): Stream[ConnectionIO, A] = {

    def prepared(ps: PreparedStatement): Stream[ConnectionIO, PreparedStatement] =
      Stream.eval[ConnectionIO, PreparedStatement] {
        val fs = IFPS.setFetchSize(chunkSize)
        IFC.embed(ps, fs *> prep).map(_ => ps)
      }

    def unrolled(rs: ResultSet): Stream[ConnectionIO, A] =
      repeatEvalChunks(IFC.embed(rs, resultset.getNextChunk[A](chunkSize)))

    val preparedStatement: Stream[ConnectionIO, PreparedStatement] =
      Stream.bracket(create)(IFC.embed(_, IFPS.close)).flatMap(prepared)

    def results(ps: PreparedStatement): Stream[ConnectionIO, A] = {
      Stream.bracket(IFC.embed(ps, exec))(IFC.embed(_, IFRS.close)).flatMap(unrolled)
    }

    preparedStatement.flatMap(results)

  }

  private def runPreExecWithLogging[A](connio: ConnectionIO[A], loggingInfo: LoggingInfo): ConnectionIO[A] = {
    connio.onError { case e =>
      // Duration is zero because we haven't actually gotten to "executing the SQL" yet
      IFC.performLogging(
        LogEvent.execFailure(loggingInfo, Duration.Zero, e)
      )
    }
  }

  private def calcDuration(a: Long, b: Long): FiniteDuration = FiniteDuration((a - b).abs, NANOSECONDS)

  private def attemptTimed[F[_]: Sync, A](fa: F[A]): F[(Either[Throwable, A], FiniteDuration)] = {
    for {
      start <- Sync[F].delay(System.nanoTime())
      res <- fa.attempt
      end <- Sync[F].delay(System.nanoTime())
    } yield (res, calcDuration(start, end))
  }

  /** Construct a prepared statement from the given `sql`, configure it with the given `PreparedStatementIO` action, and
    * return results via a `Stream`.
    *
    * @group Prepared Statements
    */
  @deprecated("Use the other stream which supports logging", "1.0.0-RC6")
  def stream[A: Read](sql: String, prep: PreparedStatementIO[Unit], chunkSize: Int): Stream[ConnectionIO, A] =
    liftStream(chunkSize, IFC.prepareStatement(sql), prep, IFPS.executeQuery)

  /** Construct a prepared update statement with the given return columns (and readable destination type `A`) and sql
    * source, configure it with the given `PreparedStatementIO` action, and return the generated key results via a
    * `Stream`.
    *
    * @group Prepared Statements
    */
  @deprecated(
    "Consider using Update#withGeneratedKeys or " +
      "doobie.HC.stream if you need more customization",
    "1.0.0-RC6")
  def updateWithGeneratedKeys[A: Read](cols: List[String])(
      sql: String,
      prep: PreparedStatementIO[Unit],
      chunkSize: Int
  ): Stream[ConnectionIO, A] =
    liftStream(
      chunkSize = chunkSize,
      create = IFC.prepareStatement(sql, cols.toArray),
      prep = prep,
      exec = IFPS.executeUpdate *> IFPS.getGeneratedKeys
    )

  /** @group Prepared Statements */
  @deprecated(
    "Consider using Update#updateManyWithGeneratedKeys or " +
      "doobie.HC.stream if you need more customization",
    "1.0.0-RC6")
  def updateManyWithGeneratedKeys[F[_]: Foldable, A: Write, B: Read](
      cols: List[String]
  )(
      sql: String,
      prep: PreparedStatementIO[Unit],
      fa: F[A],
      chunkSize: Int
  ): Stream[ConnectionIO, B] = {
    liftStream[B](
      chunkSize = chunkSize,
      create = IFC.prepareStatement(sql, cols.toArray),
      prep = prep,
      exec = IHPS.addBatchesAndExecute(fa) *> IFPS.getGeneratedKeys
    )
  }

  /** @group Transaction Control */
  val commit: ConnectionIO[Unit] =
    IFC.commit

  /** Construct an analysis for the provided `sql` query, given writable parameter type `A` and readable resultset row
    * type `B`.
    */
  def prepareQueryAnalysis[A: Write, B: Read](sql: String): ConnectionIO[Analysis] =
    prepareAnalysis(sql, IHPS.getParameterMappings[A], IHPS.getColumnMappings[B])

  def prepareQueryAnalysis0[B: Read](sql: String): ConnectionIO[Analysis] =
    prepareAnalysis(sql, IFPS.pure(Nil), IHPS.getColumnMappings[B])

  def prepareUpdateAnalysis[A: Write](sql: String): ConnectionIO[Analysis] =
    prepareAnalysis(sql, IHPS.getParameterMappings[A], IFPS.pure(Nil))

  def prepareUpdateAnalysis0(sql: String): ConnectionIO[Analysis] =
    prepareAnalysis(sql, IFPS.pure(Nil), IFPS.pure(Nil))

  private def prepareAnalysis(
      sql: String,
      params: PreparedStatementIO[List[(Put[?], Nullability.NullabilityKnown) `Ior` ParameterMeta]],
      columns: PreparedStatementIO[List[(Get[?], Nullability.NullabilityKnown) `Ior` ColumnMeta]]
  ) = {
    val mappings = prepareStatementPrimitive(sql) {
      (params, columns).tupled
    }
    (getMetaData(IFDMD.getDriverName), mappings).mapN { case (driver, (p, c)) =>
      Analysis(driver, sql, p, c)
    }
  }

  /** @group Statements */
  def createStatement[A](k: StatementIO[A]): ConnectionIO[A] =
    IFC.createStatement.bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFS.close))

  /** @group Statements */
  def createStatement[A](rst: ResultSetType, rsc: ResultSetConcurrency)(k: StatementIO[A]): ConnectionIO[A] =
    IFC.createStatement(rst.toInt, rsc.toInt).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFS.close))

  /** @group Statements */
  def createStatement[A](
      rst: ResultSetType,
      rsc: ResultSetConcurrency,
      rsh: Holdability
  )(k: StatementIO[A]): ConnectionIO[A] =
    IFC.createStatement(rst.toInt, rsc.toInt, rsh.toInt).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFS.close))

  /** @group Connection Properties */
  val getCatalog: ConnectionIO[String] =
    IFC.getCatalog

  /** @group Connection Properties */
  def getClientInfo(key: String): ConnectionIO[Option[String]] =
    IFC.getClientInfo(key).map(Option(_))

  /** @group Connection Properties */
  val getClientInfo: ConnectionIO[Map[String, String]] =
    IFC.getClientInfo.map(propertiesToScala)

  /** @group Connection Properties */
  val getHoldability: ConnectionIO[Holdability] =
    IFC.getHoldability.flatMap(Holdability.fromIntF[ConnectionIO])

  /** @group Connection Properties */
  def getMetaData[A](k: DatabaseMetaDataIO[A]): ConnectionIO[A] =
    IFC.getMetaData.flatMap(s => IFC.embed(s, k))

  /** @group Transaction Control */
  val getTransactionIsolation: ConnectionIO[TransactionIsolation] =
    IFC.getTransactionIsolation.flatMap(TransactionIsolation.fromIntF[ConnectionIO])

  /** @group Connection Properties */
  val isReadOnly: ConnectionIO[Boolean] =
    IFC.isReadOnly

  /** @group Callable Statements */
  def prepareCall[A](
      sql: String,
      rst: ResultSetType,
      rsc: ResultSetConcurrency
  )(k: CallableStatementIO[A]): ConnectionIO[A] =
    IFC.prepareCall(sql, rst.toInt, rsc.toInt).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFCS.close))

  /** @group Callable Statements */
  def prepareCall[A](sql: String)(k: CallableStatementIO[A]): ConnectionIO[A] =
    IFC.prepareCall(sql).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFCS.close))

  /** @group Callable Statements */
  def prepareCall[A](
      sql: String,
      rst: ResultSetType,
      rsc: ResultSetConcurrency,
      rsh: Holdability
  )(k: CallableStatementIO[A]): ConnectionIO[A] =
    IFC.prepareCall(sql, rst.toInt, rsc.toInt, rsh.toInt).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFCS.close))

  /** @group Prepared Statements */
  @deprecated(
    "Switch to use doobie.free.FC.preparedStatement (in combination with doobie.hi.FC.execute{With,Without}ResultSet) to get logging and query cancellation support",
    "1.0-RC6"
  )
  def prepareStatement[A](
      sql: String,
      rst: ResultSetType,
      rsc: ResultSetConcurrency
  )(k: PreparedStatementIO[A]): ConnectionIO[A] =
    IFC.prepareStatement(sql, rst.toInt, rsc.toInt).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFPS.close))

  /** @group Prepared Statements */
  @deprecated(
    "Switch to use doobie.free.FC.preparedStatement (in combination with doobie.hi.FC.execute{With,Without}ResultSet) to get logging and query cancellation support",
    "1.0-RC6"
  )
  def prepareStatement[A](sql: String)(k: PreparedStatementIO[A]): ConnectionIO[A] =
    IFC.prepareStatement(sql).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFPS.close))

  /** @group Prepared Statements */
  @deprecated(
    "Switch to use doobie.free.FC.preparedStatement (in combination with doobie.hi.FC.execute{With,Without}ResultSet) to get logging and query cancellation support",
    "1.0-RC6"
  )
  def prepareStatement[A](
      sql: String,
      rst: ResultSetType,
      rsc: ResultSetConcurrency,
      rsh: Holdability
  )(k: PreparedStatementIO[A]): ConnectionIO[A] =
    IFC.prepareStatement(sql, rst.toInt, rsc.toInt, rsh.toInt).bracket(s => IFC.embed(s, k))(s =>
      IFC.embed(s, IFPS.close))

  /** @group Prepared Statements */
  @deprecated(
    "Switch to use doobie.free.FC.preparedStatement (in combination with doobie.hi.FC.execute{With,Without}ResultSet) to get logging and query cancellation support",
    "1.0-RC6"
  )
  def prepareStatement[A](sql: String, agk: AutoGeneratedKeys)(k: PreparedStatementIO[A]): ConnectionIO[A] =
    IFC.prepareStatement(sql, agk.toInt).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFPS.close))

  /** @group Prepared Statements */
  @deprecated(
    "Switch to use doobie.free.FC.preparedStatementI (in combination with doobie.hi.FC.execute{With,Without}ResultSet) to get logging and query cancellation support",
    "1.0-RC6"
  )
  def prepareStatementI[A](sql: String, columnIndexes: List[Int])(k: PreparedStatementIO[A]): ConnectionIO[A] =
    IFC.prepareStatement(sql, columnIndexes.toArray).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFPS.close))

  /** @group Prepared Statements */
  @deprecated(
    "Switch to use doobie.free.FC.preparedStatementS (in combination with doobie.hi.FC.execute{With,Without}ResultSet) to get logging and query cancellation support",
    "1.0-RC6"
  )
  def prepareStatementS[A](sql: String, columnNames: List[String])(k: PreparedStatementIO[A]): ConnectionIO[A] =
    IFC.prepareStatement(sql, columnNames.toArray).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFPS.close))

  // Helper to create a PreparedStatement, but without any
  // support for query cancellation or logging
  private[doobie] def prepareStatementPrimitive[A](sql: String)(k: PreparedStatementIO[A]): ConnectionIO[A] =
    IFC.prepareStatement(sql).bracket(s => IFC.embed(s, k))(s => IFC.embed(s, IFPS.close))

  /** @group Transaction Control */
  def releaseSavepoint(sp: Savepoint): ConnectionIO[Unit] =
    IFC.releaseSavepoint(sp)

  /** @group Transaction Control */
  def rollback(sp: Savepoint): ConnectionIO[Unit] =
    IFC.rollback(sp)

  /** @group Transaction Control */
  val rollback: ConnectionIO[Unit] =
    IFC.rollback

  /** @group Connection Properties */
  def setCatalog(catalog: String): ConnectionIO[Unit] =
    IFC.setCatalog(catalog)

  /** @group Connection Properties */
  def setClientInfo(key: String, value: String): ConnectionIO[Unit] =
    IFC.setClientInfo(key, value)

  /** @group Connection Properties */
  def setClientInfo(info: Map[String, String]): ConnectionIO[Unit] =
    IFC.setClientInfo {
      // Java 11 overloads the `putAll` method with Map[*,*] along with the existing Map[Obj,Obj]
      val ps = new java.util.Properties
      info.foreach { case (k, v) =>
        ps.put(k, v)
      }
      ps
    }

  /** @group Connection Properties */
  def setHoldability(h: Holdability): ConnectionIO[Unit] =
    IFC.setHoldability(h.toInt)

  /** @group Connection Properties */
  def setReadOnly(readOnly: Boolean): ConnectionIO[Unit] =
    IFC.setReadOnly(readOnly)

  /** @group Transaction Control */
  val setSavepoint: ConnectionIO[Savepoint] =
    IFC.setSavepoint

  /** @group Transaction Control */
  def setSavepoint(name: String): ConnectionIO[Savepoint] =
    IFC.setSavepoint(name)

  /** @group Transaction Control */
  def setTransactionIsolation(ti: TransactionIsolation): ConnectionIO[Unit] =
    IFC.setTransactionIsolation(ti.toInt)

  // /**
  //  * Compute a map from native type to closest-matching JDBC type.
  //  * @group MetaData
  //  */
  // val nativeTypeMap: ConnectionIO[Map[String, JdbcType]] = {
  //   getMetaData(IFDMD.getTypeInfo.flatMap(IFDMD.embed(_, HRS.list[(String, JdbcType)].map(_.toMap))))
  // }

  final case class PreparedExecution[A](
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[ResultSet],
      process: ResultSetIO[A]
  )

  final case class PreparedExecutionWithoutProcessStep[A](
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[A]
  )
}
