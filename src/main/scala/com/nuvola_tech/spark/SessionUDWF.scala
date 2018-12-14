package com.nuvola_tech.spark

import java.util.UUID

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


object SessionUDWF {

  val defaultMaxSessionLengthms: Int = 3600 * 1000

  //noinspection ZeroIndexToHead
  case class SessionUDWF(timestamp: Expression, session: Expression,
                         sessionWindow: Expression = Literal(defaultMaxSessionLengthms))
    extends AggregateWindowFunction {
    self: Product =>

    override def children: Seq[Expression] = Seq(timestamp, session)

    override def dataType: DataType = StringType

    protected val zero = Literal(0L)
    protected val nullString = Literal(null: String)

    protected val currentSession: AttributeReference = AttributeReference("currentSession", StringType, nullable = true)()
    protected val previousTs: AttributeReference = AttributeReference("lastTs", LongType, nullable = false)()

    override val aggBufferAttributes: Seq[AttributeReference] = currentSession :: previousTs :: Nil

    protected val assignSession: Expression = If(
      predicate = LessThanOrEqual(Subtract(timestamp, previousTs), sessionWindow),
      trueValue = currentSession,
      falseValue = ScalaUDF(createNewSession, StringType, children = Nil, inputsNullSafe = true :: true :: Nil))

    override val initialValues: Seq[Expression] = nullString :: zero :: Nil

    override val updateExpressions: Seq[Expression] =
      If(IsNotNull(session), session, assignSession) ::
        timestamp ::
        Nil

    override val evaluateExpression: Expression = currentSession

    override def prettyName: String = "makeSession"
  }

  protected val createNewSession = () => org.apache.spark.unsafe.types.UTF8String.fromString(UUID.randomUUID().toString)

  def calculateSession(ts: Column, sess: Column): Column = withExpr {
    SessionUDWF(ts.expr, sess.expr, Literal(defaultMaxSessionLengthms))
  }

  def calculateSession(ts: Column, sess: Column, sessionWindow: Column): Column = withExpr {
    SessionUDWF(ts.expr, sess.expr, sessionWindow.expr)
  }

  private def withExpr(expr: Expression): Column = new Column(expr)
}
