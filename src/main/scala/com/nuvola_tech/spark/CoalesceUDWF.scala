package com.nuvola_tech.spark

import com.nuvola_tech.spark.SessionUDWF.SessionUDWF
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._


object CoalesceUDWF {

  case class CoalesceUDWF(intervalStart: Expression, intervalEnd: Expression)
    extends AggregateWindowFunction {

    override def children: Seq[Expression] = Seq(intervalStart, intervalEnd)

    val coalescedIntervalStart: AttributeReference = AttributeReference("coalescedIntervalStart", IntegerType, nullable = true)()

    val coalescedIntervalEnd: AttributeReference = AttributeReference("coalescedIntervalEnd", IntegerType, nullable = true)()

    protected val nullInt = Literal(0: Int)

    override def aggBufferAttributes: Seq[AttributeReference] = Seq(coalescedIntervalStart, coalescedIntervalEnd)

    override val initialValues: Seq[Expression] = Seq(nullInt, nullInt)

    override def dataType: DataType = DataTypes.createArrayType(IntegerType)

    override val updateExpressions: Seq[Expression] = Seq(
      If(
        EqualTo(coalescedIntervalStart, nullInt),
        intervalStart,
        If(
          LessThan(coalescedIntervalEnd, intervalStart),
          intervalStart,
          coalescedIntervalStart
        )
      ),
      If(
        EqualTo(coalescedIntervalEnd, nullInt),
        intervalEnd,
        If(LessThanOrEqual(coalescedIntervalEnd, intervalEnd),
          intervalEnd,
          coalescedIntervalEnd)
      )
    )

    override val evaluateExpression: Expression = CreateArray(Seq(coalescedIntervalStart, coalescedIntervalEnd))
    //override val evaluateExpression: Expression = Subtract(coalescedIntervalEnd,coalescedIntervalStart)
    //override val evaluateExpression: Expression = coalescedIntervalEnd
  }

  def calculateCoalescedInterval(intervalStart: Column, intervalEnd: Column): Column = withExpr {
    CoalesceUDWF(intervalStart.expr, intervalEnd.expr)
  }

  private def withExpr(expr: Expression): Column = new Column(expr)

}

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window

//import org.apache.spark.sql.SparkSession.implicits._

case class IntervalData(id: Int, start: Int, end: Int)

object CoalesceUDWFDemo {
  def main(args: Array[String]): Unit = {

    val d: Array[IntervalData] = Array[IntervalData](
      IntervalData(1, 1, 2),
      IntervalData(1, 1, 3),
      IntervalData(1, 4, 6),
      IntervalData(1, 5, 6),
      IntervalData(1, 5, 7),
      IntervalData(1, 8, 8),
      IntervalData(1, 8, 9),
      IntervalData(1, 10, 12),
      IntervalData(2, 1, 2),
      IntervalData(2, 1, 3),
      IntervalData(2, 4, 6),
      IntervalData(2, 5, 6)

    )

    val spark = SparkSession
      .builder()
      .appName("Spark SQL user-defined Datasets aggregation example")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val sqlContext = spark.sqlContext

    val df = sqlContext.createDataFrame(sc.parallelize(d))

    val specs = Window.partitionBy($"id").orderBy($"start".asc).orderBy($"end".asc)
    val res = df.withColumn("coalesced", CoalesceUDWF.calculateCoalescedInterval($"start", $"end") over specs)

    df.show(20)

    res.show(20, truncate = false)
    res.explain()
  }

}
