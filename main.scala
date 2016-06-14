import scala.annotation.tailrec
import scala.math._
import BootstrapAggregator._

trait IntegerDistribution {
  def density(k: Int): Double

  val cdf = _makeCdf(Vector.empty)

  val tolerance: Double = 10e-10

  @tailrec
  private def _makeCdf(partialCdf: Vector[Double]): Vector[Double] =

    if (partialCdf.isEmpty)
       _makeCdf(Vector(density(0)))
    else {
      val sum = partialCdf.last
      if (sum >= (1.0 - tolerance))
        return partialCdf.take(partialCdf.size - 1) :+ 1.0

      _makeCdf(partialCdf :+ (sum + density(partialCdf.length)))
    }


  def getValueFromUniform(x: Double): Int = {
    require(x >= 0.0)
    require(x <= 1.0)

    var index = 0

    while (x > cdf(index)) {
      index = index + 1
    }

    index
  }
}

case class PoissonDistribution(lambda: Double) extends IntegerDistribution {
  def factorial(n: Int): Int = {
    if (n == 0)
      1
    else
      n * factorial(n - 1)
  }

  override def density(k: Int): Double = {
    if (k < 0)
      0.0
    else
      pow(lambda, k) * exp(-lambda) / factorial(k)
  }
}

trait MonoidAggregator[A, B, C] {

  def zero : B

  def prepare(a: A): B

  def operation(a1: B, a2: B): B

  def present(b: B): C

  def compute(in: TraversableOnce[A]): C = {
    present(reduce(in.map(prepare)))
  }

  def reduce(items: TraversableOnce[B]): B =
  {
    items.foldLeft(zero){operation}
  }
}

case class SumAgg() extends MonoidAggregator[Int, Int, Int] {
  def zero = 0

  def prepare(a: Int) = a

  def operation(a1: Int, a2: Int) = a1 + a2

  def present(b: Int) = b
}

case class BootstrapState[B](normalState:B, normalCount:Long, bootstraps: Vector[(B, Long)])

case class BootstrapResult[C](normalResult:C, normalCount:Long, bootstraps: Seq[(C, Long)])

case class Bootstrapper[A, B, C](distribution: IntegerDistribution, bootstrapCount: Int, agg: MonoidAggregator[A, B, C])
  extends MonoidAggregator[A, BootstrapState[B], BootstrapResult[C]] {

  def zero = {
    var s = Vector[(B, Long)]()
    for (i <- 1 to bootstrapCount) {
      s = s :+(agg.zero, 0L)
    }

    BootstrapState(agg.zero, 0L, s)
  }


  def prepare(a: A) = {
    var s = Vector[(B, Long)]()

    val prepared = agg.prepare(a)

    for (i <- 1 to bootstrapCount) {

      val k = distribution.getValueFromUniform(scala.util.Random.nextDouble)
      var res = agg.zero

      for (j <- 1 to k) {
        res = agg.operation(res, prepared)
      }

      s = s :+(res, k.toLong)
    }

    BootstrapState(prepared, 1L, s)
  }

  def operation(a1: BootstrapState[B], a2: BootstrapState[B]): BootstrapState[B] = {
    require(a1.bootstraps.length == a2.bootstraps.length)
    require(a1.bootstraps.length == bootstrapCount)

    val zipped = a1.bootstraps zip a2.bootstraps
    val zippedBoostraps = zipped.map(
      x => (agg.operation(x._1._1, x._2._1), x._1._2 + x._2._2)
    )

    BootstrapState(agg.operation(a1.normalState, a2.normalState), a1.normalCount+a2.normalCount, zippedBoostraps)
  }

  def present(b: BootstrapState[B]) = {
    BootstrapResult[C](agg.present(b.normalState), b.normalCount, b.bootstraps.map(x => (agg.present(x._1), x._2)))
  }
}

object BootstrapAggregator {

  implicit def bootstrapMaker[A, B, C](agg: MonoidAggregator[A, B, C]) = new {
    def withBootstraps(distribution: IntegerDistribution)(bootstrapCount: Int): Bootstrapper[A, B, C] = {
      Bootstrapper[A, B, C](distribution, bootstrapCount, agg)
    }
  }

}


object o {
  def main(a: Array[String]) = {
    //println("a")


   // println(PoissonDistribution(1.0).cdf)
    //println(PoissonDistribution(2.0).cdf)


    val distribution = PoissonDistribution(1.0)
    val input = Seq(1, 2, 4, 5, 10)
    val mySumAggregator = SumAgg()
    val mySumAggregatorWithBootstraps = mySumAggregator.withBootstraps(distribution)(4)
    val result = mySumAggregatorWithBootstraps.compute(input)
    println(result)

    println(result.bootstraps.map(x => x._1).sum.toDouble / result.bootstraps.length)


  }

}