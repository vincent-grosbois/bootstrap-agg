
import scala.math._

trait IntegerDistribution {
  def density(k: Int): Double

  val cdf = __makeCdf(Seq.empty)

  val tolerance: Double = 10e-10

  private def __makeCdf(partialCdf: Seq[Double]): Seq[Double] = {
    if (partialCdf.isEmpty)
      return __makeCdf(Seq(density(0)))

    val sum = partialCdf.last
    if (sum >= (1.0 - tolerance))
      return partialCdf.take(partialCdf.size - 1) :+ 1.0

    return __makeCdf(partialCdf ++ Seq(sum + density(partialCdf.length)))
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

trait Aggregator[A, B, C] {
  def prepare(a: A): B

  def operation(a1: B, a2: B): B

  def present(b: B): C

  def zero: B

  def compute(in: Iterable[A]): C = {
    present(in.foldLeft[B](zero)((b, a) => operation(b, prepare(a))))
  }
}

case class SumAgg() extends Aggregator[Int, Int, Int] {
  def zero = 0

  def prepare(a: Int) = a

  def operation(a1: Int, a2: Int) = a1 + a2

  def present(b: Int) = b
}

case class BootstrapResult[C](bootstraps: Seq[(C, Long)])

case class Bootstrapper[A, B, C](distribution: IntegerDistribution, bootstrapCount: Int, agg: Aggregator[A, B, C])
  extends Aggregator[A, Seq[(B, Long)], BootstrapResult[C]] {

  def zero = {
    var s = Seq[(B, Long)]()
    for (i <- 1 to bootstrapCount) {
      s = s :+(agg.zero, 0L)
    }
    s
  }

  def prepare(a: A) = {
    var s = Seq[(B, Long)]()

    for (i <- 1 to bootstrapCount) {

      val k = distribution.getValueFromUniform(scala.util.Random.nextDouble)
      var res = agg.zero
      val prepared = agg.prepare(a)

      for (j <- 1 to k) {
        res = agg.operation(res, prepared)
      }

      s = s :+(res, k.toLong)
    }

    s

  }

  def operation(a1: Seq[(B, Long)], a2: Seq[(B, Long)]): Seq[(B, Long)] = {
    require(a1.length == a2.length)

    val zipped = a1 zip a2
    zipped.map(
      x => (agg.operation(x._1._1, x._2._1), x._1._2 + x._2._2)
    )
  }

  def present(b: Seq[(B, Long)]) = {
    BootstrapResult[C](b.map(x => (agg.present(x._1), x._2)))
  }

}

object o {

  def main(a: Array[String]) = {
    println("a")


    println(PoissonDistribution(1.0).cdf)
    println(PoissonDistribution(2.0).cdf)

    val k = PoissonDistribution(1.0)

    println(k.getValueFromUniform(0.43))

    println(k.getValueFromUniform(scala.util.Random.nextDouble))

    val s = Seq(1, 2, 4, 5, 10, 10)

    val a = Bootstrapper(k, 1000, SumAgg())

    val b = a.compute(s)

    println(b)

    println(b.bootstraps.map(x => x._1).sum.toDouble / b.bootstraps.length)

    
  }

}
