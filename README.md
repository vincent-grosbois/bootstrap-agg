Algebird extension to have aggregators with bootstraps

usage:
```scala
 val distribution = PoissonDistribution(1.0)
    val input = Seq(1, 2, 4, 5, 10)
    val mySumAggregator = SumAgg()
    val mySumAggregatorWithBootstraps = mySumAggregator.withBootstraps(distribution)(4)
    val result = mySumAggregatorWithBootstraps.compute(input)
    println(result)
```

will display:
BootstrapResult(22,5,Vector((30,5), (28,6), (41,10), (33,7)))