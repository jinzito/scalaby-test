import java.io.{BufferedWriter, File, FileWriter}

/**
  * Created by Evgeny Rybakov on 17.02.2016.
  */
object ScalaApp {
  def main(args: Array[String]) {

    val startTime = System.nanoTime

    var rangeList: List[(Int, Int, String)] = List[(Int, Int, String)]()
    /*
    rangeList = (10, 20, "aaa") :: rangeList
    rangeList = (1, 11, "bbb") :: rangeList
    rangeList = (12, 30, "ccc") :: rangeList
    rangeList = (9, 100, "ddd") :: rangeList
    */

    def getIp(s: String): Int = {
      val a = s.split(".".toArray).map(_.toInt)
      a(0) << 24 | a(1) << 16 | a(2) << 8 | a(3)
    }

    val rangesSource = scala.io.Source.fromFile("resources/ranges.tsv")
    for (line <- rangesSource.getLines()) {
      val cols = line.split("\t").map(_.trim)
      val range = cols(0).split("-").map(_.trim)
      val ip1 = getIp(range(0))
      val ip2 = getIp(range(1))
      rangeList =  ((ip1, ip2, cols(1))) :: rangeList
    }
    rangesSource.close

    var transactionList = List[(Long, Int)]()
    /*
    transactionList = (1000.toLong, 13) :: transactionList
    transactionList = (2000.toLong, 21) :: transactionList
    */

    val transactionsSource = scala.io.Source.fromFile("resources/transactions.tsv")
    for (line <- transactionsSource.getLines()) {
      val cols = line.split("\t").map(_.trim)
      val id:Long = cols(0).toLong
      val ip = getIp(cols(1))
      transactionList = ((id, ip)) :: transactionList
    }
    transactionsSource.close

    def defineSegments(clientId:Long, clientIp:Int):List[(Long, String)] = {
      (rangeList.filter(clientIp >= _._1).filter(clientIp <= _._2).map(_._3).map((clientId, _)))
    }

    val resultList = scala.collection.mutable.Set[(Long, String)]()
    transactionList.foreach(e => defineSegments(e._1, e._2).foreach(z => resultList += z))

    val outputFile = new File("output.tsv")
    val bw = new BufferedWriter(new FileWriter(outputFile))
    bw.write(resultList.map(e => e._1 + "\t" + e._2).mkString("\n"))
    bw.close()

    println("complete in ", System.nanoTime - startTime, "ns")
  }
}