
/*
 * Copyright (C) 2018  by Mariem Brahem
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package fr.uvsq.adam.astroide.executor

import java.io.IOException

import adql.parser.{ADQLParser, ParseException}
import adql.translator.TranslationException
import fr.uvsq.adam.astroide.AstroideUDF
import fr.uvsq.adam.astroide.optimizer.MainTranslator
import fr.uvsq.adam.astroide.util.{Arguments, DirCheck}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source


/**
  * Execute the main ADQL queries using query rewriting
  *
  */

object AstroideRewriter {


  val usage =

    """
      				  |Usage: AstroideRewriter [-fs hdfs://...] infile infile2 healpixlevel queryfile action
    			""".stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {

      case Nil ⇒ map
      case "-fs" :: hdfs :: tail =>
        parseArguments(map ++ Map('hdfs -> hdfs), tail)
      case infile :: infile2 :: healpixlevel :: queryfile :: action :: Nil ⇒
        map ++ Map('infile -> infile) ++ Map('infile2 -> infile2) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('queryfile -> queryfile) ++ Map('action -> action)
      case infile :: infile2 :: healpixlevel :: queryfile :: Nil ⇒
        map ++ Map('infile -> infile) ++ Map('infile2 -> infile2) ++ Map('healpixlevel -> healpixlevel.toInt) ++ Map('queryfile -> queryfile) ++ Map('action -> None)
      case option :: tail ⇒
        println(usage)
        throw new IllegalArgumentException(s"Unknown argument $option");
    }

  }

  def checkFile(file: String, hdfs: String) = {
    if (!DirCheck.dirExists(file, hdfs)) {
      throw new IOException("Input file " + file + " does not exist in HDFS")
    }
    else if (FilenameUtils.getExtension(file) != "parquet")
      throw new Exception("Input file " + file + " should be partitioned in parquet format")

    else if (!DirCheck.dirParquet(file, hdfs)) {
      throw new Exception("Input file " + file + " should be partitioned in parquet format\n Please use: " + BuildHealpixPartitioner.usage)
    }
  }

  def checkOrder(order: Int) = {
    if (order < 0 || order > 29) {
      throw new Exception("HEALPix order should be in range [0,29]")
    }
  }

  def checkAction(action: String) = {
    val ListAction = List("count", "show", "save")
    if (!ListAction.contains(action))
      throw new Exception("Action should be listed in " + ListAction)
  }

  def printResult(action: String, result: DataFrame) = action match {

    case ("count") => println(result.rdd.count())
    case ("show") => result.show(1000)
    case ("save") => {
      val resultFile = variables.getFile1.substring(0, variables.getFile1.lastIndexOf("/") + 1)
      result.write.mode(SaveMode.Overwrite).format("csv").save(resultFile + "queryResult")
      //for duplicated column, add an alias
      println("Query result is saved in HDFS, please check directory " + resultFile + "queryResult")
    }
    case ("None") =>
  }

  var variables = new Arguments()

  def main(args: Array[String]) {

    val configuration = parseArguments(Map(), args.toList)
    println(configuration)

    variables.setFile1(configuration('infile).toString)
    variables.setFile2(configuration('infile2).toString)
    variables.setOrder(configuration('healpixlevel).asInstanceOf[Int])
    variables.setQueryFile(configuration('queryfile).toString)
    variables.setHDFS(configuration('hdfs).toString)
    val action = configuration('action).toString

    checkFile(variables.getFile1, variables.getHDFS())
    checkOrder(variables.getOrder())
    val spark = SparkSession.builder().appName("astroide").getOrCreate()

    AstroideUDF.RegisterUDF(spark, variables.getOrder())

    var testQuery = Source.fromFile(variables.getQueryFile()).getLines.mkString

    val inputData = spark.read.parquet(variables.getFile1())


    try {
      val parser = new ADQLParser()
      val query = parser.parseQuery(testQuery)
      println("Correct ADQL Syntax")

      val from = new SearchFrom()
      from.search(query)

      val ntables = from.getNbMatch
      val it = from.iterator()

      val join = new SearchJoin()
      join.search(query)

      val njoin = join.getNbMatch

      if (ntables == 1) {
        println("test")
        inputData.createOrReplaceTempView(it.next().toADQL)
      }
      else if (ntables == 2) {
        if (njoin == 1) {
          inputData.createOrReplaceTempView(it.next().toADQL)

          checkFile(variables.getFile2(), variables.getHDFS())

          val inputData2 = spark.read.parquet(variables.getFile2())

          inputData2.createOrReplaceTempView(it.next().toADQL)
        }
        else {
          it.next()
          inputData.createOrReplaceTempView(it.next().toADQL)
        }
      }
      else if (ntables == 3) {
        it.next()

        inputData.createOrReplaceTempView(it.next().toADQL)
        checkFile(variables.getFile2(), variables.getHDFS())

        val inputData2 = spark.read.parquet(variables.getFile2())

        inputData2.createOrReplaceTempView(it.next().toADQL)
      }
      else throw new Exception("This case will be supported in future versions")

      val translatedQuery = MainTranslator.translateWithoutRules(query)
      println(translatedQuery)

      val result = spark.sql(translatedQuery)
      result.explain()

      printResult(action, result)

    } catch {
      case e: ParseException ⇒ println("ADQL syntax incorrect between " + e.getPosition() + ":" + e.getMessage())
      case f: TranslationException ⇒ println("ADQL Translation error " + f.getMessage)
      case p: Exception ⇒ println("Error occurred while executing the query in Spark " + p.getMessage)
    }

    spark.stop()
  }

}

