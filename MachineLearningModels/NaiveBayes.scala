/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the LICENSE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}

// Import data
val spark = SparkSession.builder().getOrCreate()
var data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("../dataML/*.csv")

// That array contains label columns
val ColumnsLabel = Array("Assigned Group")
// Array with not relevant columns
val ColumnsNotRelevant = Array("Urgency", "Impact", "Priority")
for ( i <- ColumnsNotRelevant ) {
data = data.drop(i)
}
// Array with real numerical columns
val ColumnsNumerical = Array("Priority Weight")
// Prueba con tratamiento de todas las entradas como numerico
// (mejora la precision la precision para este modelo)
//val ColumnsNumerical = data.columns.diff(ColumnsLabel)


// Seleccionamos la columna label (cambiada por for después)
//val SelectedLabel = ColumnsLabel(0)
for ( SelectedLabel <- ColumnsLabel ) {

  // Eliminamos el resto de labels
  var logregdataall = data
  for ( i <- ColumnsLabel.diff(Array(SelectedLabel)) ) {
    logregdataall = logregdataall.drop(i)
  }

  // Renombramos el label a "label" y eliminamos los registros con nulos
  val logregdata = logregdataall.withColumnRenamed(SelectedLabel, "label").na.drop()

  // Generamos un array con todas las columnas categoricas (incluida la columna "label" si procede)
  val ColumnsCategorical = if ( ColumnsNumerical.contains(Array(SelectedLabel)) ) {
    logregdata.columns.diff(ColumnsNumerical).diff(Array("label"))
  } else {
    logregdata.columns.diff(ColumnsNumerical)
  }

  // Generamos un array con los StringIndexer y los Encoders correspondientes
  var arrayIndexers: Array[PipelineStage] = Array()
  var arrayEncoders: Array[PipelineStage] = Array()

  for ( i <- ColumnsCategorical ) {
    arrayIndexers ++= Array[PipelineStage]( new StringIndexer()
      .setInputCol(i)
      .setOutputCol( "indexed" ++ i.capitalize )
      .setHandleInvalid("skip") )
    arrayEncoders ++= Array[PipelineStage]( new OneHotEncoder()
      .setInputCol( "indexed" ++ i.capitalize )
      .setOutputCol( "encoded" ++ i.capitalize ) )
  }

  // Assemble everything together to be ("label","features") format
  var ColumnsFeatures = ColumnsNumerical.diff(ColumnsLabel)
  for ( i <- ColumnsCategorical.diff(Array("label")) ) {
    ColumnsFeatures ++= Array( "encoded" ++ i.capitalize )
  }

  val assembler = ( new VectorAssembler()
    .setInputCols(ColumnsFeatures)
    .setOutputCol("features") )

  // Split the data into training and test sets (30% held out for testing)
  val Array(trainingData, testData) = logregdata.randomSplit(Array(0.7, 0.3))


  // Elección del modelo
  val nv = new NaiveBayes().setLabelCol("indexedLabel")

  // Chain indexers and tree in a Pipeline.
  val pipeline = ( new Pipeline()
    .setStages(arrayIndexers ++ arrayEncoders ++ Array(assembler, nv)) )

  val model = pipeline.fit(trainingData)

  val predictions = model.transform(testData)

  // Accuracy
  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
  println(SelectedLabel)
  println( evaluator.evaluate(predictions) )


  /************************CONTAR LOS CAMBIOS DE GRUPO NECESARIOS PARA ACERTAR*********************************/
  // Creamos una udf que nos devuelva en qué número de intento se asigna el grupo correcto
  val steps = udf {( p : org.apache.spark.ml.linalg.Vector, l : Double ) =>
    val array = p.toArray
    val indexArray = array.map(_.toDouble)
    val sortedArray = indexArray.sorted.reverse
    // Buscamos en la lista ordenada de probabilidades qué posición ocupa
    // la probabilidad correspondiente al label real
    sortedArray.indexOf( indexArray(l.toInt) ) + 1
  }

  // Generamos la columna correspondiente al número de grupos asignados para cada registro
  // y luego los agrupamos por ese valor y los contamos
  val n_assign = ( predictions.select("indexedLabel", "probability","prediction")
  .withColumn("Groups_assigned", steps(predictions("probability"),predictions("indexedLabel")))
  .groupBy("Groups_assigned").count().sort("Groups_assigned") )

  // Lo salvamos a un fichero csv
  n_assign.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("n_groups_NB")
  /***********************************************************************************************************/

}
