// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._

// Create DataFrame and read csv
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").option("delimiter",";").csv("data.csv")


// Distinct IDs
val nID = df.select("ID").distinct.count

// Numero de registros totales
val nReg = df.count

// Distinct IDs without last register (clousure register)
val df2 = df.filter(($"Status" !== "Resolved  (4)") && ($"Status" !== "Cancelled  (6)"))
val nID_filt = df2.select("ID").distinct.count

// Creamos la variable que almacenara una lista de tuplas de la forma (Nombre de columna, % de constancia, % de nulos)
val b : ArrayBuffer[(String, Double, Double)] = ArrayBuffer()

// Damos valores a la lista dinamica
for( i <- df2.columns.drop(2)) b += ((i, df2.select("ID", i).distinct.groupBy("ID").count.filter($"count" === 1).count.toDouble/nID_filt, (df.filter(df(i).isNull).count + df.filter(df(i) === "t9jZkpJT4Nc=").count).toDouble/nReg))

// La pasamos a lista estatica
val ColumnasSeleccionadas = b.toArray

// Visualizacion ordenada de los datos extraidos
//for ( i <- ColumnasSeleccionadas ) println(i)

// Seleccionamos los limites para el % de constancia y de % de nulos (mínimo y máximo respectivamente)
val Lim_Const = 0.8
val Lim_nulls = 0.5

// Seleccionamos manualmente las columnas cuyo dato es el objetivo de la prediccion (targets)
val ColumnasObjetivo = Array("Closure Product Category Tier3", "Assigned Group")

// Copiamos el DataFrame para irlo reduciendo progresivamente (aprovechamos para eliminar la fecha)
var df_reducido = df.drop("DATE")

// Creamos un Array dinamico (ArrayBuffer) donde almacenar los nombres de las columnas que tomaremos como entradas del modelo ML
var ColumnasEntrada = ArrayBuffer[String]()

// Eliminamos las columnas que no superen el criterio (dejamos aún la columna "ID")
for ( i <- ColumnasSeleccionadas ) {
	// testeamos si hay que conservar la columna por ser target
	var aux = false
	for ( j <- ColumnasObjetivo ) {
		if ( i._1 == j ) aux = true;
	}
	// Comprobamos si hay que conservar la columna a pesar de los criterios de Lim_Const y Lim_nulls
	if ( i._1 == "ID" || aux ) {
	}else if ( i._2 < Lim_Const || i._3 > Lim_nulls) {
		df_reducido = df_reducido.drop(i._1);
	}else ColumnasEntrada += i._1
}

// Generamos el map necesario para seleccionar los valores del primer y 
// último registro según si es dato de entrada u objetivo respectivamente
val map_columns = (ColumnasEntrada.map( _ -> "first") ++ ColumnasObjetivo.map( _ -> "last")).toMap

// Generamos el DataFrame con las reducciones apropiadas según el Map anterior
// y eliminamos la columna ID que ya no aporta información
val df_combinado = df_reducido.groupBy("ID").agg(map_columns).drop("ID")


// Crear DataFrame con columnas ordenadas
// Primero creamos un "Array" de las columnas ordenadas (tipo columna)
val mySortedCols = df_combinado.columns.sorted.map(str => col(str))
// Ejecutamos un select sobre estas columnas
val df_CombinadoSorted = df_combinado.select(mySortedCols:_*)



var df_tmp = df_CombinadoSorted	// DataFrame para ir modificando
// Seleccionamos la diferentes columnas según el tipo de datos almacenados (para su procesamiento apropiado)
val ColConNumeros = Array("first(Urgency)", "first(Impact)", "first(Priority)")
val ColNumericas = Array("first(Priority Weight)")
val ColString = df_tmp.columns.diff(ColConNumeros).diff(ColNumericas)

// Hacemos nulos los valores con el Hash que sabemos que es equivalente "t9jZkpJT4Nc="
for ( i <- df_tmp.columns ) df_tmp = df_tmp.withColumn( i, when( df_tmp(i) !== "t9jZkpJT4Nc=", df_tmp(i)) )

// Buscamos en las columnas con valores númericos mezclados con texto sólo los numeros
val numPattern = "[0-9]+".r

val findNumber = udf {columna: String => numPattern.findFirstIn(columna).getOrElse("null")}

for  (columna <- ColConNumeros) df_tmp = df_tmp.withColumn(columna, when(df_tmp(columna).isNull, lit(null)).otherwise(findNumber(df_tmp(columna))))

// Pasamos a entero la columna "first(Priority Weight)" que ya era "numerica" y las nuevas extraidas de los campos alfanumericos
for (i <- (ColConNumeros ++ ColNumericas) ) df_tmp = df_tmp.withColumn( i, df_tmp(i).cast(IntegerType) )


// Creamos un array único para todos los valores 
var valoresPosibles = Array[String]()
for ( i <- ColString ) valoresPosibles = valoresPosibles ++ df_tmp.select(i).filter(df_tmp(i).isNotNull).distinct.collect.map(_.getString(0))
valoresPosibles = valoresPosibles.distinct

// Esta funcion reemplaza el String por la posicion que ocupa (el indice) dentro del vector (+1 para evitar el 0)
val replaceStringToNumber = udf {columna: String => valoresPosibles.indexOf(columna)+1}
// Ejecutamos la sustitucion sobre las columnas cifradas
for (columna <- ColString) df_tmp = df_tmp.withColumn(columna, when(df_tmp(columna).isNull, lit(null)).otherwise( replaceStringToNumber(df_tmp(columna))))







