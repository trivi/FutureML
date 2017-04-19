// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

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
val b : ListBuffer[(String, Double, Double)] = ListBuffer()

// Damos valores a la lista dinamica
for( i <- df2.columns.drop(2)) b += ((i, df2.select("ID", i).distinct.groupBy("ID").count.filter($"count" === 1).count.toDouble/nID_filt, (df.filter(df(i).isNull).count + df.filter(df(i) === "t9jZkpJT4Nc=").count).toDouble/nReg))

// La pasamos a lista estatica
val ColumnasSeleccionadas = b.toList

// Visualizacion ordenada de los datos extraidos
for ( i <- ColumnasSeleccionadas) println(i)

// Seleccionamos los limites para el % de constancia y de % de nulos (mínimo y máximo respectivamente)
val Lim_Const = 0.8
val Lim_nulls = 0.5

// Seleccionamos manualmente las columnas cuyo dato es el objetivo de la prediccion (targets)
val ColumnasObjetivo = List("Closure Product Category Tier3", "Assigned Group")

// Copiamos el DataFrame para irlo reduciendo progresivamente (aprovechamos para eliminar la fecha)
var df_reducido = df.drop("DATE")

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
	}
}


