// Databricks notebook source
val filePath: String =  "dbfs:/FileStore/depression_data.csv"

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

// COMMAND ----------

display(
  df
)

// COMMAND ----------

val totalPersonas = df.count()

// COMMAND ----------

val desempleadas = df.filter($"Employment Status" === "Unemployed").count()

// COMMAND ----------

val porcentajeDesempleadas = (desempleadas.toDouble / totalPersonas) * 100

// COMMAND ----------

val desempleadosConDepresion = df.filter($"Employment Status" === "Unemployed" && $"Family History of Depression" === "Yes")

// COMMAND ----------

display(
  desempleadosConDepresion
)

// COMMAND ----------

val proporcion = desempleadosConDepresion.count().toDouble / desempleadas * 100

// COMMAND ----------

val relacion = df.groupBy("Employment Status")
  .agg(count(when($"History of Mental Illness" === "Yes", 1)).as("Con Enfermedad Mental"))

// COMMAND ----------

relacion.show()

// COMMAND ----------

val ingresoPromedio = df.filter($"Family History of Depression" === "Yes")
  .groupBy("Employment Status")
  .agg(avg("Income").as("Ingreso Promedio"))

// COMMAND ----------

ingresoPromedio.show()

// COMMAND ----------

val totalPersonas = df.count()

// COMMAND ----------

val desempleadas = df.filter($"Employment Status" === "Unemployed").count()

// COMMAND ----------

val desempleadasConSueñoPobre = df.filter($"Employment Status" === "Unemployed" && $"Sleep Patterns" === "Poor").count()

// COMMAND ----------

val porcentajeSueñoPobre = (desempleadasConSueñoPobre.toDouble / desempleadas) * 100

// COMMAND ----------

val altoAlcohol = df.filter($"Alcohol Consumption" === "High").count()

// COMMAND ----------

val altoAlcoholConSueñoPobre = df.filter($"Alcohol Consumption" === "High" && $"Sleep Patterns" === "Poor").count()

// COMMAND ----------

val proporcionSueñoPobre = (altoAlcoholConSueñoPobre.toDouble / altoAlcohol) * 100

// COMMAND ----------

val fumadores = df.filter($"Smoking Status" === "Former")

// COMMAND ----------

val fumadoresConAbuso = fumadores.filter($"History of Substance Abuse" === "Yes")

// COMMAND ----------

val totalFumadores = fumadores.count()

// COMMAND ----------

val totalFumadoresConAbuso = fumadoresConAbuso.count()

// COMMAND ----------

val porcentaje = if (totalFumadores > 0) {
  (totalFumadoresConAbuso.toDouble / totalFumadores) * 100
} else {
  0.0
}

// COMMAND ----------

val promedioHijosPorEducacion = df.groupBy("Education Level")
  .agg(avg("Number of Children").as("Promedio Número de Hijos"))

// COMMAND ----------

promedioHijosPorEducacion.show()

// COMMAND ----------

val casados = df.filter($"Marital Status" === "Married")

// COMMAND ----------

val actividadFisicaCasados = casados.groupBy("Physical Activity Level")
  .count()
  .withColumnRenamed("count", "Cantidad")

// COMMAND ----------

val nivelMasComún = actividadFisicaCasados.orderBy(desc("Cantidad")).limit(3)

// COMMAND ----------

nivelMasComún.show()

// COMMAND ----------

val ingresoPromedio = df.agg(avg("Income")).first().getDouble(0)

// COMMAND ----------

val personasConAltosIngresos = df.filter($"Income" > ingresoPromedio)

// COMMAND ----------

val personasConAbusoDeSustancias = personasConAltosIngresos.filter($"History of Substance Abuse" === "Yes")

// COMMAND ----------

val cantidad = personasConAbusoDeSustancias.count()

// COMMAND ----------


