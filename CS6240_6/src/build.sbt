// Author: Adib Alwani

lazy val root = (project in file(".")).
	settings(
		name := "Missed Connection",
		libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2"),
		libraryDependencies += ("org.apache.spark" %% "spark-sql" % "1.5.2")
	)
