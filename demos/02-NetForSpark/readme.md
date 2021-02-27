



# Crear un proyecto SparkNet con .Net Core 5

Con los siguientes pasos creamos un proyecto de **SparkNet** con .Net Core:

**Paso 1:** Crear proyecto de .Net Core 5 con soporte para SparkNet

Creamos directorio de la app

```bash
mkdir dotnet5-spark
```

Accedemos a la carpeta 

```bash
cd dotnet5-spark
```

Creeamos una app de consola de .Net Core 5

```bash
dotnet new console -f net5.0
```

Instalamos paquete de Spark

```bash
dotnet add dotnet5-spark.csproj package Microsoft.Spark
```

En Program.cs, borramos el contenido y agregamos lo siguiente:

```c#
using Microsoft.Spark.Sql;

using static Microsoft.Spark.Sql.Functions;

namespace MySparkApp
{
  class Program
  {
    static void Main(string[] args)
    {

      // Create Spark session
      SparkSession spark =
        SparkSession
          .Builder()
          .AppName("word_count_sample")
          .GetOrCreate();

 

      // Create initial DataFrame
      string filePath = args[0];
      DataFrame dataFrame = spark.Read().Text(filePath);

 

      //Count words
      DataFrame words =
        dataFrame
         .Select(Split(Col("value")," ").Alias("words"))
          .Select(Explode(Col("words")).Alias("word"))
          .GroupBy("word")
          .Count()
          .OrderBy(Col("count").Desc());

      // Display results
      words.Show();

      // Stop Spark session
      spark.Stop();

    }

  }

}
```



**Paso 2:** Publicar proyecto 

Compilamos proyecto

```bash
dotnet build
```

Publicar proyecto para Ubuntu/Linux

```bash
dotnet publish -f net5.0 -r ubuntu.18.04-x64 -o app-ubuntu
```



**Paso 3:** Lanzar job de spark en WSL/Ubuntu

Como hemos publicado el proyecto para Ubuntu, vamos a lanzarlo desde el WSL (Windows Subsystem Linux)

Los pasos para configurar .Net for Spark en Ubuntu están en la url https://docs.microsoft.com/es-es/dotnet/spark/how-to-guides/ubuntu-instructions

Accedemos WSL

Accedemos a la ruta de la app generada, un ejemplo de ruta en wsl puede ser:

```bash
cd /mnt/c/projects/2021-02-sparkta-charla/demos/dotnet5-spark/app-ubuntu
```

Lanzamos proyecto con Spark-submit

```bash
 spark-submit \

  --conf spark.ui.port=4050 \

 --class org.apache.spark.deploy.dotnet.DotnetRunner \

 --master local \

 ./microsoft-spark-3-0_2.12-1.0.0.jar \

 ./dotnet5-spark
```

![image-20210221081403047](file://C:/Users/alejandro.garcia/AppData/Roaming/Typora/typora-user-images/image-20210221081403047.png?lastModify=1614462220)

**Fuentes:** 



Ejemplo basado en https://kontext.tech/column/dotnet_framework/550/get-started-on-net-5-with-apache-spark