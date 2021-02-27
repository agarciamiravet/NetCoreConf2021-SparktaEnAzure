# Crear cluster de AKS con un nodepool de tipo Spot

Pasos a seguir son:

1. Login en con nuestro usuario de Azure

   ```bash
   az login
   ```

   

2. Seleccionamos nuestra subscripción de Azure

   ```bash
   az account set --subscription "<nombre_subscripcion>"
   ```

   

3. Creamos el resource group para nuestro AKS

   ```bash
   az group create --location westeurope --name rg-sparkta
   ```

   

4. Creamos un cluster AKS con un nodepool normal de 1 único nodo

   ```bash
   az aks create --resource-group rg-sparkta --name aks-sparkta --node-count 1 --generate-ssh-keys
   ```

   

5. Creamos un node pool de tipo SpotVm de 1 a 3 nodos con autoescalado

   ```bash
   az aks nodepool add \
       --resource-group rg-sparkta \
       --cluster-name aks-sparkta \
       --name spotnodepool \
       --priority Spot \
       --eviction-policy Delete \
       --spot-max-price -1 \
       --enable-cluster-autoscaler \
       --min-count 1 \
       --max-count 3 \
       --node-vm-size Standard_D2as_v4 \
       --no-wait
   ```

   

   # Instalación de SparkOperator

   1. Instalamos spark-k8s-operator

      ```bash
      helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
      
      helm install my-release spark-operator/spark-operator --namespace spark-operator --create-namespace --set webhook.enable=true
      ```

   2. Comprobamos estado spark operator

       ```bash
       	helm status --namespace spark-operator my-release
       ```

   3. Creamos serviaccount spark

       ```
       kubectl create serviceaccount spark
       
       kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
       ```
   
   # Desplegar spark job con SparkOperator
   
    1. Creamos fichero **spark-py-youtube.yaml** con el suguiente contenido:
   
       ```yaml
       #
       # Copyright 2018 Google LLC
       #
       # Licensed under the Apache License, Version 2.0 (the "License");
       # you may not use this file except in compliance with the License.
       # You may obtain a copy of the License at
       #
       #   https://www.apache.org/licenses/LICENSE-2.0
       #
       # Unless required by applicable law or agreed to in writing, software
       # distributed under the License is distributed on an "AS IS" BASIS,
       # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       # See the License for the specific language governing permissions and
       # limitations under the License.
       #
       # Support for Python is experimental, and requires building SNAPSHOT image of Apache Spark,
       # with `imagePullPolicy` set to Always
       
       
       apiVersion: "sparkoperator.k8s.io/v1beta2"
       
       kind: SparkApplication
       
       metadata:
       
        name: sparkta-py-youtube
       
        namespace: default
       
       spec:
       
        dynamicAllocation:
       
         enabled: true
       
         initialExecutors: 3
       
         minExecutors: 3
       
         maxExecutors: 4
       
        sparkConf:
       
          "spark.hadoop.fs.azure.account.key.adlsparkta.blob.core.windows.net": "[ACCESS_KEY]""
       
        type: Python
       
        pythonVersion: "3"
       
        mode: cluster
       
        image: "alexgarciamiravet/spark-youtube:3.1.1-rc2"
       
        imagePullPolicy: Always
       
        mainApplicationFile: local:///opt/spark/work-dir/youtube.py
       
        sparkVersion: "3.1.1-rc2"
       
        restartPolicy:
       
         type: OnFailure
       
         onFailureRetries: 3
       
         onFailureRetryInterval: 10
       
         onSubmissionFailureRetries: 5
       
         onSubmissionFailureRetryInterval: 20
       
        driver:  
       
         cores: 1
       
         coreLimit: "1200m"
       
         memory: "512m"
       
         labels:
       
          version: 3.1.1-rc2
       
         serviceAccount: spark
       
        executor:
       
         tolerations:
       
         - key: kubernetes.azure.com/scalesetpriority
       
          operator: Equal #this is the default value
       
          value: spot
       
          effect: NoSchedule
       
         memory: "500m"
       
         labels:
       
          version: 3.1.1-rc2
       ```
   
       

2. Desplegamos el job de spark

   ```bash
   kubectl apply -f spark-py-youtube.yaml 
   ```

   