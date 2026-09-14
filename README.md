Pipeline de Ingesta y Procesamiento de Telemetría F1 en AWS
Proyecto de arquitectura serverless en AWS diseñado para ingerir, almacenar y analizar datos de telemetría en tiempo real. La solución implementa un flujo de datos desacoplado, optimizado para costes y con mantenimiento predictivo para la salud del motor.

Arquitectura
El flujo de datos sigue un modelo serverless organizado en las siguientes etapas:

Ingesta y Simulación: AWS Lambda genera registros de telemetría (velocidad, RPM, presión de freno, temperatura de motor) estructurados por piloto.

Almacenamiento (Data Lake): Los datos se guardan en Amazon S3 en formato JSON, utilizando un esquema de particionado tipo Hive (year/month/day/piloto) para optimizar lecturas.

Catalogación: Un crawler de AWS Glue escanea el bucket de S3 y registra el esquema en el AWS Glue Data Catalog.

Consulta: Amazon Athena ejecuta consultas ANSI SQL sobre los datos almacenados en S3 sin necesidad de provisionar bases de datos relacionales.

Automatización: Amazon EventBridge ejecuta la función Lambda de forma periódica (cada 5 minutos).

Alertas: Lógica interna en Lambda calcula la degradación térmica del motor y envía alertas vía Amazon SNS al detectar riesgo de fallo mecánico.

Plaintext
[EventBridge] 
     │
     ▼
[AWS Lambda] ───(Publica alerta)───> [Amazon SNS] ───> [Email / Notificación]
     │
     ▼
[Amazon S3 (Data Lake)]
     │
     ▼
[AWS Glue Crawler]
     │
     ▼
[Glue Data Catalog]
     │
     ▼
[Amazon Athena] ───> [Análisis / Exportación CSV]
Componentes y Configuración
1. Amazon S3
Bucket principal: f1-telemetria-almacenamiento
Versionado activado para prevenir borrados accidentales.

Lifecycle Policy:

Transición a S3 Standard-IA a los 30 días.

Transición a S3 Glacier Flexible Retrieval a los 90 días.

Estructura de objetos en S3:

Plaintext
telemetria/
  └── year=2026/
      └── month=03/
          └── day=19/
              ├── piloto=Russell/
              │   └── Russell_173443.json
              ├── piloto=Verstappen/
              │   └── Verstappen_173443.json
              └── piloto=Antonelli/
                  └── Antonelli_173443.json
2. IAM y Seguridad
Se aplica el principio de mínimo privilegio asignando permisos específicos para:

Escritura de logs en CloudWatch (AWSLambdaBasicExecutionRole).

Acceso de lectura/escritura únicamente sobre el bucket asignado en S3.

Permiso de publicación sobre el Topic de SNS.

(Nota: En el entorno de pruebas de AWS Academy / Learning Lab, el despliegue se ajustó al rol LabRole debido a restricciones del laboratorio).

3. Código de Ingesta (AWS Lambda)
Funcionamiento en Python (boto3):

```python
import json
import boto3
from datetime import datetime
import random

def lambda_handler(event, context):
    NOMBRE_MI_BUCKET = "f1-telemetria-almacenamiento" 
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:324254793988:AlertaF1"
    
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    
    pilotos = ["Verstappen", "Russell", "Antonelli"]
    fecha_base = datetime.now()
    
    path_base = f"year={fecha_base.year}/month={fecha_base.month:02d}/day={fecha_base.day:02d}"

    for piloto in pilotos:
        telemetria_piloto = []
        temp_motor = 118.0 if piloto == "Russell" else 95.0 
        
        for segundo in range(1, 21):
            if segundo < 8:
                speed_base = 150 + (segundo * 18)
                freno = 0
                if piloto == "Russell": speed_base += 7 
                elif piloto == "Verstappen": speed_base += 12 
            elif segundo >= 8 and segundo < 12:
                freno_base = 80
                speed_base = 280 - ((segundo - 7) * 40)
                if piloto == "Verstappen": speed_base += 20; freno_base += 15
                elif piloto == "Russell": speed_base -= 5; freno_base -= 10
                elif piloto == "Antonelli": speed_base -= 15; freno_base = random.randint(70, 90)
                freno = freno_base
            else:
                speed_base = 100 + ((segundo - 11) * 22)
                freno = 0
                if piloto == "Verstappen": speed_base += 10
                elif piloto == "Russell": speed_base += 5
                elif piloto == "Antonelli": speed_base -= 8
            
            velocidad = max(60, speed_base + random.randint(-2, 2))
            incremento = 0.6 if piloto in ["Russell", "Antonelli"] else 0.1
            temp_motor += incremento
            
            limite_critico = 145.0 
            vueltas_restantes = round((limite_critico - temp_motor) / (incremento * 90), 1)

            # Lógica de detección y envío de alertas vía SNS
            if piloto in ["Russell", "Antonelli"] and temp_motor >= 128.0:
                if segundo == 18: 
                    mensaje_alerta = (
                        f"CRITICAL ALERT - {piloto.upper()} ENGINE\n"
                        f"--------------------------------------\n"
                        f"Temperatura Actual: {temp_motor:.1f} C (UMBRAL: 130 C)\n"
                        f"Estado: Degradacion termica acelerada.\n"
                        f"Prediccion: Fallo mecanico en {vueltas_restantes} vueltas.\n"
                        f"Accion: Cambiar a Strategy 3 para enfriar."
                    )
                    try:
                        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=f"Urgente: Telemetria {piloto}", Message=mensaje_alerta)
                    except Exception as e:
                        print(f"Error SNS: {str(e)}")

            registro = {
                "nombre_piloto": piloto,
                "gp": "Espana",
                "fecha": fecha_base.strftime("%Y-%m-%d"),
                "timestamp": f"{fecha_base.strftime('%H:%M:%S')}.{segundo:02d}",
                "segundo_vuelta": segundo,
                "velocidad": int(velocidad),
                "temp_motor": round(temp_motor, 1),
                "vueltas_vida_estimadas": vueltas_restantes,
                "presion_freno": freno,
                "rpm": int(11000 + (velocidad * 10)),
                "marcha": min(8, max(1, int(velocidad / 35)))
            }
            telemetria_piloto.append(json.dumps(registro))

        nombre_archivo = f"telemetria/{path_base}/piloto={piloto}/{piloto}_{fecha_base.strftime('%H%M%S')}.json"
        s3.put_object(Bucket=NOMBRE_MI_BUCKET, Key=nombre_archivo, Body="\n".join(telemetria_piloto))

    return {'statusCode': 200, 'body': "OK"}
```
    
4. Consultas en Amazon Athena
Ejemplo de consulta SQL para extraer métricas de rendimiento por sector:

SQL
SELECT 
    nombre_piloto, 
    segundo_vuelta, 
    velocidad, 
    temp_motor, 
    presion_freno 
FROM "f1_db"."telemetria"
WHERE gp = 'Espana'
ORDER BY segundo_vuelta ASC;

5. Decisiones Técnicas y Limitaciones
Sustitución de API externa por generación interna: Debido a que el entorno de laboratorio no disponía de una NAT Gateway para dar salida a Internet a la subred (restringido por presupuesto), se integró la lógica de generación de telemetría dentro de la propia Lambda en lugar de consumir la API pública de Ergast.

Optimización de costes: Toda la infraestructura corre sobre servicios 100% serverless bajo demanda, manteniendo el coste total del proyecto por debajo de 0.20 €.

Particionado en S3: Organizar los datos mediante el esquema de carpetas Hive permite a Athena escanear únicamente los archivos de la partición consultada, reduciendo drásticamente el tiempo de respuesta y el coste por terabyte escaneado.

6. Posibles Mejoras
Infraestructura como Código: Migrar el aprovisionamiento manual de la consola a plantillas reutilizables en Terraform.

Gestión de Secretos: Integrar AWS Secrets Manager para evitar incluir identificadores o ARNs estáticos en el código de la función.

Streaming: Sustituir la ejecución por intervalos de EventBridge por una ingesta continua mediante Amazon Kinesis Data Streams o Managed Streaming for Apache Kafka (MSK).
