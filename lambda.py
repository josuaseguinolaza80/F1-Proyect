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
