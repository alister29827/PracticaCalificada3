const mqtt = require('mqtt');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const config = require('../config'); // Nuestra config MQTT

// --- Configuración InfluxDB (leída desde variables de entorno) ---
const influxUrl = process.env.INFLUXDB_URL || 'http://localhost:8086';
const influxToken = process.env.INFLUXDB_TOKEN || 'mySuperSecretToken123!';
const influxOrg = process.env.INFLUXDB_ORG || 'utp';
const influxBucket = process.env.INFLUXDB_BUCKET || 'sensors';

// --- Configuración MQTT ---
const brokerUrl = `mqtt://${config.broker.address}:${config.broker.port}`;
const topic = config.topics.telemetry('+'); // Escucha telemetría de todos los dispositivos
const clientId = `persistence_sub_${Math.random().toString(16).slice(2, 8)}`;

// --- Configuración Reloj Vectorial ---
// NOTA: Ajustado a 5 para coincidir con los 5 sensores de la Fase 0
const VECTOR_PROCESS_COUNT = 5; 
const PROCESS_ID = parseInt(process.env.PROCESS_ID || '99'); // Usamos 99 para el suscriptor para no chocar con sensores
let vectorClock = new Array(VECTOR_PROCESS_COUNT).fill(0);

// --- Reloj Lógico de Lamport para el suscriptor ---
let lamportClock = 0;

// --- Inicialización Clientes ---
console.log('[INFO] Iniciando Suscriptor de Persistencia...');

// Cliente InfluxDB
const influxDB = new InfluxDB({ url: influxUrl, token: influxToken });
const writeApi = influxDB.getWriteApi(influxOrg, influxBucket, 'ns'); // Precisión en nanosegundos
console.log(`[INFO] Conectado a InfluxDB: ${influxUrl}, Org: ${influxOrg}, Bucket: ${influxBucket}`);

// Cliente MQTT
const mqttClient = mqtt.connect(brokerUrl, { clientId });

mqttClient.on('connect', () => {
  console.log(`[INFO] Conectado al broker MQTT en ${brokerUrl}`);
  mqttClient.subscribe(topic, { qos: 1 }, (err) => {
    if (!err) {
      console.log(`[INFO] Suscrito a telemetría en: ${topic}`);
    } else {
      console.error('[ERROR] Error al suscribirse a MQTT:', err);
    }
  });
});

mqttClient.on('error', (error) => {
  console.error('[ERROR] Error de conexión MQTT:', error);
});

// --- Procesamiento de Mensajes ---
mqttClient.on('message', (receivedTopic, message) => {
  
  // --- REGLA 1 (VECTORIAL): Evento interno ---
  // El suscriptor (si tuviera ID dentro del rango) actualizaría su reloj, 
  // pero como es observador, mantenemos su lógica de incrementar para marcar recepción.
  // Nota: Si PROCESS_ID es 99, esto no afectará al array de tamaño 5, lo cual es correcto para un sink pasivo.
  lamportClock++;

  console.log(`\n[MSG] Mensaje recibido en [${receivedTopic}]`);

  try {
    const data = JSON.parse(message.toString());
    const deviceId = data.deviceId;

    // =====================================================================
    //           [FASE 1] BARRERA DE ENTRADA TEMPORAL (CORREGIDA PROFE)
    // =====================================================================
    // Verificar si el mensaje viene del futuro
    if (data.timestamp) {
      const msgTime = new Date(data.timestamp).getTime();
      const localTime = Date.now();
      const difference = msgTime - localTime;

      // Umbral de 2000ms (2 segundos)
      if (difference > 2000) {
         // Si msgTime > localTime + 2s -> DESCARTAR.
         console.warn(`[SECURITY] RECHAZADO: Paquete del futuro detectado de ${deviceId}. Diff: ${difference}ms.`);
         return; 
      }
    }
   
    // --- REGLA 3 (LAMPORT): Parte 2 (Fusión) ---
    const receivedLamportTS = data.lamport_ts || 0;
    lamportClock = Math.max(lamportClock, receivedLamportTS);
    console.log(`[LAMPORT] Reloj local actualizado a: ${lamportClock} (recibido: ${receivedLamportTS})`);

    // --- REGLA 3 (VECTORIAL): Parte 2 (Fusión) ---
    const receivedVectorClock = data.vector_clock || new Array(VECTOR_PROCESS_COUNT).fill(0);
    
    // Fusionamos los relojes: tomamos el máximo de cada posición
    // Aseguramos que los arrays tengan el mismo tamaño antes de iterar
    for (let i = 0; i < VECTOR_PROCESS_COUNT; i++) {
      const receivedVal = receivedVectorClock[i] || 0;
      vectorClock[i] = Math.max(vectorClock[i] || 0, receivedVal);
    }
    console.log(`[VECTOR] Reloj local actualizado a: [${vectorClock.join(',')}]`);

    if (!deviceId || data.temperatura === undefined || data.humedad === undefined) {
      console.warn('[WARN] Mensaje incompleto recibido, ignorando:', data);
      return;
    }

    // Crear un punto de datos para InfluxDB
    const point = new Point('sensor_data')
      .tag('device_id', deviceId)
      .floatField('temperature', data.temperatura)
      .floatField('humidity', data.humedad)

      // Relojes Lógicos (Lamport)
      .intField('lamport_ts_sensor', receivedLamportTS)
      .tag('lamport_ts_persistence', lamportClock.toString())

      // --- NUEVO: Añadimos relojes vectoriales a InfluxDB ---
      .tag('vector_clock_sensor', JSON.stringify(receivedVectorClock))
      .tag('vector_clock_persistence', JSON.stringify(vectorClock))

      .timestamp(new Date(data.timestamp || Date.now()));

    console.log(`[DB] Preparando punto para InfluxDB: ${point.toString()}`);

    // Escribir el punto en InfluxDB
    writeApi.writePoint(point);
    
    // Forzar el envío inmediato de los datos
    writeApi.flush()
      .then(() => {
        console.log('[DB] Punto escrito exitosamente en InfluxDB.');
      })
      .catch(error => {
        console.error('[ERROR] Error al escribir en InfluxDB:', error);
      });

  } catch (error) {
    console.error('[ERROR] Error al procesar mensaje MQTT o escribir en DB:', error);
  }
});

// --- Manejo de Cierre Limpio ---
process.on('SIGINT', async () => {
  console.log('\n[INFO] Cerrando conexiones...');
  mqttClient.end();
  try {
    await writeApi.close();
    console.log('[INFO] Conexión InfluxDB cerrada.');
  } catch (e) {
    console.error('[ERROR] Error cerrando InfluxDB:', e);
  }
  process.exit(0);
});