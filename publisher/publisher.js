// /publisher/publisher.js

const mqtt = require('mqtt');
const config = require('../config');

// --- Configuración Básica ---
const CLOCK_DRIFT_RATE = parseFloat(process.env.CLOCK_DRIFT_RATE || '0');
const DEVICE_ID = process.env.DEVICE_ID || 'sensor-default';
const PROCESS_ID = parseInt(process.env.PROCESS_ID || '0');

// --- Configuración de Elección (Bully) ---
const MY_PRIORITY = parseInt(process.env.PROCESS_PRIORITY || '0');
let currentLeaderPriority = 100; // Asumimos que hay alguien superior al inicio
let isCoordinator = false;
let electionInProgress = false;
let lastHeartbeatTime = Date.now();
const HEARTBEAT_INTERVAL = 2000; // Enviar PING cada 2s
const LEADER_TIMEOUT = 5000;     // Líder muerto si no responde en 5s
const ELECTION_TIMEOUT = 3000;   // Tiempo espera respuestas ALIVE

// --- Estado Mutex (Cliente) ---
let sensorState = 'IDLE';
const CALIBRATION_INTERVAL_MS = 20000 + (Math.random() * 5000);
const CALIBRATION_DURATION_MS = 5000;

// --- Estado Mutex (Servidor/Coordinador) - SOLO SE USA SI isCoordinator = true ---
let coord_isLockAvailable = true;
let coord_lockHolder = null;
let coord_waitingQueue = [];

// --- Sincronización Reloj (Cristian, Lamport, Vector) ---
// ... (Variables reducidas para brevedad, la lógica se mantiene)
let lastRealTime = Date.now();
let lastSimulatedTime = Date.now();
let clockOffset = 0;
let lamportClock = 0;
const VECTOR_PROCESS_COUNT = 3;
let vectorClock = new Array(VECTOR_PROCESS_COUNT).fill(0);

// --- Conexión MQTT ---
const statusTopic = config.topics.status(DEVICE_ID);
const brokerUrl = `mqtt://${config.broker.address}:${config.broker.port}`;
const client = mqtt.connect(brokerUrl, {
  clientId: `pub_${DEVICE_ID}_${Math.random().toString(16).slice(2, 5)}`,
  will: { topic: statusTopic, payload: JSON.stringify({ deviceId: DEVICE_ID, status: 'offline' }), qos: 1, retain: true }
});

// ============================================================================
//                            LÓGICA DEL CICLO DE VIDA
// ============================================================================

client.on('connect', () => {
  console.log(`[INFO] ${DEVICE_ID} (Prio: ${MY_PRIORITY}) conectado.`);

  // 1. Suscripciones Básicas
  client.subscribe(config.topics.time_response(DEVICE_ID));
  client.subscribe(config.topics.mutex_grant(DEVICE_ID));

  // 2. Suscripciones de Elección
  client.subscribe(config.topics.election.heartbeat); // Escuchar PONG
  client.subscribe(config.topics.election.messages);  // Escuchar ELECTION, ALIVE
  client.subscribe(config.topics.election.coordinator); // Escuchar VICTORY

  // 3. Iniciar Ciclos
  // A. Telemetría
  setInterval(publishTelemetry, 5000);
  // B. Sincronización Reloj (Cristian)
  setInterval(syncClock, 30000);
  // C. Intentos de Calibración (Cliente Mutex)
  setTimeout(() => { setInterval(requestCalibration, CALIBRATION_INTERVAL_MS); }, 5000);

  // D. Monitoreo del Líder (Heartbeat Check)
  setInterval(checkLeaderStatus, 1000);

  // E. Enviar Heartbeats (PING)
  setInterval(sendHeartbeat, HEARTBEAT_INTERVAL);

  // Publicar estado online
  client.publish(statusTopic, JSON.stringify({ deviceId: DEVICE_ID, status: 'online' }), { retain: true });
});

client.on('message', (topic, message) => {
  const payload = JSON.parse(message.toString());

  // --- 1. MANEJO DE ELECCIÓN (Bully) ---
  if (topic.startsWith('utp/sistemas_distribuidos/grupo1/election')) {
    handleElectionMessages(topic, payload);
    return;
  }

  // --- 2. SI SOY COORDINADOR: MANEJAR SOLICITUDES MUTEX ---
  if (isCoordinator) {
    if (topic === config.topics.mutex_request) {
      handleCoordRequest(payload.deviceId);
      return;
    }
    if (topic === config.topics.mutex_release) {
      handleCoordRelease(payload.deviceId);
      return;
    }
  }

  // --- 3. SI SOY CLIENTE: MANEJAR RESPUESTAS ---
  if (topic === config.topics.mutex_grant(DEVICE_ID)) {
    if (sensorState === 'REQUESTING') {
      console.log(`[MUTEX-CLIENT] Permiso recibido.`);
      sensorState = 'CALIBRATING';
      enterCriticalSection();
    }
  }

// --- 4. SINCRONIZACIÓN DE RELOJ (CRISTIAN MEJORADO - FASE 1) ---
  // AQUI ESTÁ LA CORRECCIÓN SOLICITADA
  if (topic === config.topics.time_response(DEVICE_ID)) {
    const t1 = payload.t1;             // Hora a la que enviamos (cliente)
    const serverTime = payload.serverTime; 
    const t4 = Date.now();             // Hora actual de recepción
    
    // Calcular RTT
    const rtt = t4 - t1; 

    // [REQUISITO FASE 1] Validación de RTT
    // Si el RTT es mayor a 500ms, descartamos la sincronización
    if (rtt > 500) {
      console.warn(`[CLOCK] Sincronización DESCARTADA. RTT muy alto: ${rtt}ms (>500ms)`);
      return; // No actualizamos el reloj, salimos de la función
    }

    // Si el RTT es aceptable, aplicamos algoritmo de Cristian
    const correctTime = serverTime + (rtt / 2);
    
    // Calculamos el nuevo offset comparando con nuestro reloj simulado
    const currentSimulated = getSimulatedTime().getTime();
    clockOffset = correctTime - currentSimulated;
    
    console.log(`[CLOCK] Sincronización Exitosa. RTT: ${rtt}ms. Nuevo Offset: ${clockOffset.toFixed(2)}ms`);
  }
});

// ============================================================================
//                          ALGORITMO DE ELECCIÓN (BULLY)
// ============================================================================

function sendHeartbeat() {
  // Solo enviamos PING si NO somos el coordinador
  if (!isCoordinator) {
    client.publish(config.topics.election.heartbeat, JSON.stringify({ type: 'PING', fromPriority: MY_PRIORITY }));
  }
}

function checkLeaderStatus() {
  if (isCoordinator) return; // Si soy líder, no monitoreo a nadie

  // Si pasó mucho tiempo desde el último PONG o mensaje del líder
  if (Date.now() - lastHeartbeatTime > LEADER_TIMEOUT) {
    console.warn(`[BULLY] ¡Líder caído! (Timeout). Iniciando elección.`);
    startElection();
  }
}

function startElection() {
  if (electionInProgress) return;
  electionInProgress = true;
  lastHeartbeatTime = Date.now(); // Reset timer para no spammear

  console.log(`[BULLY] Convocando elección... Buscando nodos con prioridad > ${MY_PRIORITY}`);

  // 1. Enviar mensaje ELECTION a todos los nodos con prioridad superior
  client.publish(config.topics.election.messages, JSON.stringify({
    type: 'ELECTION',
    fromPriority: MY_PRIORITY
  }));

  // 2. Esperar respuesta (ALIVE)
  setTimeout(() => {
    if (electionInProgress) {
      // Si llegamos aquí y electionInProgress sigue true, es que NADIE respondió ALIVE.
      // ¡Significa que somos el nodo vivo con mayor prioridad!
      declareVictory();
    }
  }, ELECTION_TIMEOUT);
}

function handleElectionMessages(topic, payload) {
  // A. HEARTBEATS
  if (topic === config.topics.election.heartbeat) {
    if (payload.type === 'PONG' && payload.fromPriority > MY_PRIORITY) {
      // El líder respondió, todo está bien.
      lastHeartbeatTime = Date.now();
    }
    return;
  }

  // B. MENSAJES DE ELECCIÓN
  if (topic === config.topics.election.messages) {
    // Si alguien con MENOR prioridad inicia elección, le decimos que estamos vivos
    if (payload.type === 'ELECTION' && payload.fromPriority < MY_PRIORITY) {
      console.log(`[BULLY] Recibida elección de inferior (${payload.fromPriority}). Enviando ALIVE.`);
      client.publish(config.topics.election.messages, JSON.stringify({
        type: 'ALIVE', toPriority: payload.fromPriority, fromPriority: MY_PRIORITY
      }));
      // E iniciamos nuestra propia elección por si acaso el líder real murió
      startElection();
    }
    // Si recibimos ALIVE de alguien SUPERIOR, nos callamos y esperamos
    else if (payload.type === 'ALIVE' && payload.fromPriority > MY_PRIORITY) {
      console.log(`[BULLY] Recibido ALIVE de superior (${payload.fromPriority}). Me retiro.`);
      electionInProgress = false; // Dejamos de intentar ser líderes
    }
    return;
  }

  // C. ANUNCIO DE COORDINADOR (VICTORY)
  if (topic === config.topics.election.coordinator) {
    console.log(`[BULLY] Nuevo Coordinador electo: ${payload.coordinatorId} (Prio: ${payload.priority})`);
    currentLeaderPriority = payload.priority;
    lastHeartbeatTime = Date.now(); // El líder está vivo
    electionInProgress = false;

    // Chequear si soy yo (por si acaso)
    if (payload.priority === MY_PRIORITY) {
      becomeCoordinator();
    } else {
      isCoordinator = false;
      // Dejar de escuchar peticiones de mutex si antes era coordinador
      client.unsubscribe(config.topics.mutex_request);
      client.unsubscribe(config.topics.mutex_release);
    }
  }
}

function declareVictory() {
  console.log(`[BULLY] ¡Nadie superior respondió! ME DECLARO COORDINADOR.`);
  const msg = JSON.stringify({ type: 'VICTORY', coordinatorId: DEVICE_ID, priority: MY_PRIORITY });
  client.publish(config.topics.election.coordinator, msg, { retain: true });
  becomeCoordinator();
}

function becomeCoordinator() {
  if (isCoordinator) return;
  isCoordinator = true;
  electionInProgress = false;
  console.log(`[ROLE] *** ASCENDIDO A COORDINADOR DE BLOQUEO ***`);

  // Reiniciar estado del mutex (para evitar bloqueos heredados)
  coord_isLockAvailable = true;
  coord_lockHolder = null;
  coord_waitingQueue = [];

  // Suscribirse a los tópicos que debe escuchar el líder
  client.subscribe(config.topics.mutex_request, { qos: 1 });
  client.subscribe(config.topics.mutex_release, { qos: 1 });

  // Publicar estado inicial
  publishCoordStatus();
}

// ============================================================================
//                  LÓGICA DE SERVIDOR MUTEX (Solo si isCoordinator)
// ============================================================================
// (Esta lógica es idéntica a la de lock-coordinator.js, ahora embebida aquí)

function handleCoordRequest(requesterId) {
  console.log(`[COORD] Procesando solicitud de: ${requesterId}`);
  if (coord_isLockAvailable) {
    grantCoordLock(requesterId);
  } else {
    if (!coord_waitingQueue.includes(requesterId) && coord_lockHolder !== requesterId) {
      coord_waitingQueue.push(requesterId);
    }
  }
  publishCoordStatus();
}

function handleCoordRelease(requesterId) {
  if (coord_lockHolder === requesterId) {
    console.log(`[COORD] Liberado por: ${requesterId}`);
    coord_lockHolder = null;
    coord_isLockAvailable = true;
    if (coord_waitingQueue.length > 0) {
      grantCoordLock(coord_waitingQueue.shift());
    }
  }
  publishCoordStatus();
}

function grantCoordLock(requesterId) {
  coord_isLockAvailable = false;
  coord_lockHolder = requesterId;
  client.publish(config.topics.mutex_grant(requesterId), JSON.stringify({ status: 'granted' }), { qos: 1 });
}

function publishCoordStatus() {
  client.publish(config.topics.mutex_status, JSON.stringify({
    isAvailable: coord_isLockAvailable,
    holder: coord_lockHolder,
    queue: coord_waitingQueue
  }), { retain: true });
}

// ============================================================================
//                            FUNCIONES AUXILIARES
// ============================================================================

function getSimulatedTime() {
  const now = Date.now();
  const realElapsed = now - lastRealTime;
  const simulatedElapsed = realElapsed + (realElapsed * CLOCK_DRIFT_RATE / 1000);
  lastSimulatedTime = lastSimulatedTime + simulatedElapsed;
  lastRealTime = now;
  return new Date(Math.floor(lastSimulatedTime));
}

function syncClock() {
  const payload = JSON.stringify({ deviceId: DEVICE_ID, t1: Date.now() });
  client.publish(config.topics.time_request, payload, { qos: 0 });
}

function requestCalibration() {
  if (sensorState === 'IDLE' && !isCoordinator) { // El coordinador no se auto-solicita en este ejemplo simple
    console.log(`[MUTEX-CLIENT] Solicitando...`);
    sensorState = 'REQUESTING';
    client.publish(config.topics.mutex_request, JSON.stringify({ deviceId: DEVICE_ID }), { qos: 1 });
  }
}

function enterCriticalSection() {
  setTimeout(() => {
    console.log(`[MUTEX-CLIENT] Fin calibración.`);
    releaseLock();
  }, CALIBRATION_DURATION_MS);
}

function releaseLock() {
  sensorState = 'IDLE';
  client.publish(config.topics.mutex_release, JSON.stringify({ deviceId: DEVICE_ID }), { qos: 1 });
}

function publishTelemetry() {
  lamportClock++;
  vectorClock[PROCESS_ID]++;
  const correctedTime = new Date(getSimulatedTime().getTime() + clockOffset);

  const telemetryData = {
    deviceId: DEVICE_ID,
    temperatura: (Math.random() * 30).toFixed(2),
    humedad: (Math.random() * 100).toFixed(2),
    timestamp: correctedTime.toISOString(),
    timestamp_simulado: getSimulatedTime().toISOString(),
    clock_offset: clockOffset.toFixed(0),
    lamport_ts: lamportClock,
    vector_clock: [...vectorClock],
    sensor_state: isCoordinator ? 'COORDINATOR' : sensorState // Mostrar rol especial si es líder
  };
  client.publish(config.topics.telemetry(DEVICE_ID), JSON.stringify(telemetryData));
}