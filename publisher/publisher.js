const mqtt = require('mqtt');
const config = require('../config');
const fs = require('fs');           // [FASE 3] Necesario para escribir en disco
const path = require('path');       // [FASE 3] Manejo de rutas

// --- [FASE 3] CONFIGURACIÓN DE WAL (MUTEX) ---
// Ruta absoluta dentro del contenedor (coincide con volumen docker-compose)
const WAL_FILE = path.join('/usr/src/app/wal_logs', 'mutex_wal.log');

// --- Configuración Básica ---
const CLOCK_DRIFT_RATE = parseFloat(process.env.CLOCK_DRIFT_RATE || '0');
const DEVICE_ID = process.env.DEVICE_ID || 'sensor-default';
const PROCESS_ID = parseInt(process.env.PROCESS_ID || '0');

// --- Configuración de Elección (Bully) ---
const MY_PRIORITY = parseInt(process.env.PROCESS_PRIORITY || '0');
let currentLeaderPriority = 100; 
let isCoordinator = false;
let electionInProgress = false;
let lastHeartbeatTime = Date.now();
const HEARTBEAT_INTERVAL = 2000; 
const LEADER_TIMEOUT = 5000;     
const ELECTION_TIMEOUT = 3000;   

// --- [FASE 2] VARIABLES DE LEASE (ARRENDAMIENTO) ---
const LEASE_DURATION = 5000;     
const LEASE_RENEWAL = 2000;      
let leaseTimeout = null;         
let lastLeaseSeen = Date.now();  
const TOPIC_LEASE = 'utp/sistemas_distribuidos/grupo1/election/lease';

// --- Estado Mutex (Cliente) ---
let sensorState = 'IDLE';
const CALIBRATION_INTERVAL_MS = 20000 + (Math.random() * 5000);
const CALIBRATION_DURATION_MS = 5000;

// --- Estado Mutex (Servidor/Coordinador) ---
let coord_isLockAvailable = true;
let coord_lockHolder = null;
let coord_waitingQueue = [];

// --- Sincronización Reloj (Cristian, Lamport, Vector) ---
let lastRealTime = Date.now();
let lastSimulatedTime = Date.now();
let clockOffset = 0;
let lamportClock = 0;
const VECTOR_PROCESS_COUNT = 5; // Ajustado a 5 nodos
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

  // Suscripciones
  client.subscribe(config.topics.time_response(DEVICE_ID));
  client.subscribe(config.topics.mutex_grant(DEVICE_ID));
  client.subscribe(config.topics.election.heartbeat); 
  client.subscribe(config.topics.election.messages);  
  client.subscribe(config.topics.election.coordinator); 
  client.subscribe(TOPIC_LEASE); // [FASE 2]

  // Timers
  setInterval(publishTelemetry, 5000);
  setInterval(syncClock, 30000);
  setTimeout(() => { setInterval(requestCalibration, CALIBRATION_INTERVAL_MS); }, 5000);
  setInterval(checkLeaderStatus, 1000);
  setInterval(sendHeartbeat, HEARTBEAT_INTERVAL);

  client.publish(statusTopic, JSON.stringify({ deviceId: DEVICE_ID, status: 'online' }), { retain: true });
});

client.on('message', (topic, message) => {
  const payload = JSON.parse(message.toString());

  // --- [FASE 2] MANEJO DE LEASES ---
  if (topic === TOPIC_LEASE) {
    lastLeaseSeen = Date.now(); 
    // PROTECCIÓN SPLIT-BRAIN
    if (isCoordinator && payload.coordinatorId !== DEVICE_ID) {
       console.warn(`[SPLIT-BRAIN] Detectado otro líder activo (${payload.coordinatorId}). RENUNCIO.`);
       stepDown();
    }
    return;
  }

  // --- ELECCIÓN ---
  if (topic.startsWith('utp/sistemas_distribuidos/grupo1/election')) {
    handleElectionMessages(topic, payload);
    return;
  }

  // --- COORDINADOR ---
  if (isCoordinator) {
    if (topic === config.topics.mutex_request) { handleCoordRequest(payload.deviceId); return; }
    if (topic === config.topics.mutex_release) { handleCoordRelease(payload.deviceId); return; }
  }

  // --- CLIENTE MUTEX ---
  if (topic === config.topics.mutex_grant(DEVICE_ID)) {
    if (sensorState === 'REQUESTING') {
      console.log(`[MUTEX-CLIENT] Permiso recibido.`);
      sensorState = 'CALIBRATING';
      enterCriticalSection();
    }
  }

  // --- [FASE 1] SINCRONIZACIÓN DE RELOJ (RTT CHECK) ---
  if (topic === config.topics.time_response(DEVICE_ID)) {
    const t1 = payload.t1;             
    const serverTime = payload.serverTime; 
    const t4 = Date.now();             
    const rtt = t4 - t1; 

    if (rtt > 500) {
      console.warn(`[CLOCK] Sincronización DESCARTADA. RTT muy alto: ${rtt}ms (>500ms)`);
      return; 
    }

    const correctTime = serverTime + (rtt / 2);
    const currentSimulated = getSimulatedTime().getTime();
    clockOffset = correctTime - currentSimulated;
    
    console.log(`[CLOCK] Sincronización Exitosa. RTT: ${rtt}ms. Nuevo Offset: ${clockOffset.toFixed(2)}ms`);
  }
});

// ============================================================================
//                          ALGORITMO DE ELECCIÓN (BULLY + LEASE)
// ============================================================================

function sendHeartbeat() {
  if (!isCoordinator) {
    client.publish(config.topics.election.heartbeat, JSON.stringify({ type: 'PING', fromPriority: MY_PRIORITY }));
  }
}

function checkLeaderStatus() {
  if (isCoordinator) return; 
  const now = Date.now();
  // [FASE 2] Asesino Silencioso
  if (now - lastLeaseSeen > LEASE_DURATION) {
    console.warn(`[BULLY] ¡Lease del Líder EXPIRÓ! (Hace ${now - lastLeaseSeen}ms). Asesino Silencioso inicia elección.`);
    lastLeaseSeen = Date.now(); 
    startElection();
  }
}

function startElection() {
  if (electionInProgress) return;
  electionInProgress = true;
  lastHeartbeatTime = Date.now(); 
  console.log(`[BULLY] Convocando elección... Buscando nodos con prioridad > ${MY_PRIORITY}`);

  client.publish(config.topics.election.messages, JSON.stringify({
    type: 'ELECTION',
    fromPriority: MY_PRIORITY
  }));

  setTimeout(() => {
    if (electionInProgress) {
      declareVictory();
    }
  }, ELECTION_TIMEOUT);
}

function handleElectionMessages(topic, payload) {
  if (topic === config.topics.election.heartbeat) {
    if (payload.type === 'PONG' && payload.fromPriority > MY_PRIORITY) {
      lastHeartbeatTime = Date.now();
    }
    return;
  }

  if (topic === config.topics.election.messages) {
    if (payload.type === 'ELECTION' && payload.fromPriority < MY_PRIORITY) {
      console.log(`[BULLY] Recibida elección de inferior (${payload.fromPriority}). Enviando ALIVE.`);
      client.publish(config.topics.election.messages, JSON.stringify({
        type: 'ALIVE', toPriority: payload.fromPriority, fromPriority: MY_PRIORITY
      }));
      startElection();
    }
    else if (payload.type === 'ALIVE' && payload.fromPriority > MY_PRIORITY) {
      console.log(`[BULLY] Recibido ALIVE de superior (${payload.fromPriority}). Me retiro.`);
      electionInProgress = false; 
    }
    return;
  }

  if (topic === config.topics.election.coordinator) {
    console.log(`[BULLY] Nuevo Coordinador electo: ${payload.coordinatorId} (Prio: ${payload.priority})`);
    currentLeaderPriority = payload.priority;
    lastLeaseSeen = Date.now(); 
    electionInProgress = false;

    if (payload.priority === MY_PRIORITY) {
      becomeCoordinator();
    } else {
      if (isCoordinator) stepDown();
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

  // --- [FASE 3] RECUPERACIÓN DE ESTADO (WAL) ---
  // Recuperamos la cola del disco ANTES de declararnos listos
  const restoredQueue = restoreStateFromWAL();

  isCoordinator = true;
  electionInProgress = false;
  console.log(`[ROLE] *** ASCENDIDO A COORDINADOR (LÍDER) ***`);

  renewLease(); 
  if (leaseTimeout) clearInterval(leaseTimeout);
  leaseTimeout = setInterval(renewLease, LEASE_RENEWAL);

  coord_isLockAvailable = true; 
  coord_lockHolder = null;
  coord_waitingQueue = restoredQueue; // <--- ASIGNAMOS LA COLA RECUPERADA

  client.subscribe(config.topics.mutex_request, { qos: 1 });
  client.subscribe(config.topics.mutex_release, { qos: 1 });

  publishCoordStatus();

  // Si hay gente esperando recuperada y el recurso está libre, atendemos al primero
  if (coord_isLockAvailable && coord_waitingQueue.length > 0) {
      const next = coord_waitingQueue.shift();
      // Registrar REMOVE en WAL
      writeToWAL('REMOVE', next); 
      grantCoordLock(next);
  }
}

function stepDown() {
  isCoordinator = false;
  if (leaseTimeout) clearInterval(leaseTimeout);
  console.log('[ROLE] Descendido a Seguidor (Step Down).');
  client.unsubscribe(config.topics.mutex_request);
  client.unsubscribe(config.topics.mutex_release);
  lastLeaseSeen = Date.now();
}

function renewLease() {
  if (!isCoordinator) return;
  const payload = JSON.stringify({ coordinatorId: DEVICE_ID, timestamp: Date.now() });
  client.publish(TOPIC_LEASE, payload, { qos: 0 });
}

// ============================================================================
//                  LÓGICA DE SERVIDOR MUTEX
// ============================================================================

function handleCoordRequest(requesterId) {
  console.log(`[COORD] Procesando solicitud de: ${requesterId}`);
  if (coord_isLockAvailable) {
    grantCoordLock(requesterId);
  } else {
    if (!coord_waitingQueue.includes(requesterId) && coord_lockHolder !== requesterId) {
      console.log(`[WAL] Escribiendo ADD para ${requesterId}`);
      // --- [FASE 3] Escribir en WAL (ADD) ---
      writeToWAL('ADD', requesterId);
      
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
      // Extraemos al siguiente de la cola
      const next = coord_waitingQueue.shift();
      // --- [FASE 3] Escribir en WAL (REMOVE) ---
      writeToWAL('REMOVE', next);
      
      grantCoordLock(next);
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
  if (sensorState === 'IDLE' && !isCoordinator) { 
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
  if (vectorClock[PROCESS_ID] !== undefined) {
      vectorClock[PROCESS_ID]++;
  }
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
    sensor_state: isCoordinator ? 'COORDINATOR' : sensorState 
  };
  client.publish(config.topics.telemetry(DEVICE_ID), JSON.stringify(telemetryData));
}

// ============================================================================
//                        PERSISTENCIA (WAL) - FASE 3
// ============================================================================

function writeToWAL(action, deviceId) {
  // Formato simple: JSON por línea
  const logEntry = JSON.stringify({ action, deviceId, timestamp: Date.now() });
  try {
    // appendFileSync bloquea un poco el proceso pero garantiza consistencia
    fs.appendFileSync(WAL_FILE, logEntry + '\n');
  } catch (err) {
    console.error('[WAL] Error escribiendo en disco:', err.message);
  }
}

function restoreStateFromWAL() {
  console.log('[WAL] Iniciando recuperación de estado...');
  
  if (!fs.existsSync(WAL_FILE)) {
    console.log('[WAL] No existe archivo de log previo. Iniciando limpio.');
    return [];
  }

  const tempQueue = [];
  try {
    const fileContent = fs.readFileSync(WAL_FILE, 'utf-8');
    const lines = fileContent.split('\n');

    lines.forEach(line => {
      if (!line.trim()) return; 
      try {
        const entry = JSON.parse(line);
        if (entry.action === 'ADD') {
          if (!tempQueue.includes(entry.deviceId)) {
            tempQueue.push(entry.deviceId);
          }
        } else if (entry.action === 'REMOVE') {
          // FIFO: el remove siempre saca al primero que entró
          tempQueue.shift();
        }
      } catch (e) {
        console.warn('[WAL] Línea corrupta en log:', line);
      }
    });

    console.log(`[RECOVERY] Restored queue: [${tempQueue.join(', ')}]`);
    return tempQueue;

  } catch (err) {
    console.error('[WAL] Error crítico leyendo WAL:', err.message);
    return [];
  }
}