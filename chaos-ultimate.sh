#!/bin/bash

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW} INICIANDO PROYECTO EL CURSO FACILITO: PROTOCOLO DE EVALUACIÓN ${NC}"
echo "Asegúrese de tener 5 publishers corriendo: publisher-1 al publisher-5"
sleep 2

# ==========================================
# NIVEL 1: RESGUARDANDO EL TIEMPO
# ==========================================
echo -e "\n${YELLOW} [FASE 1] Probando Integridad Temporal...${NC}"

echo " -> Inyectando mensaje del año 2050..."
# Nota: Ajusta 'mqtt-broker' si tu contenedor se llama diferente
docker exec mqtt-broker mosquitto_pub -t "utp/sistemas_distribuidos/grupo1/sensor-malicioso/telemetry" -m '{"deviceId": "malicious-node", "timestamp": "2050-01-01T00:00:00Z", "temperatura": 1000, "humedad": 50}'

sleep 2

# Verificamos si el suscriptor lo rechazó
if docker logs persistence-subscriber 2>&1 | grep -q "future packet"; then
    echo -e "${GREEN} ÉXITO: El ataque temporal fue neutralizado.${NC}"
else
    echo -e "${RED} FALLO: El sistema aceptó datos corruptos o no logueó el rechazo.${NC}"
    # No salimos con exit 1 para permitir probar las otras fases, pero en examen estricto sería exit 1
fi

# ==========================================
# NIVEL 2: SPLIT-BRAIN & QUÓRUM
# ==========================================
echo -e "\n${YELLOW} [FASE 2] Probando Estabilidad de Liderazgo...${NC}"

# Identificar al líder actual (buscamos en los logs quien dice ser COORDINADOR)
# Buscamos en los 5 publishers
LEADER_CONTAINER=""
for i in {1..5}; do
    if docker logs publisher-$i 2>&1 | grep -q "ASCENDIDO A COORDINADOR"; then
        LEADER_CONTAINER="publisher-$i"
    fi
done

if [ -z "$LEADER_CONTAINER" ]; then
    echo -e "${RED} ERROR: No se detectó ningún líder activo. Asegúrate que el sistema arrancó.${NC}"
    LEADER_CONTAINER="publisher-5" # Fallback por defecto
else 
    echo " -> Líder actual detectado: $LEADER_CONTAINER"
fi

# Pausamos al líder (simulamos congelamiento)
echo " -> Congelando al líder ($LEADER_CONTAINER) con SIGSTOP..."
docker pause $LEADER_CONTAINER

echo " -> Esperando 7 segundos (Mayor que el Lease de 5s)..."
sleep 7 

# Verificamos si alguien más tomó el mando
echo " -> Buscando nuevo líder..."
NEW_LEADER_FOUND=false
for i in {1..5}; do
    # Saltamos al líder congelado
    if [ "publisher-$i" == "$LEADER_CONTAINER" ]; then continue; fi
    
    # Buscamos mensaje de victoria reciente
    if docker logs --tail 20 publisher-$i 2>&1 | grep -q "VICTORY"; then
        echo -e "${GREEN} Nuevo líder encontrado: publisher-$i ${NC}"
        NEW_LEADER_FOUND=true
        break
    fi
done

if [ "$NEW_LEADER_FOUND" = false ]; then
    echo -e "${RED} FALLO: Nadie tomó el liderazgo tras la caída del líder.${NC}"
fi

# Descongelamos
echo " -> Descongelando al viejo líder..."
docker unpause $LEADER_CONTAINER
sleep 3

# Verificamos que el viejo líder renuncie (Step Down)
if docker logs --tail 20 $LEADER_CONTAINER 2>&1 | grep -q "Step Down"; then
    echo -e "${GREEN} ÉXITO: Transición de poder ordenada y recuperación de Split-Brain.${NC}"
else
    echo -e "${YELLOW} ALERTA: Verificar manualmente si hay dos líderes. Buscando 'Step Down' en logs.${NC}"
fi

# ==========================================
# NIVEL 3: PERSISTENCIA (WAL)
# ==========================================
echo -e "\n${YELLOW} [FASE 3] Probando Recuperación de Estado (WAL) ${NC}"

echo " -> Generando tráfico para llenar la cola..."
# Simulamos peticiones de recurso desde el nodo 2
docker exec -d publisher-2 node -e "
const mqtt = require('mqtt');
const client = mqtt.connect('mqtt://mqtt-broker:1883');
client.on('connect', () => {
    client.publish('utp/sistemas_distribuidos/grupo1/mutex/request', JSON.stringify({deviceId: 'sensor-test-wal'}));
    setTimeout(() => process.exit(0), 1000);
});"

sleep 2

# Matamos violentamente al contenedor líder actual (o publisher-5 si no sabemos)
TARGET_KILL=${LEADER_CONTAINER:-publisher-5}
echo " -> KILL -9 al Líder ($TARGET_KILL)..."
docker kill $TARGET_KILL
# Docker compose está configurado para reiniciar on-failure o restart: always? 
# Si no, lo iniciamos manual
docker start $TARGET_KILL

echo " -> Esperando reinicio..."
sleep 5

# Verificamos si recuperó la memoria
if docker logs --tail 50 $TARGET_KILL 2>&1 | grep -E "WAL|Restored|queue"; then
    echo -e "${GREEN} ÉXITO: El sistema recordó el estado previo a la muerte (WAL).${NC}"
else
    echo -e "${RED} FALLO: El líder revivió con Amnesia (sin logs de recuperación).${NC}"
fi

echo -e "\n${YELLOW} EVALUACIÓN COMPLETADA ${NC}"