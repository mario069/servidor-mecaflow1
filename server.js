// =========================================================================
// 🤖 MecaFlow Cloud SCADA - Core Backend Gateway Server (server.js)
// =========================================================================
const express = require('express');
const admin = require('firebase-admin');
const http = require('http');
const WebSocket = require('ws');
require('dotenv').config();

const app = express();
app.use(express.json()); 

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// URL Real extraída de tu consola de Firebase
const DB_URL_REAL = "https://mecaflow-42953-default-rtdb.firebaseio.com/";

// 🔥 INICIALIZACIÓN BLINDADA PARA RENDER (Sin condicionales que fallen)
try {
    const formattedPrivateKey = process.env.FIREBASE_PRIVATE_KEY
        ? process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n').replace(/"/g, '').trim()
        : null;

    if (!process.env.FIREBASE_PROJECT_ID || !process.env.FIREBASE_CLIENT_EMAIL || !formattedPrivateKey) {
        throw new Error("Faltan variables de entorno críticas en el panel de Render.");
    }

    admin.initializeApp({
        credential: admin.credential.cert({
            projectId: process.env.FIREBASE_PROJECT_ID,
            clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
            privateKey: formattedPrivateKey
        }),
        databaseURL: DB_URL_REAL
    });
    console.log("🔥 Firebase Admin SDK enlazado exitosamente a la base de datos Realtime.");
} catch (error) {
    console.error("❌ ERROR CRÍTICO EN ENLACE: Intentando contingencia por variables...", error.message);
    
    try {
        const serviceAccount = require("./mecaflow-42953-firebase-adminsdk-fbsvc-b873cf106b.json");
        admin.initializeApp({
            credential: admin.credential.cert(serviceAccount),
            databaseURL: DB_URL_REAL
        });
        console.log("💻 Conectado mediante archivo JSON físico de respaldo.");
    } catch (e) {
        console.error("❌ Fallo total de inicialización de credenciales de Firebase:", e.message);
    }
}

const db = admin.database();
const ref = db.ref("mecaflow");

const PRECIO_M3_CANCUN = 72.50; 

// Estructura intermedia en memoria para procesar el Heartbeat sin depender de cambios en la BD
let estadoGlobalMemoria = {
    state: "OFF",
    liters: 0.0,
    flowRate: 0.0,
    target: 0.0,
    historicalLiters: 0.0,
    leakWarning: false,
    lastBatchVolume: 0.0,
    precioM3: PRECIO_M3_CANCUN,
    totalCostMXN: 0.0,
    simulatedLeak: false,
    timestamp: Date.now()
};

// Inicialización de estructura limpia en Firebase si está vacía
ref.once("value", (snapshot) => {
    if (!snapshot.exists()) {
        ref.set(estadoGlobalMemoria);
    } else {
        estadoGlobalMemoria = snapshot.val();
    }
});

// Escucha activa de Firebase: Sincroniza la memoria RAM del servidor con los cambios externos
ref.on('value', (snapshot) => {
    const data = snapshot.val();
    if (data) {
        estadoGlobalMemoria = data;
        retransmitirEstadoActualizado();
    }
});

// ⚡ REVISOR INDEPENDIENTE PERIÓDICO (Heartbeat Industrial)
// Evalúa la latencia del ESP32 de forma obligatoria cada 2.5 segundos
setInterval(() => {
    if (estadoGlobalMemoria.timestamp) {
        const tiempoInactivo = Date.now() - estadoGlobalMemoria.timestamp;
        const estaDesconectado = tiempoInactivo > 6000; // Tolerancia de 6 segundos

        // Forzar actualización a las interfaces HMI reportando la caída real del hardware
        broadcastToHMIs({
            ...estadoGlobalMemoria,
            hardwareOnline: !estaDesconectado,
            esp32Connected: !estaDesconectado
        });
    }
}, 2500);

function calcularCosto(litros) {
    return parseFloat(((litros / 1000) * PRECIO_M3_CANCUN).toFixed(2));
}

function retransmitirEstadoActualizado() {
    const isHardwareOffline = Date.now() - (estadoGlobalMemoria.timestamp || 0) > 6000;
    broadcastToHMIs({
        ...estadoGlobalMemoria,
        hardwareOnline: !isHardwareOffline,
        esp32Connected: !isHardwareOffline
    });
}

function broadcastToHMIs(payload) {
    const message = JSON.stringify(payload);
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

// =========================================================================
// 🌐 REST API HTTP ENDPOINTS
// =========================================================================
app.get('/api/status', (req, res) => {
    const isHardwareOffline = Date.now() - (estadoGlobalMemoria.timestamp || 0) > 6000;
    res.json({
        ...estadoGlobalMemoria,
        hardwareOnline: !isHardwareOffline,
        esp32Connected: !isHardwareOffline
    });
});

app.post('/api/valve', async (req, res) => {
    const { state, target } = req.body;
    try {
        let updatePayload = { state, timestamp: Date.now() };
        if (state === "PORTION") {
            updatePayload.target = parseFloat(target);
            updatePayload.liters = 0.0;
            updatePayload.totalCostMXN = 0.0;
        } else {
            updatePayload.target = 0.0;
        }
        await ref.update(updatePayload);
        res.json({ success: true });
    } catch (err) { res.status(500).send(err.message); }
});

app.post('/api/maintenance/reset', async (req, res) => {
    try {
        await ref.update({ 
            liters: 0.0, flowRate: 0.0, historicalLiters: 0.0, lastBatchVolume: 0.0, 
            totalCostMXN: 0.0, leakWarning: false, simulatedLeak: false, state: "OFF", target: 0.0, timestamp: Date.now()
        });
        res.json({ success: true });
    } catch (err) { res.status(500).send(err.message); }
});

// =========================================================================
// 📡 COMUNICACIÓN REAL-TIME WEBSOCKET
// =========================================================================
wss.on('connection', (ws) => {
    console.log('[WebSocket] Terminal HMI o ESP32 acoplado al bus.');
    
    // Envío inmediato al acoplarse una nueva pantalla
    ws.send(JSON.stringify(estadoGlobalMemoria));

    ws.on('message', async (message) => {
        try {
            const incoming = JSON.parse(message);

            // Telemetría directa del ESP32
            if (incoming.flowRate !== undefined) {
                let currentLiters = estadoGlobalMemoria.liters || 0.0;

                if (incoming.flowRate > 0.3) {
                    currentLiters += (incoming.flowRate / 60.0);
                }

                const nuevoCosto = calcularCosto(currentLiters);
                // Lógica de Válvula NA: Cerrada = ON. Si hay flujo estando cerrada -> Fuga activa
                const alarmaFugaActiva = (estadoGlobalMemoria.state === "ON" && incoming.flowRate > 0.4);

                let updatePayload = {
                    flowRate: incoming.flowRate,
                    liters: parseFloat(currentLiters.toFixed(2)),
                    totalCostMXN: nuevoCosto,
                    leakWarning: alarmaFugaActiva || estadoGlobalMemoria.simulatedLeak,
                    timestamp: Date.now()
                };

                if (estadoGlobalMemoria.state === "PORTION" && currentLiters >= estadoGlobalMemoria.target) {
                    updatePayload.state = "OFF";
                    updatePayload.target = 0.0;
                    updatePayload.lastBatchVolume = parseFloat(currentLiters.toFixed(2));
                }

                await ref.update(updatePayload);
            }
            // Comandos de acción desde las HMIs
            else if (incoming.action === "setValve") {
                await ref.update({ state: incoming.state, target: incoming.target || 0.0, timestamp: Date.now() });
            }
            else if (incoming.action === "resetSystem") {
                await ref.update({ liters: 0.0, flowRate: 0.0, totalCostMXN: 0.0, leakWarning: false, state: "OFF", target: 0.0, timestamp: Date.now() });
            }
        } catch (err) {
            console.error('Error en bus WebSocket:', err.message);
        }
    });

    // Envío directo de tramas al ESP32 ante conmutaciones en tiempo real
    const stateRef = db.ref('mecaflow/state');
    const stateListener = stateRef.on('value', (snapshot) => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ state: snapshot.val() }));
        }
    });

    ws.on('close', () => {
        stateRef.off('value', stateListener);
        console.log('[WebSocket] Conexión cerrada en el bus.');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`=======================================================`);
    console.log(`🤖 MecaFlow SCADA Cloud Server levantado con éxito.`);
    console.log(`📡 Servidor Compartido Express + WebSockets en puerto: ${PORT}`);
    console.log(`=======================================================`);
});