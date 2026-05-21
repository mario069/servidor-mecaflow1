const express = require('express');
const admin = require('firebase-admin');
const http = require('http');
const WebSocket = require('ws');
require('dotenv').config(); // 👈 Lee las variables ocultas del archivo .env en local y nube

const app = express();
// Habilitar lectura de JSON en las peticiones HTTP POST
app.use(express.json()); 

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 🔥 Inicialización de Firebase segura para la Nube y Local
if (process.env.NODE_ENV === 'production') {
  // En Render se consumen las credenciales desde las variables de entorno
  admin.initializeApp({
    credential: admin.credential.cert({
      projectId: process.env.FIREBASE_PROJECT_ID,
      clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
      privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n') // Corrige los saltos de línea del certificado
    }),
    databaseURL: process.env.FIREBASE_DATABASE_URL
  });
} else {
  // En tu PC local por si sigues haciendo pruebas antes de subir todo
  const serviceAccount = require("./mecaflow-42953-firebase-adminsdk-fbsvc-b873cf106b.json");
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://mecaflow-42953-default-rtdb.firebaseio.com/"
  });
}

const db = admin.database();
const ref = db.ref("mecaflow");

// Constante metrológica del costo del agua
const PRECIO_M3_MEXICO = (24.50 * 1.045).toFixed(2); 

// Inicialización de la estructura de datos en Firebase si no existe
ref.once("value", (snapshot) => {
    if (!snapshot.exists()) {
        ref.set({ 
            state: "OFF", 
            liters: 0.0, 
            flowRate: 0.0, 
            target: 0.0, 
            historicalLiters: 0.0, 
            leakWarning: false, 
            lastBatchVolume: 0.0,
            precioM3: parseFloat(PRECIO_M3_MEXICO),
            totalCostMXN: 0.0,
            simulatedLeak: false 
        });
    }
});

function calcularCosto(litros) {
    const metrosCubicos = litros / 1000;
    return (metrosCubicos * parseFloat(PRECIO_M3_MEXICO)).toFixed(2);
}

// =========================================================================
// 🌐 ENDPOINTS HTTP (Para la App Móvil APK y la Aplicación de Escritorio .EXE)
// =========================================================================

app.get('/api/status', (req, res) => {
    ref.once('value', (snapshot) => { 
        res.json(snapshot.val()); 
    });
});

app.post('/api/valve', async (req, res) => {
    const { state, target } = req.body;
    try {
        if (state === "PORTION") {
            await ref.update({ state, target: parseFloat(target), liters: 0.0, totalCostMXN: 0.0 });
        } else {
            await ref.update({ state, target: 0.0 });
        }
        res.json({ success: true });
    } catch (err) { res.status(500).send(err.message); }
});

app.post('/api/maintenance/leak-simulation', async (req, res) => {
    const { simulate } = req.body; 
    try {
        await ref.update({ simulatedLeak: simulate });
        res.json({ success: true, simulatedLeak: simulate });
    } catch (err) { res.status(500).send(err.message); }
});

app.post('/api/maintenance/reset', async (req, res) => {
    try {
        await ref.update({ 
            liters: 0.0, 
            flowRate: 0.0, 
            historicalLiters: 0.0, 
            lastBatchVolume: 0.0, 
            totalCostMXN: 0.0,
            leakWarning: false,
            simulatedLeak: false,
            state: "OFF",
            target: 0.0
        });
        res.json({ success: true, message: "Sistema MecaFlow restablecido a 0 de forma segura." });
    } catch (err) { res.status(500).send(err.message); }
});

app.post('/api/esp32/update', async (req, res) => {
    const { liters, flowRate, leakWarning, addedVolume } = req.body;
    try {
        if (addedVolume > 0) {
            await ref.child('historicalLiters').transaction((currentValue) => {
                return (currentValue || 0) + addedVolume;
            });
        }

        const nuevoCosto = calcularCosto(liters);

        await ref.update({ 
            liters, 
            flowRate, 
            leakWarning,
            precioM3: parseFloat(PRECIO_M3_MEXICO),
            totalCostMXN: parseFloat(nuevoCosto)
        });
        
        ref.once('value', (snapshot) => {
            const data = snapshot.val();
            const alertaFugaActiva = data.leakWarning || data.simulatedLeak;

            if (data.state === "PORTION" && data.liters >= data.target) {
                ref.update({ state: "OFF", target: 0.0, lastBatchVolume: data.liters });
                res.json({ state: "OFF", target: 0.0, leakAlarms: alertaFugaActiva });
            } else {
                res.json({ state: data.state, target: data.target, leakAlarms: alertaFugaActiva });
            }
        });
    } catch (err) { res.status(500).send(err.message); }
});

// =========================================================================
// 📡 CANAL DE WEBSOCKETS (Para transmisión en tiempo real de tu ESP32 Freenove)
// =========================================================================

wss.on('connection', (ws) => {
    console.log('[Cloud SCADA] Freenove ESP32 conectado con éxito por canal WebSocket.');

    // Escuchar telemetría entrante desde el hardware
    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);

            if (data.flowRate != undefined) {
                // Leer datos actuales de Firebase de forma rápida para cálculos internos
                ref.once('value', async (snapshot) => {
                    const currentData = snapshot.val() || {};
                    let currentLiters = currentData.liters || 0.0;
                    
                    // Cálculo dinámico del volumen si está pasando agua
                    if (data.flowRate > 0.3) {
                        currentLiters += (data.flowRate / 60.0); // Integración de flujo por segundo
                    }

                    const nuevoCosto = calcularCosto(currentLiters);
                    const alarmaFugaActiva = (currentData.state === "OFF" && data.flowRate > 0.5);

                    // Actualizar Firebase de un solo golpe
                    let updatePayload = {
                        flowRate: data.flowRate,
                        liters: parseFloat(currentLiters.toFixed(2)),
                        totalCostMXN: parseFloat(nuevoCosto),
                        leakWarning: alarmaFugaActiva
                    };

                    // Lógica de corte de agua automático en dosificación por lotes
                    if (currentData.state === "PORTION" && currentLiters >= currentData.target) {
                        updatePayload.state = "OFF";
                        updatePayload.target = 0.0;
                        updatePayload.lastBatchVolume = parseFloat(currentLiters.toFixed(2));
                    }

                    await ref.update(updatePayload);
                });
            }
        } catch (err) {
            console.error('Error procesando datos del WebSocket del ESP32:', err);
        }
    });

    // Monitorear cambios en el estado de la válvula en Firebase para mandárselos al ESP32 al milisegundo
    const nodeValveRef = db.ref('mecaflow/state');
    const valveListener = nodeValveRef.on('value', (snapshot) => {
        const estadoActual = snapshot.val();
        const payload = JSON.stringify({ state: estadoActual });
        
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(payload); // Envía la instrucción directa al ESP32
        }
    });

    // Limpieza de eventos al desconectar el hardware
    ws.on('close', () => {
        nodeValveRef.off('value', valveListener);
        console.log('[Cloud SCADA] Freenove ESP32 desconectado del socket.');
    });
});

// =========================================================================
// 🚀 ARRANQUE MULTIPLEXADO EN LA NUBE (Render / Local)
// =========================================================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`=======================================================`);
    console.log(`🤖 MecaFlow SCADA Cloud Server levantado con éxito.`);
    console.log(`📡 Servidor Compartido Express + WebSockets en puerto: ${PORT}`);
    console.log(`=======================================================`);
});