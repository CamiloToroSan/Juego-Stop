const amqp = require('amqplib');

// ========= CONFIGURACION =========
const RABBITMQ_URL = 'amqp://172.162.1.1';

// ========= EXCHANGES =========
const EX_DIRECTO = 'stop_directo';
const EX_TOPICO  = 'stop_topico';
const EX_FANOUT  = 'stop_fanout';

// ========= CATEGORIAS =========
const CATEGORIAS = [
    'nombre',
    'apellido',
    'ciudad',
    'pais',
    'animal',
    'fruta',
    'color',
    'cosa',
    'profesion',
    'marca'
];

const LETRAS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');

// ========= DATOS DE LA CLASE =========
const ESTUDIANTES = [
    'toro',
    'alvarado',
    'arias',
    'arevalo',
    'bastidas',
    'burbanoJose',
    'burbanoValentina',
    'cajigas',
    'calzada',
    'castillo',
    'cordoba',
    'galvez',
    'garcia',
    'gomez',
    'guerrero',
    'limas',
    'lucero',
    'luna',
    'mallama',
    'martinez1',
    'martinez2',
    'maya',
    'meneses',
    'mideros',
    'montero',
    'mora',
    'moreno',
    'noguera',
    'ojeda',
    'ortega',
    'paredes',
    'pulsara',
    'ramirez',
    'solarte',
    'valencia',
    'villota',
    'wagimin',
    'yepes',
    'zambrano'
];

const DURACION_MAX_MS = 180000; // 3 minutos maximo
const sleep = ms => new Promise(r => setTimeout(r, ms));

let channel;
let respuestasRonda = new Map();
let stopTimes = new Map();
let rondaInicio = 0;

async function main() {
    const conn = await amqp.connect(RABBITMQ_URL);
    channel = await conn.createChannel();

    await channel.assertExchange(EX_DIRECTO, 'direct', { durable: false });
    await channel.assertExchange(EX_TOPICO,  'topic',  { durable: false });
    await channel.assertExchange(EX_FANOUT,  'fanout', { durable: false });

    console.log('=== MODERADOR: JUEGO STOP ===');
    console.log(`Jugadores: ${ESTUDIANTES.length}`);
    console.log(`Categorias: ${CATEGORIAS.join(', ')}`);
    console.log(`Todos empiezan con 0 puntos.\n`);

    await sleep(3000);

    channel.publish(EX_FANOUT, '', Buffer.from(JSON.stringify({
        texto: 'EL JUEGO STOP COMIENZA EN 3 SEGUNDOS'
    })));
    await sleep(3000);

    await jugarRonda();

    console.log('\n=== JUEGO TERMINADO ===');
    await sleep(2000);
    await channel.close();
    await conn.close();
    process.exit(0);
}

async function jugarRonda() {
    respuestasRonda.clear();
    stopTimes.clear();

    // Inicializar todos con 0
    ESTUDIANTES.forEach(id => respuestasRonda.set(id, {}));

    // Escuchar respuestas y stops
    const rq = await channel.assertQueue('', { exclusive: true });
    await channel.bindQueue(rq.queue, EX_TOPICO, 'respuesta.#');
    await channel.bindQueue(rq.queue, EX_TOPICO, 'stop.#');

    channel.consume(rq.queue, (msg) => {
        const rk = msg.fields.routingKey;
        const data = JSON.parse(msg.content.toString());

        if (rk.startsWith('respuesta.')) {
            if (!respuestasRonda.has(data.de)) respuestasRonda.set(data.de, {});
            respuestasRonda.get(data.de)[data.categoria] = data.palabra;
            console.log(`  <- ${data.de} [${data.categoria}]: "${data.palabra}"`);
        } else if (rk.startsWith('stop.')) {
            if (!stopTimes.has(data.de)) {
                const t = (Date.now() - rondaInicio) / 1000;
                stopTimes.set(data.de, t);
                console.log(`  !! ${data.de} TERMINO en ${t.toFixed(2)}s (${data.correctas} categorias)`);
            }
        }
    }, { noAck: true });

    // Asignar letras aleatorias
    const letrasDisponibles = [...LETRAS].sort(() => Math.random() - 0.5);

    console.log('\n--- ASIGNANDO LETRAS ---');
    ESTUDIANTES.forEach((id, i) => {
        const letra = letrasDisponibles[i % letrasDisponibles.length];
        channel.publish(EX_DIRECTO, id, Buffer.from(JSON.stringify({
            letra,
            categorias: CATEGORIAS
        })));
        console.log(`  ${id} -> "${letra.toUpperCase()}"`);
    });

    rondaInicio = Date.now();

    channel.publish(EX_FANOUT, '', Buffer.from(JSON.stringify({
        texto: 'RONDA INICIADA. Responde todas las categorias!'
    })));

    console.log(`\nEsperando respuestas... (maximo ${DURACION_MAX_MS/1000}s)`);
    console.log(`La ronda termina cuando TODOS envien STOP.\n`);

    const inicioEspera = Date.now();
    while (
        stopTimes.size < ESTUDIANTES.length &&
        (Date.now() - inicioEspera) < DURACION_MAX_MS
    ) {
        await sleep(3000);
        const transcurrido = ((Date.now() - inicioEspera) / 1000).toFixed(0);
        console.log(`  [${transcurrido}s] Terminaron: ${stopTimes.size}/${ESTUDIANTES.length}`);
    }

    console.log('\n--- FIN DE LA RONDA ---');
    if (stopTimes.size < ESTUDIANTES.length) {
        console.log(`  Se acabo el tiempo. ${stopTimes.size} de ${ESTUDIANTES.length} terminaron.`);
    } else {
        console.log(`  Todos terminaron!`);
    }

    const puntajes = calcularPuntajes();
    const mensaje = generarMensajeFinal(puntajes);
    channel.publish(EX_FANOUT, '', Buffer.from(JSON.stringify({
        texto: mensaje
    })));
    console.log('\n' + mensaje);
}

function calcularPuntajes() {
    console.log('\n--- PUNTAJES POR PALABRAS ---');

    const porCategoria = {};
    CATEGORIAS.forEach(cat => { porCategoria[cat] = []; });

    for (const [id, resp] of respuestasRonda) {
        for (const cat of CATEGORIAS) {
            if (resp[cat]) {
                porCategoria[cat].push({ id, palabra: resp[cat].toLowerCase().trim() });
            }
        }
    }

    const puntajes = {};
    ESTUDIANTES.forEach(id => { puntajes[id] = 0; });

    for (const cat of CATEGORIAS) {
        const palabras = porCategoria[cat];
        const contador = {};
        palabras.forEach(p => {
            contador[p.palabra] = (contador[p.palabra] || 0) + 1;
        });

        palabras.forEach(p => {
            const puntos = contador[p.palabra] === 1 ? 10 : 5;
            puntajes[p.id] += puntos;
            console.log(`  ${p.id} - ${cat}: "${p.palabra}" (${contador[p.palabra] === 1 ? 'UNICA' : 'repetida'}) -> +${puntos}`);
        });
    }

    const ordenStop = [...stopTimes.entries()].sort((a, b) => a[1] - b[1]);
    const bonus = [50, 30, 15];

    console.log('\n--- BONUS POR VELOCIDAD ---');
    if (ordenStop.length === 0) {
        console.log('  Nadie termino a tiempo.');
    } else {
        ordenStop.forEach(([id, t], i) => {
            if (i < bonus.length) {
                puntajes[id] += bonus[i];
                console.log(`  ${i+1}° ${id} (${t.toFixed(2)}s) -> +${bonus[i]} pts`);
            } else {
                console.log(`  ${i+1}° ${id} (${t.toFixed(2)}s) -> sin bonus`);
            }
        });
    }

    console.log('\n--- TOTALES ---');
    Object.entries(puntajes)
        .sort((a, b) => b[1] - a[1])
        .forEach(([id, pts]) => {
            console.log(`  ${id}: ${pts} puntos`);
        });

    return puntajes;
}

function generarMensajeFinal(puntajes) {
    const ranking = Object.entries(puntajes).sort((a, b) => b[1] - a[1]);
    const top3 = ranking.slice(0, 3);

    return `FIN DE RONDA - TOP 3: 1°) ${top3[0][0]} (${top3[0][1]}pts), 2°) ${top3[1] ? top3[1][0] : 'N/A'} (${top3[1] ? top3[1][1] : 0}pts), 3°) ${top3[2] ? top3[2][0] : 'N/A'} (${top3[2] ? top3[2][1] : 0}pts)`;
}

main().catch(err => {
    console.error('Error:', err.message);
    process.exit(1);
});