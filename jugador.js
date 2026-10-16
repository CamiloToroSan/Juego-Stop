const readline = require('readline');
const amqp = require('amqplib');

// ========= CONFIGURACION =========
const RABBITMQ_URL = 'amqp://172.162.1.17';

// ========= DATOS DEL JUGADOR =========
const MI_ID = process.argv[2] || 'toro';

// ========= EXCHANGES =========
const EX_DIRECTO = 'stop_directo';
const EX_TOPICO  = 'stop_topico';
const EX_FANOUT  = 'stop_fanout';

// ========= CATEGORIAS (con preguntas) =========
const CATEGORIAS = [
    { id: 'nombre',    pregunta: 'NOMBRE',    pista: 'Un nombre de persona' },
    { id: 'apellido',  pregunta: 'APELLIDO',  pista: 'Un apellido de persona' },
    { id: 'ciudad',    pregunta: 'CIUDAD',    pista: 'Una ciudad del mundo' },
    { id: 'pais',      pregunta: 'PAIS',      pista: 'Un pais' },
    { id: 'animal',    pregunta: 'ANIMAL',    pista: 'Un animal' },
    { id: 'fruta',     pregunta: 'FRUTA',     pista: 'Una fruta o verdura' },
    { id: 'color',     pregunta: 'COLOR',     pista: 'Un color' },
    { id: 'cosa',      pregunta: 'COSA',      pista: 'Un objeto' },
    { id: 'profesion', pregunta: 'PROFESION', pista: 'Una profesion u oficio' },
    { id: 'marca',     pregunta: 'MARCA',     pista: 'Una marca comercial' }
];

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

let channel;
let miLetra = null;
let respuestas = {};
let puntajeLocal = 0;

function preguntar(texto) {
    return new Promise(resolve => {
        rl.question(texto, respuesta => resolve(respuesta));
    });
}

async function iniciar() {
    const conn = await amqp.connect(RABBITMQ_URL);
    channel = await conn.createChannel();

    await channel.assertExchange(EX_DIRECTO, 'direct', { durable: false });
    await channel.assertExchange(EX_TOPICO,  'topic',  { durable: false });
    await channel.assertExchange(EX_FANOUT,  'fanout', { durable: false });

    // DIRECTO: cola personal con mi ID
    const qDirecto = await channel.assertQueue(MI_ID, { durable: false, autoDelete: true });
    await channel.bindQueue(qDirecto.queue, EX_DIRECTO, MI_ID);

    // FANOUT: anuncios globales
    const qFanout = await channel.assertQueue('', { exclusive: true });
    await channel.bindQueue(qFanout.queue, EX_FANOUT, '');

    console.log(`\n=== ${MI_ID} EN JUEGO STOP ===`);
    console.log(`Conectado a ${RABBITMQ_URL}`);
    console.log(`Categorias del juego:`);
    CATEGORIAS.forEach((c, i) => console.log(`  ${i+1}. ${c.pregunta}`));
    console.log(`\nEsperando letra del moderador...\n`);

    // CONSUMER DIRECTO: recibo mi letra y arranco el juego
    channel.consume(qDirecto.queue, async (msg) => {
        const data = JSON.parse(msg.content.toString());
        miLetra = data.letra;
        respuestas = {};
        puntajeLocal = 0;

        console.log(`\n${'='.repeat(50)}`);
        console.log(`   TU LETRA ES: "${miLetra.toUpperCase()}"`);
        console.log(`   Puntaje actual: 0`);
        console.log(`${'='.repeat(50)}\n`);
        console.log(`Responde ${CATEGORIAS.length} categorias. Todas deben empezar con "${miLetra}".\n`);

        await jugarInteractivo();
    }, { noAck: true });

    // CONSUMER FANOUT
    channel.consume(qFanout.queue, (msg) => {
        try {
            const data = JSON.parse(msg.content.toString());
            console.log(`\n*** ${data.texto} ***\n`);
        } catch (e) {
            console.log(`\n*** ${msg.content.toString()} ***\n`);
        }
    }, { noAck: true });

    process.on('SIGINT', async () => {
        console.log('\nCerrando...');
        rl.close();
        try { await channel.close(); } catch (_) {}
        try { await conn.close(); } catch (_) {}
        process.exit(0);
    });
}

async function jugarInteractivo() {
    const inicio = Date.now();
    let correctas = 0;

    for (let i = 0; i < CATEGORIAS.length; i++) {
        const cat = CATEGORIAS[i];
        console.log(`--- Categoria ${i+1}/${CATEGORIAS.length}: ${cat.pregunta} (${cat.pista}) ---`);

        let palabraValida = null;
        let intentos = 0;
        const MAX_INTENTOS = 5;

        while (!palabraValida && intentos < MAX_INTENTOS) {
            const resp = await preguntar(`[${cat.pregunta}] Palabra con "${miLetra}" (o "skip" para saltar): `);
            const limpia = resp.trim();
            intentos++;

            // Permitir saltar
            if (limpia.toLowerCase() === 'skip') {
                console.log(`   [~] Saltaste ${cat.pregunta}\n`);
                break;
            }

            if (limpia.length === 0) {
                console.log(`   [!] No puedes dejar vacio. Intenta de nuevo.\n`);
                continue;
            }
            if (limpia.charAt(0).toUpperCase() !== miLetra.toUpperCase()) {
                console.log(`   [!] "${limpia}" empieza con "${limpia.charAt(0)}". Debe empezar con "${miLetra}".\n`);
                continue;
            }
            palabraValida = limpia;
        }

        if (palabraValida) {
            respuestas[cat.id] = palabraValida;
            // Enviar respuesta por TOPIC
            channel.publish(EX_TOPICO, `respuesta.${cat.id}`, Buffer.from(JSON.stringify({
                de: MI_ID,
                categoria: cat.id,
                palabra: palabraValida
            })));
            console.log(`   [OK] ${cat.pregunta}: "${palabraValida}" enviado.\n`);
            correctas++;
        } else if (intentos >= MAX_INTENTOS) {
            console.log(`   [x] Demasiados intentos. Saltando ${cat.pregunta}.\n`);
        }
    }

    const tiempo = ((Date.now() - inicio) / 1000).toFixed(2);
    console.log(`\n${'='.repeat(50)}`);
    console.log(`   HAS TERMINADO!`);
    console.log(`   Categorias respondidas: ${correctas}/${CATEGORIAS.length}`);
    console.log(`   Tiempo: ${tiempo} segundos`);
    console.log(`${'='.repeat(50)}`);
    console.log(`\n*** Enviando STOP al moderador... ***\n`);

    // Enviar STOP por TOPIC
    channel.publish(EX_TOPICO, `stop.${MI_ID}`, Buffer.from(JSON.stringify({
        de: MI_ID,
        tiempo: Date.now(),
        duracion: tiempo,
        correctas
    })));
}

iniciar().catch(err => {
    console.error('Error:', err.message);
    process.exit(1);
});