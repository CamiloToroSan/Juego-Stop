const readline = require('readline');
const amqp = require('amqplib');

// ========= CONFIGURACION =========
const RABBITMQ_URL = 'amqp://172.162.1.1';

// ========= DATOS DEL JUGADOR =========
const MI_ID = process.argv[2] || 'toro';

// ========= EXCHANGES =========
const EX_DIRECTO = 'stop_directo';
const EX_TOPICO  = 'stop_topico';
const EX_FANOUT  = 'stop_fanout';

// ========= CATEGORIAS =========
const CATEGORIAS = [
    { id: 'nombre',    pregunta: 'NOMBRE' },
    { id: 'apellido',  pregunta: 'APELLIDO' },
    { id: 'ciudad',    pregunta: 'CIUDAD' },
    { id: 'pais',      pregunta: 'PAIS' },
    { id: 'animal',    pregunta: 'ANIMAL' },
    { id: 'fruta',     pregunta: 'FRUTA' },
    { id: 'color',     pregunta: 'COLOR' },
    { id: 'cosa',      pregunta: 'COSA' },
    { id: 'profesion', pregunta: 'PROFESION' },
    { id: 'marca',     pregunta: 'MARCA' }
];

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

let channel;
let miLetra = null;
let respuestas = {};
let respondiendo = false;       // true mientras pregunta
let mensajesPendientes = [];    // fanout que llegan mientras pregunta

function preguntar(texto) {
    return new Promise(resolve => {
        rl.question(texto, respuesta => resolve(respuesta));
    });
}

function linea() {
    console.log('--------------------------------------------------');
}

async function iniciar() {
    const conn = await amqp.connect(RABBITMQ_URL);
    channel = await conn.createChannel();

    await channel.assertExchange(EX_DIRECTO, 'direct', { durable: false });
    await channel.assertExchange(EX_TOPICO,  'topic',  { durable: false });
    await channel.assertExchange(EX_FANOUT,  'fanout', { durable: false });

    const qDirecto = await channel.assertQueue(MI_ID, { durable: false, autoDelete: true });
    await channel.bindQueue(qDirecto.queue, EX_DIRECTO, MI_ID);

    const qFanout = await channel.assertQueue('', { exclusive: true });
    await channel.bindQueue(qFanout.queue, EX_FANOUT, '');

    console.log('');
    linea();
    console.log(`${MI_ID.toUpperCase()} - JUEGO STOP`);
    linea();
    console.log('');
    console.log(`Conectado a: ${RABBITMQ_URL}`);
    console.log('');
    console.log(`Categorias del juego (${CATEGORIAS.length}):`);
    CATEGORIAS.forEach((c, i) => {
        console.log(`  ${i + 1}. ${c.pregunta}`);
    });
    console.log('');
    linea();
    console.log('Esperando que el moderador te asigne una letra...');
    linea();
    console.log('');

    channel.consume(qDirecto.queue, async (msg) => {
        const data = JSON.parse(msg.content.toString());
        miLetra = data.letra;
        respuestas = {};

        console.log('');
        linea();
        console.log(`TU LETRA ES: "${miLetra.toUpperCase()}"`);
        linea();
        console.log('');
        console.log(`Todas las palabras deben empezar con "${miLetra.toUpperCase()}".`);
        console.log(`Puedes escribir "skip" para saltar una categoria.`);
        console.log('');

        await jugarInteractivo();
    }, { noAck: true });

    channel.consume(qFanout.queue, (msg) => {
        let texto = '';
        try {
            const data = JSON.parse(msg.content.toString());
            texto = data.texto;
        } catch (e) {
            texto = msg.content.toString();
        }

        if (respondiendo) {
            // Guardar para mostrarlo después de la pregunta actual
            mensajesPendientes.push(texto);
        } else {
            console.log('');
            linea();
            console.log(texto);
            linea();
            console.log('');
        }
    }, { noAck: true });

    process.on('SIGINT', async () => {
        console.log('');
        console.log('Cerrando...');
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

        let palabraValida = null;
        let intentos = 0;
        const MAX_INTENTOS = 5;

        respondiendo = true;
        while (!palabraValida && intentos < MAX_INTENTOS) {
            const resp = await preguntar(`${cat.pregunta}: `);
            const limpia = resp.trim();
            intentos++;

            if (limpia.toLowerCase() === 'skip') {
                console.log(`Saltaste ${cat.pregunta}.`);
                console.log('');
                break;
            }

            if (limpia.length === 0) {
                console.log('No puedes dejar vacio. Intenta de nuevo.');
                continue;
            }

            if (limpia.charAt(0).toUpperCase() !== miLetra.toUpperCase()) {
                console.log(`"${limpia}" empieza con "${limpia.charAt(0).toUpperCase()}". Debe empezar con "${miLetra.toUpperCase()}".`);
                continue;
            }

            palabraValida = limpia;
        }
        respondiendo = false;

        // Mostrar mensajes fanout que llegaron mientras respondia
        if (mensajesPendientes.length > 0) {
            mensajesPendientes.forEach(m => {
                console.log('');
                linea();
                console.log(m);
                linea();
                console.log('');
            });
            mensajesPendientes = [];
        }

        if (palabraValida) {
            respuestas[cat.id] = palabraValida;
            channel.publish(EX_TOPICO, `respuesta.${cat.id}`, Buffer.from(JSON.stringify({
                de: MI_ID,
                categoria: cat.id,
                palabra: palabraValida
            })));
            console.log(`"${palabraValida}" enviado.`);
            console.log('');
            correctas++;
        } else if (intentos >= MAX_INTENTOS) {
            console.log(`Demasiados intentos. Saltando ${cat.pregunta}.`);
            console.log('');
        }
    }

    const tiempo = ((Date.now() - inicio) / 1000).toFixed(2);

    console.log('');
    linea();
    console.log('HAS TERMINADO!');
    linea();
    console.log('');
    console.log(`Categorias respondidas: ${correctas}/${CATEGORIAS.length}`);
    console.log(`Tiempo total: ${tiempo} segundos`);
    console.log('');
    linea();
    console.log('Enviando STOP al moderador...');
    linea();
    console.log('');

    channel.publish(EX_TOPICO, `stop.${MI_ID}`, Buffer.from(JSON.stringify({
        de: MI_ID,
        tiempo: Date.now(),
        duracion: tiempo,
        correctas
    })));

    console.log('Esperando resultados finales del moderador...');
    console.log('');
}

iniciar().catch(err => {
    console.error('Error:', err.message);
    process.exit(1);
});