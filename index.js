const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const getData = require('./dataRequest');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };
    
    const dropSchema = async () => {
        await db.dropSchema(DATABASE_SCHEMA, { cascade: true });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    const insertData = async (data) => {
        await Promise.all(data.map((item) => {
            return db[DATABASE_SCHEMA].api_data.insert({
                api_name: 'datausa.io',
                doc_id: item['ID Nation'],
                doc_name: `Population USA year ${item.Year}`,
                doc_record: item
            });
        }));
    };

    const sumPopulationForYear = (data, startYear, endYear) => {
        return data.reduce((acc, item) => {
            if (item.Year >= startYear && item.Year <= endYear) {
                acc += item.Population;
            }
            return acc;
        }, 0);
    };

    try {
        await dropSchema();
        await migrationUp();
        const data = await getData();
        await insertData(data);
        const sumPopulation = sumPopulationForYear(data, 2018, 2020);
        console.log(`Population from 2018 to 2020 is ${sumPopulation}`);
        
        //exemplo select
        const result2 = await db[DATABASE_SCHEMA].api_data.find({
            is_active: true
        });
        console.log('result2 >>>', result2);

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();
