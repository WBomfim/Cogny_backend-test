const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const axios = require('axios');
const massive = require('massive');
const monitor = require('pg-monitor');

const URL_API_DATAUSA = "https://datausa.io/api/data?drilldowns=Nation&measures=Population";

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

    const getData = async () => {
        const { data: { data } } = await axios.get(URL_API_DATAUSA);
        return data;
    }

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
        const sumPopulationBeforeInsertion = sumPopulationForYear(data, 2018, 2020);

        await insertData(data);

        const responseMassive = await db[DATABASE_SCHEMA].api_data.find({ is_active: true });
        const extractedYearsInformations = responseMassive.map((item) => item.doc_record);
        const sumPopulationAfterInsertion = sumPopulationForYear(
            extractedYearsInformations, 2018, 2020
        );

        const [ { total: sumPopulationByQuery } ] = await db.query(
            `SELECT SUM(
                CAST(jsonb_extract_path_text(doc_record, 'Population') AS INTEGER)
            ) as total
            FROM ${DATABASE_SCHEMA}.api_data
            WHERE doc_record->>'Year' >= '2018' AND doc_record->>'Year' <= '2020';`
        );

        console.log(
            `
            The sum of the population Before insertion is: ${sumPopulationBeforeInsertion}
            The sum of the population After insertion is: ${sumPopulationAfterInsertion}
            The sum of the population by query is: ${sumPopulationByQuery}
            `
        );

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();
