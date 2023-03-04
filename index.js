const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const axios = require('axios');
const massive = require('massive');
const monitor = require('pg-monitor');

const URL_API_DATAUSA = "https://datausa.io/api/data?drilldowns=Nation&measures=Population";
const START_YEAR = 2018;
const END_YEAR = 2020;

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

    const sumPopulationForYear = (data) => {
        return data.reduce((acc, item) => {
            if (item.Year >= START_YEAR && item.Year <= END_YEAR) {
                acc += item.Population;
            }
            return acc;
        }, 0);
    };

    const findData = async () => {
        const response = await db[DATABASE_SCHEMA].api_data.find({ is_active: true });
        return response.map((item) => item.doc_record);
    };

    const querySumpopulation = `SELECT SUM(
        CAST(jsonb_extract_path_text(doc_record, 'Population') AS INTEGER)
    ) as total
    FROM ${DATABASE_SCHEMA}.api_data
    WHERE doc_record->>'Year' >= '${START_YEAR}' AND doc_record->>'Year' <= '${END_YEAR}';`

    const queryViewSumPopulation = `SELECT * FROM ${DATABASE_SCHEMA}.vw_total_population;`

    try {
        await dropSchema();
        await migrationUp();

        const dataApi = await getData();
        const sumPopulationBeforeInsertion = sumPopulationForYear(dataApi);

        await insertData(dataApi);

        const dataBanco = await findData();
        const sumPopulationAfterInsertion = sumPopulationForYear(dataBanco);

        const [ { total: sumPopulationByQuery } ] = await db.query(querySumpopulation);

        const [ { total: viewSumPopulation } ] = await db.query(queryViewSumPopulation);

        console.log(
            `
            Sum population between years ${START_YEAR} to ${END_YEAR}:
            > The sum Before insertion is: ${sumPopulationBeforeInsertion}
            > The sum After insertion is: ${sumPopulationAfterInsertion}
            > The sum by query is: ${sumPopulationByQuery}
            > The sum by view is: ${viewSumPopulation}
            `
        );

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();
