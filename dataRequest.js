const axios = require('axios');

const URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population";

const getData = async () => {
  const { data: { data } } = await axios.get(URL);
  return data;
}

module.exports = getData;
