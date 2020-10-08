import mongodb from "mongodb";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });

const keysToPopulate = [
  "Artist Display Name",
  "Artist Display Bio",
  "Artist Alpha Sort",
  "Artist Nationality",
  "Artist Begin Date",
  "Artist End Date",
  "Artist Gender",
  "Artist ULAN URL",
  "Artist Wikidata URL",
];

const conformCountry = (object, index) => {
  const country = {};
  keysToPopulate.forEach((key) => {
    const values = object[key].split("|");
    country[key] = values[index] ? values[index].trim() : "";
  });
  return country;
};

const populate_countries = async () => {
  console.log("Populating countries...");
  try {
    await client.connect();
    const database = client.db("metmuseum-raw");
    const objects = database.collection("objects");
    const countries = database.collection("countries");

    const cursor = objects.find({});

    let object;
    let count = 0;
    let inserted = 0;

    while ((object = await cursor.next())) {
      count += 1;
      const objectCountries = object.Country.split("|");

      const countriesData = [];

      objectCountries.forEach((country, index) => {
        if (country) {
          countriesData.push({ Country: country });
        }
      });

      if (countriesData.length) {
        await countriesData.reduce(async (prev, countryData) => {
          await prev;
          const countryInDB = await countries
            .find({ Country: countryData.Country })
            .count();

          if (countryInDB) {
            return Promise.resolve();
          }
          inserted += 1;
          return countries.insertOne(countryData);
        }, Promise.resolve());
      }
      process.stdout.write(
        `Analyzing object: ${count}, inserted: ${inserted}\r`
      );
    }

    process.stdout.write("\r\n");
    console.log("Inserted ", inserted, " countries.");
    await client.close();
  } catch (err) {
    throw err;
  }
};

populate_countries();
