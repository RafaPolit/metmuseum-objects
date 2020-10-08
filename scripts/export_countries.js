import fs from "fs";
import mongodb from "mongodb";

const mongoUri = "mongodb://localhost/";
const client = new mongodb.MongoClient(mongoUri, { useUnifiedTopology: true });

const escapeQuotes = (string) => {
  return string.replace(/\"/g, '""');
};

const export_countries = async () => {
  console.log("Exporting countries...");
  try {
    await client.connect();
    const database = client.db("metmuseum-raw");
    const countries = database.collection("countries");

    const cursor = countries.find({});

    let country;
    let exported = 0;

    var countriesCsv = fs.createWriteStream("../csvs/countries.csv", {
      flags: "a",
    });

    countriesCsv.write("title\r\n");

    while ((country = await cursor.next())) {
      process.stdout.write(`Exporting: ${country.Country}\r`);
      const row = [];
      row.push(`"${escapeQuotes(country.Country)}"`);
      countriesCsv.write(`${row.join(";")}\r\n`);
      exported += 1;
    }
    process.stdout.write("\r\n");

    console.log("Exported ", exported, " countries.");
    await client.close();
  } catch (err) {
    await client.close();
    throw err;
  }
};

export_countries();
