import mongodb from "mongodb";
import PromisePool from "@supercharge/promise-pool";

const mongoUri = "mongodb://localhost/";
const rawClient = new mongodb.MongoClient(mongoUri, {
  useUnifiedTopology: true,
});
const uwaziClient = new mongodb.MongoClient(mongoUri, {
  useUnifiedTopology: true,
});
const passedArgs = process.argv.slice(2);

const artistRelationType = mongodb.ObjectID("5f7b7fb11f9be41f0ff3c4be");
const countryRelationType = mongodb.ObjectID("5f7b85e01f9be41f0ff3f7f1");

const populateImages = async () => {
  console.log("Populating relationships...");
  console.time("populating");
  try {
    await rawClient.connect();
    await uwaziClient.connect();
    const raw = rawClient.db("metmuseum-raw");
    const uwazi = uwaziClient.db("metmuseum");
    const objects = raw.collection("objects");
    const entities = uwazi.collection("entities");
    const connections = uwazi.collection("connections");

    const startID = passedArgs[0] || 0;
    const endID = passedArgs[1] || 1000000;

    let added = 0;

    const countries = await entities
      .find({ template: mongodb.ObjectID("5f7b85d21f9be41f0ff3f6d8") })
      .toArray();

    const countriesHash = countries.reduce(
      (memo, c) => ({ ...memo, [c.title]: c.sharedId }),
      {}
    );

    const idsToProcess = await objects
      .find({
        $and: [
          { "Object ID": { $gte: Number(startID) } },
          { "Object ID": { $lte: Number(endID) } },
        ],
      })
      .sort({ "Object ID": 1 })
      .toArray();

    const { results, errors } = await PromisePool.for(idsToProcess)
      .withConcurrency(50)
      .process(async (object) => {
        process.stdout.write(
          `Processing: ${object["Object ID"]}, total additions: ${added}\r`
        );

        try {
          if (object["Artist Display Name"] || object.Country) {
            const entity = await entities.findOne(
              {
                "metadata.object_id.0.value": object["Object ID"],
              },
              { projection: { title: 1, sharedId: 1 } }
            );

            const artistsToAdd = [];
            const countriesToAdd = [];
            const relationshipsToAdd = [];

            if (object["Artist Display Name"]) {
              const artistsNames = object["Artist Display Name"]
                .split("|")
                .filter((n) => Boolean(n));

              const artistsHub = mongodb.ObjectID();

              relationshipsToAdd.push({
                entity: entity.sharedId,
                hub: artistsHub,
              });

              await artistsNames.reduce(async (prev, artistDisplayName) => {
                await prev;
                const artistSearchName = artistDisplayName.trim();
                const uwaziArtist = await entities.findOne(
                  {
                    template: mongodb.ObjectID("5f7b7f931f9be41f0ff3c2ee"),
                    title: artistSearchName,
                  },
                  {
                    projection: {
                      title: 1,
                      sharedId: 1,
                      "metadata.artist_nationality": 1,
                    },
                  }
                );
                if (!uwaziArtist) {
                  process.stdout.write("\r\n");
                  console.log(
                    "Error, no artist found for",
                    artistSearchName,
                    object["Object ID"]
                  );
                  throw new Error(
                    `Error, no artist found for ${object["Object ID"]}`
                  );
                }

                relationshipsToAdd.push({
                  entity: uwaziArtist.sharedId,
                  hub: artistsHub,
                  template: artistRelationType,
                });
                artistsToAdd.push(uwaziArtist);
              }, Promise.resolve());
            }

            if (object.Country) {
              const countriesNames = object.Country.split("|").filter((c) =>
                Boolean(c)
              );

              const countriesHub = mongodb.ObjectID();

              relationshipsToAdd.push({
                entity: entity.sharedId,
                hub: countriesHub,
              });

              countriesNames.forEach((countryName) => {
                const uwaziCountry = countriesHash[countryName.trim()];
                if (!uwaziCountry) {
                  process.stdout.write("\r\n");
                  console.log(
                    "Error, no country found for",
                    countryName.trim(),
                    object["Object ID"]
                  );
                  throw new Error(
                    `Error, no artist found for ${object["Object ID"]}`
                  );
                }

                relationshipsToAdd.push({
                  entity: uwaziCountry,
                  hub: countriesHub,
                  template: countryRelationType,
                });

                countriesToAdd.push({
                  title: countryName.trim(),
                  sharedId: uwaziCountry,
                });
              });
            }

            if (artistsToAdd.length || countriesToAdd.length) {
              const artist_display_name = artistsToAdd.map((a) => ({
                value: a.sharedId,
                label: a.title,
                icon: null,
                type: "entity",
              }));
              const artist_nationality = artistsToAdd
                .reduce(
                  (memo, a) =>
                    memo.concat(
                      a.metadata ? a.metadata.artist_nationality || [] : []
                    ),
                  []
                )
                .filter((n) => Boolean(n));

              const country = countriesToAdd.map((c) => ({
                value: c.sharedId,
                label: c.title,
                icon: null,
                type: "entity",
              }));

              await connections.insertMany(relationshipsToAdd);

              await entities.updateOne(
                { _id: entity._id },
                {
                  $set: {
                    "metadata.artist_display_name": artist_display_name,
                    "metadata.artist_nationality": artist_nationality,
                    "metadata.country": country,
                  },
                }
              );

              added += 1;
            }
          }
        } catch (err) {
          process.stdout.write("\r\n");
          console.log("Error on:", object["Object ID"]);
          err["Object ID"] = object["Object ID"];
          throw err;
        }
      });

    process.stdout.write("\r\n");
    console.log("Errors", errors);
    console.log("Populated", added, "entities.");
    console.timeEnd("populating");
    await rawClient.close();
    await uwaziClient.close();
  } catch (err) {
    console.timeEnd("populating");
    await rawClient.close();
    await uwaziClient.close();
    throw err;
  }
};

populateImages();
