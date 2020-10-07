import path from "path";
import { CSVLoader } from "../csv/csvInsert.js";

const insertFiles = async () => {
  const csvFile = path.join(path.resolve(path.dirname("")), "/MetObjects.csv");
  const loader = new CSVLoader();
  await loader.insertRowsFrom(csvFile);
};

console.log("About to insert objects...");
insertFiles();
