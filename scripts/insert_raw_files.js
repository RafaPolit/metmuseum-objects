import path from "path";
import { CSVLoader } from "../csv/csvLoader.js";

const insertFiles = async () => {
  const csvFile = path.join(
    path.resolve(path.dirname("")),
    "/../MetObjects.csv"
  );
  const loader = new CSVLoader();
  await loader.load(csvFile);
};

console.log("About to insert files...");
insertFiles();
