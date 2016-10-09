import fs from 'mz/fs';
import path from 'path';
import command from './helpers/cliHelper';
import { size, isNull, flatten } from 'lodash';
import { getConfig } from './helpers/configHelper';
import { parseConfiguration } from './helpers/keboolaHelper';
import {
  fetchData,
  authorization,
  dataValidation,
  applyPagination,
  fetchFirebaseIds,
  generateDataArray,
  getFirebasePromise,
  generateOutputFiles,
  prepareDataForOutput,
  groupDataByEventType,
  generateArrayOfPromises,
  generateOutputManifests,
  convertArrayOfObjectsIntoObject
} from './helpers/firebaseHelper';
import { CONFIG_FILE, DEFAULT_TABLES_OUT_DIR } from './constants';

/**
 * This is the main part of the program.
 */
(async() => {
  try {
    // Reading of the input configuration.
    const {
      apiKey,
      domain,
      endpoint,
      pageSize,
      privateKey,
      authDomain,
      bucketName,
      clientEmail,
      databaseURL,
      storageBucket
    } = await parseConfiguration(getConfig(path.join(command.data, CONFIG_FILE)));
    const tableOutDir = path.join(command.data, DEFAULT_TABLES_OUT_DIR);
    const firebaseIds = await fetchData({ apiKey, domain, endpoint, shallow: true });
    const keys = Object.keys(firebaseIds).sort();
    console.log('Keys: ', size(keys));
    const pageCount = keys.length / pageSize;
    const promises = generateDataArray({ apiKey, domain, endpoint, pageCount, pageSize, keys });
    console.log('promises: ', size(promises));
    for (const promise of promises) {
      const events = groupDataByEventType(flatten(prepareDataForOutput(await promise)));
      const result = await Promise.all(generateOutputFiles(tableOutDir, events));
    }
    console.log('Data downloaded!');
    const manifests = await Promise.all(generateOutputManifests(tableOutDir, bucketName, await fs.readdir(tableOutDir)));
    console.log('Manifests created');
    process.exit(0);
  } catch(error) {
    console.log(error);
    process.exit(1);
  }
})();
