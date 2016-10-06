import path from 'path';
import firebase from 'firebase';
import command from './helpers/cliHelper';
import { isNull, flatten } from 'lodash';
import { getConfig } from './helpers/configHelper';
import { parseConfiguration } from './helpers/keboolaHelper';
import {
  authorization,
  dataValidation,
  applyPagination,
  fetchFirebaseIds,
  getFirebasePromise,
  generateOutputFiles,
  prepareDataForOutput,
  groupDataByEventType,
  generateArrayOfPromises,
  generateOutputManifests,
  convertArrayOfObjectsIntoObject
} from './helpers/firebaseHelper';
import { CONFIG_FILE, DEFAULT_TABLES_OUT_DIR, DEFAULT_BATCH } from './constants';

/**
 * This is the main part of the program.
 */
(async() => {
  try {
    // Reading of the input configuration.
    const {
      batch,
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
    // connection with the firebase.
    // const firebase = applyPagination(authorization({ apiKey, authDomain, databaseURL, storageBucket }), pageSize, apiKey);
    const db = authorization({ databaseURL, domain, clientEmail, privateKey }).database();
    const ref = db.ref(`${endpoint}`);
    const tableOutDir = path.join(command.data, DEFAULT_TABLES_OUT_DIR);
    // We should read all available keys from Firebase. This will help us with the pagination.
    const firebaseIds = await fetchFirebaseIds({ apiKey, domain, endpoint });
    const keys = Object.keys(firebaseIds).sort();
    const pageCount = keys.length / pageSize;
    const promises = generateArrayOfPromises(pageCount, pageSize, keys, ref);
    const half = promises.length / 2;
    const slicedPromises = batch === DEFAULT_BATCH ? promises.slice(0, half) : promises.slice(half);
    for (const promise of slicedPromises) {
      const data = await promise;
      const events = groupDataByEventType(flatten(prepareDataForOutput(data.val())));
      const result = await Promise.all(generateOutputFiles(tableOutDir, events));
      const manifests = await Promise.all(generateOutputManifests(tableOutDir, bucketName, events));
    }
    // const data = await Promise.all(promises);
    // for (const firebaseRecord of data) {
    //   const events = groupDataByEventType(flatten(prepareDataForOutput(firebaseRecord.val())));
    //   const result = await Promise.all(generateOutputFiles(tableOutDir, events));
    //   const manifests = await Promise.all(generateOutputManifests(tableOutDir, bucketName, events));
    // }
    console.log('Data downloaded!');
    process.exit(0);
  } catch(error) {
    console.log(error);
    process.exit(1);
  }
})();
