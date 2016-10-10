import fs from 'mz/fs';
import path from 'path';
import rimraf from 'rimraf-promise';
import command from './helpers/cliHelper';
import { size, isNull, flatten } from 'lodash';
import { getConfig } from './helpers/configHelper';
import * as firebase from './helpers/firebaseHelper';
import { parseConfiguration } from './helpers/keboolaHelper';
import {
  CONFIG_FILE,
  DEFAULT_TABLES_OUT_DIR
} from './constants';
import {
  createTmpDirectory,
  transferFilesBetweenDirectories
} from './helpers/fsHelper';
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
    const tmp = await createTmpDirectory();
    const tmpDir = tmp.path;
    const tableOutDir = path.join(command.data, DEFAULT_TABLES_OUT_DIR);
    const firebaseIds = await firebase.fetchData({ apiKey, domain, endpoint, shallow: true });
    const keys = Object.keys(firebaseIds).sort();
    const pageCount = keys.length / pageSize;
    const promises = firebase.generateDataArray({ apiKey, domain, endpoint, pageCount, pageSize, keys });
    // This part creates files in tmp directory
    for (const promise of promises) {
      const events = firebase.groupDataByEventType(flatten(firebase.prepareDataForOutput(await promise)));
      const result = await Promise.all(firebase.generateOutputFiles(tmpDir, events));
    }
    const files = await fs.readdir(tmpDir);
    const filePromises = files.map(file => transferFilesBetweenDirectories(tmpDir, tableOutDir, file));
    // This loop uses streams to transfer the files from tmp directory intp data/out
    for (const promise of filePromises) {
      const result = await promise;
      console.log(result);
    }
    const manifests = await Promise.all(firebase.generateOutputManifests(tableOutDir, bucketName, files));
    console.log('Manifests created');
    const clean = await rimraf(tmpDir);
    process.exit(0);
  } catch(error) {
    console.log(error);
    process.exit(1);
  }
})();
