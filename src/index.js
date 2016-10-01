import path from 'path';
import firebase from 'firebase';
import command from './helpers/cliHelper';
import { isNull, flatten, size } from 'lodash';
import { getConfig } from './helpers/configHelper';
import { parseConfiguration } from './helpers/keboolaHelper';
import {
  fetchData,
  authorization,
  dataValidation,
  applyPagination,
  generateOutputFiles,
  prepareDataForOutput,
  groupDataByEventType,
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
      bucketName
    } = await parseConfiguration(getConfig(path.join(command.data, CONFIG_FILE)));
    console.log('Configuration parsed');
    const tableOutDir = path.join(command.data, DEFAULT_TABLES_OUT_DIR);
    const data = await fetchData({ apiKey, domain, endpoint });
    console.log(size(Object.keys(data)));
    // const validKeys = [];
    // const validData = dataValidation(data, ['eventId','eventType','raised','intervall','idKey','idValue','data']);
    console.log('Data fetched');
    if (!isNull(data)) {
      const events = groupDataByEventType(flatten(prepareDataForOutput(data)));
      const result = await Promise.all(generateOutputFiles(tableOutDir, events));
      console.log('Output files created');
      const manifests = await Promise.all(generateOutputManifests(tableOutDir, bucketName, events));
      console.log('Manifests created');
    }
    console.log('Data downloaded!');
    process.exit(0);
  } catch(error) {
    console.log(error);
    process.exit(1);
  }
})();
