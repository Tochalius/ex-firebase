import path from 'path';
import command from './helpers/cliHelper';
import { isNull, flatten } from 'lodash';
import { getConfig } from './helpers/configHelper';
import { parseConfiguration } from './helpers/keboolaHelper';
import {
  fetchData,
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
    const tableOutDir = path.join(command.data, DEFAULT_TABLES_OUT_DIR);
    const data = await fetchData({ apiKey, domain, endpoint });
    if (!isNull(data)) {
      const events = groupDataByEventType(flatten(prepareDataForOutput(data)));
      const result = await Promise.all(generateOutputFiles(tableOutDir, events));
      const manifests = await Promise.all(generateOutputManifests(tableOutDir, bucketName, events));
    }
    console.log('Data downloaded!');
    process.exit(0);
  } catch(error) {
    console.log(error);
    process.exit(1);
  }
})();
