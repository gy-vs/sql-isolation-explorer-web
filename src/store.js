'use strict';

const fs = require('fs');
const path = require('path');

/**
 * JSON-file persistence. The full engine state is stored after every step,
 * so reloading never re-executes operations. Finished runs are archived into
 * `history` and never modified afterwards.
 */
class Store {
  constructor(file) {
    this.file = file;
    this.data = { schedules: {} };
    try {
      this.data = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      /* first boot */
    }
  }

  save() {
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const tmp = this.file + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(this.data));
    fs.renameSync(tmp, this.file);
  }
}

module.exports = { Store };
